/*-------------------------------------------------------------------------
 *
 * reaper.c
 *
 * Copyright (c) 2017, PipelineDB
 *
 * IDENTIFICATION
 *    src/include/pipeline/reaper.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "postgres.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_query_fn.h"
#include "executor/spi.h"
#include "executor/tstoreReceiver.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "parser/parse_expr.h"
#include "pipeline/analyzer.h"
#include "pipeline/miscutils.h"
#include "pipeline/matrel.h"
#include "pipeline/reaper.h"
#include "pipeline/scheduler.h"
#include "storage/lmgr.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/hsearch.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/snapmgr.h"

#define DEFAULT_SLEEP_S 2 /* Sleep for 2s unless there are CVs with TTLs */

#define DELETE_TEMPLATE "DELETE FROM \"%s\".\"%s\" WHERE \"$pk\" IN (%s);"
#define SELECT_PK_WITH_LIMIT "SELECT \"$pk\" FROM \"%s\".\"%s\" WHERE %s < now() - interval '%d seconds' LIMIT %d FOR UPDATE SKIP LOCKED"
#define SELECT_PK_NO_LIMIT "SELECT \"$pk\" FROM \"%s\".\"%s\" WHERE %s < now() - interval '%d seconds' FOR UPDATE SKIP LOCKED"

int continuous_query_ttl_expiration_batch_size;
int continuous_query_ttl_expiration_threshold;

static char *
get_delete_sql(RangeVar *cvname, RangeVar *matrelname)
{
	StringInfoData delete_sql;
	StringInfoData select_sql;
	char *ttl_col;
	int ttl;

	GetTTLInfo(cvname, &ttl_col, &ttl);

	initStringInfo(&select_sql);

	if (continuous_query_ttl_expiration_batch_size)
		appendStringInfo(&select_sql, SELECT_PK_WITH_LIMIT,
				matrelname->schemaname, matrelname->relname, ttl_col, ttl, continuous_query_ttl_expiration_batch_size);
	else
		appendStringInfo(&select_sql, SELECT_PK_NO_LIMIT, matrelname->schemaname, matrelname->relname, ttl_col, ttl);

	initStringInfo(&delete_sql);
	appendStringInfo(&delete_sql, DELETE_TEMPLATE,
			matrelname->schemaname, matrelname->relname, select_sql.data);

	return delete_sql.data;
}

/*
 * DeleteTTLExpiredRows
 */
int
DeleteTTLExpiredRows(RangeVar *cvname, RangeVar *matrel)
{
	bool save_continuous_query_materialization_table_updatable = continuous_query_materialization_table_updatable;
	char *delete_cmd;
	int num_deleted = 0;

	/* We need to lock the relation to prevent it from being dropped before we run the DELETE */
	Relation rel = heap_openrv_extended(matrel, AccessShareLock, true);

	if (!rel)
		return 0;

	continuous_query_materialization_table_updatable = true;
	Assert(IsTTLContView(cvname));

	/* Now we're certain relid is for a TTL continuous view's matrel */
	delete_cmd = get_delete_sql(cvname, matrel);

	PushActiveSnapshot(GetTransactionSnapshot());

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "could not connect to SPI manager");

	if (SPI_execute(delete_cmd, false, 0) != SPI_OK_DELETE)
		elog(ERROR, "SPI_execute failed: %s", delete_cmd);

	num_deleted = SPI_processed;

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	PopActiveSnapshot();
	continuous_query_materialization_table_updatable = save_continuous_query_materialization_table_updatable;

	heap_close(rel, AccessShareLock);

	return num_deleted;
}

typedef struct ReaperEntry
{
	Oid relid;
	TimestampTz last_expired;
	int last_deleted;
	int ttl;
} ReaperEntry;

static HTAB *last_expired = NULL;

/*
 * should_expire
 */
static bool
should_expire(Oid relid)
{
	ReaperEntry *entry = (ReaperEntry *) hash_search(last_expired, &relid, HASH_FIND, NULL);
	int threshold_ms;

	Assert(entry);

	/*
	 * If there were any deletions on the last run, we want to keep expiring without waiting
	 */
	if (entry->last_deleted)
		return true;

	/* We attempt to expire when a configurable percentage of the overall TTL has elapsed */
	threshold_ms = entry->ttl * (1000 * continuous_query_ttl_expiration_threshold / 100.0);

	if (TimestampDifferenceExceeds(entry->last_expired, GetCurrentTimestamp(), threshold_ms))
		return true;

	return false;
}

/*
 * set_last_expiration
 */
static void
set_last_expiration(Oid relid, int deleted)
{
	ReaperEntry *entry = (ReaperEntry *) hash_search(last_expired, &relid, HASH_FIND, NULL);
	Assert(entry);

	entry->last_expired = GetCurrentTimestamp();
	entry->last_deleted = deleted;
}

/*
 * reset_entries
 */
static void
reset_entries(void)
{
	ReaperEntry *entry;
	HASH_SEQ_STATUS iter;

	hash_seq_init(&iter, last_expired);

	while ((entry = (ReaperEntry *) hash_seq_search(&iter)) != NULL)
		entry->last_deleted = 0;
}

/*
 * get_ttl_rels
 */
static List *
get_ttl_rels(int *min_ttl)
{
	List *result = NIL;
	int id = -1;
	Bitmapset *ids = GetContinuousViewIds();

	Assert(min_ttl);
	Assert(last_expired);

	while ((id = bms_next_member(ids, id)) >= 0)
	{
		ContQuery *cq = GetContQueryForId(id);
		List *tup = NIL;
		ReaperEntry *entry;
		bool found;

		if (!cq)
			continue;
		if (!AttributeNumberIsValid(cq->ttl_attno))
			continue;

		tup = lappend(tup, makeInteger(cq->relid));
		tup = lappend(tup, cq->name);
		tup = lappend(tup, cq->matrel);
		result = lappend(result, tup);

		*min_ttl = Min(*min_ttl, cq->ttl);

		entry = (ReaperEntry *) hash_search(last_expired, &cq->relid, HASH_ENTER, &found);
		if (!found)
		{
			entry->last_expired = 0;
			entry->ttl = cq->ttl;
		}
	}

	return result;
}

void
ContinuousQueryReaperMain(void)
{
	HASHCTL hctl;
	MemoryContext cxt = AllocSetContextCreate(TopMemoryContext, "Reaper Context",
				ALLOCSET_DEFAULT_MINSIZE,
				ALLOCSET_DEFAULT_INITSIZE,
				ALLOCSET_DEFAULT_MAXSIZE);

	MemSet(&hctl, 0, sizeof(hctl));
	hctl.hcxt = CurrentMemoryContext;
	hctl.keysize = sizeof(Oid);
	hctl.entrysize = sizeof(ReaperEntry);
	last_expired = hash_create("ReaperHash", 32, &hctl, HASH_CONTEXT | HASH_ELEM | HASH_BLOBS);

	for (;;)
	{
		List *ttl_rels = NIL;
		ListCell *lc;
		int min_sleep = DEFAULT_SLEEP_S;
		int total_deleted = 0;

		CHECK_FOR_INTERRUPTS();

		if (get_sigterm_flag())
			break;

		/*
		 * Keep trying to expire rows until nothing was actually deleted, then sleep for a reasonable amount of time
		 */
		for (;;)
		{
			int deleted = 0;
			bool error = false;

			PG_TRY();
			{
				MemoryContext old;
				StartTransactionCommand();

				MemoryContextReset(cxt);
				old = MemoryContextSwitchTo(cxt);
				ttl_rels = get_ttl_rels(&min_sleep);
				MemoryContextSwitchTo(old);

				CommitTransactionCommand();

				foreach(lc, ttl_rels)
				{
					List *rels = lfirst(lc);
					RangeVar *cv;
					RangeVar *matrel;
					Oid relid;

					CHECK_FOR_INTERRUPTS();

					if (get_sigterm_flag())
						break;

					Assert(list_length(rels) == 3);

					relid = intVal(linitial(rels));
					cv = lsecond(rels);
					matrel = lthird(rels);

					/*
					 * Skip any relations that aren't likely to have newly expired rows
					 */
					if (should_expire(relid))
					{
						TimestampTz start = GetCurrentTimestamp();
						StartTransactionCommand();
						SetCurrentStatementStartTimestamp();

						deleted = DeleteTTLExpiredRows(cv, matrel);
						total_deleted += deleted;
						set_last_expiration(relid, deleted);

						elog(LOG, "deleted %d rows from %s in %ld ms", deleted, matrel->relname, ((GetCurrentTimestamp() - start) / 1000));

						CommitTransactionCommand();
					}
				}
			}
			PG_CATCH();
			{
				EmitErrorReport();
				FlushErrorState();

				error = true;

				if (ActiveSnapshotSet())
					PopActiveSnapshot();

				AbortCurrentTransaction();
				StartTransactionCommand();
			}
			PG_END_TRY();

			/*
			 * If nothing was deleted on this run, we're done for now
			 */
			if (error || !deleted)
				break;
		}

		reset_entries();
		total_deleted = 0;
		elog(LOG, "sleeping for %d s", min_sleep);
		pg_usleep(min_sleep * 1000 * 1000);
	}
}
