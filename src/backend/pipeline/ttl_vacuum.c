/*-------------------------------------------------------------------------
 *
 * ttl_vacuum.c
 *
 *   Support for vacuuming discarded tuples for continuous views with TTLs.
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 * src/backend/pipeline/ttl_vacuum.c
 *
 *-------------------------------------------------------------------------
 */
#include "pipeline/ttl_vacuum.h"

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
#include "pipeline/cont_analyze.h"
#include "pipeline/cqmatrel.h"
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

#define DELETE_TEMPLATE "DELETE FROM %s.%s WHERE \"$pk\" IN " \
                           "(SELECT \"$pk\" FROM %s.%s WHERE %s < now() - interval '%d seconds' FOR UPDATE SKIP LOCKED);"

static char *
get_delete_sql(RangeVar *cvname, RangeVar *matrelname)
{
	StringInfoData buf;
	char *ttl_col;
	int ttl;

	GetTTLInfo(cvname, &ttl_col, &ttl);

	initStringInfo(&buf);
	appendStringInfo(&buf, DELETE_TEMPLATE,
			matrelname->schemaname, matrelname->relname, matrelname->schemaname, matrelname->relname, ttl_col, ttl);

	return buf.data;
}

/*
 * DeleteTTLExpiredTuples
 */
void
DeleteTTLExpiredTuples(Oid relid)
{
	char *relname;
	char *namespace;
	RangeVar *matrel;
	RangeVar *cvname;
	MemoryContext oldcxt;
	MemoryContext runctx;
	bool save_continuous_query_materialization_table_updatable = continuous_query_materialization_table_updatable;
	char *delete_cmd;

	continuous_query_materialization_table_updatable = true;

	StartTransactionCommand();

	runctx = AllocSetContextCreate(CurrentMemoryContext,
			"DeleteSWExpiredTuplesContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	oldcxt = MemoryContextSwitchTo(runctx);

	relname = get_rel_name(relid);

	if (!relname)
		goto end;

	namespace = get_namespace_name(get_rel_namespace(relid));
	matrel = makeRangeVar(namespace, relname, -1);

	cvname = GetCVNameFromMatRelName(matrel);

	if (!cvname)
		goto end;

	if (!IsTTLContView(cvname))
		goto end;

	/* Now we're certain relid is for a TTL continuous view's matrel */
	delete_cmd = get_delete_sql(cvname, matrel);

	PushActiveSnapshot(GetTransactionSnapshot());

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "could not connect to SPI manager");

	if (SPI_execute(delete_cmd, false, 0) != SPI_OK_DELETE)
		elog(ERROR, "SPI_execute failed: %s", delete_cmd);

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	PopActiveSnapshot();

end:
	continuous_query_materialization_table_updatable = save_continuous_query_materialization_table_updatable;

	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(runctx);

	CommitTransactionCommand();
}

/*
 * NumTTLExpiredTuples
 */
uint64_t
NumTTLExpiredTuples(Oid relid)
{
	uint64_t count = 0;
	char *relname = get_rel_name(relid);
	char *namespace = get_namespace_name(get_rel_namespace(relid));
	RangeVar *matrel = makeRangeVar(namespace, relname, -1);
	RangeVar *cvname;
	StringInfoData sql;
	MemoryContext oldcontext;
	MemoryContext runctx;
	bool locked;

	if (!relname)
		return 0;

	locked = ConditionalLockRelationOid(PipelineQueryRelationId, AccessShareLock);
	if (!locked)
		return 0;

	cvname = GetCVNameFromMatRelName(matrel);
	UnlockRelationOid(PipelineQueryRelationId, AccessShareLock);

	if (!cvname)
		return 0;

	if (!IsTTLContView(cvname))
		return 0;

	runctx = AllocSetContextCreate(CurrentMemoryContext,
			"NumSWExpiredTuplesContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	oldcontext = MemoryContextSwitchTo(runctx);

	initStringInfo(&sql);
	appendStringInfo(&sql, "SELECT COUNT(*) FROM %s.%s", namespace, relname);

	PushActiveSnapshot(GetTransactionSnapshot());

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "could not connect to SPI manager");

	if (SPI_execute(sql.data, false, 0) != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute failed: %s", sql.data);

	if (SPI_processed > 0)
	{
		char *v = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
		count = DatumGetInt64(DirectFunctionCall1(int8in, (Datum) v));
	}

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	PopActiveSnapshot();

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(runctx);

	return count;
}
