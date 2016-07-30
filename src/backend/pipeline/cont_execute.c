/*-------------------------------------------------------------------------
 *
 * cont_execute.c
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/cont_execute.c
 *
 */

#include "postgres.h"

#include "access/htup.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_stream_fn.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "pgstat.h"
#include "pipeline/cont_execute.h"
#include "pipeline/cont_scheduler.h"
#include "pipeline/ipc/reader.h"
#include "pipeline/miscutils.h"
#include "pipeline/stream.h"
#include "storage/shm_alloc.h"
#include "tcop/tcopprot.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#define MAX_IN_XACT_TIMEOUT 5000

ContExecutor *
ContExecutorNew(ContQueryStateInit initfn)
{
	ContExecutor *exec;
	MemoryContext cxt;
	MemoryContext old;

	/* Allocate continuous query execution state in a long-lived memory context. */
	cxt = AllocSetContextCreate(TopMemoryContext, "ContExecutor Context",
				ALLOCSET_DEFAULT_MINSIZE,
				ALLOCSET_DEFAULT_INITSIZE,
				ALLOCSET_DEFAULT_MAXSIZE);

	old = MemoryContextSwitchTo(cxt);

	exec = palloc0(sizeof(ContExecutor));
	exec->cxt = cxt;
	exec->tmp_cxt = AllocSetContextCreate(cxt, "ContExecutor Exec Context",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	exec->initfn = initfn;
	exec->ptype = MyContQueryProc->type;
	exec->pname = GetContQueryProcName(MyContQueryProc);

	MemoryContextSwitchTo(old);

	ipc_tuple_reader_init();
	pgstat_report_activity(STATE_RUNNING, exec->pname);

	return exec;
}

void
ContExecutorDestroy(ContExecutor *exec)
{
	ipc_tuple_reader_destroy();
	MemoryContextDelete(exec->cxt);
}

void
ContExecutorStartBatch(ContExecutor *exec, int timeout)
{
	bool success;

	exec->batch = NULL;

	if (IsTransactionState())
		timeout = Min(MAX_IN_XACT_TIMEOUT, timeout);

	/* TODO(usmanm): report activity */
	success = ipc_tuple_reader_poll(timeout);

	if (!IsTransactionState())
		StartTransactionCommand();

	if (success)
	{
		exec->batch = ipc_tuple_reader_pull();

		if (bms_is_empty(exec->all_queries))
		{
			MemoryContext old;

			old = MemoryContextSwitchTo(exec->cxt);

			/* Combiners only need to execute view queries */
			if (exec->ptype == Combiner)
				exec->all_queries = GetContinuousViewIds();
			else
				exec->all_queries = GetContinuousQueryIds();

			MemoryContextSwitchTo(old);
		}

		if (!bms_is_subset(exec->batch->queries, exec->all_queries))
		{
			Bitmapset *queries;
			MemoryContext old;

			old = MemoryContextSwitchTo(exec->cxt);

			queries = exec->all_queries;
			exec->all_queries = bms_union(queries, exec->batch->queries);
			bms_free(queries);

			MemoryContextSwitchTo(old);
		}
	}

	MemoryContextSwitchTo(exec->tmp_cxt);
	ContQueryBatchContext = exec->tmp_cxt;

	if (exec->batch)
		exec->exec_queries = bms_copy(exec->batch->queries);
	else
		exec->exec_queries = NULL;

	pgstat_start_cq_batch();
}

static ContQueryState *
init_query_state(ContExecutor *exec, ContQueryState *state)
{
	MemoryContext state_cxt;
	MemoryContext old_cxt;

	state_cxt = AllocSetContextCreate(exec->cxt, "QueryStateCxt",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	old_cxt = MemoryContextSwitchTo(state_cxt);

	state->query_id = exec->curr_query_id;
	state->state_cxt = state_cxt;
	state->query = GetContQueryForId(exec->curr_query_id);
	state->tmp_cxt = AllocSetContextCreate(state_cxt, "QueryStateTmpCxt",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	if (state->query == NULL)
		return state;

	pgstat_init_cqstat((PgStat_StatCQEntry *) &state->stats, state->query->id, 0);
	state = exec->initfn(exec, state);

	MemoryContextSwitchTo(old_cxt);

	return state;
}

static ContQueryState *
get_query_state(ContExecutor *exec)
{
	ContQueryState *state = exec->states[exec->curr_query_id];
	HeapTuple tup;

	MyStatCQEntry = NULL;

	/* Entry missing? Start a new transaction so we read the latest pipeline_query catalog. */
	if (state == NULL)
	{
		CommitTransactionCommand();
		StartTransactionCommand();
	}

	PushActiveSnapshot(GetTransactionSnapshot());

	tup = SearchSysCache1(PIPELINEQUERYID, Int32GetDatum(exec->curr_query_id));

	/* Was the continuous view removed? */
	if (!HeapTupleIsValid(tup))
	{
		PopActiveSnapshot();
		ContExecutorPurgeQuery(exec);
		return NULL;
	}

	if (state != NULL)
	{
		/* Was the continuous view modified? In our case this means remove the old view and add a new one. */
		if (HeapTupleGetOid(tup) != state->query->oid)
		{
			MemoryContextDelete(state->state_cxt);
			exec->states[exec->curr_query_id] = NULL;
			state = NULL;
		}
		else
		{
			Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tup);
			state->query->active = row->active;
		}
	}

	ReleaseSysCache(tup);

	if (state == NULL)
	{
		MemoryContext old_cxt = MemoryContextSwitchTo(exec->cxt);
		state = palloc0(sizeof(ContQueryState));
		state = init_query_state(exec, state);
		exec->states[exec->curr_query_id] = state;
		MemoryContextSwitchTo(old_cxt);

		if (state->query == NULL)
		{
			PopActiveSnapshot();
			return NULL;
		}
	}

	PopActiveSnapshot();

	return state;
}

Oid
ContExecutorStartNextQuery(ContExecutor *exec, int timeout)
{
	MemoryContextSwitchTo(exec->tmp_cxt);

	for (;;)
	{
		int id = bms_first_member(exec->exec_queries);

		if (id == -1)
		{
			exec->curr_query_id = InvalidOid;
			break;
		}

		exec->curr_query_id = id;

		/*
		 * If we have a timeout, we want to force an execution of this
		 * query regardless of whether or not it actually has data. This
		 * is primarily used for ticking SW queries that are writing to output streams.
		 */
		if (timeout)
			break;

		if (!exec->batch)
		{
			exec->curr_query_id = InvalidOid;
			break;
		}

		if (bms_is_member(exec->curr_query_id, exec->batch->queries))
			break;
	}

	if (exec->curr_query_id == InvalidOid)
		exec->curr_query = NULL;
	else
	{
		exec->curr_query = get_query_state(exec);
		if (exec->curr_query)
		{
			debug_query_string = exec->curr_query->query->name->relname;
			MyStatCQEntry = (PgStat_StatCQEntry *) &exec->curr_query->stats;
			pgstat_start_cq(MyStatCQEntry);
		}
	}

	return exec->curr_query_id;
}

void
ContExecutorPurgeQuery(ContExecutor *exec)
{
	MemoryContext old;
	ContQueryState *state = exec->states[exec->curr_query_id];

	old = MemoryContextSwitchTo(exec->cxt);
	exec->all_queries = bms_del_member(exec->all_queries, exec->curr_query_id);
	MemoryContextSwitchTo(old);

	if (state)
	{
		MemoryContextDelete(state->state_cxt);
		exec->states[exec->curr_query_id] = NULL;
	}

	exec->curr_query = NULL;
}

void
ContExecutorEndQuery(ContExecutor *exec)
{
	if (!exec->batch)
		return;

	pgstat_increment_cq_exec(1);

	exec->curr_query_id = InvalidOid;
	if (exec->curr_query)
	{
		pgstat_end_cq(MyStatCQEntry);
		if (exec->ptype == Worker)
			pgstat_report_cqstat(false);
	}
	else
		pgstat_send_cqpurge(exec->curr_query_id, 0, exec->ptype);

	ipc_tuple_reader_rewind();
	debug_query_string = NULL;
}

void
ContExecutorEndBatch(ContExecutor *exec, bool commit)
{
	Assert(IsTransactionState());

	if (commit)
		CommitTransactionCommand();

	if (exec->batch)
		pgstat_end_cq_batch(exec->batch->ntups, exec->batch->nbytes);

	ipc_tuple_reader_ack();
	ipc_tuple_reader_reset();

	MemoryContextResetAndDeleteChildren(exec->tmp_cxt);
	exec->exec_queries = NULL;
	exec->batch = NULL;
}
