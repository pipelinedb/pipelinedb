/*-------------------------------------------------------------------------
 *
 * trigger.c
 *	  Functionality for continuous triggers
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/xact.h"
#include "pipeline/trigger/alert_server.h"
#include "pipeline/trigger/batching.h"
#include "catalog/pipeline_query.h"
#include "commands/trigger.h"
#include "executor/tstoreReceiver.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "parser/parse_oper.h"
#include "pipeline/cont_analyze.h"
#include "pipeline/cont_plan.h"
#include "rewrite/rewriteManip.h"
#include "pipeline/trigger/sliding_window.h"
#include "pipeline/trigger/trigger.h"
#include "pipeline/trigger/tuple_formatter.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "pipeline/trigger/wal.h"
#include "pipeline/trigger/util.h"
#include "storage/ipc.h"
#include "executor/executor.h"
#include "catalog/pg_database.h"
#include "catalog/pipeline_database.h"
#include "pipeline/cont_scheduler.h"
#include "pipeline/trigger/triggerfuncs.h"
#include "catalog/pg_trigger.h"

/* guc */
int alert_socket_mem;
int alert_server_port;
bool continuous_triggers_enabled;
char *alert_server_address;

#define TRIGGER_CACHE_CLEANUP_INTERVAL 1 * 1000 /* 10s */

AlertServer *MyAlertServer = NULL;

volatile sig_atomic_t got_SIGHUP = false;
volatile sig_atomic_t got_SIGTERM = false;

static void
sigterm_handle(int action)
{
	got_SIGTERM = true;
}

static void
ResetTriggerCacheEntry(TriggerProcessState *state, TriggerCacheEntry *entry);

static void
trigger_cache_cleanup(TriggerProcessState *state)
{
	ListCell *lc;
	TriggerCacheEntry *entry;
	List *to_del = NIL;
	TimestampTz cur_time = GetCurrentTimestamp();
	HASH_SEQ_STATUS seq;

	if (!TimestampDifferenceExceeds(state->last_trigger_cache_cleanup,
			cur_time, TRIGGER_CACHE_CLEANUP_INTERVAL))
		return;

	state->dirty_syscache = true;
	StartTransactionCommand();

	entry = NULL;
	hash_seq_init(&seq, state->trigger_cache);

	while ((entry = (TriggerCacheEntry*) hash_seq_search(&seq)) != NULL)
	{
		Relation rel = try_relation_open(entry->matrelid, AccessShareLock);

		if (!rel)
		{
			MemoryContext old = MemoryContextSwitchTo(state->cache_cxt);
			to_del = lappend(to_del, entry);
			MemoryContextSwitchTo(old);
		}
		else
			relation_close(rel, AccessShareLock);
	}

	CommitTransactionCommand();

	foreach(lc, to_del)
	{
		Oid relid;
		entry = lfirst(lc);
		ResetTriggerCacheEntry(state, entry);

		relid = entry->matrelid;
		hash_search(state->trigger_cache, &relid, HASH_REMOVE, NULL);
	}

	list_free(to_del);

	state->last_trigger_cache_cleanup = cur_time;
}

static void
create_trigger_cache(TriggerProcessState *state)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(HASHCTL));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(TriggerCacheEntry);
	ctl.hash = oid_hash;
	ctl.hcxt = state->cache_cxt;

	state->trigger_cache = hash_create("RelCache", 32, &ctl,
		HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
}

/*
 * create_trigger_process_state
 */
static TriggerProcessState *
create_trigger_process_state()
{
	TriggerProcessState *state = palloc0(sizeof(TriggerProcessState));

	state->cache_cxt = CurrentMemoryContext;
	state->check_buf = makeStringInfo();
	state->server = create_alert_server(state);
	state->last_trigger_cache_cleanup = 0;

	create_trigger_cache(state);

	return state;
}

/*
 * destroy_trigger_process_state
 */
static void
destroy_trigger_process_state(TriggerProcessState *state)
{
	/* TODO - clean up other parts */
	destroy_alert_server(state->server);
}

static void
synchronize(TriggerProcessState *state);

static void
trigger_do_periodic(TriggerProcessState *state);

/*
 * check_syscache_dirty
 */
static void
check_syscache_dirty(TriggerProcessState *state)
{
	if (state->dirty_syscache)
	{
		InvalidateSystemCaches();
		state->dirty_syscache = false;
	}
}

/*
 * trigger_main
 */
void
trigger_main()
{
	TriggerProcessState *state;
	WalStream *ws;
	bool saw_catalog_changes = false;

	CHECK_FOR_INTERRUPTS();

	pqsignal(SIGHUP, sighup_handle);
	pqsignal(SIGTERM, sigterm_handle);
	wal_init();

	state = create_trigger_process_state();
	Assert(MyAlertServer == NULL);

	MyAlertServer = state->server;
	ws = create_wal_stream(state);

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		if (got_SIGTERM)
			break;

		check_syscache_dirty(state);
		wal_stream_read(ws, &saw_catalog_changes);

		check_syscache_dirty(state);

		if (saw_catalog_changes)
		{
			InvalidateSystemCaches();
			synchronize(state);
			state->dirty_syscache = true;
			got_SIGHUP = false;
		}

		trigger_do_periodic(state);
		process_batches(state);
		alert_server_handle(state->server);
	}

	destroy_trigger_process_state(state);
	destroy_wal_stream(ws);
	proc_exit(0);
}

static inline List *
make_sentinel()
{
	return (List *) UINTPTR_MAX;
}

static inline bool
is_sentinel(List *list)
{
	return list == (List *) UINTPTR_MAX;
}

/*
 * should_fire_trigger
 *
 * Determine if the given trigger should be fired for the given event.
 * This is a slightly modified version of trigger.c:TriggerEnabled.
 */
static bool
should_fire_trigger(TriggerCacheEntry *entry, TrigInfo *info,
		TriggerEvent event, HeapTuple oldtup, HeapTuple newtup)
{
	Trigger *trigger = info->trigger;
	EState *estate = entry->estate;
	TupleDesc tupdesc = entry->output_desc;
	ExprContext *econtext;
	TupleTableSlot *oldslot = NULL;
	TupleTableSlot *newslot = NULL;
	MemoryContext oldContext;

	if (!TRIGGER_FOR_INSERT(info->trigger->tgtype) &&
			event == TRIGGER_EVENT_INSERT)
		return false;

	if (!TRIGGER_FOR_UPDATE(info->trigger->tgtype) &&
			event == TRIGGER_EVENT_UPDATE)
		return false;

	/* Check for WHEN clause */
	if (!trigger->tgqual)
		return true;

	if (is_sentinel(info->when))
		return true;

	Assert(estate != NULL);

	/*
	 * If first time through for this WHEN expression, build expression
	 * nodetrees for it.  Keep them in the per-query memory context so
	 * they'll survive throughout the query.
	 */
	if (info->when == NIL)
	{
		Node *tgqual;

		oldContext = MemoryContextSwitchTo(estate->es_query_cxt);
		tgqual = stringToNode(trigger->tgqual);

		/* Change references to OLD and NEW to INNER_VAR and OUTER_VAR */
		ChangeVarNodes(tgqual, PRS2_OLD_VARNO, INNER_VAR, 0);
		ChangeVarNodes(tgqual, PRS2_NEW_VARNO, OUTER_VAR, 0);
		/* ExecQual wants implicit-AND form */
		tgqual = (Node *) make_ands_implicit((Expr *) tgqual);
		info->when = (List *) ExecPrepareExpr((Expr *) tgqual, estate);

		/*
		 * This happens when the expression is when(true)
		 * set when to a sentinel so we don't keep trying
		 * to make an expression
		 */

		if (!info->when)
			info->when = make_sentinel();

		MemoryContextSwitchTo(oldContext);
	}

	if (is_sentinel(info->when))
		return true;

	/*
	 * We will use the EState's per-tuple context for evaluating WHEN
	 * expressions (creating it if it's not already there).
	 */
	econtext = GetPerTupleExprContext(estate);

	/*
	 * Put OLD and NEW tuples into tupleslots for expression evaluation.
	 * These slots can be shared across the whole estate, but be careful
	 * that they have the current resultrel's tupdesc.
	 */
	if (HeapTupleIsValid(oldtup))
	{
		if (estate->es_trig_oldtup_slot == NULL)
		{
			oldContext = MemoryContextSwitchTo(estate->es_query_cxt);
			estate->es_trig_oldtup_slot = ExecInitExtraTupleSlot(estate);
			MemoryContextSwitchTo(oldContext);
		}
		oldslot = estate->es_trig_oldtup_slot;
		if (oldslot->tts_tupleDescriptor != tupdesc)
			ExecSetSlotDescriptor(oldslot, tupdesc);

		ExecStoreTuple(oldtup, oldslot, InvalidBuffer, false);
		econtext->ecxt_innertuple = oldslot;
	}

	if (HeapTupleIsValid(newtup))
	{
		if (estate->es_trig_newtup_slot == NULL)
		{
			oldContext = MemoryContextSwitchTo(estate->es_query_cxt);
			estate->es_trig_newtup_slot = ExecInitExtraTupleSlot(estate);
			MemoryContextSwitchTo(oldContext);
		}
		newslot = estate->es_trig_newtup_slot;
		if (newslot->tts_tupleDescriptor != tupdesc)
			ExecSetSlotDescriptor(newslot, tupdesc);

		ExecStoreTuple(newtup, newslot, InvalidBuffer, false);
		econtext->ecxt_outertuple = newslot;
	}

	/*
	 * Finally evaluate the expression, making the old and/or new tuples
	 * available as INNER_VAR/OUTER_VAR respectively.
	 */

	if (!ExecQual(info->when, econtext, false))
			return false;

	return true;
}

/*
 * exec_trigger_proc
 */
static void
exec_trigger_proc(TriggerData *tcontext, FmgrInfo *finfo,
		MemoryContext context)
{
	FunctionCallInfoData fcinfo;
	PgStat_FunctionCallUsage fcusage;
	MemoryContext oldContext;
	Trigger *trigger = tcontext->tg_trigger;

	/*
	 * We cache fmgr lookup info, to avoid making the lookup again on each
	 * call.
	 */
	if (finfo->fn_oid == InvalidOid)
		fmgr_info(trigger->tgfoid, finfo);

	Assert(finfo->fn_oid == trigger->tgfoid);

	/*
	 * Do the function evaluation in the per-tuple memory context, so that
	 * leaked memory will be reclaimed once per tuple. Note in particular that
	 * any new tuple created by the trigger function will live till the end of
	 * the tuple cycle.
	 */
	oldContext = MemoryContextSwitchTo(context);

	/*
	 * Call the function, passing no arguments but setting a context.
	 */
	InitFunctionCallInfoData(fcinfo, finfo, 0,
							 InvalidOid, (Node *) tcontext, NULL);

	pgstat_init_function_usage(&fcinfo, &fcusage);

	PG_TRY();
	{
		/* Consume return value */
		FunctionCallInvoke(&fcinfo);
	}
	PG_CATCH();
	{
		EmitErrorReport();
		FlushErrorState();
	}
	PG_END_TRY();

	pgstat_end_function_usage(&fcusage, true);

	MemoryContextSwitchTo(oldContext);
}

/*
 * fire_triggers
 *
 * Fire all qualifying triggers for the given tuple
 */
void
fire_triggers(TriggerCacheEntry *entry, Relation rel,
		Relation cvrel,
		TriggerProcessChangeType action,
		HeapTuple old_tup, HeapTuple new_tup)
{
	dlist_iter iter;
	TriggerData context;

	bool free_old_tup = false;
	bool free_new_tup = false;

	int trig_action = map_trig_change_to_event(action);

	if (trig_action == TRIGGER_EVENT_DELETE)
		return;

	/* Project tuple using view plan projection if neccessary */

	if (entry->proj_inner && old_tup)
	{
		TupleTableSlot *slot;
		ExecStoreTuple(old_tup, entry->econtext->ecxt_scantuple,
					   InvalidBuffer, false);

		slot = ExecProject(entry->proj_inner, NULL);
		old_tup = ExecCopySlotTuple(slot);
		free_old_tup = true;
	}

	if (entry->proj_outer && new_tup)
	{
		TupleTableSlot *slot;
		ExecStoreTuple(new_tup, entry->econtext->ecxt_scantuple,
					   InvalidBuffer, false);
		slot = ExecProject(entry->proj_outer, NULL);
		new_tup = ExecCopySlotTuple(slot);
		free_new_tup = true;
	}

	context.type = T_TriggerData;
	context.tg_event = TRIGGER_EVENT_ROW | trig_action;
	context.tg_relation = rel;
	context.tg_newtuplebuf = InvalidBuffer;
	context.tg_trigtuplebuf = InvalidBuffer;

	if (TRIGGER_FIRED_BY_INSERT(context.tg_event))
	{
		context.tg_trigtuple = new_tup;
		context.tg_newtuple = old_tup;
	}
	else if (TRIGGER_FIRED_BY_UPDATE(context.tg_event))
	{
		context.tg_newtuple = new_tup;
		context.tg_trigtuple = old_tup;
	}

	dlist_foreach(iter, &entry->triggers)
	{
		TrigInfo *info =
			dlist_container(TrigInfo, list_node, iter.cur);

		if (!should_fire_trigger(entry, info, trig_action, old_tup, new_tup))
			continue;

		context.tg_trigger = info->trigger;

		exec_trigger_proc(&context, &info->fmgr_info,
				GetPerTupleMemoryContext(entry->estate));
	}

	if (free_new_tup)
		heap_freetuple(new_tup);

	if (free_old_tup)
		heap_freetuple(old_tup);

	ReScanExprContext(entry->econtext);
	ResetPerTupleExprContext(entry->estate);
}

/*
 * make_full_name
 *
 * Build a name with format view_name.trig_name for the trigger server
 */
static StringInfo
make_full_name(RangeVar *rv, const char *tgname)
{
	StringInfo info = makeStringInfo();
	appendStringInfo(info, "%s.%s", rv->relname, tgname);
	return info;
}

/*
 * GetMatrelOidForView
 *
 * Lookup a cv in the system catalog, and return its matrel oid if it is valid
 */
static Oid
GetMatrelOidForView(const char *name)
{
	RangeVar *rv = makeRangeVar(0, (char*) name, -1);
	Oid matrel_oid = InvalidOid;

	HeapTuple tuple = GetPipelineQueryTuple(rv);
	Form_pipeline_query row;

	if (HeapTupleIsValid(tuple))
	{
		row = (Form_pipeline_query) GETSTRUCT(tuple);
		matrel_oid = row->matrel;
		ReleaseSysCache(tuple);
	}

	return matrel_oid;
}

static Oid
get_trig_oid(TriggerProcessState *state, const char *vname, const char *tname);

/*
 * trigger_check_catalog
 *
 * To be called when the alert server can't find a matching subscription name.
 *
 * This will happen if a trigger has been created, but no data has been
 * inserted into the matrel.
 */
void
trigger_check_catalog(void *data, const char *name)
{
	Oid tgoid;
	char *last;
	char *first;
	char *saveptr;

	MemoryContext old;
	TriggerProcessState *state = (TriggerProcessState *) (data);

	old = MemoryContextSwitchTo(state->cache_cxt);

	resetStringInfo(state->check_buf);
	appendStringInfoString(state->check_buf, name);
	first = strtok_r(state->check_buf->data, ".", &saveptr);

	if (!first)
		return;

	last = strtok_r(NULL, ".", &saveptr);

	if (!last)
		return;

	StartTransactionCommand();
	tgoid = get_trig_oid(state, first, last);
	CommitTransactionCommand();

	if (tgoid != InvalidOid)
		alert_server_add(state->server, tgoid, pstrdup(name));

	MemoryContextSwitchTo(old);
}

/*
 * init_cache_entry
 */
static void
init_cache_entry(TriggerCacheEntry *entry, Relation rel)
{
	entry->cv = GetContQueryForViewId(entry->cvid);

	PushActiveSnapshot(GetTransactionSnapshot());
	entry->pstmt = GetContinuousViewOverlayPlan(entry->cv);
	PopActiveSnapshot();

	entry->estate = CreateExecutorState();
	entry->econtext = CreateStandaloneExprContext();

	entry->econtext->ecxt_scantuple =
		MakeSingleTupleTableSlot(
				CreateTupleDescCopyConstr(RelationGetDescr(rel)));

	if (!entry->is_sw)
	{
		entry->proj_inner = build_projection(
				entry->pstmt->planTree->targetlist,
				entry->estate, entry->econtext,
				RelationGetDescr(rel));

		entry->proj_outer = build_projection(
				entry->pstmt->planTree->targetlist,
				entry->estate,
				entry->econtext,
				RelationGetDescr(rel));

		entry->output_desc =
			ExecTypeFromTL(entry->pstmt->planTree->targetlist, false);

		entry->trig_func = fire_triggers;
		register_formatter(RelationGetRelid(rel), entry->output_desc);
	}
	else
	{
		init_sw(entry, rel);
	}
}

/*
 * is_trigger_in_desc
 *
 * Scan a Trig desc for a given tgoid
 */
static bool
is_trigger_in_desc(TriggerDesc *desc, Oid oid)
{
	int num = desc ? desc->numtriggers : 0;
	int i = 0;

	for (i = 0; i < num; ++i)
	{
		if (desc->triggers[i].tgoid == oid)
			return true;
	}

	return false;
}

/*
 * remove_trigger
 *
 * Remove TrigInfo from cache entry and free it, notify the alert server
 */
static void
remove_trigger(TriggerProcessState *state, TriggerCacheEntry *entry,
		TrigInfo *info)
{
	alert_server_remove(state->server, info->tgoid);

	dlist_delete(&info->list_node);
	pfree(info);
}

/*
 * add_trigger
 *
 * Add a new trigger to cache entry, notify alert server
 */
static void
add_trigger(TriggerProcessState *state, TriggerCacheEntry *entry,
		Trigger *trigger)
{
	StringInfo info;
	MemoryContext old = MemoryContextSwitchTo(state->cache_cxt);

	TrigInfo *trig_info = palloc0(sizeof(TrigInfo));

	trig_info->tgoid = trigger->tgoid;
	trig_info->trigger = CopyTrigger(trigger);

	dlist_push_tail(&entry->triggers, &trig_info->list_node);

	info = make_full_name(entry->cv_name, trigger->tgname);
	alert_server_add(state->server, trigger->tgoid, info->data);

	MemoryContextSwitchTo(old);
}

static void
do_decode_change(TriggerProcessState *state,
				 XactBatch *xact_batch,
				 Relation rel,
				 TriggerProcessChangeType action,
				 HeapTuple old_tup,
				 HeapTuple new_tup);

static TrigInfo*
find_trigger_info(TriggerCacheEntry *entry, Oid tgoid);

/*
 * diff_triggers
 *
 * Compare two trigger lists, and generate a set of add/remove trigger calls.
 */
void
diff_triggers(TriggerProcessState *state, TriggerCacheEntry *entry,
		Relation rel,
		TriggerDesc *newdesc)
{
	dlist_mutable_iter iter;
	int num_old = entry->numtriggers;
	int num_new = newdesc ? newdesc->numtriggers : 0;
	int i = 0;

	/* check removed */
	dlist_foreach_modify(iter, &entry->triggers)
	{
		TrigInfo *info =
			dlist_container(TrigInfo, list_node, iter.cur);

		if (!is_trigger_in_desc(newdesc, info->tgoid))
			remove_trigger(state, entry, info);
	}

	/* check added */
	for (i = 0; i < num_new; ++i)
	{
		Trigger *trigger = newdesc->triggers + i;

		TrigInfo *info = find_trigger_info(entry, trigger->tgoid);

		if (!info)
			add_trigger(state, entry, trigger);
	}

	/* just started */
	if (num_old == 0 && num_new != 0)
	{
		MemoryContext old;
		entry->xmin = GetTopTransactionId();
		old = MemoryContextSwitchTo(state->cache_cxt);
		init_cache_entry(entry, rel);
		MemoryContextSwitchTo(old);
	}

	/* just stopped */
	if (num_new == 0 && num_old != 0)
		ResetTriggerCacheEntry(state, entry);

	entry->numtriggers = num_new;
}

/*
 * trigger_plugin_decode_begin_txn
 */
void
trigger_plugin_decode_begin_txn(LogicalDecodingContext *ctx,
			  		  ReorderBufferTXN *txn)
{
	TriggerProcessState *state = (TriggerProcessState *)
		(ctx->output_plugin_private);

	if (!state)
		return;

	Assert(state->cur_wal_batch == NULL);
	state->cur_wal_batch =
		start_new_batch(state, "wal", txn->xid, txn->commit_time);
}

/*
 * trigger_plugin_decode_commit_txn
 */
void
trigger_plugin_decode_commit_txn(LogicalDecodingContext *ctx,
					   ReorderBufferTXN *txn,
					   XLogRecPtr commit_lsn)
{
	TriggerProcessState *state = (TriggerProcessState *)
		(ctx->output_plugin_private);

	if (!state)
		return;

	if (MyAlertServer)
		alert_server_flush(MyAlertServer);

	LogicalConfirmReceivedLocation(ctx->reader->EndRecPtr);

	Assert(state->cur_wal_batch);

	state->last_xid = state->cur_wal_batch->xmin;
	state->last_commit_time = state->cur_wal_batch->commit_time;

	state->cur_wal_batch->finished = true;
	state->cur_wal_batch = NULL;
}

/*
 * ResetTriggerCacheEntry
 */
static void
ResetTriggerCacheEntry(TriggerProcessState *state, TriggerCacheEntry *entry)
{
	/* XXX - this currently leaks */

	TriggerCacheEntry tmp;
	dlist_iter iter;

	dlist_foreach(iter, &entry->triggers)
	{
		TrigInfo *info =
			dlist_container(TrigInfo, list_node, iter.cur);

		alert_server_remove(state->server, info->tgoid);
	}

	memcpy(&tmp, entry, sizeof(TriggerCacheEntry));
	memset(entry, 0, sizeof(TriggerCacheEntry));

	entry->matrelid  =  tmp.matrelid;
	entry->cvrelid   =  tmp.cvrelid;
	entry->cvid      =  tmp.cvid;
	entry->cv_name   =  tmp.cv_name;
	entry->is_sw     =  tmp.is_sw;
	entry->is_adhoc  =  tmp.is_adhoc;
}

/*
 * GetCVInfo
 *
 * Get all relevant info about the CV from the system catalog
 */
static void
get_cv_Info(Relation matrel, TriggerCacheEntry *entry, MemoryContext cache_cxt)
{
	Relation rel;
	MemoryContext old;
	HeapTuple tup;
	Oid namespace = RelationGetNamespace(matrel);
	Form_pipeline_query row;
	RangeVar *cv;

	Assert(OidIsValid(namespace));

	tup = SearchSysCache2(PIPELINEQUERYNAMESPACEMATREL,
			ObjectIdGetDatum(namespace),
			RelationGetRelid(matrel));

	if (!HeapTupleIsValid(tup))
		return;

	row = (Form_pipeline_query) GETSTRUCT(tup);

	cv = makeRangeVar(get_namespace_name(namespace),
			pstrdup(NameStr(row->name)), -1);

	rel = heap_openrv(cv, AccessShareLock);
	entry->cvrelid = RelationGetRelid(rel);
	entry->cvid = row->id;

	old = MemoryContextSwitchTo(cache_cxt);

	entry->cv_name = makeRangeVar(get_namespace_name(namespace),
			pstrdup(NameStr(row->name)), -1);

	MemoryContextSwitchTo(old);

	entry->is_sw = row->gc;
	entry->is_adhoc = row->adhoc;

	heap_close(rel, AccessShareLock);
	ReleaseSysCache(tup);
}

/*
 * synchronize
 *
 * Create a sync batch, and scan the pipeline_query system catalog for CQs.
 * Call do_decode_change for each one.
 *
 * This will have the effect of adding a bunch of empty changelists for each rel
 * to a batch.
 *
 * process_batches will subsequently pick up this batch and
 * run diff_triggers on it etc
 */
static void
do_synchronize(TriggerProcessState *state)
{
	HeapTuple tup;
	XactBatch *batch = start_new_batch(state, "sync", GetTopTransactionId(),
			GetCurrentTimestamp());

	RangeVar *rv = makeRangeVar(NULL, "pipeline_query", 0);
	Relation prel = heap_openrv(rv, AccessShareLock);

	HeapScanDesc scan = heap_beginscan(prel, GetTransactionSnapshot(), 0, NULL);

	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tup);
		Oid relid = row->matrel;

		Relation rel = heap_open(relid, AccessShareLock);

		do_decode_change(state, batch, rel, TRIGGER_PROCESS_CHANGE_NOOP,
				NULL, NULL);

		heap_close(rel, AccessShareLock);
	}

	heap_endscan(scan);
	heap_close(prel, NoLock);

	batch->finished = true;
}

/*
 * synchronize
 *
 * Create a sync batch inside a transaction.
 * (This is done in response to a HUP).
 *
 * The main use of this is to ensure internal state is synchronized before
 * any inserts or updates are seen.
 */
static void
synchronize(TriggerProcessState *state)
{
	StartTransactionCommand();
	do_synchronize(state);
	CommitTransactionCommand();
}

/*
 * do_decode_change
 *
 * Add a change (action, old_tup, new_tup) to a relation to a batch.
 *
 * Some basic book keeping of TriggerCacheEntry is performed here,
 * and also some simple filtering.
 *
 * We ignore changes to non CV matrels, and drop any changes that occur before
 * the batch processor has noticed triggers on a CV. entry->numtriggers is
 * used to track that state.
 */
static void
do_decode_change(TriggerProcessState *state,
				 XactBatch *xact_batch,
				 Relation rel,
				 TriggerProcessChangeType action,
				 HeapTuple old_tup,
				 HeapTuple new_tup)
{
	ChangeList *cl;
	TriggerCacheEntry *entry;
	Oid	relid;
	bool found;

	relid = RelationGetRelid(rel);

	entry = hash_search(state->trigger_cache, &relid,
				HASH_ENTER, &found);

	if (!found)
	{
		memset(entry, 0, sizeof(TriggerCacheEntry));
		entry->matrelid = relid;
		get_cv_Info(rel, entry, state->cache_cxt);
	}

	if (entry->cvrelid == InvalidOid || entry->is_adhoc)
		return;

	/* This next line is important, because we *do* want to pass empty
	 * changelists to the batch processor so that it can notice new triggers
	 * etc */

	cl = get_changelist(state, xact_batch, relid);

	if (action == TRIGGER_PROCESS_CHANGE_DELETE ||
	 	action == TRIGGER_PROCESS_CHANGE_NOOP)
		return;

	if (!entry->numtriggers)
		return;

	add_change(state, cl, action, old_tup, new_tup);
}

/*
 * trigger_plugin_decode_change
 *
 * Translate a WAL change into our internal format and call
 * do_decode_change
 */
void
trigger_plugin_decode_change(LogicalDecodingContext *ctx,
				   ReorderBufferTXN *txn, Relation rel,
				   ReorderBufferChange *change)
{
	HeapTuple old_tup;
	HeapTuple new_tup;

	TriggerProcessState *state = (TriggerProcessState *)
		(ctx->output_plugin_private);

	if (!state)
		return;

	old_tup = change->data.tp.oldtuple ?
		&change->data.tp.oldtuple->tuple : NULL;

	new_tup = change->data.tp.newtuple ?
		&change->data.tp.newtuple->tuple : NULL;

	do_decode_change(state, state->cur_wal_batch, rel,
					 map_rb_change_to_trig(change->action),
					 old_tup, new_tup);
}

/*
 * trigger_do_periodic
 */
static void
trigger_do_periodic(TriggerProcessState *state)
{
	trigger_cache_cleanup(state);
	sw_vacuum(state);
}

/*
 * The remaining functions are in support of trigger_check_catalog, which is
 * needed by the alert server in the edge case of trying to
 * subscribe to a trigger that exists, but where no WAL changes have been
 * seen yet.
 */

/*
 * find_trigger_info
 *
 * Find TrigInfo in cache entry with matching tgoid
 */
static TrigInfo*
find_trigger_info(TriggerCacheEntry *entry, Oid tgoid)
{
	dlist_iter iter;

	dlist_foreach(iter, &entry->triggers)
	{
		TrigInfo *info =
			dlist_container(TrigInfo, list_node, iter.cur);

		if (info->tgoid == tgoid)
			return info;
	}

	return NULL;
}

/*
 * get_trig_oid_from_cvrel
 *
 * Find oid of Trigger on a CV with a given name
 */
static Oid
get_trig_oid_from_cvrel(Relation cvrel, const char *tname)
{
	TriggerDesc *desc = cvrel->trigdesc;
	int i = 0;

	for (i = 0; i < desc->numtriggers; ++i)
	{
		Trigger *trigger = &desc->triggers[i];

		if (strcmp(trigger->tgname, tname) == 0)
			return trigger->tgoid;
	}

	return InvalidOid;
}

/*
 * get_trig_oid_from_matrel
 *
 * Given an open matrel, determine if it is a CV, and find the oid
 * of a trigger (on the cv rel) with the given name.
 */
static Oid
get_trig_oid_from_matrel(Relation matrel, const char *tname)
{
	Relation rel;
	HeapTuple tup;
	Oid namespace = RelationGetNamespace(matrel);
	Form_pipeline_query row;
	RangeVar *cv;
	Oid tgoid = InvalidOid;

	Assert(OidIsValid(namespace));

	tup = SearchSysCache2(PIPELINEQUERYNAMESPACEMATREL,
			ObjectIdGetDatum(namespace),
			RelationGetRelid(matrel));

	if (!HeapTupleIsValid(tup))
		return InvalidOid;

	row = (Form_pipeline_query) GETSTRUCT(tup);
	cv = makeRangeVar(get_namespace_name(namespace),
			pstrdup(NameStr(row->name)), -1);

	rel = heap_openrv(cv, AccessShareLock);
	tgoid = get_trig_oid_from_cvrel(rel, tname);
	heap_close(rel, AccessShareLock);
	ReleaseSysCache(tup);

	return tgoid;
}

/*
 * get_trig_oid
 *
 * Given a view name and a trigger name, find the oid of that trigger
 * on the view
 *
 * Also creates a 'check' batch to notify the batch processor of the
 * existence of the trigger.
 */
static Oid
get_trig_oid(TriggerProcessState *state, const char *vname, const char *tname)
{
	Relation rel;
	Oid reloid = GetMatrelOidForView(vname);
	Oid tgoid = InvalidOid;

	if (!reloid)
		return InvalidOid;

	rel = heap_open(reloid, AccessShareLock);
	tgoid = get_trig_oid_from_matrel(rel, tname);

	if (tgoid != InvalidOid)
	{
		XactBatch *batch = start_new_batch(state, "check",
				GetTopTransactionId(), GetCurrentTimestamp());

		do_decode_change(state,
				batch,
				rel,
				TRIGGER_PROCESS_CHANGE_NOOP,
				NULL,
				NULL);

		batch->finished = true;
	}

	heap_close(rel, AccessShareLock);

	return tgoid;
}
