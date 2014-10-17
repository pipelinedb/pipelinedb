/*-------------------------------------------------------------------------
 *
 * combiner.c
 *
 *	  Combiner process functionality
 *
 * src/backend/pipeline/combiner.c
 *
 *-------------------------------------------------------------------------
 */
#include <sys/socket.h>
#include <time.h>
#include <sys/un.h>
#include <sys/unistd.h>

#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pipeline_queries.h"
#include "catalog/pipeline_queries_fn.h"
#include "commands/pipelinecmds.h"
#include "executor/tupletableReceiver.h"
#include "executor/tstoreReceiver.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/planner.h"
#include "optimizer/tlist.h"
#include "parser/parse_clause.h"
#include "parser/parse_expr.h"
#include "parser/parse_type.h"
#include "pipeline/combiner.h"
#include "pipeline/cqplan.h"
#include "pipeline/cvmetadata.h"
#include "tcop/tcopprot.h"
#include "tcop/pquery.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#define NAME_PREFIX "combiner_"
#define WORKER_BACKLOG 32


static void
accept_worker(CombinerDesc *desc)
{
  int len;
  struct sockaddr_un local;
  struct sockaddr_un remote;
  struct timeval to;
  socklen_t addrlen;

  local.sun_family = AF_UNIX;
  strcpy(local.sun_path, desc->name);
  unlink(local.sun_path);

  len = strlen(local.sun_path) + sizeof(local.sun_family);
  if (bind(desc->sock, (struct sockaddr *) &local, len) == -1)
  	elog(ERROR, "could not bind to combiner \"%s\": %m", desc->name);

  if (listen(desc->sock, WORKER_BACKLOG) == -1)
  	elog(ERROR, "could not listen on socket %d: %m", desc->sock);

	if ((desc->sock = accept(desc->sock, (struct sockaddr *) &remote, &addrlen)) == -1)
		elog(LOG, "could not accept connections on socket %d: %m", desc->sock);

	/*
	 * Timeouts must be specified in terms of both seconds and usecs,
	 * usecs here must be < 1m
	 */
	if (desc->recvtimeoutms == 0)
	{
		/* 0 means a blocking recv(), which we don't want, so use a reasonable default */
		to.tv_sec = 0;
		to.tv_usec = 1000;
	}
	else
	{
		to.tv_sec = (desc->recvtimeoutms / 1000);
		to.tv_usec = (desc->recvtimeoutms - (to.tv_sec * 1000)) * 1000;
	}

	if (setsockopt(desc->sock, SOL_SOCKET, SO_RCVTIMEO, (char *) &to, sizeof(struct timeval)) == -1)
		elog(ERROR, "could not set combiner recv() timeout: %m");
}

static void
receive_tuple(CombinerDesc *combiner, TupleTableSlot *slot)
{
	HeapTuple tup;
	uint32 len;
	int read;

	ExecClearTuple(slot);

	/*
	 * This socket has a receive timeout set on it, so if we get an EAGAIN it just
	 * means that no new data has arrived.
	 */
	read = recv(combiner->sock, &len, 4, 0);
	if (read < 0)
	{
		/* no new data yet, we'll try again on the next call */
		if (errno == EAGAIN)
			return;
		elog(ERROR, "combiner failed to receive tuple length: %m");
	}
	else if (read == 0)
	{
		return;
	}

	len = ntohl(len);

	tup = (HeapTuple) palloc0(HEAPTUPLESIZE + len);
	tup->t_len = len;
	tup->t_data = (HeapTupleHeader) ((char *) tup + HEAPTUPLESIZE);

	read = recv(combiner->sock, tup->t_data, tup->t_len, 0);
	if (read < 0)
		elog(ERROR, "combiner failed to receive tuple data");

	ExecStoreTuple(tup, slot, InvalidBuffer, false);
}

CombinerDesc *
CreateCombinerDesc(QueryDesc *query)
{
	char *name = query->plannedstmt->cq_target->relname;
	CombinerDesc *desc = palloc(sizeof(CombinerDesc));

	desc->name = palloc(strlen(NAME_PREFIX) + strlen(name) + 1);
	desc->recvtimeoutms = query->plannedstmt->cq_state->maxwaitms;

	memcpy(desc->name, NAME_PREFIX, strlen(NAME_PREFIX));
	memcpy(desc->name + strlen(NAME_PREFIX), name, strlen(name) + 1);

	if ((desc->sock = socket(AF_UNIX, SOCK_STREAM, 0)) == -1)
		elog(ERROR, "could not create combiner socket for \"%s\"", name);

	return desc;
}

void
ContinuousQueryCombinerRun(Portal portal, CombinerDesc *combiner, QueryDesc *queryDesc, ResourceOwner owner)
{
	RangeVar *rv = queryDesc->plannedstmt->cq_target;
	ResourceOwner save = CurrentResourceOwner;
	TupleTableSlot *slot;
	TupleDesc workerdesc;
	Tuplestorestate *store;
	long count = 0;
	int batchsize = queryDesc->plannedstmt->cq_state->batchsize;
	int timeout = queryDesc->plannedstmt->cq_state->maxwaitms;
	char *cvname = rv->relname;
	Query *query;
	PlannedStmt *combineplan;
	TimestampTz lastCombineTime = GetCurrentTimestamp();
	int32 cq_id = queryDesc->plannedstmt->cq_state->id;
	bool *activeFlagPtr = GetActiveFlagPtr(cq_id);

	MemoryContext runctx = AllocSetContextCreate(TopMemoryContext,
			"CombinerRunContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContext combinectx = AllocSetContextCreate(runctx,
			"CombineContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContext tmpctx = AllocSetContextCreate(runctx,
			"CombineTmpContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	MemoryContext oldcontext;

	CurrentResourceOwner = owner;

	elog(LOG, "\"%s\" combiner %d running", cvname, MyProcPid);

	IncrementProcessGroupCount(cq_id);

	accept_worker(combiner);

	StartTransactionCommand();

	oldcontext = MemoryContextSwitchTo(runctx);

	store = tuplestore_begin_heap(true, true, work_mem);
	combineplan = GetCombinePlan(cvname, queryDesc->plannedstmt, store, &query, &workerdesc);
	slot = MakeSingleTupleTableSlot(workerdesc);

	MemoryContextSwitchTo(oldcontext);

	CommitTransactionCommand();

	for (;;)
	{
		bool force = false;
		CurrentResourceOwner = owner;

		receive_tuple(combiner, slot);

		/*
		 * If we get a null tuple, we either want to combine the current batch
		 * or wait a little while longer for more tuples before forcing the batch
		 */
		if (TupIsNull(slot))
		{
			if (timeout > 0)
			{
				if (!TimestampDifferenceExceeds(lastCombineTime, GetCurrentTimestamp(), timeout))
					continue; /* timeout not reached yet, keep scanning for new tuples to arrive */
			}
			force = true;
		}
		else
		{
			tuplestore_puttupleslot(store, slot);
			count++;
		}

		if (count > 0 && (count == batchsize || force))
		{
			MemoryContext oldcontext;

			StartTransactionCommand();

			oldcontext = MemoryContextSwitchTo(combinectx);
			Combine(query, combineplan, workerdesc, store, tmpctx);
			MemoryContextSwitchTo(oldcontext);

			CommitTransactionCommand();

			tuplestore_clear(store);
			MemoryContextReset(combinectx);
			MemoryContextSwitchTo(oldcontext);

			lastCombineTime = GetCurrentTimestamp();
			count = 0;
		}

		/* Check the shared metadata to see if the CV has been deactivated */
		if (!*activeFlagPtr)
			break;
	}

	DecrementProcessGroupCount(cq_id);

	CurrentResourceOwner = save;
}

/*
 * GetCombinePlan
 *
 * Retrieves a the cached combine plan for a continuous view, creating it if necessary
 */
PlannedStmt *
GetCombinePlan(char *cvname, PlannedStmt *plan, Tuplestorestate *store, Query **query, TupleDesc *desc)
{
	char	*query_string;
	List	*parsetree_list;
	List	*query_list;
	SelectStmt *stmt;
	Query	*q;
	TuplestoreScan *scan;

	/*
	 * TODO(usmanm/derekjn): Right now we use the Query node
	 * to figure out the merge attributes (essentially
	 * columns being grouped on). We should NOT do this
	 * because it requires us to do this whole parsing shit
	 * again. Instead pull them out of Plan->Agg->grpColIfx.
	 */
	query_string = GetQueryString(cvname, true);
	parsetree_list = pg_parse_query(query_string);

	/* CVs should only have a single query */
	Assert(parsetree_list->length == 1);

	stmt = (SelectStmt *) linitial(parsetree_list);
	stmt = GetSelectStmtForCQWorker(stmt);
	stmt->forContinuousView = true;

	query_list = pg_analyze_and_rewrite((Node *) stmt, query_string, NULL, 0);

	/* CVs should only have a single query */
	Assert(query_list->length == 1);
	q = (Query *) linitial(query_list);

	q->is_combine = true;
	q->is_continuous = false;

	/*
	 * Mark plan as not continuous now because we'll be repeatedly
	 * executing it in a new portal.
	 */
	plan->is_continuous = false;

	if (IsA(plan->planTree, TuplestoreScan))
		scan = (TuplestoreScan *) plan->planTree;
	else if (IsA(plan->planTree->lefttree, TuplestoreScan))
		scan = (TuplestoreScan *) plan->planTree->lefttree;

	scan->store = store;

	*query = q;
	*desc = ExecTypeFromTL(((Plan *) scan)->targetlist, false);

	return plan;
}

/*
 * GetCombineColumns
 *
 * Given a continuous query, determine the columns in the underlying table
 * that correspond to the GROUP BY clause of the query
 */
List *
GetCombineColumns(Query *query)
{
	List *result = NIL;
	ListCell *tl;
	AttrNumber col = 0;
	foreach(tl, query->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(tl);
		col++;
		if (get_grouping_column_index(query, tle) >= 0)
			result = lcons_int(col, result);
	}
	return result;
}

/*
 * GetTuplesToCombineWith
 *
 * Gets the plan for retrieving all of the existing tuples that are going
 * to be combined with the incoming tuples
 */
void
GetTuplesToCombineWith(char *cvname, TupleDesc desc,
		Tuplestorestate *incoming_merges, AttrNumber merge_attr,
		List *group_clause, TupleHashTable merge_targets)
{
	Node *raw_parse_tree;
	List *parsetree_list;
	List *query_list;
	PlannedStmt *plan;
	Query *query;
	A_Expr *in_expr;
	Node *where;
	ColumnRef *cref;
	List *constants = NIL;
	SelectStmt *stmt;
	Portal portal;
	TupleTableSlot 	*slot;
	ParseState *ps;
	DestReceiver *dest;
	Type typeinfo;
	int length;
	char stmt_name[strlen(cvname) + 9 + 1];
	char base_select[14 + strlen(cvname) + 1];
	Form_pg_attribute attr = desc->attrs[merge_attr - 1];
	List *name = list_make1(makeString("="));
	int incoming_size = 0;
	HASH_SEQ_STATUS status;
	HeapTupleEntry entry;

	strcpy(stmt_name, cvname);
	sprintf(base_select, "SELECT * FROM %s", cvname);

	slot = MakeSingleTupleTableSlot(desc);
	name = list_make1(makeString("="));
	ps = make_parsestate(NULL);

	dest = CreateDestReceiver(DestTupleTable);

	parsetree_list = pg_parse_query(base_select);
	Assert(parsetree_list->length == 1);

	/*
	 * We need to do this to populate the ParseState's p_varnamespace member
	 */
	stmt = (SelectStmt *) linitial(parsetree_list);
	transformFromClause(ps, stmt->fromClause);

	raw_parse_tree = (Node *) linitial(parsetree_list);
	query_list = pg_analyze_and_rewrite(raw_parse_tree, base_select, NULL, 0);

	Assert(query_list->length == 1);
	query = (Query *) linitial(query_list);

	typeinfo = typeidType(attr->atttypid);
	length = typeLen(typeinfo);
	ReleaseSysCache((HeapTuple) typeinfo);

	/*
	 * We need to extract all of the merge column values from our incoming merge
	 * tuples so we can use them in an IN clause when retrieving existing tuples
	 * from the continuous view
	 */
	foreach_tuple(slot, incoming_merges)
	{
		bool isnull;
		Datum d = slot_getattr(slot, merge_attr, &isnull);
		Const *c = makeConst(attr->atttypid, attr->atttypmod, 0, length, d, isnull, true);

		constants = lcons(c, constants);
		incoming_size++;
	}

	/*
	 * Now construct an IN clause from the List of merge column values we just built
	 */
	cref = makeNode(ColumnRef);
	cref->fields = list_make1(makeString(attr->attname.data));
	cref->location = -1;

	in_expr = makeA_Expr(AEXPR_IN, name,
			   (Node *) cref, (Node *) constants, -1);

	/*
	 * This query is now of the form:
	 *
	 * SELECT * FROM <continuous view> WHERE <merge column> IN (incoming merge column values)
	 */
	if (merge_targets->numCols > 0)
	{
		where = transformAExprIn(ps, in_expr);
		query->jointree = makeFromExpr(query->jointree->fromlist, where);
	}

	plan = pg_plan_query(query, 0, NULL);

	/*
	 * Now run the query that retrieves existing tuples to merge this merge request with.
	 * This query outputs to the tuplestore currently holding the incoming merge tuples.
	 */
	portal = CreatePortal("__merge_retrieval__", true, true);
	portal->visible = false;

	PortalDefineQuery(portal,
					  NULL,
					  NULL,
					  "SELECT",
					  list_make1(plan),
					  NULL);

	SetTupleTableDestReceiverParams(dest, merge_targets, CacheMemoryContext, true);

	PortalStart(portal, NULL, 0, GetActiveSnapshot());

	(void) PortalRun(portal,
					 FETCH_ALL,
					 true,
					 dest,
					 dest,
					 NULL);

	/*
	 * Now add the merge targets that already exist in the continuous view's table
	 * to the input of the final merge query
	 */
	hash_seq_init(&status, merge_targets->hashtab);
	while ((entry = (HeapTupleEntry) hash_seq_search(&status)) != NULL)
	{
		tuplestore_puttuple(incoming_merges, entry->tuple);
	}
}

/*
 * SyncCombine
 *
 * Writes the combine results to a continuous view's table, performing
 * UPDATES or INSERTS as necessary
 */
void
SyncCombine(char *cvname, Tuplestorestate *results,
		TupleTableSlot *slot, AttrNumber merge_attr, TupleHashTable merge_targets)
{
	Relation rel = heap_openrv(makeRangeVar(NULL, cvname, -1), RowExclusiveLock);
	bool *replace_all = palloc(sizeof(bool) * slot->tts_tupleDescriptor->natts);
	MemSet(replace_all, true, sizeof(replace_all));

	foreach_tuple(slot, results)
	{
		HeapTupleEntry update = NULL;
		slot_getallattrs(slot);

		if (merge_targets)
			update = (HeapTupleEntry) LookupTupleHashEntry(merge_targets, slot, NULL);

		if (update)
		{
			/*
			 * The slot has the updated values, so store them in the updatable physical tuple
			 */
			HeapTuple updated = heap_modify_tuple(update->tuple, slot->tts_tupleDescriptor,
					slot->tts_values, slot->tts_isnull, replace_all);

			simple_heap_update(rel, &update->tuple->t_self, updated);
		}
		else
		{
			/* No existing tuple found, so it's an INSERT */
			heap_insert(rel, slot->tts_tuple, GetCurrentCommandId(true), 0, NULL);
		}
	}
	relation_close(rel, RowExclusiveLock);
}

/*
 * Combine
 *
 * Combines partial results of a continuous query with existing rows in the continuous view
 */
void
Combine(Query *query, PlannedStmt *plan, TupleDesc cvdesc,
		Tuplestorestate *store, MemoryContext tmpctx)
{
	TupleTableSlot *slot;
	Portal portal;
	DestReceiver *dest = CreateDestReceiver(DestTuplestore);
	Tuplestorestate *merge_output = NULL;
	AttrNumber merge_attr = 1;
	List *merge_attrs;
	List *group_clause;
	TupleHashTable merge_targets = NULL;
	AttrNumber *cols;
	FmgrInfo *eq_funcs;
	FmgrInfo *hash_funcs;
	int num_cols = 0;
	int num_buckets = 1;
	char *cvname = GetCQMatRelName(plan->cq_target->relname);

	PushActiveSnapshot(GetTransactionSnapshot());

	slot = MakeSingleTupleTableSlot(cvdesc);
	group_clause = query->groupClause;

	merge_attrs = GetCombineColumns(query);
	if (merge_attrs)
		merge_attr = (AttrNumber) lfirst_int(merge_attrs->head);

	if (group_clause)
	{
		num_cols = list_length(group_clause);
		if (num_cols > 1)
			elog(ERROR, "grouping on more than one column is not supported yet (attempted to group on %d)", num_cols);
		cols = (AttrNumber *) palloc(sizeof(AttrNumber) * num_cols);
		cols[0] = merge_attr;
		execTuplesHashPrepare(num_cols, extract_grouping_ops(group_clause), &eq_funcs, &hash_funcs);
		num_buckets = 1000;

		merge_targets = BuildTupleHashTable(num_cols, cols, eq_funcs, hash_funcs, num_buckets,
				sizeof(HeapTupleEntryData), CacheMemoryContext, tmpctx);

		GetTuplesToCombineWith(cvname, cvdesc, store, merge_attr, group_clause, merge_targets);
	}

	portal = CreatePortal("__merge__", true, true);
	portal->visible = false;

	PortalDefineQuery(portal,
					  NULL,
					  NULL,
					  "SELECT",
					  list_make1(plan),
					  NULL);

	merge_output = tuplestore_begin_heap(true, true, work_mem);
	SetTuplestoreDestReceiverParams(dest, merge_output, PortalGetHeapMemory(portal), true);

	PortalStart(portal, NULL, EXEC_FLAG_COMBINE, GetActiveSnapshot());

	(void) PortalRun(portal,
					 FETCH_ALL,
					 true,
					 dest,
					 dest,
					 NULL);

	PopActiveSnapshot();

	SyncCombine(cvname, merge_output, slot, merge_attr, merge_targets);

	if (merge_targets)
		hash_destroy(merge_targets->hashtab);
}
