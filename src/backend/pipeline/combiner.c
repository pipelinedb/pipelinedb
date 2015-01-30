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
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_query_fn.h"
#include "commands/pipelinecmds.h"
#include "executor/tupletableReceiver.h"
#include "executor/tstoreReceiver.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/planner.h"
#include "optimizer/tlist.h"
#include "parser/parse_clause.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_type.h"
#include "pipeline/combiner.h"
#include "pipeline/cqmatrel.h"
#include "pipeline/cqplan.h"
#include "pipeline/cqproc.h"
#include "tcop/tcopprot.h"
#include "tcop/pquery.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#define WORKER_BACKLOG 16
#define RECV_TIMEOUT 10 /* ms */
#define RECV_LOOP_TIMEOUT 10 * 1000 /* ms */

typedef struct
{
	int sock;
	pid_t pid;
} WorkerEntry;

typedef struct
{
	int sock;
	int num_workers;
	WorkerEntry *workers;
	fd_set readset;
	int max_fd;
	struct timeval timeout;
} ServerState;

static ServerState *serv = NULL;

static void
ipc_init(CQProcEntry *entry, int recvtimeoutms)
{
	struct sockaddr_un local;
	int len;

	/* 0 means a blocking recv(), which we don't want, so use a reasonable default */
	if (recvtimeoutms == 0)
		recvtimeoutms = RECV_TIMEOUT;

	serv = palloc0(sizeof(ServerState));
	serv->num_workers = entry->pg_size - 1;
	serv->workers = palloc0(serv->num_workers * sizeof(WorkerEntry));
	serv->timeout.tv_sec = (recvtimeoutms / 1000);
	serv->timeout.tv_usec = (recvtimeoutms - (serv->timeout.tv_sec * 1000)) * 1000;

	FD_ZERO(&serv->readset);

	local.sun_family = AF_UNIX;
	strcpy(local.sun_path, entry->sock_name);
	unlink(local.sun_path);

	if ((serv->sock = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0)) == -1)
		elog(ERROR, "combiner could not create socket \"%s\": %m",
				entry->sock_name);

	len = strlen(local.sun_path) + sizeof(local.sun_family);
	if (bind(serv->sock, (struct sockaddr *) &local, len) == -1)
		elog(ERROR, "could not bind to combiner \"%s\": %m", entry->sock_name);

	if (listen(serv->sock, WORKER_BACKLOG) == -1)
		elog(ERROR, "could not listen on socket %d: %m", serv->sock);

	FD_SET(serv->sock, &serv->readset);
	serv->max_fd = serv->sock;
}

static pid_t
do_handshake(int sock)
{
	pid_t pid;

	if (send(sock, &MyProcPid, sizeof(pid_t), 0) < sizeof(pid_t))
		return -1;

	if (recv(sock, &pid, sizeof(pid_t), MSG_WAITALL) != sizeof(pid_t))
		return -1;

	return pid;
}

static void
accept_worker()
{
	struct sockaddr_un remote;
	socklen_t addrlen;
	int worker_sock;
	pid_t worker_pid;
	int i;
	bool found;
	WorkerEntry *entry;
	pid_t *worker_pids = GetWorkerPids(MyCQId);

	for (i = 0; i < serv->num_workers; i++)
	{
		int j;
		found = false;

		for (j = 0; j < serv->num_workers; j++)
		{
			entry = &serv->workers[i];
			if (entry->pid == worker_pids[j])
			{
				found = true;
				break;
			}
		}

		if (found)
			continue;

		break;
	}

	pfree(worker_pids);

	if (found)
		return;

	if ((worker_sock = accept(serv->sock, (struct sockaddr *) &remote, &addrlen)) == -1)
		return;

	if (setsockopt(worker_sock, SOL_SOCKET, SO_RCVTIMEO, (char *) &serv->timeout,
			sizeof(struct timeval)) == -1 ||
		setsockopt(worker_sock, SOL_SOCKET, SO_SNDTIMEO, (char *) &serv->timeout,
			sizeof(struct timeval)) == -1)
	{
		close(worker_sock);
		return;
	}

	worker_pid = do_handshake(worker_sock);

	if (worker_pid <= 0)
	{
		close(worker_sock);
		return;
	}

	FD_CLR(entry->sock, &serv->readset);
	entry->pid = worker_pid;
	entry->sock = worker_sock;
	FD_SET(worker_sock, &serv->readset);
	serv->max_fd = Max(serv->max_fd, worker_sock);

	elog(LOG, "accepted new worker");
}

/*
 * receive_tuple
 */
static bool
receive_tuple(TupleTableSlot *slot, bool need_merge)
{
	HeapTuple tup;
	int32 len;
	ssize_t read;
	fd_set readset;
	int num_ready;
	int i;
	int sock;
	int32 remaining;
	int32 offset = 0;
	TimestampTz start;

	if (!TupIsNull(slot))
		ExecClearTuple(slot);

	memcpy(&readset, &serv->readset, sizeof(fd_set));

	if (need_merge)
	{
		struct timeval timeout;
		memcpy(&timeout, &serv->timeout, sizeof(struct timeval));
		num_ready = select(serv->max_fd + 1, &readset, NULL, NULL, &timeout);
	}
	else
		num_ready = select(serv->max_fd + 1, &readset, NULL, NULL, NULL);

	if (num_ready == 0)
		return false;

	if (FD_ISSET(serv->sock, &readset))
	{
		num_ready--;
		accept_worker();
		FD_CLR(serv->sock, &readset);
	}

	if (num_ready < 0)
	{
		/* One of the readset fds is invalid? Probably means a worker crash */
		if (errno == EBADF)
		{
			accept_worker();
			return false;
		}

		if (errno == EINTR)
			return false;

		elog(ERROR, "combiner select() error: %m.");
	}

	for (i = 0; i < serv->num_workers; i++)
	{
		sock = serv->workers[i].sock;
		if (FD_ISSET(sock, &readset))
			break;
	}

	read = recv(sock, &len, sizeof(int32), MSG_WAITALL);

	if (read < 0)
	{
		/*
		 * TODO(usmanm): This should eventually be removed. Only here
		 * because it makes attaching the debugger to the combiner proc
		 * easy.
		 */
		if (errno != EINTR)
			accept_worker();

		if (errno != EAGAIN)
			elog(LOG, "combiner failed to receive tuple length: %m");

		return false;
	}
	else if (read < sizeof(int32))
		return false;

	len = ntohl(len);
	remaining = len;

	tup = (HeapTuple) palloc0(HEAPTUPLESIZE + len);
	tup->t_len = len;
	tup->t_data = (HeapTupleHeader) ((char *) tup + HEAPTUPLESIZE);
	start = GetCurrentTimestamp();

	while (remaining > 0 && !TimestampDifferenceExceeds(start, GetCurrentTimestamp(), RECV_LOOP_TIMEOUT))
	{
		read = recv(sock, (char *) tup->t_data + offset, remaining, 0);
		if (read < 0)
		{
			if (errno == EAGAIN || errno == EINTR)
				continue;
			elog(LOG, "combiner failed to receive tuple data: %m");
		}
		remaining -= read;
		offset += read;
	}

	/* This could happen if the worker crashed while writing to the socket */
	if (remaining > 0)
	{
		elog(LOG, "combiner only received %d/%d bytes of tuple data", len - remaining, len);
		return false;
	}

	ExecStoreTuple(tup, slot, InvalidBuffer, false);
	return true;
}

/*
 * prepare_combine_plan
 *
 * Retrieves a the cached combine plan for a continuous view, creating it if necessary
 */
static PlannedStmt *
prepare_combine_plan(PlannedStmt *plan, Tuplestorestate *store, TupleDesc *desc)
{
	TuplestoreScan *scan;

	/*
	 * Mark plan as not continuous now because we'll be repeatedly
	 * executing it in a new portal. We also need to set its batch
	 * size to 0 so that TuplestoreScans don't return early. Since
	 * they're not being executed continuously, they'd never
	 * see anything after the first batch was consumed.
	 *
	 */
	plan->is_continuous = false;
	plan->cq_state->batchsize = 0;

	if (IsA(plan->planTree, TuplestoreScan))
		scan = (TuplestoreScan *) plan->planTree;
	else if (IsA(plan->planTree->lefttree, TuplestoreScan))
		scan = (TuplestoreScan *) plan->planTree->lefttree;
	else
		elog(ERROR, "couldn't find TuplestoreScan node in combiner's plan");

	scan->store = store;

	*desc = ExecTypeFromTL(((Plan *) scan)->targetlist, false);

	return plan;
}

/*
 * get_retrieval_where_clause
 *
 * Given a tuplestore containing incoming tuples to combine with
 * on-disk tuples, generates a WHERE clause that can be used to
 * retrieve all corresponding on-disk tuples with a single query.
 */
static Node*
get_retrieval_where_clause(Tuplestorestate *incoming, TupleDesc desc,
		AttrNumber *merge_attrs, int num_merge_attrs, ParseState *ps)
{
	List *name = list_make1(makeString("="));
	Node *where;
	Expr *dnf = NULL;
	List *args = NIL;
	TupleTableSlot *slot = MakeSingleTupleTableSlot(desc);

	foreach_tuple(slot, incoming)
	{
		List *cnfExprs = NIL;
		Node *arg;
		int i;

		for (i = 0; i < num_merge_attrs; i++)
		{
			AttrNumber merge_attr = merge_attrs[i];
			Form_pg_attribute attr = desc->attrs[merge_attr - 1];
			ColumnRef *cref;
			Type typeinfo;
			int length;
			A_Expr *expr;
			bool isnull;
			Datum d;
			Const *c;

			typeinfo = typeidType(attr->atttypid);
			length = typeLen(typeinfo);
			ReleaseSysCache((HeapTuple) typeinfo);

			d = slot_getattr(slot, merge_attr, &isnull);
			c = makeConst(attr->atttypid, attr->atttypmod,
					attr->attcollation, length, d, isnull, attr->attbyval);

			cref = makeNode(ColumnRef);
			cref->fields = list_make1(makeString(NameStr(attr->attname)));
			cref->location = -1;

			expr = makeA_Expr(AEXPR_OP, name, (Node *) cref, (Node *) c, -1);

			/*
			 * If we're grouping on multiple columns, the WHERE predicate must
			 * include a conjunction of all GROUP BY column values for each incoming
			 * tuple. For example, consider the query:
			 *
			 *   SELECT col0, col1, COUNT(*) FROM stream GROUP BY col0, col1
			 *
			 * If we have incoming tuples,
			 *
			 *   (0, 1, 10), (4, 3, 200)
			 *
			 * then we need to look for any tuples on disk for which the following
			 * predicate evaluates to true:
			 *
			 *   (col0 = 0 AND col1 = 1) OR (col0 = 4 AND col1 = 3)
			 *
			 * Her we're creating the conjunction clauses, and we pass them all in
			 * as a list of arguments to an OR expression at the end.
			 */
			cnfExprs = lappend(cnfExprs, transformExpr(ps, (Node *) expr, AEXPR_OP));
		}

		if (list_length(cnfExprs) == 1)
			arg = (Node *) linitial(cnfExprs);
		else
			arg = transformExpr(ps, (Node *) makeBoolExpr(AND_EXPR, cnfExprs, -1), AEXPR_OP);

		args = lappend(args, arg);
	}

	/* this is one big disjunction of conjunctions that match GROUP BY criteria */
	dnf = makeBoolExpr(OR_EXPR, args, -1);
	assign_expr_collations(ps, (Node *) dnf);

	where = transformExpr(ps, (Node *) dnf, EXPR_KIND_WHERE);

	return (Node *) where;
}

/*
 * get_tuples_to_combine_with
 *
 * Gets the plan for retrieving all of the existing tuples that are going
 * to be combined with the incoming tuples
 */
static void
get_tuples_to_combine_with(char *cvname, TupleDesc desc,
		Tuplestorestate *incoming_merges, AttrNumber *merge_attrs,
		int num_merge_attrs, TupleHashTable merge_targets)
{
	Node *raw_parse_tree;
	List *parsetree_list;
	List *query_list;
	PlannedStmt *plan;
	Query *query;
	SelectStmt *stmt;
	Portal portal;
	ParseState *ps;
	DestReceiver *dest;
	char base_select[14 + strlen(cvname) + 1];
	HASH_SEQ_STATUS status;
	HeapTupleEntry entry;
	TupleTableSlot *slot = MakeSingleTupleTableSlot(desc);
	List *tups = NIL;
	ListCell *lc;

	sprintf(base_select, "SELECT * FROM %s", cvname);

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

	if (num_merge_attrs > 0)
	{
		Node *where = get_retrieval_where_clause(incoming_merges, desc, merge_attrs,
				num_merge_attrs, ps);
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

	tuplestore_rescan(incoming_merges);
	foreach_tuple(slot, incoming_merges)
	{
		HeapTuple tup = ExecCopySlotTuple(slot);
		tups = lappend(tups, tup);
	}
	tuplestore_clear(incoming_merges);

	/*
	 * Now add the merge targets that already exist in the continuous view's table
	 * to the input of the final merge query
	 */
	hash_seq_init(&status, merge_targets->hashtab);
	while ((entry = (HeapTupleEntry) hash_seq_search(&status)) != NULL)
	{
		tuplestore_puttuple(incoming_merges, entry->tuple);
	}

	foreach(lc, tups)
	{
		HeapTuple tup = (HeapTuple) lfirst(lc);
		tuplestore_puttuple(incoming_merges, tup);
	}

	PortalDrop(portal, false);
}

/*
 * sync_combine
 *
 * Writes the combine results to a continuous view's table, performing
 * UPDATES or INSERTS as necessary
 */
static void
sync_combine(char *cvname, Tuplestorestate *results,
		TupleTableSlot *slot, TupleHashTable merge_targets)
{
	Relation rel = heap_openrv(makeRangeVar(NULL, cvname, -1), RowExclusiveLock);
	int size = sizeof(bool) * slot->tts_tupleDescriptor->natts;
	bool *replace_all = palloc0(size);
	ResultRelInfo *ri = CQMatViewOpen(rel);

	MemSet(replace_all, true, size);

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

			ExecStoreTuple(updated, slot, InvalidBuffer, false);
			ExecCQMatRelUpdate(ri, slot);
		}
		else
		{
			/* No existing tuple found, so it's an INSERT */
			ExecCQMatRelInsert(ri, slot);
		}
	}
	CQMatViewClose(ri);
	relation_close(rel, RowExclusiveLock);
}

/*
 * combine
 *
 * Combines partial results of a continuous query with existing rows in the continuous view
 */
static void
combine(PlannedStmt *plan, TupleDesc cvdesc,
		Tuplestorestate *store, MemoryContext tmpctx)
{
	TupleTableSlot *slot;
	Portal portal;
	DestReceiver *dest = CreateDestReceiver(DestTuplestore);
	Tuplestorestate *merge_output = NULL;
	AttrNumber *merge_attrs = NULL;
	Oid *merge_attr_ops;
	TupleHashTable merge_targets = NULL;
	FmgrInfo *eq_funcs;
	FmgrInfo *hash_funcs;
	int num_merge_attrs = 0;
	char *matrelname = NameStr(plan->cq_state->matrelname);
	Agg *agg = NULL;

	PushActiveSnapshot(GetTransactionSnapshot());

	slot = MakeSingleTupleTableSlot(cvdesc);

	if (IsA(plan->planTree, Agg))
	{
		agg = (Agg *) plan->planTree;
		merge_attrs = agg->grpColIdx;
		num_merge_attrs = agg->numCols;
		merge_attr_ops = agg->grpOperators;
	}

	if (agg != NULL)
	{
		execTuplesHashPrepare(num_merge_attrs, merge_attr_ops, &eq_funcs, &hash_funcs);
		merge_targets = BuildTupleHashTable(num_merge_attrs, merge_attrs, eq_funcs, hash_funcs, 1000,
				sizeof(HeapTupleEntryData), CacheMemoryContext, tmpctx);
		get_tuples_to_combine_with(matrelname, cvdesc, store, merge_attrs, num_merge_attrs, merge_targets);
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

	sync_combine(matrelname, merge_output, slot, merge_targets);

	if (merge_targets)
		hash_destroy(merge_targets->hashtab);
}

/*
 * ContinuousQueryCombinerRun
 */
void
ContinuousQueryCombinerRun(Portal portal, ContinuousViewState *state, QueryDesc *queryDesc, ResourceOwner owner)
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
	PlannedStmt *combineplan;
	TimestampTz lastCombineTime = GetCurrentTimestamp();
	int32 cq_id = state->id;
	CQProcEntry *entry = GetCQProcEntry(cq_id);
	bool is_worker_done = false;
	bool found_tuple = false;

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

	MarkCombinerAsRunning(MyCQId);

	ipc_init(entry, queryDesc->plannedstmt->cq_state->maxwaitms);

	StartTransactionCommand();

	oldcontext = MemoryContextSwitchTo(runctx);

	/*
	 * Create tuple store and slot outside of combinectx and tmpctx,
	 * so that we don't lose received tuples in case of errors in the loop
	 * below.
	 */
	store = tuplestore_begin_heap(true, true, work_mem);
	combineplan = prepare_combine_plan(queryDesc->plannedstmt, store, &workerdesc);
	slot = MakeSingleTupleTableSlot(workerdesc);

	MemoryContextSwitchTo(oldcontext);

	CommitTransactionCommand();

	oldcontext = MemoryContextSwitchTo(combinectx);

	for (;;)
	{
		PG_TRY();
		{
			bool force = false;

			CurrentResourceOwner = owner;

			found_tuple = receive_tuple(slot, count > 0);

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
				StartTransactionCommand();

				combine(combineplan, workerdesc, store, tmpctx);

				CommitTransactionCommand();

				tuplestore_clear(store);
				MemoryContextResetAndDeleteChildren(combinectx);
				MemoryContextResetAndDeleteChildren(tmpctx);

				lastCombineTime = GetCurrentTimestamp();
				count = 0;
			}

			/*
			 * If we received a tuple in this iteration, poll the socket again.
			 */
			if (found_tuple)
				continue;

			/* Has the CQ been deactivated? */
			if (!entry->active)
			{
				/*
				 * Ensure that the worker has terminated. There is a continue here
				 * because we wanna poll the socket one last time after the worker has
				 * terminated.
				 */
				if (!is_worker_done)
				{
					is_worker_done = AreCQWorkersStopped(cq_id);
					continue;
				}

				/*
				 * By this point the worker process has terminated and
				 * we received no new tuples in the previous iteration.
				 * If there are some unmerged tuples, force merge them.
				 */
				if (count)
				{
					force = true;
					continue;
				}

				goto exit_loop;
			}
		}
		PG_CATCH();
		{
			EmitErrorReport();
			FlushErrorState();

			MemoryContextResetAndDeleteChildren(combinectx);
			MemoryContextResetAndDeleteChildren(tmpctx);
		}
		PG_END_TRY();
	}

exit_loop:
	MemoryContextDelete(runctx);

	CurrentResourceOwner = save;
}
