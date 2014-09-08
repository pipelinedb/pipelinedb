/*-------------------------------------------------------------------------
 *
 * merge.c
 *
 *	  Support for incremental updates of continuous views
 *
 * src/backend/pipeline/merge.c
 *
 *-------------------------------------------------------------------------
 */
#include "pipeline/merge.h"

/*
 * exec_merge_retrieval
 *
 * Gets the plan for retrieving all of the existing tuples that
 * this merge request will merge with
 */
extern void
exec_merge_retrieval(char *cvname, TupleDesc desc,
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
	MemoryContext oldcontext;
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

	oldcontext = MemoryContextSwitchTo(MessageContext);

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

	MemoryContextSwitchTo(oldcontext);

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
		tuplestore_puttuple(incoming_merges, entry->tuple);
}


/*
 * sync_merge_results
 *
 * Writes the merge results to a continuous view's table, performing
 * UPDATES or INSERTS as necessary
 */
extern void
sync_merge_results(char *cvname, Tuplestorestate *results,
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
 * get_merge_columns
 *
 * Given a continuous query, determine the columns in the underlying table
 * that correspond to the GROUP BY clause of the query
 */
extern List *
get_merge_columns(Query *query)
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
 * get_cached_merge_plan
 *
 * Retrieves a the cached merge plan for a continuous view, creating it if necessary
 */
extern CachedPlan *
get_merge_plan(char *cvname, CachedPlanSource **src)
{
	RangeVar *rel = makeRangeVar(NULL, cvname, -1);
	char *query_string;
	CachedPlanSource *psrc;
	MemoryContext oldContext;
	Node	   *raw_parse_tree;
	List	   *parsetree_list;
	List		 *query_list;
	Query 	 *query;
	PreparedStatement *pstmt = FetchPreparedStatement(cvname, false);

	if (pstmt)
	{
		psrc = pstmt->plansource;
	}
	else
	{
		/*
		 * It doesn't exist, so create and cache it
		 */
		oldContext = MemoryContextSwitchTo(CacheMemoryContext);

		query_string = GetQueryString(rel, NULL, true);
		parsetree_list = pg_parse_query(query_string);

		/* CVs should only have a single query */
		Assert(parsetree_list->length == 1);

		raw_parse_tree = (Node *) linitial(parsetree_list);
		((SelectStmt *) raw_parse_tree)->forContinuousView = true;

		query_list = pg_analyze_and_rewrite(raw_parse_tree, query_string, NULL, 0);

		/* CVs should only have a single query */
		Assert(query_list->length == 1);
		query = (Query *) linitial(query_list);

		if (query->sql_statement == NULL)
			query->sql_statement = pstrdup(query_string);
		query->cq_is_merge = true;

		psrc = CreateCachedPlan(raw_parse_tree, query_string, cvname, "SELECT");

		/* TODO: size this appropriately */
		psrc->store = tuplestore_begin_heap(true, true, 1000);
		psrc->desc = RelationNameGetTupleDesc(cvname);
		psrc->query = query;

		CompleteCachedPlan(psrc, query_list, NULL, 0, 0, NULL,  NULL, 0, true);
		StorePreparedStatement(cvname, psrc, false);

		MemoryContextSwitchTo(oldContext);
	}

	*src = psrc;

	return GetCachedPlan(psrc, 0, false);
}

/*
 * DoRemoteMerge
 *
 * Sends a batch of partial result tuples down to the datanodes for final merging
 */
extern void
DoRemoteMerge(RemoteMergeState mergeState)
{
	AttrNumber distCol = mergeState.locinfo->partAttrNum - 1;
	TupleTableSlot *slot = mergeState.slot;
	PGXCNodeAllHandles *handles = get_handles(GetAllDataNodes(), NIL, false, true);
	const char *target = mergeState.targetRelation->relname;
	int targetLen = strlen(target) + 1; /* we want to send the '\0' */
	int msglens[handles->dn_conn_count];
	int i;
	List *tuplesByNode[handles->dn_conn_count];
	TransactionId gxid = GetCurrentTransactionId();
	List *nodeslist;

	for (i=0; i<handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *handle = handles->datanode_handles[i];

		/* Total message length. Length of this field is included */
		msglens[i] = 4;

		/* we want to include the '\0' */
		msglens[i] += strlen(target) + 1;

		tuplesByNode[i] = NIL;

		pgxc_node_send_gxid(handle, gxid);
		pgxc_node_send_timestamp(handle, GetCurrentGTMStartTimestamp());
		pgxc_node_send_snapshot(handle, GetActiveSnapshot());
	}

	/*
	 * Serialize all of the tuples in the store to raw messages. We do this
	 * all at once before flushing because message lengths are variable, so we
	 * don't know their lengths (which we'll need for deserialization) until
	 * after they're serialized.
	 */
	while (tuplestore_gettupleslot(mergeState.store, true, false, slot))
	{
		ListCell *nlc;

		if (distCol >= 0)
		{
			Oid type = slot->tts_tupleDescriptor->attrs[distCol]->atttypid;
			bool isnull;
			Datum distValue = slot_getattr(slot, distCol + 1, &isnull);

			ExecNodes *nodes = GetRelationNodes(mergeState.locinfo,
							distValue, isnull, type, RELATION_ACCESS_INSERT);

			nodeslist = nodes->nodeList;
		}
		else
		{
			/* replicated */
			nodeslist = GetAllDataNodes();
		}

		/*
		 * Put each tuple in a queue bound for one or more datanodes
		 */
		foreach(nlc, nodeslist)
		{
			int node = lfirst_int(nlc);
			StringInfo raw = TupleToBytes(mergeState.bufprint, slot);

			/* each tuple will have a 4-byte length prefix */
			msglens[node] += raw->len + 4;
			tuplesByNode[node] = lcons(raw, tuplesByNode[node]);
		}
	}

	/*
	 * Now that we have all of our raw messages with lengths, we can
	 * flush each datanode's entire buffer (one message per conn).
	 */
	for (i=0; i<handles->dn_conn_count; i++)
	{
		int msglen = msglens[i];
		PGXCNodeHandle *handle = handles->datanode_handles[i];
		List *rawtups = tuplesByNode[i];
		ListCell *lc;
		StringInfoData result;

		if (list_length(rawtups) == 0)
			continue;

		initStringInfo(&result);

		/* message prefix */
		appendStringInfoChar(&result, '+');

		/* total message length */
		msglen = htonl(msglen);
		appendBinaryStringInfo(&result, (char *) &msglen, 4);

		/* target output relation */
		appendBinaryStringInfo(&result, target, targetLen);

		foreach(lc, rawtups)
		{
			int rowlen;
			StringInfo bytes = (StringInfo) lfirst(lc);

			/* length of DataRow message (this uses memcpy so pointing to rowlen is safe here) */
			rowlen = htonl(bytes->len);
			appendBinaryStringInfo(&result, (char *) &rowlen, 4);

			/* DataRow message */
			appendBinaryStringInfo(&result, bytes->data, bytes->len);
		}

		memcpy(handle->outBuffer + handle->outEnd, result.data, result.len);
		handle->outEnd += result.len;

		pgxc_node_flush(handle);
	}

	tuplestore_clear(mergeState.store);
}
