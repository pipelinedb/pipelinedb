/*-------------------------------------------------------------------------
 *
 * stream.c
 *
 *	  Functions for handling event streams
 *
 * src/backend/pipeline/stream.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pipeline_queries.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "parser/analyze.h"
#include "pgxc/locator.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/execRemote.h"
#include "pipeline/decode.h"
#include "pipeline/stream.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/tqual.h"

#define SEND_EVENTS_RESPONSE_COMPLETE 0
#define SEND_EVENTS_RESPONSE_MISMATCH 1
#define SEND_EVENTS_RESPONSE_FAILED	2

typedef struct StreamTagsEntry
{
	char key[NAMEDATALEN]; /* hash key --- MUST BE FIRST */
	Bitmapset *tags;
} StreamTagsEntry;

/*
 * open_stream
 *
 * Opens the necessary connections to interact with a
 * stream across all datanodes
 */
EventStream
OpenStream(void)
{
	EventStream stream = (EventStream) palloc(sizeof(EventStream));
	PGXCNodeAllHandles *handles = get_handles(GetAllDataNodes(), NIL, false, false);

	if (handles->dn_conn_count <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not connect to stream: no datanodes available")));

	stream->handles = handles->datanode_handles;
	stream->state = STREAM_STATE_OPEN;
	stream->handle_count = handles->dn_conn_count;

	return stream;
}

/*
 * respond_send_events
 *
 * Sends a response from a datanode to a coordinator, signifying
 * that the given number of events were received
 */
int
RespondSendEvents(int numevents)
{
	StringInfoData resp;
	pq_beginmessage(&resp, '#');
	pq_sendint(&resp, numevents, 4);
	pq_endmessage(&resp);

	return pq_flush();
}

/*
 * handle_send_events_response
 *
 * Waits for a response from a datanode after sending it events
 */
static int
handle_send_events_response(PGXCNodeHandle *conn, int expected)
{
	int msg_len;
	char *msg;
	char msg_type;
	StringInfoData buf;

	initStringInfo(&buf);

	for (;;)
	{
		/*
		 * If we are in the process of shutting down, we
		 * may be rolling back, and the buffer may contain other messages.
		 * We want to avoid a procarray exception
		 * as well as an error stack overflow.
		 */
		if (proc_exit_inprogress)
			conn->state = DN_CONNECTION_STATE_ERROR_FATAL;

		/* don't read from from the connection if there is a fatal error */
		if (conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
			return RESPONSE_COMPLETE;

		/* No data available, exit */
		if (!HAS_MESSAGE_BUFFERED(conn))
			return RESPONSE_EOF;

		msg_type = get_message(conn, &msg_len, &msg);
		appendBinaryStringInfo(&buf, msg, msg_len);

		switch (msg_type)
		{
			case '#':
				{
					int numreceived = pq_getmsgint(&buf, 4);

					return numreceived;
				}
		}
	}

	return 0;
}

/*
 * send_events
 *
 * Partitions raw events by datanode and sends each batch of partitioned
 * events to their respective datanodes
 */
int
SendEvents(EventStream stream, const char *encoding,
		const char *channel, List *fields, List *events)
{
	List *events_by_node[stream->handle_count];
	ListCell *lc;
	int i = 0;
	int lengths[stream->handle_count];
	int encodinglen = strlen(encoding) + 1;
	int channellen = strlen(channel) + 1;
	int numfields = list_length(fields);
	int fieldslen = 0;
	int result = 0;

	foreach(lc, fields)
	{
		char *field = (char *) lfirst(lc);
		fieldslen += strlen(field) + 1;
	}

	for (i=0; i<stream->handle_count; i++)
	{
		events_by_node[i] = NIL;
		lengths[i] = 4 + encodinglen + channellen + 4 + fieldslen;
	}

	/*
	 * Here we partition the events by datanode.
	 * We also need to know the total message length for each message we're
	 * going to send to each datanode
	 */
	foreach(lc, events)
	{
		int node = i++ % stream->handle_count;
		StreamEvent ev = (StreamEvent) lfirst(lc);
		events_by_node[node] = lcons(ev, events_by_node[node]);
		lengths[node] += ev->len + 4;
	}

	/* build each message prefix  */
	for (i=0; i<stream->handle_count; i++)
	{
		PGXCNodeHandle *handle = stream->handles[i];
		int msglen;
		int desclen;
		int headerlen = 1 + 4 + encodinglen + channellen + 4 + fieldslen;

		if (ensure_out_buffer_capacity(handle->outEnd + headerlen, handle) != 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
							errmsg("out of memory")));
		}

		handle->outBuffer[handle->outEnd++] = ']';
		msglen = htonl(lengths[i]);
		memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
		handle->outEnd += 4;

		memcpy(handle->outBuffer + handle->outEnd, encoding, encodinglen);
		handle->outEnd += encodinglen;

		memcpy(handle->outBuffer + handle->outEnd, channel, channellen);
		handle->outEnd += channellen;

		desclen = htonl(numfields);
		memcpy(handle->outBuffer + handle->outEnd, &desclen, 4);
		handle->outEnd += 4;

		foreach (lc, fields)
		{
			char *field = (char *) lfirst(lc);
			int len = strlen(field) + 1;
			memcpy(handle->outBuffer + handle->outEnd, field, len);
			handle->outEnd += len;
		}
	}

	/* now serialize each event to each datanode's message buffer, and flush */
	for (i=0; i<stream->handle_count; i++)
	{
		List *evs = events_by_node[i];
		PGXCNodeHandle *handle = stream->handles[i];

		foreach(lc, evs)
		{
			int msglen;
			StreamEvent ev = (StreamEvent) lfirst(lc);

			if (ensure_out_buffer_capacity(handle->outEnd + ev->len + 4, handle) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
								errmsg("out of memory")));
			}

			msglen = htonl(ev->len);
			memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
			handle->outEnd += 4;
			memcpy(handle->outBuffer + handle->outEnd, ev->raw, ev->len);
			handle->outEnd += ev->len;
		}

		handle->state = DN_CONNECTION_STATE_QUERY;
		pgxc_node_flush(handle);
	}

	for (i=0; i<stream->handle_count; i++)
	{
		PGXCNodeHandle *conn = stream->handles[i];
		pgxc_node_receive(1, &conn, NULL);
		result += handle_send_events_response(conn, list_length(events_by_node[i]));
	}

	return result;
}

/*
 * close_stream
 *
 * Closes datanode connections and cleans up stream state
 */
void
CloseStream(EventStream stream)
{

}

/*
 * GetStreamTargets
 *
 * Builds a mapping from stream name to continuous view tagindexes that read from the stream
 */
StreamTargets *
CreateStreamTargets(void)
{
	HASHCTL ctl;
	StreamTargets *targets;
	Relation rel;
	HeapScanDesc scandesc;
	Form_pipeline_queries catrow;
	HeapTuple tup;
	MemoryContext oldcontext;

	MemSet(&ctl, 0, sizeof(ctl));

	ctl.keysize = NAMEDATALEN;
	ctl.entrysize = sizeof(StreamTagsEntry);

	oldcontext = MemoryContextSwitchTo(CacheMemoryContext);
	targets = hash_create("StreamTargets", 32, &ctl, HASH_ELEM);

	rel = heap_open(PipelineQueriesRelationId, AccessExclusiveLock);
	scandesc = heap_beginscan(rel, SnapshotNow, 0, NULL);

	while ((tup = heap_getnext(scandesc, ForwardScanDirection)) != NULL)
	{
		char *querystring;
		ListCell *lc;
		List *parsetree_list;
		Node *parsetree;
		CreateContinuousViewStmt *cv;
		SelectStmt *select;

		catrow = (Form_pipeline_queries) GETSTRUCT(tup);
		querystring = TextDatumGetCString(&(catrow->query));

		parsetree_list = pg_parse_query(querystring);
		parsetree = (Node *) lfirst(parsetree_list->head);
		cv = (CreateContinuousViewStmt *) parsetree;
		select = (SelectStmt *) cv->query;

		foreach(lc, select->fromClause)
		{
			Node *node = (Node *) lfirst(lc);
			if (IsA(node, JoinExpr))
			{
				JoinExpr *j = (JoinExpr *) node;
				char *relname = ((RangeVar *) j->larg)->relname;
				bool found;
				StreamTagsEntry *entry =
						(StreamTagsEntry *) hash_search(targets, (void *) relname, HASH_ENTER, &found);

				if (!found)
					entry->tags = NULL;

				entry->tags = bms_add_member(entry->tags, catrow->id);

				relname = ((RangeVar *) j->rarg)->relname;
				entry = (StreamTagsEntry *) hash_search(targets, (void *) relname, HASH_ENTER, NULL);
				entry->tags = bms_add_member(entry->tags, catrow->id);
			}
			else if (IsA(node, RangeVar))
			{
				RangeVar *rv = (RangeVar *) node;
				bool found;
				StreamTagsEntry *entry =
						(StreamTagsEntry *) hash_search(targets, (void *) rv->relname, HASH_ENTER, &found);

				if (!found)
					entry->tags = NULL;

				entry->tags = bms_add_member(entry->tags, catrow->id);
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("unrecognized node type found when determining stream targets: %d", nodeTag(node))));
			}
		}
	}

	heap_endscan(scandesc);
	heap_close(rel, AccessExclusiveLock);

	MemoryContextSwitchTo(oldcontext);

	return targets;
}

/*
 * CopyStreamTargets
 *
 * Copies the bitmap of the given stream's target CVs into the specified address.
 * This is used to load the bitmap into the stream buffer's shared memory.
 */
Bitmapset *
GetTargetsFor(const char *stream, StreamTargets *s)
{
	bool found;
	StreamTagsEntry *entry =
			(StreamTagsEntry *) hash_search(s, stream, HASH_ENTER, &found);
	if (!found)
		return NULL;

	return entry->tags;
}

/*
 * DestroyStreamTargets
 *
 * Cleans up a StreamTargets object
 */
void
DestroyStreamTargets(StreamTargets *s)
{
	hash_destroy(s);
}

/*
 * InsertTargetIsStream
 *
 * Is the given INSERT statements target relation a stream?
 * We assume it is if the relation doesn't exist in the catalog as a normal relation.
 */
bool InsertTargetIsStream(InsertStmt *ins)
{
	Oid reloid = RangeVarGetRelid(ins->relation, NoLock, true);

	if (reloid != InvalidOid)
		return false;

	if (!GlobalStreamBuffer)
		InitGlobalStreamBuffer();

	if (IsInputStream(ins->relation->relname))
		return true;

	ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_TABLE),
			 errmsg("relation \"%s\" does not exist",
					ins->relation->relname)));

	return false;
}

/*
 * InsertIntoStream
 *
 * Send INSERT-encoded events to the given stream
 *
 *
 */
int
InsertIntoStream(EventStream stream, InsertStmt *ins)
{
	SelectStmt *sel = (SelectStmt *) ins->selectStmt;
	ListCell *lc;
	List *events = NIL;
	List *fields = NIL;
	int numcols = list_length(ins->cols);
	int i;

	/* make sure all tuples are of the correct length before sending any */
	foreach (lc, sel->valuesLists)
	{
		List *vals = (List *) lfirst(lc);
		if (list_length(vals) < numcols)
			elog(ERROR, "VALUES tuples must have at least %d values", numcols);
	}

	/* build header */
	for (i=0; i<numcols; i++)
	{
		ListCell *rtc;
		ResTarget *res = (ResTarget *) list_nth(ins->cols, i);
		int count = 0;

		/* verify that each column only appears once */
		foreach(rtc, ins->cols)
		{
			ResTarget *r = (ResTarget *) lfirst(rtc);
			if (strcmp(r->name, res->name) == 0)
				count++;
		}
		if (count > 1)
			elog(ERROR, "column \"%s\" appears more than once in columns list", res->name);

		fields = lappend(fields, res->name);
	}

	foreach (lc, sel->valuesLists)
	{
		List *vals = (List *) lfirst(lc);
		A_Const *c;
		Value *v;
		List *values = NIL;
		Size size = 0;
		StreamEvent ev = (StreamEvent) palloc(STREAMEVENTSIZE);
		ListCell *vlc;
		int offset = 0;

		for (i=0; i<numcols; i++)
		{
			char *sval;

			c = (A_Const *) list_nth(vals, i);
			v = &(c->val);

			if (IsA(v, Integer))
			{
				/* longs have a maximum of 20 digits */
				sval = palloc(20);
				sprintf(sval, "%ld", intVal(v));
			}
			else
			{
				sval = strVal(v);
			}

			values = lappend(values, sval);
			size += strlen(sval) + 1;
		}

		ev->raw = palloc(size);
		ev->len = size;

		foreach (vlc, values)
		{
			char *value = (char *) lfirst(vlc);
			int len = strlen(value) + 1;

			memcpy(ev->raw + offset, value, len);
			offset += len;
		}

		events = lcons(ev, events);
	}

	return SendEvents(stream, VALUES_ENCODING, ins->relation->relname, fields, events);
}
