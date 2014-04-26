/*-------------------------------------------------------------------------
 *
 * stream.c
 *
 *	  Functions for handling event streams
 *
 * src/backend/events/stream.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "events/stream.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "pgxc/locator.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/execRemote.h"
#include "storage/ipc.h"

/*
 * open_stream
 *
 * Opens the necessary connections to interact with a
 * stream across all datanodes
 */
EventStream
open_stream(void)
{
	EventStream stream = (EventStream) palloc(sizeof(EventStream));
	PGXCNodeAllHandles *handles = get_handles(GetAllDataNodes(), NIL, false, true);

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
respond_send_events(int numevents)
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
int
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
					return 0;
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
void
send_events(EventStream stream, const char *encoding,
		const char *channel, List *events)
{
	List *events_by_node[stream->handle_count];
	ListCell *lc;
	int i = 0;
	int lengths[stream->handle_count];
	int encodinglen = strlen(encoding) + 1;
	int channellen = strlen(channel) + 1;

	for (i=0; i<stream->handle_count; i++)
	{
		events_by_node[i] = NIL;
		lengths[i] = 4 + encodinglen + channellen;
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
		int headerlen = 1 + 4 + encodinglen + channellen;

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
		handle_send_events_response(conn, list_length(events_by_node[i]));
	}
}

/*
 * close_stream
 *
 * Closes datanode connections and cleans up stream state
 */
void
close_stream(EventStream stream)
{

}
