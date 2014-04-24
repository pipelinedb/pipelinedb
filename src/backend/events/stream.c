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
#include "pgxc/pgxcnode.h"
#include "pgxc/locator.h"


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

	stream->handles = handles->datanode_handles;
	stream->state = STREAM_STATE_OPEN;
	stream->handle_count = handles->dn_conn_count;

	return stream;
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
	 * going to send to each datanodes
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

			ensure_out_buffer_capacity(handle->outEnd + ev->len + 4, handle);

			msglen = htonl(ev->len);
			memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
			handle->outEnd += 4;
			memcpy(handle->outBuffer + handle->outEnd, ev->raw, ev->len);
			handle->outEnd += ev->len;
		}

		pgxc_node_flush(handle);
	}

	// foreach events_by_datanode
		// wait for responses
		// responses come from writing to the datanode procs' global conn
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
