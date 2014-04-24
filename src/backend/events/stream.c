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


void
send_events(EventStream stream, List *events)
{
	ListCell *lc;
	int i = 0;
	int msglen = 4;
	foreach(lc, events)
	{
		StreamEvent ev = (StreamEvent) lfirst(lc);
		PGXCNodeHandle *handle = stream->handles[i++ % stream->handle_count];
		handle->outBuffer[handle->outEnd++] = ']';
		msglen = htonl(ev->len + 4);
		memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
		handle->outEnd += 4;
		memcpy(handle->outBuffer + handle->outEnd, ev->raw, ev->len);
		handle->outEnd += ev->len;

		pgxc_node_flush(handle);
	}
}

void
close_stream(EventStream stream)
{

}
