/*-------------------------------------------------------------------------
 *
 * stream.h
 *
 * Functions for handling event streams
 *
 * src/include/events/stream.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STREAM_H
#define STREAM_H

#include "postgres.h"
#include "pgxc/pgxcnode.h"

#define EventStreamNeedsOpen(stream) (stream->state != STREAM_STATE_OPEN)

typedef enum
{
	STREAM_STATE_OPEN,
	STREAM_STATE_CLOSED
} StreamState;

typedef struct StreamData
{
	/* state of the stream */
	StreamState state;
	/* handles to remote nodes to send stream events to */
	PGXCNodeHandle **handles;
	/* number of remote nodes */
	int	handle_count;
} EventStreamData;

typedef EventStreamData *EventStream;

typedef struct EventData
{
	/* length of raw event */
	int len;
	/* raw encoded event data */
	char *raw;
} StreamEventData;

typedef StreamEventData *StreamEvent;

extern EventStream OpenStream(void);
extern int RespondSendEvents(int numevents);
extern int SendEvents(EventStream stream, const char *encoding,
		const char *channel, List *events);
extern void CloseStream(EventStream stream);

#endif
