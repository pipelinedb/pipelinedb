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
#include "nodes/bitmapset.h"
#include "pgxc/pgxcnode.h"
#include "utils/hsearch.h"

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

typedef struct StreamEventField
{
	/* name of this field */
	char *name;
	/* pointer to the beginning of this field in its StreamEvent's raw bytes */
	char *fpos;
	/* length in bytes of the field */
	Size flen;
} StreamEventField;

typedef struct EventData
{
	/* length of raw event */
	int len;
	/* raw encoded event data */
	char *raw;
	/* List * of raw fields contained in this event */
	List *fields;
} StreamEventData;

typedef StreamEventData *StreamEvent;

typedef HTAB StreamTargets;

#define STREAMEVENTSIZE sizeof(StreamEventData)

extern EventStream OpenStream(void);
extern int RespondSendEvents(int numevents);
extern int SendEvents(EventStream stream, const char *encoding,
		const char *channel, List *events);
extern void CloseStream(EventStream stream);
extern StreamTargets *CreateStreamTargets(void);
extern Bitmapset *GetTargetsFor(const char *stream, StreamTargets *s);
extern void DestroyStreamTargets(StreamTargets *s);

#endif
