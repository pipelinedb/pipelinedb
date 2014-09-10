/*-------------------------------------------------------------------------
 *
 * stream.h
 *
 * Functions for handling event streams
 *
 * src/include/pipeline/stream.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STREAM_H
#define STREAM_H

#include "postgres.h"
#include "nodes/bitmapset.h"
#include "nodes/parsenodes.h"
#include "pgxc/pgxcnode.h"
#include "utils/hsearch.h"

#define EventStreamNeedsOpen(stream) (stream->state != STREAM_STATE_OPEN)
#define QueryIsStreaming(query) ((query)->is_continuous || IS_MERGE_NODE)
#define PlanIsStreaming(stmt) ((stmt)->is_continuous || IS_MERGE_NODE)

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

/*
 * This is the last event that needs to use the fields array it's
 * associated with, so the array can be freed when an event with this
 * flag is encountered. This is to prevent the number of fields arrays
 * in shared memory from growing without bound.
 */
#define DESTROY_FIELDS_ARRAY (1 << 0)

typedef struct EventData
{
	/* special flags for this event */
	char flags;
	/* length of raw event */
	int len;
	/* raw encoded event data */
	char *raw;
	/* pointer to array in shared memory of field names for this event */
	char **fields;
	/* number of fields comprising this event */
	int nfields;
} StreamEventData;

typedef StreamEventData *StreamEvent;

typedef HTAB StreamTargets;

#define STREAMEVENTSIZE sizeof(StreamEventData)

extern EventStream OpenStream(void);
extern int RespondSendEvents(int numevents);
extern int SendEvents(EventStream stream, const char *encoding,
		const char *channel, List *fields, List *events);
extern void CloseStream(EventStream stream);
extern StreamTargets *CreateStreamTargets(void);
extern Bitmapset *GetTargetsFor(const char *stream, StreamTargets *s);
extern void DestroyStreamTargets(StreamTargets *s);
extern bool InsertTargetIsStream(InsertStmt *ins);
extern void InsertIntoStream(EventStream stream, InsertStmt *ins);

#endif
