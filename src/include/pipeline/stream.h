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
#include "utils/hsearch.h"
#include "utils/timestamp.h"

#define EventStreamNeedsOpen(stream) (stream->state != STREAM_STATE_OPEN)
#define QueryIsStreaming(query) ((query)->is_continuous)
#define QueryIsCombine(query) ((query)->is_combine)
#define PlanIsStreaming(stmt) ((stmt)->is_continuous || false)


/* Whether or not to wait on the inserted event to be consumed by the CV*/
extern bool DebugSyncCQ;

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
//	PGXCNodeHandle **handles;
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
	/* arrival time of the event */
	TimestampTz arrivaltime;
} StreamEventData;

typedef StreamEventData *StreamEvent;

typedef HTAB StreamTargets;

#define STREAMEVENTSIZE sizeof(StreamEventData)
#define ARRIVAL_TIMESTAMP "arrival_timestamp"

extern EventStream OpenStream(void);
extern int RespondSendEvents(int numevents);
extern void CloseStream(EventStream stream);
extern void CreateStreamTargets(void);
extern Bitmapset *GetTargetsFor(const char *stream);
extern void DestroyStreamTargets(StreamTargets *s);
extern bool InsertTargetIsStream(InsertStmt *ins);
extern int InsertIntoStream(InsertStmt *ins);

#endif
