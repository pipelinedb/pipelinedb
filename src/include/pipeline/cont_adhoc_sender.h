#ifndef CONT_ADHOC_SENDER_H
#define CONT_ADHOC_SENDER_H

#include "access/attnum.h"
#include "tcop/dest.h"

/* 
 * This module is responsible for streaming rows down to the frontend when doing 
 * an adhoc query.
 */

typedef struct AdhocDestReceiver AdhocDestReceiver;

extern DestReceiver *CreateAdhocDestReceiver(void);
extern void SetAdhocDestReceiverParams(DestReceiver *self, bool is_agg, AttrNumber *keyColIdx, int num_cols);

/* call this every so often to detect if the client has gone away */
extern void AdhocDestReceiverHeartbeat(AdhocDestReceiver *receiver);

#endif
