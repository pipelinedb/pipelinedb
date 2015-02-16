/*-------------------------------------------------------------------------
 *
 * combinerReceiver.c
 *	  An implementation of DestReceiver that that allows combiners to receive
 *	  tuples from worker processes.
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/combinerReceiver.c
 *
 */

#ifndef COMBINER_RECEIVER_H
#define COMBINER_RECEIVER_H

#include "pipeline/cqproc.h"
#include "tcop/dest.h"

extern DestReceiver *CreateCombinerDestReceiver(void);
extern void SetCombinerDestReceiverParams(DestReceiver *self, int32_t cq_id);

#endif
