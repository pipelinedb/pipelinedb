/*-------------------------------------------------------------------------
 *
 * cqreceive.h
 *	  Tuple-receiving functionality for continuous queries
 *
 * IDENTIFICATION
 *	  src/include/executor/cqreceive.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CVRECEIVER_H
#define CVRECEIVER_H

#include "tcop/dest.h"

extern DestReceiver *CreateContinuousViewReceiver(void);

#endif		/* CV_RECEIVER_H */
