/*-------------------------------------------------------------------------
 *
 * cqreceive.c
 *	  Tuple-receiving functionality for continuous queries
 *
 * IDENTIFICATION
 *	  src/backend/executor/cqreceive.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/cvreceive.h"


extern DestReceiver *
CreateContinuousViewReceiver(void)
{
	return NULL;
}
