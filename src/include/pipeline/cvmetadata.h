/*-------------------------------------------------------------------------
 *
 * cvmetadata.h
 *	  prototypes for cvmetadata.c.
 *
 * src/include/pipeline/cvmetadata.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef CVMETADATA_H
#define CVMETADATA_H

#include "nodes/parsenodes.h"

/* 
   CV Metadata, elements of which are stored
   in shared memory
 */
typedef struct cv_bgw_metadata
{
	uint32 key;// KEY (MUST BE THE FIRST FIELD)
	int32 pg_count;
	int32 pg_size;
	bool active;
} CVMetadata;


extern CVMetadata* GetCVMetadata(int32 id);
extern int32 GetProcessGroupCount(int32 id);
extern void DecrementProcessGroupCount(int32 id);
extern void IncrementProcessGroupCount(int32 id);
extern bool GetActiveFlag(int32 id);
extern void SetActiveFlag(int32 id, bool flag);

extern CVMetadata* EntryAlloc(int32 key, uint32 pg_size);
extern void WaitForCQProcessStart(int32 id);
extern void WaitForCQProcessEnd(int32 id);
#endif   /* CVMETADATA_H */
