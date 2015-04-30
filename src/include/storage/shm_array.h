/*-------------------------------------------------------------------------
 *
 * dsm_array.h
 *	  shared memory dynamic array
 *
 * src/include/storage/dsm_array.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DSM_ARRAY_H
#define DSM_ARRAY_H

typedef struct ShmemArray ShmemArray;

extern ShmemArray *ShmemArrayInit(Size size);
extern void *ShmemArrayGet(ShmemArray *array, int idx);
extern void ShmemArraySet(ShmemArray *array, int idx, void *val);
extern void ShmemArrayDestroy(ShmemArray *array);

#endif   /* DSM_ARRAY_H */
