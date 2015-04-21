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

typedef struct DynArray DynArray;

extern DynArray *dsm_array_new(Size size);
extern void *dsm_array_get(DynArray *array, int idx);
extern void dsm_array_set(DynArray *array, int idx, void *val);
extern void dsm_array_delete(DynArray *array);

#endif   /* DSM_ARRAY_H */
