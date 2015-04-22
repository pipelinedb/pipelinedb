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

typedef struct DSMArray DSMArray;

extern DSMArray *dsm_array_new(Size size);
extern void *dsm_array_get(DSMArray *array, int idx);
extern void dsm_array_set(DSMArray *array, int idx, void *val);
extern void dsm_array_delete(DSMArray *array);

#endif   /* DSM_ARRAY_H */
