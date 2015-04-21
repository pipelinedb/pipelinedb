/*-------------------------------------------------------------------------
 *
 * dsm_alloc.h
 *	  shared memory allocator
 *
 * src/include/storage/dsm_alloc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DSM_ALLOC_H
#define DSM_ALLOC_H

extern void InitDSMAlloc(void);

extern void *dsm_alloc(Size size);
extern void *dsm_alloc0(Size size);
extern void dsm_free(void *addr);
extern bool dsm_valid_ptr(void *);

#endif   /* DSM_ALLOC_H */
