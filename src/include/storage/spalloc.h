/*-------------------------------------------------------------------------
 *
 * spalloc.h
 *	  shared memory allocator
 *
 * src/include/storage/spalloc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SPALLOC_H
#define SPALLOC_H

extern void InitSPalloc(void);

extern void *spalloc(Size size);
extern void spfree(void *addr);

#endif   /* SPALLOC_H */
