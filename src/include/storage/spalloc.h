/* Copyright (c) 2013-2015 PipelineDB */
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
extern void *spalloc0(Size size);
extern void spfree(void *addr);
extern bool IsValidSPallocMemory(void *);

#endif   /* SPALLOC_H */
