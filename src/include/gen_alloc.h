/*-------------------------------------------------------------------------
 *
 * gen_alloc.h
 *
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/gen_alloc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GEN_ALLOC_H
#define GEN_ALLOC_H

/*
 * Common memory allocation binary interface both for Postgres and GTM processes.
 * Especially needed by gtm_serialize.c and gtm_serialize_debug.c
 */

typedef struct Gen_Alloc
{
	void * (* alloc) (void *, size_t);
	void * (* alloc0) (void *, size_t);
	void * (* realloc) (void *, size_t);
	void   (* free) (void *);
	void * (* current_memcontext) (void);
	void * (* allocTop) (size_t);
} Gen_Alloc;

extern Gen_Alloc genAlloc_class;

#define genAlloc(x)			genAlloc_class.alloc(genAlloc_class.current_memcontext(), x)
#define genRealloc(x, y)	genAlloc_class.realloc(x, y)
#define genFree(x)			genAlloc_class.free(x)
#define genAlloc0(x)		genAlloc_class.alloc0(genAlloc_class.current_memcontext(), x)
#define genAllocTop(x)		genAlloc_class.allocTop(x)

#endif /* GEN_ALLOC_H */
