/*----------------------------------------------------------------------------------
 *
 * mxct.c
 *		Postgres-XC memory context management code for applications.
 *
 * This module is for Postgres-XC application/utility programs.  Sometimes,
 * applications/utilities may need Postgres-XC internal functions which
 * depends upon mcxt.c of gtm or Postgres.
 *
 * This module "virtualize" such module-dependent memory management.
 *
 * This code is for general use, which depends only upon confentional
 * memory management functions.
 *
 * Copyright (c) 2013, Postgres-XC Development Group
 *
 *---------------------------------------------------------------------------------
 */

#include <stdlib.h>
#include <string.h>
#include "gen_alloc.h"

static void *current_cxt;

static void *memCxtAlloc(void *, size_t);
static void *memCxtRealloc(void *, size_t);
static void *memCxtAlloc0(void *, size_t);
static void memCxtFree(void *);
static void *memCxtAllocTop(size_t);
static void *memCxtCurrentContext(void);


static void *memCxtAlloc(void* current, size_t needed)
{
	return(malloc(needed));
}

static void *memCxtRealloc(void *addr, size_t needed)
{
	return(realloc(addr, needed));
}

static void *memCxtAlloc0(void *current, size_t needed)
{
	void *allocated;

	allocated = malloc(needed);
	if (allocated == NULL)
		return(NULL);
	memset(allocated, 0, needed);
	return(allocated);
}

static void memCxtFree(void *addr)
{
	free(addr);
	return;
}

static void *memCxtCurrentContext()
{
	return((void *)&current_cxt);
}

static void *memCxtAllocTop(size_t needed)
{
	return(malloc(needed));
}


Gen_Alloc genAlloc_class = {(void *)memCxtAlloc,
							(void *)memCxtAlloc0,
							(void *)memCxtRealloc,
							(void *)memCxtFree,
							(void *)memCxtCurrentContext,
							(void *)memCxtAllocTop};
