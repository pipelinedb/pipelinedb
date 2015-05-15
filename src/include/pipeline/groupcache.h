/* Copyright (c) 2013-2015 PipelineDB */
/*-------------------------------------------------------------------------
 *
 * groupcache.h
 *	  Interface for aggregate group cache used by the combiner
 *
 * src/include/pipeline/groupcache.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_GROUPCACHE_H
#define PIPELINE_GROUPCACHE_H

#include "lib/ilist.h"

typedef struct GroupCache {
	/* context to store cached entries in */
	MemoryContext context;
	/* underlying hashtable, keyed by group */
	TupleHashTable htab;
	/* slot to hash tuples with */
	TupleTableSlot *slot;
	/* available memory left to consume before evicting existing entries */
	Size available;
	/* maximum size of this cache */
	Size maxsize;
	/*
	 * Queue of pointers to cache elements, ordered in descending order by last access time.
	 * We use a a doubly linked list for constant-time deletions.
	 */
	dlist_head lru;
} GroupCache;

extern GroupCache *GroupCacheCreate(Size size, int ngroupatts, AttrNumber *groupatts,
		Oid *groupops, TupleTableSlot *slot, MemoryContext context, MemoryContext tmpcontext);
extern bool GroupCachePut(GroupCache *cache, TupleTableSlot *slot);
extern HeapTuple GroupCacheGet(GroupCache *cache, TupleTableSlot *slot);
extern void test_groupcache(void);

#endif
