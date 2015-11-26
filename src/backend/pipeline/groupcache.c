/*-------------------------------------------------------------------------
 *
 * groupcache.c
 *	  Cache used by the combiner to cache aggregate groups. This avoids
 *	  on-disk lookups for groups to update.
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/backend/pipeline/groupcache.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "executor/executor.h"
#include "executor/tupletableReceiver.h"
#include "nodes/execnodes.h"
#include "pipeline/groupcache.h"

#define ENTRY_SIZE(tup) (HEAPTUPLESIZE + (tup)->t_len + sizeof(HeapTupleEntry) + sizeof(GroupCacheEntry) + sizeof(LRUEntry))

/* cache entry */
typedef struct GroupCacheEntry
{
	TupleHashEntryData shared;	/* common header for hash table entries */
	HeapTuple tuple;
	struct LRUEntry *lru;
} GroupCacheEntry;

/* node type for the queue of cache accesses used for LRU eviction */
typedef struct LRUEntry
{
	dlist_node node;
	GroupCacheEntry *entry;
} LRUEntry;

/*
 * evict
 *
 * Using an LRU policy, evict enough cache entries to free the given amount of space
 */
static bool
evict(GroupCache *cache, Size len)
{
	dlist_node *node = dlist_tail_node(&cache->lru);
	LRUEntry *entry = (LRUEntry *) dlist_container(LRUEntry, node, node);

	while (node && cache->available < len)
	{
		entry = (LRUEntry *) dlist_container(LRUEntry, node, node);
		dlist_delete(node);

		if (!entry->entry)
			break;

		ExecStoreTuple(entry->entry->tuple, cache->slot, InvalidBuffer, false);
		RemoveTupleHashEntry(cache->htab, cache->slot);

		cache->available += ENTRY_SIZE(entry->entry->tuple);
		node = dlist_tail_node(&cache->lru);

		heap_free_minimal_tuple(entry->entry->shared.firstTuple);
		heap_freetuple(entry->entry->tuple);
		pfree(entry->entry->lru);
	}

	return (cache->available >= len);
}

/*
 * GroupCacheCreate
 *
 * Create an empty GroupCache
 */
GroupCache *
GroupCacheCreate(Size size, int ngroupatts, AttrNumber *groupatts,
		Oid *groupops, TupleTableSlot *slot, MemoryContext context, MemoryContext tmpcontext)
{
	FmgrInfo *eq_funcs;
	FmgrInfo *hash_funcs;
	MemoryContext old = MemoryContextSwitchTo(context);
	GroupCache *result = palloc0(sizeof(GroupCache));

	result->available = size;
	result->maxsize = size;
	result->context = context;
	result->slot = MakeSingleTupleTableSlot(slot->tts_tupleDescriptor);

	dlist_init(&result->lru);

	execTuplesHashPrepare(ngroupatts, groupops, &eq_funcs, &hash_funcs);
	result->htab = BuildTupleHashTable(ngroupatts, groupatts, eq_funcs, hash_funcs, 1000,
			sizeof(GroupCacheEntry), context, tmpcontext);

	MemoryContextSwitchTo(old);

	return result;
}

/*
 * GroupCachePut
 *
 * Add a tuple to the GroupCache, moving its access position to the head of the LRU queue
 */
bool
GroupCachePut(GroupCache *cache, TupleTableSlot *slot)
{
	bool isnew;
	GroupCacheEntry *entry;
	HeapTuple tup = ExecMaterializeSlot(slot);
	MemoryContext old;
	Size needed = ENTRY_SIZE(tup);

	/* don't bother if there isn't possibly enough room */
	if (needed > cache->maxsize)
		return false;

	entry = (GroupCacheEntry *) LookupTupleHashEntry(cache->htab, slot, &isnew);

	old = MemoryContextSwitchTo(cache->context);

	if (!isnew)
	{
		cache->available += ENTRY_SIZE(entry->tuple);
//		 this tuple might still be referenced by input
		heap_freetuple(entry->tuple);
		dlist_delete(&(entry->lru->node));
	}
	else
		entry->lru = palloc0(sizeof(LRUEntry));

	/* evict enough entries to make room for it */
	if (needed > cache->available)
		evict(cache, needed);

	/* copy it into long-lived context */
	entry->tuple = ExecCopySlotTuple(slot);
	entry->lru->entry = entry;

	dlist_push_head(&(cache->lru), &(entry->lru->node));

	MemoryContextSwitchTo(old);

	cache->available -= needed;

	return true;
}

/*
 * GroupCacheGet
 *
 * Retrieve a tuple from the GroupCache, moving its access position to the head of the LRU queue
 * Return NULL if the GroupCache doesn't contain the given tuple
 */
HeapTuple
GroupCacheGet(GroupCache *cache, TupleTableSlot *slot)
{
	GroupCacheEntry *entry = (GroupCacheEntry *) LookupTupleHashEntry(cache->htab, slot, NULL);

	if (entry == NULL)
		return NULL;

	/* move it from its existing location to the head of the queue */
	dlist_delete(&(entry->lru->node));
	dlist_push_head(&(cache->lru), &(entry->lru->node));

	return entry->tuple;
}

/*
 * GroupCacheDelete
 */
void
GroupCacheDelete(GroupCache *cache, TupleTableSlot *slot)
{
	GroupCacheEntry *entry = (GroupCacheEntry *) LookupTupleHashEntry(cache->htab, slot, NULL);

	if (entry == NULL)
		return;

	dlist_delete(&(entry->lru->node));
	RemoveTupleHashEntry(cache->htab, slot);
}
