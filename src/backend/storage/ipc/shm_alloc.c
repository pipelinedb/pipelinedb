/* Copyright (c) 2013-2015 PipelineDB */
/*-------------------------------------------------------------------------
 *
 * dsm_alloc.c
 *	  shared memory allocator
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/dsm_alloc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "c.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/shm_alloc.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/elog.h"

/* Memory Blocks */

#define IS_ALIGNED(ptr) ((BlockInfo) ptr % __WORDSIZE == 0)
#define ALIGN(size) (((size) + __WORDSIZE - 1) & ~(__WORDSIZE - 1))
#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))
#define BLOCK_SIZE(size) ((size) + sizeof(Header))
#define MAGIC 0x1CEB00DA /* Yeeeeah, ICE BOODA! */
#define MIN_SHMEM_ALLOC_SIZE 1024

typedef struct Header
{
	int32_t magic;
	Size size;
	bool is_allocated;
} Header;

typedef struct MemoryBlock
{
	Header header;
	void **prev; /* if not allocated */
	void **next; /* if not allocated */
	/* variable length allocation space */
} MemoryBlock;

#define MIN_BLOCK_SIZE ALIGN(sizeof(MemoryBlock))
#define MIN_ALLOC_SIZE ALIGN(sizeof(void **) * 2)

static Header *
get_header(void *ptr)
{
	return (Header *) ((intptr_t) ptr - sizeof(Header));
}

static Size
get_size(void *ptr)
{
	return get_header(ptr)->size;
}

static void *
get_end(void *ptr)
{
	return (void *) ((intptr_t) ptr + get_size(ptr));
}

static void
mark_free(void *ptr)
{
	Header *header = get_header(ptr);
	header->is_allocated = false;
}

static void
mark_allocated(void *ptr)
{
	Header *header = get_header(ptr);
	header->is_allocated = true;
}

static void*
get_next(void *ptr)
{
	return *((void **) ptr);
}

static void
set_next(void *ptr, void *next)
{
	*(void **) ptr = next;
}

static void*
get_prev(void *ptr)
{
	return *((void **) ((intptr_t) ptr + sizeof(void **)));
}

static void
set_prev(void *ptr, void *prev)
{
	*((void **) ((intptr_t) ptr + sizeof(void *))) = prev;
}

static bool
is_allocated(void *ptr)
{
	Header *header = get_header(ptr);
	return header->is_allocated && header->magic == MAGIC;
}

/*
 * Shared Memory Allocator
 *
 * This is just a bare bones implementation that performs a few small
 * tricks to minimize fragmentation. But beyond that it's pretty dumb.
 * Concurrency performance will be horrible--it can be improved alot by
 * making locking more fine grained.
 */

typedef struct ShemDynAllocState
{
	void *head;
	void *tail;
	slock_t mutex;
} ShemDynAllocState;

static ShemDynAllocState *GlobalShemDynAllocState = NULL;

static bool
coalesce_blocks(void *ptr1, void *ptr2)
{
	void *tmpptr = MIN(ptr1, ptr2);
	Size new_size;
	void *next;

	ptr2 = MAX(ptr1, ptr2);
	ptr1 = tmpptr;

	if (get_end(ptr1) != get_header(ptr2))
		return false;

	Assert(get_next(ptr1) == ptr2);
	Assert(!is_allocated(ptr1));
	Assert(!is_allocated(ptr2));

	new_size = get_size(ptr1) + BLOCK_SIZE(get_size(ptr2));
	get_header(ptr1)->size = new_size;
	/* Mark ptr2 as no longer an ICE BOODA. */
	get_header(ptr2)->magic = 0;

	next = get_next(ptr2);
	set_next(ptr1, next);

	if (next)
		set_prev(next, ptr1);

	return true;
}

static void
init_block(void *ptr, Size size, bool is_new)
{
	Header *header = get_header(ptr);
	header->size = size;
	header->magic = MAGIC;
}

static void
insert_block(void *ptr)
{
	void *next = NULL;
	void *prev = NULL;

	mark_free(ptr);

	SpinLockAcquire(&GlobalShemDynAllocState->mutex);

	if (!GlobalShemDynAllocState->head)
	{
		GlobalShemDynAllocState->head = ptr;
		GlobalShemDynAllocState->tail = ptr;
	}
	else
	{
		if ((intptr_t) ptr < (intptr_t) GlobalShemDynAllocState->head)
		{
			next = GlobalShemDynAllocState->head;
			GlobalShemDynAllocState->head = ptr;
		}
		else if ((intptr_t) ptr > (intptr_t) GlobalShemDynAllocState->tail)
		{
			prev = GlobalShemDynAllocState->tail;
			GlobalShemDynAllocState->tail = ptr;
		}
		else
		{
			prev = GlobalShemDynAllocState->head;
			while (prev)
			{
				next = get_next(prev);
				if (((intptr_t) ptr > (intptr_t) prev) &&
						((intptr_t) ptr < (intptr_t) next))
					break;
				prev = next;
			}
//			Assert(prev != NULL);
//			Assert(next != NULL);
		}
	}

	set_prev(ptr, prev);
	set_next(ptr, next);

	if (prev)
	{
		set_next(prev, ptr);
		if (coalesce_blocks(prev, ptr))
		{
			if (GlobalShemDynAllocState->tail == ptr)
				GlobalShemDynAllocState->tail = prev;
			ptr = prev;
		}
	}

	if (next)
	{
		set_prev(next, ptr);
		coalesce_blocks(ptr, next);
	}

	SpinLockRelease(&GlobalShemDynAllocState->mutex);
}

static void
split_block(void *ptr, Size size)
{
	Size orig_size = get_size(ptr);
	void *new_block = (void *) ((intptr_t) ptr + size + sizeof(Header));
	Header *header = get_header(ptr);

	init_block(new_block, orig_size - size - sizeof(Header), false);
	insert_block(new_block);

	header->size = size;
	header->is_allocated = true;
}

static void *
get_block(Size size)
{
	void *block;
	void *best = NULL;
	int best_size;

	SpinLockAcquire(&GlobalShemDynAllocState->mutex);

	block = GlobalShemDynAllocState->head;
	while (block)
	{
		Size block_size = get_size(block);
		if (size > block_size)
		{
			block = get_next(block);
			continue;
		}
		if (!best || best_size > block_size)
		{
			best = block;
			best_size = block_size;
		}
		block = get_next(block);
	}

	if (best)
	{
		void *prev;
		void *next;

		mark_allocated(best);
		prev = get_prev(best);
		next = get_next(best);

		if (!prev)
			GlobalShemDynAllocState->head = next;
		else
			set_next(prev, next);
		if (!next)
			GlobalShemDynAllocState->tail = prev;
		else
			set_prev(next, prev);
	}

	SpinLockRelease(&GlobalShemDynAllocState->mutex);

	return best;
}

/*
 * InitShmemDynAllocator
 */
void
InitShmemDynAllocator(void)
{
	bool found;

	LWLockAcquire(PipelineMetadataLock, LW_EXCLUSIVE);

	GlobalShemDynAllocState = (ShemDynAllocState *) ShmemInitStruct("SPallocState", sizeof(ShemDynAllocState) , &found);
	if (!found)
	{
		GlobalShemDynAllocState->head = NULL;
		GlobalShemDynAllocState->tail = NULL;
		SpinLockInit(&GlobalShemDynAllocState->mutex);
	}

	LWLockRelease(PipelineMetadataLock);
}

/*
 * ShmemDynAlloc
 */
void *
ShmemDynAlloc(Size size)
{
	void *block;
	Size padded_size;

	size = MAX(ALIGN(size), MIN_ALLOC_SIZE);
	for (padded_size = 1; padded_size < size && padded_size <= 1024; padded_size *= 2);
	size = MAX(size, padded_size);

	block = get_block(size);

	if (block == NULL)
	{
		/*
		 * Don't request fewer than 1k from ShmemAlloc.
		 * The more contiguous memory we have, the better we
		 * can combat fragmentation.
		 */
		Size alloc_size = MAX(size, MIN_SHMEM_ALLOC_SIZE);
		block = ShmemAlloc(BLOCK_SIZE(alloc_size));
		memset(block, 0, BLOCK_SIZE(alloc_size));
		block = (void *) ((intptr_t) block + sizeof(Header));
		init_block(block, alloc_size, true);
		mark_allocated(block);
	}

	if (get_size(block) - size >= MIN_BLOCK_SIZE)
		split_block(block, size);

	Assert(is_allocated(block));

	return block;
}

void *
ShmemDynAlloc0(Size size)
{
	char *addr = ShmemDynAlloc(size);
	MemSet(addr, 0, get_size(addr));
	return addr;
}

/*
 * ShmemDynFree
 */
void
ShmemDynFree(void *addr)
{
	if (!ShmemDynAddrIsValid(addr))
		elog(ERROR, "ShmemDynFree: invalid/double freeing (%p)", addr);

	insert_block(addr);
}

bool
ShmemDynAddrIsValid(void *addr)
{
	return ShmemAddrIsValid(addr) && is_allocated(addr);
}
