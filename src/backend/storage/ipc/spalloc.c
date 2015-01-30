/* Copyright (c) 2013-2015 PipelineDB */
/*-------------------------------------------------------------------------
 *
 * spalloc.c
 *	  shared memory allocator
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/spalloc.c
 *
 *-------------------------------------------------------------------------
 */
#include "c.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/spalloc.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/elog.h"

#define DEBUG 0

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

typedef struct SPallocState
{
	void *head;
	void *tail;
	slock_t mutex;
	/* DEBUG metadata */
	int nblocks;
	int nfree;
	Size mem_size;
	Size total_alloc;
	Size total_free;
} SPallocState;

static SPallocState *GlobalSPallocState = NULL;

static Size
free_blocks_size()
{
	void *block;
	Size size = 0;

	block = GlobalSPallocState->head;
	while (block)
	{
		size += BLOCK_SIZE(get_size(block));
		block = get_next(block);
	}

	return size;
}

static void
print_block(void *block)
{
	if (!block)
		return;
	elog(LOG, "  addr: %p, end: %p, prev: %p, next: %p, size: %zu", block, get_end(block), get_prev(block), get_next(block), get_size(block));
}

static void
print_allocator()
{
	void *block = GlobalSPallocState->head;
	elog(LOG, "=== ShmemAllocator ===\n");

	elog(LOG, "num free blocks %d", GlobalSPallocState->nfree);
	elog(LOG, "total free: %zu", free_blocks_size());
	elog(LOG, "num all blocks %d", GlobalSPallocState->nblocks);
	elog(LOG, "total resident: %zu", GlobalSPallocState->mem_size);
	elog(LOG, "head: %p", GlobalSPallocState->head);
	elog(LOG, "tail: %p", GlobalSPallocState->tail);
	elog(LOG, "blocks:");

	while (block)
	{
		print_block(block);
		block = get_next(block);
	}

	elog(LOG, "======================");
}

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


	if (DEBUG)
		GlobalSPallocState->nblocks--;
		GlobalSPallocState->nfree--;

	return true;
}

static void
init_block(void *ptr, Size size, bool is_new)
{
	Header *header = get_header(ptr);
	header->size = size;
	header->magic = MAGIC;

	if (DEBUG)
	{
		SpinLockAcquire(&GlobalSPallocState->mutex);
		GlobalSPallocState->nblocks++;
		if (is_new)
			GlobalSPallocState->mem_size += BLOCK_SIZE(size);
		SpinLockRelease(&GlobalSPallocState->mutex);
	}
}

static void
insert_block(void *ptr)
{
	void *next = NULL;
	void *prev = NULL;

	mark_free(ptr);

	SpinLockAcquire(&GlobalSPallocState->mutex);

	if (!GlobalSPallocState->head)
	{
		GlobalSPallocState->head = ptr;
		GlobalSPallocState->tail = ptr;
	}
	else
	{
		if ((intptr_t) ptr < (intptr_t) GlobalSPallocState->head)
		{
			next = GlobalSPallocState->head;
			GlobalSPallocState->head = ptr;
		}
		else if ((intptr_t) ptr > (intptr_t) GlobalSPallocState->tail)
		{
			prev = GlobalSPallocState->tail;
			GlobalSPallocState->tail = ptr;
		}
		else
		{
			prev = GlobalSPallocState->head;
			while (prev)
			{
				next = get_next(prev);
				if (((intptr_t) ptr > (intptr_t) prev) &&
						((intptr_t) ptr < (intptr_t) next))
					break;
				prev = next;
			}
			Assert(prev != NULL);
			Assert(next != NULL);
		}
	}

	set_prev(ptr, prev);
	set_next(ptr, next);

	if (prev)
	{
		set_next(prev, ptr);
		if (coalesce_blocks(prev, ptr))
		{
			if (GlobalSPallocState->tail == ptr)
				GlobalSPallocState->tail = prev;
			ptr = prev;
		}
	}

	if (next)
	{
		set_prev(next, ptr);
		coalesce_blocks(ptr, next);
	}

	if (DEBUG)
		GlobalSPallocState->nfree++;

	SpinLockRelease(&GlobalSPallocState->mutex);
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

	SpinLockAcquire(&GlobalSPallocState->mutex);

	block = GlobalSPallocState->head;
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
			GlobalSPallocState->head = next;
		else
			set_next(prev, next);
		if (!next)
			GlobalSPallocState->tail = prev;
		else
			set_prev(next, prev);

		if (DEBUG)
			GlobalSPallocState->nfree--;
	}

	SpinLockRelease(&GlobalSPallocState->mutex);

	return best;
}

static void
test_allocator()
{
	void *a;
	void *b;

	print_allocator();
	a = spalloc(23);
	b = spalloc(500);
	spfree(a);
	spfree(b);
	a = spalloc(250);
	b = spalloc(120);
	spfree(a);
	spfree(b);
	a = spalloc(sizeof(void *) * 10);
	b = spalloc(sizeof(Header));
	spfree(a);
	spfree(b);
}

/*
 * InitSPallocState
 */
void
InitSPalloc(void)
{
	bool found;

	LWLockAcquire(PipelineMetadataLock, LW_EXCLUSIVE);

	GlobalSPallocState = (SPallocState *) ShmemInitStruct("SPallocState", sizeof(SPallocState) , &found);
	if (!found)
	{
		GlobalSPallocState->head = NULL;
		GlobalSPallocState->tail = NULL;
		GlobalSPallocState->nblocks = 0;
		GlobalSPallocState->nfree = 0;
		SpinLockInit(&GlobalSPallocState->mutex);
	}

	LWLockRelease(PipelineMetadataLock);

	if (DEBUG)
		test_allocator();
}

/*
 * spalloc
 */
void *
spalloc(Size size)
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

	if (DEBUG)
	{
		SpinLockAcquire(&GlobalSPallocState->mutex);

		GlobalSPallocState->total_alloc += size;
		printf("> spalloc %zu\n", size);
		print_allocator();

		SpinLockRelease(&GlobalSPallocState->mutex);
	}

	Assert(is_allocated(block));

	return block;
}

void *
spalloc0(Size size)
{
	char *addr = spalloc(size);
	memset(addr, 0, size);
	return addr;
}

/*
 * spfree
 */
void
spfree(void *addr)
{
	Size size;

	if (!is_allocated(addr))
		elog(ERROR, "spfree: invalid/double freeing (%p)", addr);

	size = get_size(addr);
	insert_block(addr);

	if (DEBUG)
	{
		SpinLockAcquire(&GlobalSPallocState->mutex);

		printf("> spfree %p\n", addr);
		print_allocator();
		GlobalSPallocState->total_free += size;

		SpinLockRelease(&GlobalSPallocState->mutex);
	}
}

bool
IsValidSPallocMemory(void *addr)
{
	return is_allocated(addr);
}
