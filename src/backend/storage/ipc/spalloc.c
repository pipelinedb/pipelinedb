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
#include "storage/lwlock.h"
#include "storage/spalloc.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/elog.h"

#define DEBUG 0

/* Memory Blocks */

#if (__WORDSIZE == 8)
typedef uint64_t Header;
#else
typedef uint32_t Header;
#endif

#define IS_ALIGNED(ptr) ((BlockInfo) ptr % __WORDSIZE == 0)
#define ALIGN(size) (((size) + __WORDSIZE - 1) & ~(__WORDSIZE - 1))
#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))
#define BLOCK_SIZE(size) ((size) + sizeof(Header))

typedef struct MemoryBlock
{
	Header header;
	void **prev; /* if not allocated */
	void **next; /* if not allocated */
	/* variable length allocation space */
} MemoryBlock;

#define MIN_BLOCK_SIZE ALIGN(sizeof(MemoryBlock))
#define MIN_ALLOC_SIZE ALIGN(sizeof(void *) * 2)

static Header *
get_header(void *ptr)
{
	return (Header *) ((intptr_t) ptr - sizeof(Header));
}

static Size
get_size(void *ptr)
{
	return *get_header(ptr) >> 1;
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
	*header &= ~1;
}

static void
mark_allocated(void *ptr)
{
	Header *header = get_header(ptr);
	*header |= 1;
}

static bool
is_allocated(void *ptr)
{
	return *get_header(ptr) & 1;
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
	return *((void **) ((intptr_t) ptr + sizeof(void *)));
}

static void
set_prev(void *ptr, void *prev)
{
	*((void **) ((intptr_t) ptr + sizeof(void *))) = prev;
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
	int nblocks;
	int nfree;
	Size mem_size;
	slock_t mutex;
} SPallocState;

static SPallocState *GlobalSPallocState = NULL;

static Size
free_list_size()
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
	printf("  addr: %p, end: %p, prev: %p, next: %p, size: %zu\n", block, get_end(block), get_prev(block), get_next(block), get_size(block));
}

static void
print_allocator()
{
	void *block = GlobalSPallocState->head;
	printf("=== ShmemAllocator ===\n");

	printf("num free blocks %d\n", GlobalSPallocState->nfree);
	printf("total free: %zu\n", free_list_size());
	printf("num all blocks %d\n", GlobalSPallocState->nblocks);
	printf("total resident: %zu\n", GlobalSPallocState->mem_size);
	printf("head: %p\n", GlobalSPallocState->head);
	printf("tail: %p\n", GlobalSPallocState->tail);
	printf("blocks:\n");

	while (block)
	{
		print_block(block);
		block = get_next(block);
	}

	printf("======================\n");
}

static bool
coalesce_blocks(void *ptr1, void *ptr2)
{
	void *tmpptr = MIN(ptr1, ptr2);
	Size new_size;
	Header *header;

	ptr2 = MAX(ptr1, ptr2);
	ptr1 = tmpptr;

	if (get_end(ptr1) != get_header(ptr2))
		return false;

	Assert(get_next(ptr1) == ptr2);
	Assert(!is_allocated(ptr1));
	Assert(!is_allocated(ptr2));

	new_size = get_size(ptr1) + BLOCK_SIZE(get_size(ptr2));
	header = get_header(ptr1);
	*header = new_size << 1;
	set_next(ptr1, get_next(ptr2));

	GlobalSPallocState->nblocks--;
	GlobalSPallocState->nfree--;

	return true;
}

static void
init_block(void *ptr, Size size, bool is_new)
{
	Header *header = get_header(ptr);

	*header = size << 1;

	GlobalSPallocState->nblocks++;
	if (is_new)
		GlobalSPallocState->mem_size += BLOCK_SIZE(size);
}

static void
insert_block(void *ptr)
{
	void *next = NULL;
	void *prev = NULL;

	mark_free(ptr);

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
				prev = get_next(prev);
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

	GlobalSPallocState->nfree++;
}

static void
split_block(void *ptr, Size size)
{
	Size orig_size = get_size(ptr);
	void *new_block = (void *) ((intptr_t) ptr + size + sizeof(Header));
	Header *header = get_header(ptr);

	init_block(new_block, orig_size - size - sizeof(Header), false);
	insert_block(new_block);

	*header = size << 1 | 1;
}

static void *
get_block(Size size)
{
	void *block;
	void *best = NULL;
	int best_size;

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

		GlobalSPallocState->nfree--;
	}

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
InitSPallocState(void)
{
	bool found;

	LWLockAcquire(PipelineMetadataLock, LW_EXCLUSIVE);

	GlobalSPallocState = (SPallocState *) ShmemInitStruct("ShmemFreeList", sizeof(SPallocState) , &found);
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

	SpinLockAcquire(&GlobalSPallocState->mutex);

	block = get_block(size);

	if (block == NULL)
	{
		/*
		 * Don't request fewer than 1k from ShmemAlloc.
		 * The more contiguous memory we have, the better we
		 * can combat fragmentation.
		 */
		Size alloc_size = MAX(size, 1024);
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
		printf("> spalloc %zu\n", size);
		print_allocator();
	}

	SpinLockRelease(&GlobalSPallocState->mutex);

	Assert(is_allocated(block));

	return block;
}

/*
 * spfree
 */
void
spfree(void *addr)
{
	Assert(is_allocated(addr));

	SpinLockAcquire(&GlobalSPallocState->mutex);

	insert_block(addr);
	if (DEBUG)
	{
		printf("> spfree %p\n", addr);
		print_allocator();
	}

	SpinLockRelease(&GlobalSPallocState->mutex);
}
