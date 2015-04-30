/*-------------------------------------------------------------------------
 *
 * dsm_array.c
 *	  shared memory dynamic array
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/dsm_array.c
 *
 *-------------------------------------------------------------------------
 */
#include "c.h"
#include "storage/shm_alloc.h"
#include "storage/shm_array.h"
#include "storage/spin.h"
#include "utils/elog.h"

#define ARRAY_SEGMENT_SIZE 32

typedef struct DSMArraySegment {
	struct DSMArraySegment *next;
	uint8_t bytes[1];
} ShmemArraySegment;

struct ShmemArray {
	ShmemArraySegment *tail;
	int length;
	Size size;
	slock_t mutex;
	ShmemArraySegment segment;
};

ShmemArray *ShmemArrayInit(Size size)
{
	ShmemArray *array = ShmemDynAlloc0(sizeof(ShmemArray) + (size * ARRAY_SEGMENT_SIZE));
	array->length = ARRAY_SEGMENT_SIZE;
	array->size = size;
	SpinLockInit(&array->mutex);
	return array;
}

static void *get_idx_addr(ShmemArray *array, int idx)
{
	int offset;
	int seg_num;
	ShmemArraySegment *segment;

	if (idx >= array->length)
		return NULL;

	segment = &array->segment;
	offset = idx % ARRAY_SEGMENT_SIZE;
	seg_num = idx / ARRAY_SEGMENT_SIZE;

	while (seg_num--)
		segment = segment->next;

	offset = offset * array->size;
	return (void *) &segment->bytes[offset];
}

void ShmemArraySet(ShmemArray *array, int idx, void *value)
{
	void *addr;

	if (idx >= array->length)
	{
		SpinLockAcquire(&array->mutex);

		while (idx >= array->length)
		{
			ShmemArraySegment *tail;
			ShmemArraySegment *new = ShmemDynAlloc0(sizeof(ShmemArraySegment) * (array->size * ARRAY_SEGMENT_SIZE));

			if (array->tail == NULL)
				tail = &array->segment;
			else
				tail = array->tail;

			tail->next = new;
			array->tail = new;
			array->length += ARRAY_SEGMENT_SIZE;
		}

		SpinLockRelease(&array->mutex);
	}

	addr = get_idx_addr(array, idx);

	if (value)
		memcpy(addr, value, array->size);
	else
		MemSet(addr, 0, array->size);
}

void *ShmemArrayGet(ShmemArray *array, int idx)
{
	void *addr = get_idx_addr(array, idx);
	return addr ? addr : NULL;
}

void ShmemArrayDelete(ShmemArray *array)
{
	ShmemArraySegment *segment;

	if (!array)
		return;

	segment = array->segment.next;

	while (segment)
	{
		ShmemArraySegment *next = segment->next;
		ShmemDynFree(segment);
		segment = next;
	}

	ShmemDynFree(array);
}
