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
#include "storage/dsm_alloc.h"
#include "storage/dsm_array.h"
#include "storage/spin.h"
#include "utils/elog.h"

#define DEBUG 0
#define ARRAY_SEGMENT_SIZE 32

typedef struct DSMArraySegment {
	struct DSMArraySegment *next;
	uint8_t bytes[1];
} DSMArraySegment;

struct DSMArray {
	DSMArraySegment *tail;
	int length;
	Size size;
	slock_t mutex;
	DSMArraySegment segment;
};

DSMArray *dsm_array_new(Size size)
{
	DSMArray *array = dsm_alloc0(sizeof(DSMArray) + (size * ARRAY_SEGMENT_SIZE));
	array->length = ARRAY_SEGMENT_SIZE;
	array->size = size;
	SpinLockInit(&array->mutex);
	return array;
}

static void *get_idx_addr(DSMArray *array, int idx)
{
	int offset;
	int seg_num;
	DSMArraySegment *segment;

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

void dsm_array_set(DSMArray *array, int idx, void *value)
{
	void *addr;

	if (idx >= array->length)
	{
		SpinLockAcquire(&array->mutex);

		while (idx >= array->length)
		{
			DSMArraySegment *tail;
			DSMArraySegment *new = dsm_alloc0(sizeof(DSMArraySegment) * (array->size * ARRAY_SEGMENT_SIZE));

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

void *dsm_array_get(DSMArray *array, int idx)
{
	void *addr = get_idx_addr(array, idx);
	return addr ? addr : NULL;
}

void dsm_array_delete(DSMArray *array)
{
	DSMArraySegment *segment;

	if (!array)
		return;

	segment = array->segment.next;

	while (segment)
	{
		DSMArraySegment *next = segment->next;
		dsm_free(segment);
		segment = next;
	}

	dsm_free(array);
}
