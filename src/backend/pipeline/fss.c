/*-------------------------------------------------------------------------
 *
 * fss.c
 *	  Filtered Space Saving implementation
 *
 *	  http://www.l2f.inesc-id.pt/~fmmb/wiki/uploads/Work/misnis.ref0a.pdf
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/backend/pipeline/fss.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>
#include "pipeline/fss.h"
#include "pipeline/miscutils.h"
#include "utils/elog.h"
#include "utils/palloc.h"

#define DEFAULT_SS_M_FACTOR 5.0
#define DEFAULT_M_FACTOR (DEFAULT_SS_M_FACTOR / 2)
#define DEFAULT_H_FACTOR (DEFAULT_M_FACTOR * 6)
#define HASH_SIZE (sizeof(uint64_t))

#define MURMUR_SEED 0x99496f1ddc863e6fL

FSS *
FSSCreateWithMAndH(uint64_t k, TypeCacheEntry *typ, uint64_t m, uint64_t h)
{
	Size sz = sizeof(FSS) + (sizeof(Counter) * h) + (sizeof(MonitoredElement) * m);
	char *pos;
	FSS *fss;

	if (k > m)
		elog(ERROR, "maximum value of k exceeded");

	/* TODO(usmanm): Check bounds for k, m, h */

	/* TODO(usmanm): Add support for ref types */
	if (!typ->typbyval)
		elog(ERROR, "fss doesn't support types by reference");

	/* XXX(usmanm): This will fail for 32-bit machines, but who cares? */
	Assert(sizeof(Datum) == HASH_SIZE);

	/* We only store datums if they're passed by value. */
	if (typ->typbyval)
		sz += sizeof(Datum) * k;

	pos = palloc0(sz);

	fss = (FSS *) pos;
	pos += sizeof(FSS);
	fss->bitmap_counter = (Counter *) pos;
	pos += sizeof(Counter) * h;
	fss->monitored_elements = (MonitoredElement *) pos;

	if (!typ->typbyval)
	{
		pos += sizeof(MonitoredElement) * m;
		fss->top_k = (Datum *) pos;
	}
	else
		fss->top_k = NULL;

	fss->packed = true;
	fss->h = h;
	fss->m = m;
	fss->k = k;
	fss->typ.typoid = typ->type_id;
	fss->typ.typlen = typ->typlen;
	fss->typ.typbyval = typ->typbyval;

	SET_VARSIZE(fss, FSSSize(fss));

	return fss;
}

FSS *
FSSCreate(uint64_t k, TypeCacheEntry *typ)
{
	return FSSCreateWithMAndH(k, typ, k * DEFAULT_M_FACTOR, k * DEFAULT_H_FACTOR);
}

void
FSSDestroy(FSS *fss)
{
	FSSCompress(fss);
	pfree(fss);
}

FSS *
FSSCopy(FSS *fss)
{
	Size size = FSSSize(fss);
	char *new = palloc(size);
	memcpy(new, (char *) fss, size);
	return (FSS *) new;
}

static int
element_cmp(const void *a, const void *b)
{
	MonitoredElement *m1 = (MonitoredElement *) a;
	MonitoredElement *m2 = (MonitoredElement *) b;

	if (!m1->set)
		return 1;
	if (!m2->set)
		return -1;

	/* We sort by (-frequency, error) */
	if (m1->frequency > m2->frequency)
		return -1;
	if (m2->frequency > m1->frequency)
		return 1;

	if (m1->error > m2->error)
		return 1;
	if (m2->error > m1->error)
		return -1;

	return 0;
}

void
FSSIncrement(FSS *fss, Datum datum)
{
	StringInfo buf;
	uint64_t hash;
	Counter *counter;
	bool compare_hash;
	Datum elt;
	int free_slot = -1;
	MonitoredElement *m_elt;
	int slot;
	bool needs_sort = true;
	int counter_idx;
	TypeCacheEntry typ;

	typ.type_id = fss->typ.typoid;
	typ.typbyval = fss->typ.typbyval;
	typ.typlen = fss->typ.typlen;

	buf = makeStringInfo();
	DatumToBytes(datum, &typ, buf);
	hash = MurmurHash3_64(buf->data, buf->len, MURMUR_SEED);
	pfree(buf->data);
	pfree(buf);

	counter_idx = hash % fss->h;
	counter = &fss->bitmap_counter[counter_idx];
	compare_hash = FSS_STORES_DATUMS(fss);

	if (compare_hash)
		elt = (Datum) hash;
	else
		elt = datum;

	if (counter->count > 0)
	{
		int i;
		bool found = false;

		for (i = 0; i < fss->m; i++)
		{
			m_elt = &fss->monitored_elements[i];

			if (!m_elt->set)
			{
				free_slot = i;
				break;
			}

			if (m_elt->value == elt)
			{
				found = true;
				break;
			}
		}

		/* We found datum, so its monitored */
		if (found)
		{
			m_elt->frequency++;
			slot = i;
			goto done;
		}
	}
	else if (!fss->monitored_elements[fss->m - 1].set)
	{
		int i;

		/* Find the first free slot */
		for (i = 0; i < fss->m; i++)
		{
			if (!fss->monitored_elements[i].set)
			{
				free_slot = i;
				break;
			}
		}
	}


	/* This is only executed if datum is not monitored */
	if (counter->alpha + 1 >= fss->monitored_elements[fss->m - 1].frequency)
	{
		/* Need to evict an element? */
		if (free_slot == -1)
		{
			Counter *counter;

			/*
			 * We always evict the last element because the monitored element array is
			 * sorted by (-frequency, error).
			 */
			slot = fss->m - 1;
			m_elt = &fss->monitored_elements[slot];
			counter = &fss->bitmap_counter[m_elt->counter];
			counter->count--;
			counter->alpha = m_elt->frequency;

		}
		else
		{
			slot = free_slot;
			m_elt = &fss->monitored_elements[slot];
		}

		m_elt->value = elt;
		m_elt->frequency = counter->alpha + 1;
		m_elt->error = counter->alpha;
		m_elt->set = true;
		m_elt->counter = counter_idx;
		counter->count++;
	}
	else
	{
		counter->alpha++;
		needs_sort = false;
	}

done:
	if (needs_sort)
	{
		Assert(m_elt);

		/* First slot updated? Don't need to sort. */
		if (slot == 0)
			needs_sort = false;
		else
		{
			MonitoredElement *tmp = &fss->monitored_elements[slot - 1];

			/*
			 * Is the slot before us still good? This should be enough to check because we only
			 * ever increase the frequency or swap the last element on the monitored element array
			 * when a new frequency is entered
			 */
			if (element_cmp((void *) tmp, (void *) m_elt) <= 0)
				needs_sort = false;
		}

		if (needs_sort)
			qsort(fss->monitored_elements, fss->m, sizeof(MonitoredElement), element_cmp);
	}

	fss->count++;
}

FSS *
FSSMerge(FSS *fss, FSS *incoming)
{
	int i, j, k;
	MonitoredElement *tmp;

	Assert(fss->h == incoming->h);
	Assert(fss->m == incoming->m);

	tmp = palloc0(sizeof(MonitoredElement) * 2 * fss->m);
	memcpy(tmp, fss->monitored_elements, sizeof(MonitoredElement) * fss->m);

	for (i = 0; i < fss->h; i++)
	{
		fss->bitmap_counter[i].alpha += incoming->bitmap_counter[i].alpha;
		/* Reset counts, we'll reset them when the monitored elements lists are merged */
		fss->bitmap_counter[i].count = 0;
	}

	k = fss->m;
	for (i = 0; i < fss->m; i++)
	{
		bool found = false;
		MonitoredElement *elt1 = &incoming->monitored_elements[i];

		if (!elt1->set)
			break;

		for (j = 0; j < fss->m; j++)
		{
			MonitoredElement *elt2 = &tmp[j];

			if (elt1->value == elt2->value)
			{
				elt2->frequency += elt1->frequency;
				elt2->error += elt1->error;
				found = true;
				break;
			}
		}

		if (!found)
			tmp[k++] = *elt1;
	}

	qsort(tmp, k, sizeof(MonitoredElement), element_cmp);
	memcpy(fss->monitored_elements, tmp, sizeof(MonitoredElement) * fss->m);

	pfree(tmp);

	for (i = 0; i < fss->m; i++)
	{
		MonitoredElement *elt = &fss->monitored_elements[i];

		if (!elt->set)
			break;

		fss->bitmap_counter[elt->counter].count++;
	}

	fss->count += incoming->count;

	return fss;
}

Datum *
FSSTopK(FSS *fss, uint16_t k, uint16_t *found)
{
	int i;
	Datum *datums;

	if (k > fss->k)
		elog(ERROR, "maximum value for k exceeded");

	datums = palloc(sizeof(Datum) * k);

	for (i = 0; i < k; i++)
	{
		MonitoredElement *elt = &fss->monitored_elements[i];

		if (!elt->set)
			break;

		datums[i] = elt->value;
	}

	if (found)
		*found = i;

	return datums;
}

uint32_t *
FSSTopKCounts(FSS *fss, uint16_t k, uint16_t *found)
{
	int i;
	uint32_t *counts;

	if (k > fss->k)
		elog(ERROR, "maximum value for k exceeded");

	counts = palloc(sizeof(uint32_t) * k);

	for (i = 0; i < k; i++)
	{
		MonitoredElement *elt = &fss->monitored_elements[i];

		if (!elt->set)
			break;

		counts[i] = elt->frequency;
	}

	if (found)
		*found = i;

	return counts;
}

uint64_t
FSSTotal(FSS *fss)
{
	return fss->count;
}

Size
FSSSize(FSS *fss)
{
	Size sz = sizeof(FSS) + (sizeof(Counter) * fss->h) + (sizeof(MonitoredElement) * fss->m);

	if (FSS_STORES_DATUMS(fss))
		sz += sizeof(Datum) * fss->k;

	return sz;
}

FSS *
FSSCompress(FSS *fss)
{
	if (!FSS_STORES_DATUMS(fss))
		return fss;

	return NULL;
}

void
FSSPrint(FSS *fss)
{
	StringInfo buf = makeStringInfo();
	int i;

	appendStringInfo(buf, "FSS (%ld) \n", fss->count);

	appendStringInfo(buf, "BITMAP COUNTER:\n");

	for (i = 0; i < fss->h; i++)
	{
		Counter *counter = &fss->bitmap_counter[i];
		if (i && i % 4 == 0)
			appendStringInfoChar(buf, '\n');
		appendStringInfo(buf, "| (%d, %d) |", counter->alpha, counter->count);
	}

	appendStringInfo(buf, "\n----------\nMONITORED LIST:\n");

	for (i = 0; i < fss->m; i++)
	{
		MonitoredElement *elt = &fss->monitored_elements[i];
		if (elt->set)
			appendStringInfo(buf, "| (%ld, %d, %d) |", elt->value, elt->frequency, elt->error);
		else
			appendStringInfo(buf, "| (-, -, -) |");

		appendStringInfoChar(buf, '\n');
	}

	elog(LOG, "%s", buf->data);
	pfree(buf->data);
	pfree(buf);
}
