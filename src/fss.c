/*-------------------------------------------------------------------------
 *
 * fss.c
 *	  Filtered Space Saving implementation
 *
 *	  http://www.l2f.inesc-id.pt/~fmmb/wiki/uploads/Work/misnis.ref0a.pdf
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>
#include "fss.h"
#include "miscutils.h"
#include "utils/datum.h"
#include "utils/elog.h"
#include "utils/palloc.h"

#define DEFAULT_SS_M_FACTOR 6.0
#define DEFAULT_M_FACTOR (DEFAULT_SS_M_FACTOR / 2)
#define DEFAULT_H_FACTOR (DEFAULT_M_FACTOR * 6)

#define MURMUR_SEED 0x02cd1b4c451c1fb8L

/*
 * FSSFromBytes
 */
FSS *
FSSFromBytes(struct varlena *bytes)
{
	char *pos;
	FSS *fss = (FSS *) bytes;

	/* Fix pointers */
	pos = (char *) fss;
	pos += sizeof(FSS);
	fss->bitmap_counter = (Counter *) pos;
	pos += sizeof(Counter) * fss->h;
	fss->monitored_elements = (MonitoredElement *) pos;

	if (FSS_STORES_DATUMS(fss))
	{
		pos += sizeof(MonitoredElement) * fss->m;
		fss->top_k = (ArrayType *) pos;
	}
	else
		fss->top_k = NULL;

	return fss;
}

/*
 * FSSCreateWithMAndH
 */
FSS *
FSSCreateWithMAndH(uint16_t k, TypeCacheEntry *typ, uint16_t m, uint16_t h)
{
	Size sz = sizeof(FSS) + (sizeof(Counter) * h) + (sizeof(MonitoredElement) * m) + sizeof(ArrayType);
	char *pos;
	FSS *fss;

	Assert (k <= m);

	pos = palloc0(sz);

	fss = (FSS *) pos;
	pos += sizeof(FSS);
	fss->bitmap_counter = (Counter *) pos;
	pos += sizeof(Counter) * h;
	fss->monitored_elements = (MonitoredElement *) pos;

	if (!typ->typbyval)
	{
		int i;

		for (i = 0; i < m; i++)
			fss->monitored_elements[i].varlen_index = (Datum ) i;

		pos += sizeof(MonitoredElement) * m;
		fss->top_k = (ArrayType *) pos;

		SET_VARSIZE(fss->top_k, sizeof(ArrayType));
		fss->top_k->ndim = 0;
		fss->top_k->dataoffset = 0;
		fss->top_k->elemtype = typ->type_id;
	}
	else
		fss->top_k = NULL;

	fss->h = h;
	fss->m = m;
	fss->k = k;
	fss->typ.typoid = typ->type_id;
	fss->typ.typlen = typ->typlen;
	fss->typ.typbyval = typ->typbyval;
	fss->typ.typalign = typ->typalign;
	fss->typ.typtype = typ->typtype;

	SET_VARSIZE(fss, FSSSize(fss));

	return fss;
}

/*
 * FSSCreate
 */
FSS *
FSSCreate(uint64_t k, TypeCacheEntry *typ)
{
	return FSSCreateWithMAndH(k, typ, k * DEFAULT_M_FACTOR, k * DEFAULT_H_FACTOR);
}

/*
 * FSSDestroy
 */
void
FSSDestroy(FSS *fss)
{
	FSSCompress(fss);
	pfree(fss);
}

/*
 * We use our own reallocation function instead of just using repalloc because repalloc frees the old pointer.
 * This is problematic in the context of using FSSs in aggregates (which is their primary use case) because nodeAgg
 * automatically frees the old transition value when the pointer value changes within the transition function, which
 * would lead to a double free error if we were to free the old pointer ourselves via repalloc.
 */
static FSS *
fss_realloc(FSS *fss, Size size)
{
	FSS *result = palloc0(size);

	memcpy(result, fss, FSSSize(fss));

	return result;
}

/*
 * fss_resize_topk
 *
 * Resize the given FSS so that it can contiguously store the given top k array
 */
static FSS *
fss_resize_topk(FSS *fss, ArrayType *top_k)
{
	Size delta = Abs((int) ARR_SIZE(top_k) - (int) ARR_SIZE(fss->top_k));
	FSS *result = fss;

	if (delta != 0)
	{
		result = fss_realloc(result, FSSSize(fss) + delta);
		result = FSSFromBytes((struct varlena *) result);
	}
	memcpy(result->top_k, top_k, ARR_SIZE(top_k));
	SET_VARSIZE(result, FSSSize(result));

	return result;
}

/*
 * get_monitored_value
 *
 * Get the monitored value, which could be either byval or byref. If it's byref,
 * we need to pull the actual value out of FSS's varlena array.
 */
static Datum
get_monitored_value(FSS *fss, MonitoredElement *element, bool *isnull)
{
	Datum result = element->value;
	int index;

	*isnull = IS_NULL(element);

	if (fss->typ.typbyval)
		return result;

	Assert(fss->top_k);
	Assert(element->varlen_index < fss->m);
	Assert(element->varlen_index < ArrayGetNItems(ARR_NDIM(fss->top_k), ARR_DIMS(fss->top_k)));

	index = (int) element->varlen_index;
	result = array_ref(fss->top_k, 1, &index,
			-1, fss->typ.typlen, fss->typ.typbyval, fss->typ.typalign, isnull);

	return result;
}

/*
 * FSSCopy
 */
FSS *
FSSCopy(FSS *fss)
{
	Size size = FSSSize(fss);
	char *new = palloc(size);
	memcpy(new, (char *) fss, size);

	return (FSS *) new;
}

/*
 * MonitoredElementComparator
 */
int
MonitoredElementComparator(const void *a, const void *b)
{
	MonitoredElement *m1 = (MonitoredElement *) a;
	MonitoredElement *m2 = (MonitoredElement *) b;

	/*
	 * We should never be sorting any unset elements
	 */
	Assert(IS_SET(m1));
	Assert(IS_SET(m2));

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

/*
 * FSSIncrement
 */
FSS *
FSSIncrement(FSS *fss, Datum datum, bool isnull)
{
	return FSSIncrementWeighted(fss, datum, isnull, 1);
}

/*
 * set_varlena
 *
 * Store the monitored varlena value in the given FSS's varlena array
 */
static FSS *
set_varlena(FSS *fss, MonitoredElement *element, Datum value, bool isnull)
{
	ArrayType *result;
	int index;

	Assert(fss->top_k);
	Assert(element->varlen_index < fss->m);

	/* For byref types, the value is actually an index into the varlena array */
	index = element->varlen_index;
	result = array_set(fss->top_k, 1, &index, value,
			isnull, -1, fss->typ.typlen, fss->typ.typbyval, fss->typ.typalign);

	fss = fss_resize_topk(fss, result);

	return fss;
}

/*
 * HashDatum
 *
 * Hash a datum using the given FSS's type information
 */
uint64_t
HashDatum(FSS* fss, Datum d)
{
	uint64_t h;
	StringInfoData buf;
	TypeCacheEntry typ;

	typ.type_id = fss->typ.typoid;
	typ.typbyval = fss->typ.typbyval;
	typ.typlen = fss->typ.typlen;
	typ.typtype = fss->typ.typtype;

	initStringInfo(&buf);
	DatumToBytes(d, &typ, &buf);
	h = MurmurHash3_64(buf.data, buf.len, MURMUR_SEED);
	pfree(buf.data);

	return h;
}

/*
 * FSSIncrementWeighted
 */
FSS *
FSSIncrementWeighted(FSS *fss, Datum incoming, bool incoming_null, uint64_t weight)
{
	uint64_t incoming_hash;
	Counter *counter;
	int free_slot = -1;
	MonitoredElement *m_elt;
	int slot = -1;
	bool needs_sort = true;
	int counter_idx;
	Datum store_value;
	int i;

	incoming_hash = HashDatum(fss, incoming_null ? 0 : incoming);
	counter_idx = incoming_hash % fss->h;
	counter = &fss->bitmap_counter[counter_idx];

	store_value = fss->typ.typbyval ? incoming : incoming_hash;

	if (counter->count > 0)
	{
		bool found = false;

		for (i = 0; i < fss->m; i++)
		{
			m_elt = &fss->monitored_elements[i];

			if (!IS_SET(m_elt))
			{
				free_slot = i;
				break;
			}

			if (m_elt->value == store_value && (incoming_null == IS_NULL(m_elt)))
			{
				found = true;
				break;
			}
		}

		/* We found datum, so its monitored */
		if (found)
		{
			m_elt->frequency += weight;
			slot = i;
			goto done;
		}
	}
	else if (!IS_SET(&fss->monitored_elements[fss->m - 1]))
	{
		int i;

		/* Find the first free slot */
		for (i = 0; i < fss->m; i++)
		{
			if (!IS_SET(&fss->monitored_elements[i]))
			{
				free_slot = i;
				break;
			}
		}
	}

	/* This is only executed if datum is not monitored */
	if (counter->alpha + weight >= fss->monitored_elements[fss->m - 1].frequency)
	{
		/* Need to evict an element? */
		if (free_slot == -1)
		{
			Counter *c;
			/*
			 * We always evict the last element because the monitored element array is
			 * sorted by (-frequency, error).
			 */
			slot = fss->m - 1;
			m_elt = &fss->monitored_elements[slot];
			c = &fss->bitmap_counter[m_elt->counter];
			c->count--;
			c->alpha = m_elt->frequency;
		}
		else
		{
			slot = free_slot;
			m_elt = &fss->monitored_elements[slot];
		}

		SET(m_elt);
		if (incoming_null)
			SET_NULL(m_elt);

		m_elt->frequency = counter->alpha + weight;
		m_elt->error = counter->alpha;
		m_elt->counter = counter_idx;
		m_elt->value = store_value;
		counter->count++;

		if (!fss->typ.typbyval)
		{
			fss = set_varlena(fss, m_elt, incoming, incoming_null);
			m_elt = &fss->monitored_elements[slot];
		}
	}
	else
	{
		/*
		 * This element's frequency isn't high enough to include in the top-k,
		 * so just increment the appropriate error
		 */
		counter->alpha += weight;
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
			if (MonitoredElementComparator((void *) tmp, (void *) m_elt) <= 0)
				needs_sort = false;
		}

		if (needs_sort)
			qsort(fss->monitored_elements, FSSMonitoredLength(fss), sizeof(MonitoredElement), MonitoredElementComparator);
	}

	fss->count++;

	SET_VARSIZE(fss, FSSSize(fss));

	return fss;
}

/*
 * FSSMonitoredLength
 *
 * Determine the number of monitored elements actually present in the FSS's monitored array
 */
int
FSSMonitoredLength(FSS *fss)
{
	int i;

	/*
	 * If the last slot is set, we know that the entire array is full
	 */
	if (IS_SET(&fss->monitored_elements[fss->m - 1]))
		return fss->m;

	for (i = 0; i < fss->m; i++)
	{
		if (!IS_SET(&fss->monitored_elements[i]))
			break;
	}

	Assert(i <= fss->m - 1);

	return i;
}

/*
 * FSSMerge
 *
 * SpaceSaving summaries are mergeable:
 *   http://www.cs.utah.edu/~jeffp/papers/merge-summ.pdf
 */
FSS *
FSSMerge(FSS *fss, FSS *incoming)
{
	int i;
	int j;
	int k;
	int flen;
	int ilen;
	MonitoredElement *tmp;
	ArrayType *top_k;

	Assert(fss->h == incoming->h);
	Assert(fss->m == incoming->m);

	flen = FSSMonitoredLength(fss);
	ilen = FSSMonitoredLength(incoming);

	tmp = palloc0(sizeof(MonitoredElement) * (flen + ilen));
	memcpy(tmp, fss->monitored_elements, sizeof(MonitoredElement) * flen);

	for (i = 0; i < fss->h; i++)
	{
		fss->bitmap_counter[i].alpha += incoming->bitmap_counter[i].alpha;
		/* Reset counts, we'll reset them when the monitored elements lists are merged */
		fss->bitmap_counter[i].count = 0;
	}

	k = flen;
	for (i = 0; i < ilen; i++)
	{
		bool found = false;
		MonitoredElement *incoming_elt = &incoming->monitored_elements[i];

		Assert(IS_SET(incoming_elt));

		for (j = 0; j < flen; j++)
		{
			MonitoredElement *elt = &tmp[j];

			Assert(IS_SET(elt));

			if (incoming_elt->value == elt->value && (IS_NULL(incoming_elt) == IS_NULL(elt)))
			{
				elt->frequency += incoming_elt->frequency;
				elt->error += incoming_elt->error;
				found = true;
				break;
			}
		}

		if (!found)
		{
			tmp[k] = *incoming_elt;
			SET_NEW(&tmp[k]);
			k++;
		}
	}

	Assert(k <= flen + ilen);

	qsort(tmp, k, sizeof(MonitoredElement), MonitoredElementComparator);

	/* If we added any new byref elements, we need to rebuild the varlena array */
	if (k - flen > 0 && !fss->typ.typbyval)
	{
		top_k = construct_empty_array(fss->typ.typoid);

		/*
		 * For each monitored element in the sorted array, point its value to the
		 * varlena array attached to the output FSS
		 */
		for (i = 0; i < k; i++)
		{
			MonitoredElement *elt = &tmp[i];
			Datum value;
			bool isnull;
			ArrayType *prev;

			if (!IS_SET(elt))
				break;

			if (IS_NEW(elt))
			{
				value = get_monitored_value(incoming, elt, &isnull);
				UNSET_NEW(elt);
			}
			else
				value = get_monitored_value(fss, elt, &isnull);

			prev = top_k;
			top_k = array_set(top_k, 1, &i, value,
					isnull, -1, fss->typ.typlen, fss->typ.typbyval, fss->typ.typalign);

			Assert(prev != top_k);
			pfree(prev);

			elt->varlen_index = (Datum ) i;
		}

		fss = fss_resize_topk(fss, top_k);
	}

	memcpy(fss->monitored_elements, tmp, sizeof(MonitoredElement) * Min(k, fss->m));
	pfree(tmp);

	for (i = 0; i < fss->m; i++)
	{
		MonitoredElement *elt = &fss->monitored_elements[i];

		if (!IS_SET(elt))
			break;

		fss->bitmap_counter[elt->counter].count++;
	}

	fss->count += incoming->count;

	SET_VARSIZE(fss, FSSSize(fss));

	return fss;
}

/*
 * FSSTopK
 */
Datum *
FSSTopK(FSS *fss, uint16_t k, bool **nulls, uint16_t *found)
{
	int i;
	Datum *datums;
	bool *null_k;

	if (k > fss->k)
		elog(ERROR, "maximum value for k exceeded");

	datums = palloc(sizeof(Datum) * k);

	if (nulls)
		null_k = palloc0(sizeof(bool) * k);

	for (i = 0; i < k; i++)
	{
		MonitoredElement *elt = &fss->monitored_elements[i];
		bool isnull;

		if (!IS_SET(elt))
			break;

		datums[i] = get_monitored_value(fss, elt, &isnull);
		if (nulls)
			null_k[i] = isnull;
	}

	if (found)
		*found = i;

	if (nulls)
		*nulls = null_k;

	return datums;
}

/*
 * FSSTopKCounts
 */
uint64_t *
FSSTopKCounts(FSS *fss, uint16_t k, uint16_t *found)
{
	int i;
	uint64_t *counts;

	if (k > fss->k)
		elog(ERROR, "maximum value for k exceeded");

	counts = palloc0(sizeof(uint64_t) * k);

	for (i = 0; i < k; i++)
	{
		MonitoredElement *elt = &fss->monitored_elements[i];

		if (!IS_SET(elt))
			break;

		counts[i] = elt->frequency;
	}

	if (found)
		*found = i;

	return counts;
}

/*
 * FSSTotal
 */
uint64_t
FSSTotal(FSS *fss)
{
	return fss->count;
}

/*
 * FSSSize
 */
Size
FSSSize(FSS *fss)
{
	Size sz = sizeof(FSS) + (sizeof(Counter) * fss->h) + (sizeof(MonitoredElement) * fss->m);

	if (FSS_STORES_DATUMS(fss))
		sz += ARR_SIZE(fss->top_k);

	return sz;
}

/*
 * FSSCompress
 */
FSS *
FSSCompress(FSS *fss)
{
	if (!FSS_STORES_DATUMS(fss))
		return fss;

	return NULL;
}

/*
 * FSSPrint
 */
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
		appendStringInfo(buf, "| (%ld, %d) |", counter->alpha, counter->count);
	}

	appendStringInfo(buf, "\n----------\nMONITORED LIST:\n");

	for (i = 0; i < fss->m; i++)
	{
		MonitoredElement *elt = &fss->monitored_elements[i];
		bool isnull;
		Datum value = get_monitored_value(fss, elt, &isnull);
		if (IS_SET(elt))
			appendStringInfo(buf, "| (%ld, %ld, %d) |", value, elt->frequency, elt->error);
		else
			appendStringInfo(buf, "| (-, -, -) |");

		appendStringInfoChar(buf, '\n');
	}

	elog(LOG, "%s", buf->data);
	pfree(buf->data);
	pfree(buf);
}
