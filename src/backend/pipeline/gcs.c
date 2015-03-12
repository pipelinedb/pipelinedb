/*-------------------------------------------------------------------------
 *
 * gcs.c
 *	  Golomb-coded Set implementation.
 *
 * src/backend/pipeline/gcs.c
 *
 *-------------------------------------------------------------------------
 */
#include <limits.h>
#include <math.h>
#include "pipeline/gcs.h"
#include "pipeline/miscutils.h"
#include "utils/elog.h"
#include "utils/palloc.h"

#define DEFAULT_P 0.02
#define DEFAULT_N (2 << 17)
#define RANGE_END(gcs) ((uint32_t) ceil((gcs)->p * (gcs)->n))
#define BITMASK(n) ((1 << (n)) - 1)
#define ACCUM_BITS(x) (sizeof((x)->accum) * 8)

#define MURMUR_SEED 0xb8160b979d3087fcL

static int
int_cmp(const void * a, const void * b)
{
	return *(int32_t *) a - *(int32_t *) b;
}

BitReader *
BitReaderCreate(uint8_t *bytes, uint32_t len)
{
	BitReader *r = palloc0(sizeof(BitReader));
	r->bytes = bytes;
	r->len = len;
	return r;
}

uint32_t
BitReaderRead(BitReader *r, uint8_t nbits)
{
	uint32_t ret = 0;
	uint8_t nread;

	Assert(nbits < 32);

	r->nbits += nbits;

	while (nbits)
	{
		if (!r->naccum)
		{
			if (r->len > 4)
			{
				r->accum = (((uint32_t) r->bytes[0] << 24) |
							((uint32_t) r->bytes[1] << 16) |
							((uint32_t) r->bytes[2] << 8) |
							((uint32_t) r->bytes[3]));
				r->bytes += 4;
				r->len -= 4;
				r->naccum += 32;
			}
			else if (r->len > 0)
			{
				r->accum = r->bytes[0];
				r->bytes++;
				r->len--;
				r->naccum += 8;
			}
			else
				return 0;
		}

		nread = Min(r->naccum, nbits);
		ret <<= nread;
		ret |= r->accum >> (r->naccum - nread);
		r->naccum -= nread;
		nbits -= nread;
		r->accum &= BITMASK(r->naccum);
	}

	return ret;
}

void
BitReaderDestroy(BitReader *r)
{
	pfree(r);
}

BitWriter *
BitWriterCreate(void)
{
	BitWriter *w = palloc0(sizeof(BitWriter));
	initStringInfo(&w->buf);
	return w;
}

void
BitWriterWrite(BitWriter *w, uint8_t nbits, uint64_t val)
{
	uint8_t nwrite;

	if (nbits < ACCUM_BITS(w))
		val &= BITMASK(nbits);

	while (nbits)
	{
		nwrite = Min(ACCUM_BITS(w) - w->naccum, nbits);
		w->accum <<= nwrite;
		w->accum |= val >> (nbits - nwrite);
		w->naccum += nwrite;
		nbits -= nwrite;

		while (w->naccum >= 8)
		{
			appendStringInfoChar(&w->buf, (w->accum >> (w->naccum - 8)) & BITMASK(8));
			w->naccum -= 8;
			w->accum &= BITMASK(w->naccum);
		}
	}
}

void
BitWriterFlush(BitWriter *w)
{
	if (w->naccum > 0)
	{
		Assert(w->naccum < 8);
		w->accum <<= (8 - w->naccum);
		appendStringInfoChar(&w->buf, w->accum & BITMASK(8));
		w->naccum = 0;
		w->accum = 0;
	}
}

void
BitWriterDestroy(BitWriter *w)
{
	resetStringInfo(&w->buf);
	pfree(w->buf.data);
	pfree(w);
}

GCSReader *
GCSReaderCreate(GolombCodedSet *gcs)
{
	GCSReader *r = palloc0(sizeof(GCSReader));
	r->gcs = gcs;
	r->bit_reader = BitReaderCreate((uint8_t *) gcs->b, gcs->blen);
	r->logp = (uint32_t) ceil(log(gcs->p) / log(2));
	r->prev = 0;
	return r;
}

int32_t
GCSReaderNext(GCSReader *r)
{
	int32_t val = 0;

	if (!r->bit_reader->len && r->bit_reader->naccum < r->logp)
		return INT_MAX;

	while (BitReaderRead(r->bit_reader, 1) == 1)
		val += r->gcs->p;

	if (!r->bit_reader->len && r->bit_reader->naccum < r->logp)
		return INT_MAX;

	val += BitReaderRead(r->bit_reader, r->logp);
	val += r->prev;

	r->prev = val;

	return val;
}

void
GCSReaderDestroy(GCSReader *r)
{
	BitReaderDestroy(r->bit_reader);
	pfree(r);
}

GCSWriter *
GCSWriterCreate(GolombCodedSet *gcs)
{
	GCSWriter *r = palloc0(sizeof(GCSWriter));
	r->gcs = gcs;
	r->bit_writer = BitWriterCreate();
	r->logp = (uint32_t) ceil(log(gcs->p) / log(2));
	return r;
}

void
GCSWriterWrite(GCSWriter *w, int32_t val)
{
	int32_t q = val / w->gcs->p;
	int32_t r = val %  w->gcs->p;
	int16_t naccum_left = 8 - w->bit_writer->naccum;

	if (!val)
		return;

	if (q - naccum_left > 8)
	{
		int32_t q_8;
		int32_t q_r;

		q -= naccum_left;
		q_8 = q / 8;
		q_r = q % 8;

		BitWriterWrite(w->bit_writer, naccum_left, BITMASK(naccum_left));
		Assert(w->bit_writer->naccum == 0);

		enlargeStringInfo(&w->bit_writer->buf, w->bit_writer->buf.len + q_8);
		memset(&w->bit_writer->buf.data[w->bit_writer->buf.len], 0xff, q_8);

		w->bit_writer->buf.len += q_8;

		BitWriterWrite(w->bit_writer, q_r + 1, BITMASK(q_r) << 1);
	}
	else
		BitWriterWrite(w->bit_writer, q + 1, BITMASK(q) << 1);

	BitWriterWrite(w->bit_writer, w->logp, r);

	w->nvals++;
}

void
GCSWriterFlush(GCSWriter *w)
{
	BitWriterFlush(w->bit_writer);
}

GolombCodedSet *
GCSWriterGenerateGCS(GCSWriter *w)
{
	GolombCodedSet *gcs = palloc0(sizeof(GolombCodedSet) + w->bit_writer->buf.len);
	memcpy(gcs, w->gcs, sizeof(GolombCodedSet));
	gcs->vals = NULL;
	gcs->blen = w->bit_writer->buf.len;
	memcpy(gcs->b, w->bit_writer->buf.data, gcs->blen);
	gcs->nvals = w->nvals;
	return gcs;
}

void
GCSWriterDestroy(GCSWriter *w)
{
	BitWriterDestroy(w->bit_writer);
	pfree(w);
}

GolombCodedSet *
GolombCodedSetCreateWithPAndN(float8 p, uint32_t n)
{
	GolombCodedSet *gcs = (GolombCodedSet *) palloc0(sizeof(GolombCodedSet));
	gcs->n = n;
	gcs->p = ceil(1 / p);
	return gcs;
}

GolombCodedSet *
GolombCodedSetCreate(void)
{
	return GolombCodedSetCreateWithPAndN(DEFAULT_P, DEFAULT_N);
}

void
GolombCodedSetDestroy(GolombCodedSet *gcs)
{
	if (gcs->vals)
		list_free(gcs->vals);
	pfree(gcs);
}

GolombCodedSet *
GolombCodedSetCopy(GolombCodedSet *gcs)
{
	Size size = GolombCodedSetSize(gcs);
	char *new = palloc(size);
	memcpy(new, (char *) gcs, size);
	return (GolombCodedSet *) new;
}

void
GolombCodedSetAdd(GolombCodedSet *gcs, void *key, Size size)
{
	int32_t hash = MurmurHash3_64(key, size, MURMUR_SEED) % RANGE_END(gcs);
	gcs->vals = lappend_int(gcs->vals, hash);
}

bool
GolombCodedSetContains(GolombCodedSet *gcs, void *key, Size size)
{
	GCSReader *reader;
	int32_t hash = MurmurHash3_64(key, size, MURMUR_SEED) % RANGE_END(gcs);
	int32_t val;

	gcs = GolombCodedSetCompress(gcs);
	reader = GCSReaderCreate(gcs);

	if (gcs->indexed)
	{
		int i;
		int32_t nbits = 0;
		int32_t prev = 0;
		int32_t nbits_8;
		int32_t nbits_r;

		for (i = 0; i < INDEX_SIZE; i++)
		{
			if (gcs->idx[i][0] == hash)
				return true;
			if (gcs->idx[i][0] > hash)
				break;
			prev = gcs->idx[i][0];
			nbits = gcs->idx[i][1];
		}

		nbits_8 = nbits / 8;
		nbits_r = nbits % 8;

		reader->bit_reader->bytes += nbits_8;
		reader->bit_reader->len -= nbits_8;
		reader->prev = prev;
		BitReaderRead(reader->bit_reader, nbits_r);
	}

	while ((val = GCSReaderNext(reader)) != INT_MAX)
		if (val == hash)
			return true;
		if (val > hash)
			return false;

	return false;
}

GolombCodedSet *
GolombCodedSetShallowUnion(GolombCodedSet *result, GolombCodedSet *incoming)
{
	GCSReader *reader = GCSReaderCreate(incoming);
	int32_t val;

	while ((val = GCSReaderNext(reader)) != INT_MAX)
		result->vals = lappend_int(result->vals, val);
	result->vals = list_union_int(result->vals, incoming->vals);

	return result;
}

GolombCodedSet *
GolombCodedSetUnion(GolombCodedSet *result, GolombCodedSet *incoming)
{
	int32_t *vals;
	ListCell *lc;
	uint32_t vlen = list_length(result->vals) + list_length(incoming->vals);
	GCSReader *r1, *r2;
	GCSWriter *writer;
	int32_t v0, v1, v2;
	int32_t prev = -1;
	int i = 0;
	GolombCodedSet *new;

	if (result->n != incoming->n || result->p != incoming->p)
		elog(ERROR, "cannot union Golomb-coded sets of different hash ranges");

	if (!vlen && !incoming->nvals)
		return result;

	vals = palloc(sizeof(int32_t) * vlen);
	foreach(lc, result->vals)
		vals[i++] = lfirst_int(lc);
	foreach(lc, incoming->vals)
		vals[i++] = lfirst_int(lc);

	qsort(vals, vlen, sizeof(int32_t), int_cmp);

	r1 = GCSReaderCreate(result);
	r2 = GCSReaderCreate(incoming);
	writer = GCSWriterCreate(result);

	i = 0;
	v0 = vals[i++];
	v1 = GCSReaderNext(r1);
	v2 = GCSReaderNext(r2);

	while (v2 < INT_MAX || v1 < INT_MAX || v0 < INT_MAX)
	{
		int32_t min = Min(v0, Min(v1, v2));

		if (v0 == min)
			v0 = i >= vlen ? INT_MAX : vals[i++];
		else if (v1 == min)
			v1 = GCSReaderNext(r1);
		else
			v2 = GCSReaderNext(r2);

		if (prev == min)
			continue;

		if (prev != -1)
			GCSWriterWrite(writer, min - prev);
		else
			GCSWriterWrite(writer, min);

		prev = min;
	}

	GCSWriterFlush(writer);
	new = GCSWriterGenerateGCS(writer);

	GCSReaderDestroy(r1);
	GCSWriterDestroy(writer);

	GolombCodedSetIndex(new);
	return new;
}

GolombCodedSet *
GolombCodedSetIntersection(GolombCodedSet *result, GolombCodedSet *incoming)
{
	int32_t *vals1, *vals2;
	ListCell *lc;
	uint32_t vlen1, vlen2;
	GCSReader *r1, *r2;
	GCSWriter *writer;
	int32_t v1, v2, v3, v4;
	int32_t prev = -1;
	int i = 0, j = 0;
	GolombCodedSet *new;

	if (result->n != incoming->n || result->p != incoming->p)
		elog(ERROR, "cannot instersect Golomb-coded sets of different hash ranges");

	vlen1 = list_length(result->vals);
	vlen2 = list_length(incoming->vals);

	if (!vlen2 && !incoming->nvals)
		return GolombCodedSetCreateWithPAndN(1.0 / result->p, result->n);

	vals1 = palloc(sizeof(int32_t) * vlen1);
	foreach(lc, result->vals)
		vals1[i++] = lfirst_int(lc);

	vals2 = palloc(sizeof(int32_t) * vlen2);
	foreach(lc, incoming->vals)
		vals2[j++] = lfirst_int(lc);

	qsort(vals1, vlen1, sizeof(int32_t), int_cmp);
	qsort(vals2, vlen2, sizeof(int32_t), int_cmp);

	r1 = GCSReaderCreate(result);
	r2 = GCSReaderCreate(incoming);
	writer = GCSWriterCreate(result);

	i = j = 0;
	v1 = GCSReaderNext(r1);
	v2 = GCSReaderNext(r2);
	v3 = vals1[i++];
	v4 = vals2[j++];

	while ((v1 < INT_MAX || v3 < INT_MAX) && (v2 < INT_MAX || v4 < INT_MAX))
	{
		int32_t min1 = Min(v1, v3);
		int32_t min2 = Min(v2, v4);
		int32_t min = Min(min1, min2);
		bool eq = min1 == min2;

		if (v1 == min)
			v1 = GCSReaderNext(r1);
		if (v3 == min)
			v3 = i >= vlen1 ? INT_MAX : vals1[i++];
		if (v2 == min)
			v2 = GCSReaderNext(r2);
		if (v4 == min)
			v4 = j >= vlen2 ? INT_MAX : vals2[j++];

		if (eq && prev != min)
		{
			if (prev != -1)
				GCSWriterWrite(writer, min - prev);
			else
				GCSWriterWrite(writer, min);
			prev = min;
		}
	}

	GCSWriterFlush(writer);
	new = GCSWriterGenerateGCS(writer);

	GCSReaderDestroy(r1);
	GCSWriterDestroy(writer);

	GolombCodedSetIndex(new);
	return new;
}

GolombCodedSet *
GolombCodedSetCompress(GolombCodedSet *gcs)
{
	int32_t *vals;
	ListCell *lc;
	uint32_t vlen = list_length(gcs->vals);
	GCSReader *reader;
	GCSWriter *writer;
	int32_t v1;
	int32_t v2;
	int32_t prev = -1;
	int i = 0;
	GolombCodedSet *new;

	if (!vlen)
		return gcs;

	vals = palloc(sizeof(int32_t) * vlen);
	foreach(lc, gcs->vals)
		vals[i++] = lfirst_int(lc);

	qsort(vals, vlen, sizeof(int32_t), int_cmp);

	reader = GCSReaderCreate(gcs);
	writer = GCSWriterCreate(gcs);

	i = 0;
	v1 = vals[i++];
	v2 = GCSReaderNext(reader);

	while (v2 < INT_MAX || v1 < INT_MAX)
	{
		int32_t min = Min(v1, v2);

		if (v1 == min)
			v1 = i >= vlen ? INT_MAX : vals[i++];
		else
			v2 = GCSReaderNext(reader);

		if (prev == min)
			continue;

		if (prev != -1)
			GCSWriterWrite(writer, min - prev);
		else
			GCSWriterWrite(writer, min);

		prev = min;
	}

	GCSWriterFlush(writer);
	new = GCSWriterGenerateGCS(writer);

	GCSReaderDestroy(reader);
	GCSWriterDestroy(writer);

	GolombCodedSetIndex(new);
	return new;
}

void
GolombCodedSetIndex(GolombCodedSet *gcs)
{
	int32_t width;
	GCSReader *reader;
	int32_t val = 0;
	int32_t i = 0;
	int32_t j = 0;

	if (gcs->vals)
		elog(ERROR, "only compressed Golomb-coded sets can be indexed");

	width = Max(1, gcs->nvals / INDEX_SIZE);
	reader = GCSReaderCreate(gcs);

	memset(gcs->idx, 0, 2 * INDEX_SIZE);

	while (val != INT_MAX)
	{
		if (i % width == 0 && j < INDEX_SIZE)
		{
			gcs->idx[j][0] = val;
			gcs->idx[j][1] = reader->bit_reader->nbits;
			j++;
		}

		val = GCSReaderNext(reader);
		i++;
	}

	if (j < INDEX_SIZE)
	{
		gcs->idx[j][0] = INT_MAX;
		gcs->idx[j][1] = reader->bit_reader->nbits;
	}

	gcs->indexed = true;
}

float8
GolombCodedSetFillRatio(GolombCodedSet *gcs)
{
	return ((1.0 * gcs->nvals + list_length(gcs->vals)) / RANGE_END(gcs));
}

Size
GolombCodedSetSize(GolombCodedSet *gcs)
{
	gcs = GolombCodedSetCompress(gcs);
	return (sizeof(GolombCodedSet) + gcs->blen);
}
