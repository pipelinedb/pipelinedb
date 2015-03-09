/*-------------------------------------------------------------------------
 *
 * gcs.c
 *	  Golomb-coded Set implementation.
 *
 * src/backend/pipeline/gcs.c
 *
 *-------------------------------------------------------------------------
 */
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

static uint8_t reverse_map[16] = {
	0b0000, 0b1000, 0b0100, 0b1100,
	0b0010, 0b1010, 0b0110, 0b1101,
	0b0001, 0b1001, 0b0101, 0b1101,
	0b0011, 0b1011, 0b0111, 0b1111
};

static uint8_t
reverse_byte(uint8_t byte)
{
	if (byte == 0xff)
		return byte;
	return (reverse_map[byte & 0xf] << 4) | reverse_map[byte >> 4];
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
		elog(LOG, "accum %d, naccum %d", r->accum, r->naccum);
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
			uint8_t byte = reverse_byte((w->accum >> (w->naccum - 8)) & BITMASK(8));
			appendStringInfoChar(&w->buf, byte);
			w->naccum -= 8;
			w->accum &= BITMASK(w->naccum);
		}
	}

	w->nbits += nbits;
}

void
BitWriterFlush(BitWriter *w)
{
	if (w->naccum > 0)
	{
		Assert(w->naccum < 8);
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
	r->logp = (uint32_t) floor(log(gcs->p) / log(2));
	r->prev = 0;
	return r;
}

int32_t
GCSReaderNext(GCSReader *r)
{
	int32_t val;

	if (!r->bit_reader->len && r->bit_reader->naccum < r->logp)
		return -1;

	while (BitReaderRead(r->bit_reader, 1) == 1)
		val += r->gcs->p;

	if (!r->bit_reader->len && r->bit_reader->naccum < r->logp)
		return -1;

	val += BitReaderRead(r->bit_reader, r->logp);

	elog(LOG, "READ q %d, r %d, logp %d", val / r->gcs->p, val % r->gcs->p, r->logp);

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
	r->logp = (uint32_t) floor(log(gcs->p) / log(2));
	return r;
}

void
GCSWriterWrite(GCSWriter *w, int32_t val)
{
	uint32_t q = val / w->gcs->p;
	uint32_t r = val %  w->gcs->p;
	uint32_t q_8 = q / 8;
	uint32_t q_r = q % 8;

	if (!val)
		return;

	elog(LOG, "WRITE q %d, r %d", q, r);

	enlargeStringInfo(&w->bit_writer->buf, w->bit_writer->buf.len + q_8);
	memset(&w->bit_writer->buf.data[w->bit_writer->buf.len], 0xff, q_8);
	w->bit_writer->buf.len += q_8;

	BitWriterWrite(w->bit_writer, q_r + 1, BITMASK(q) << 1);
	BitWriterWrite(w->bit_writer, w->logp, r);
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
	gcs->nbits = w->bit_writer->nbits;
	memcpy(gcs->b, w->bit_writer->buf.data, gcs->blen);
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
	gcs->nvals++;
}

bool
GolombCodedSetContains(GolombCodedSet *gcs, void *key, Size size)
{
	gcs = GolombCodedSetCompress(gcs);
	return false;
}

GolombCodedSet *
GolombCodedSetUnion(GolombCodedSet *result, GolombCodedSet *incoming)
{
	result = GolombCodedSetCompress(result);
	result = GolombCodedSetCompress(incoming);

	return result;
}

GolombCodedSet *
GolombCodedSetIntersection(GolombCodedSet *result, GolombCodedSet *incoming)
{
	result = GolombCodedSetCompress(result);
	result = GolombCodedSetCompress(incoming);

	return result;
}

static int
int_cmp(const void * a, const void * b)
{
	return *(int32_t *) a - *(int32_t *) b;
}

GolombCodedSet *
GolombCodedSetCompress(GolombCodedSet *gcs)
{
	int32_t *vals;
	ListCell *lc;
	uint32_t vlen = list_length(gcs->vals);
	GCSReader *reader;
	GCSWriter *writer;
	int32_t l_val;
	int32_t c_val;
	int32_t prev = -1;
	int i;
	GolombCodedSet *new;

	if (!vlen)
		return gcs;

	vals = palloc(sizeof(uint32_t) * vlen);
	foreach(lc, gcs->vals)
		vals[i++] = lfirst_int(lc);
	list_free(gcs->vals);
	gcs->vals = NIL;

	qsort(vals, vlen, sizeof(uint32_t), int_cmp);

	reader = GCSReaderCreate(gcs);
	writer = GCSWriterCreate(gcs);

	i = 0;
	l_val = vals[i++];
	c_val = GCSReaderNext(reader);

	while (c_val >= 0 || l_val >= 0)
	{
		int32_t to_write;

		if (c_val == -1)
		{
			to_write = l_val;
			if (i == vlen)
				l_val = -1;
			else
				l_val = vals[i++];
		}
		else if (l_val == -1)
		{
			to_write = c_val;
			c_val = GCSReaderNext(reader);
		}
		else
		{
			if (c_val <= l_val)
			{
				to_write = c_val;
				c_val = GCSReaderNext(reader);
			}
			else
			{
				to_write = l_val;
				if (i == vlen)
					l_val = -1;
				else
					l_val = vals[i++];
			}
		}

		if (prev != -1)
			GCSWriterWrite(writer, to_write - prev);
		else
			GCSWriterWrite(writer, to_write);

		prev = to_write;
	}

	GCSWriterFlush(writer);
	new = GCSWriterGenerateGCS(writer);

	GCSReaderDestroy(reader);
	GCSWriterDestroy(writer);
	GolombCodedSetDestroy(gcs);

	return new;
}

float8
GolombCodedSetFillRatio(GolombCodedSet *gcs)
{
	return ((1.0 * gcs->nvals) / RANGE_END(gcs));
}

Size
GolombCodedSetSize(GolombCodedSet *gcs)
{
	gcs = GolombCodedSetCompress(gcs);
	return (sizeof(GolombCodedSet) + gcs->blen);
}
