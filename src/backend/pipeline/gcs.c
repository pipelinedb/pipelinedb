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

typedef struct
{
	uint8_t *bytes;
	uint32_t len;
	uint32_t accum;
	uint8_t naccum;
} bit_reader;

static bit_reader *
bit_reader_create(uint8_t *bytes, uint32_t len)
{
	bit_reader *r = palloc0(sizeof(bit_reader));
	r->bytes = bytes;
	r->len = len;
	return r;
}

static uint32_t
bit_reader_read(bit_reader *r, uint8_t nbits)
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
				r->accum += 32;
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

typedef struct
{
	StringInfoData buf;
	uint64_t accum;
	uint8_t naccum;
} bit_writer;

static bit_writer *
bit_writer_create(void)
{
	bit_writer *w = palloc0(sizeof(bit_writer));
	initStringInfo(&w->buf);
	return w;
}

static void
bit_writer_write(bit_writer *w, uint8_t nbits, uint64_t val)
{
	uint8_t nwrite;

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

static void
bit_writer_flush(bit_writer *w)
{
	if (w->naccum > 0)
	{
		Assert(w->naccum < 8);
		appendStringInfoChar(&w->buf, w->accum & BITMASK(8));
		w->naccum = 0;
		w->accum = 0;
	}
}

typedef struct
{
	bit_reader *bit_reader;
	GolombCodedSet *gcs;
	uint32_t logp;
} gcs_reader;

static gcs_reader *
gcs_reader_create(GolombCodedSet *gcs)
{
	gcs_reader *r = palloc0(sizeof(gcs_reader));
	r->gcs = gcs;
	r->bit_reader = bit_reader_create((uint8_t *) gcs->b, gcs->blen);
	r->logp = (uint32_t) floor(log(gcs->p) / log(2));
	return r;
}

static int32_t
gcs_reader_next(gcs_reader *r)
{
	int32_t val;

	if (!r->bit_reader->len)
		return -1;

	while (bit_reader_read(r->bit_reader, 1))
	{
		val += r->gcs->p;
		if (!r->bit_reader->len)
			return -1;
	}

	val += bit_reader_read(r->bit_reader, r->logp);
	return val;
}

void
gcs_reader_destroy(gcs_reader *r)
{
	pfree(r);
}

typedef struct
{
	bit_writer *bit_writer;
	GolombCodedSet *gcs;
	uint32_t logp;
} gcs_writer;

static gcs_writer *
gcs_writer_create(GolombCodedSet *gcs)
{
	gcs_writer *r = palloc0(sizeof(gcs_writer));
	r->gcs = gcs;
	r->bit_writer = bit_writer_create();
	r->logp = (uint32_t) floor(log(gcs->p) / log(2));
	return r;
}

static void
gcs_writer_write(gcs_writer *w, uint32_t val)
{
	uint32_t q = val / w->gcs->p;
	uint32_t r = val - q * w->gcs->p;

	bit_writer_write(w->bit_writer, q + 1, BITMASK(q) << 1);
	bit_writer_write(w->bit_writer, w->logp, r);
}

static void
gcs_writer_flush(gcs_writer *w)
{
	bit_writer_flush(w->bit_writer);
}

static GolombCodedSet *
gcs_writer_generate_gcs(gcs_writer *w)
{

}

static void
gcs_writer_destroy(gcs_writer *w)
{

}

GolombCodedSet *
GolombCodedSetCreateWithPAndN(float8 p, uint32_t n)
{
	GolombCodedSet *gcs = (GolombCodedSet *) palloc0(sizeof(GolombCodedSet));
	gcs->n = n;
	gcs->p = (uint32_t) ceil(1 / p);
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
	gcs = GolombCodedSetCompress(gcs);
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
	gcs_reader *reader;
	gcs_writer *writer;
	int32_t l_val;
	int32_t c_val;
	int32_t prev_val = -1;
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

	reader = gcs_reader_create(gcs);
	writer = gcs_writer_create(gcs);

	i = 0;
	l_val = vals[i++];
	c_val = gcs_reader_next(reader);

	while (c_val >= 0 || l_val >= 0)
	{
		if (c_val == -1)
		{
			if (l_val != prev_val)
				gcs_writer_write(writer, l_val);
			if (i == vlen)
				l_val = -1;
			else
				l_val = vals[i++];
		}
		else if (l_val == -1)
		{
			if (c_val != prev_val)
				gcs_writer_write(writer, c_val);
			c_val = gcs_reader_next(reader);
		}
		else
		{
			if (c_val <= l_val)
			{
				if (c_val != prev_val)
					gcs_writer_write(writer, c_val);
				c_val = gcs_reader_next(reader);
			}
			else
			{
				if (l_val != prev_val)
					gcs_writer_write(writer, l_val);
				if (i == vlen)
					l_val = -1;
				else
					l_val = vals[i++];
			}
		}
	}

	gcs_writer_flush(writer);
	new = gcs_writer_generate_gcs(writer);

	gcs_reader_destroy(reader);
	gcs_writer_destroy(writer);
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
