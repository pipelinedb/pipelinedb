/*-------------------------------------------------------------------------
 *
 * hll.c
 *
 *	  HyperLogLog++ implementation - Based on:
 *
 *	  http://static.googleusercontent.com/media/research.google.com/fr//pubs/archive/40671.pdf
 *
 *	  and
 *
 *	  https://github.com/antirez/redis/blob/3.0/src/hyperloglog.c
 *
 *	  Attempting to understand this code without reading the above paper first
 *	  is probably a hopeless endeavour
 *
 * src/backend/pipeline/hll.h
 *
 *-------------------------------------------------------------------------
 */
#include <math.h>

#include "pipeline/hll.h"
#include "utils/elog.h"
#include "utils/palloc.h"

#include "utils/memutils.h"
/*
 * Returns the number of leading zeroes for the hash code of the
 * given element. The value of m is set to the register
 */
static uint8
num_leading_zeroes(HyperLogLog *hll, void *elem, Size size, int *m)
{
	uint64 h = MurmurHash64A(elem, size);
	uint8 count = 0;

	/* register index is the first p bits of the hash */
	*m = (h >> (64 - hll->p));

	/* now determine how many leading zeroes follow the first p bits */
	h <<= hll->p;

	while ((h & MSB_64) == 0)
	{
		h <<= 1;
		count ++;
	}

	return count;
}

/*
 * sparse_add
 *
 * Adds an element to the given HLL using the sparse representation
 */
static HyperLogLog *
hll_sparse_add(HyperLogLog *hll, void *elem, Size size, int *result)
{
	int m;
	int leading;
	uint8 oldleading;
	uint8 *sparse;
	uint8 *end;
	uint8 *pos;
	uint8 *next;
	long first;
	long span;
	bool iszero = false;
	bool isxzero = false;
	bool isval = false;
	long runlen = 0;
  uint8 seq[5];
  uint8 *n = seq;
  int last;
  int len;
  int seqlen;
  int oldlen;
  int deltalen;

	leading = num_leading_zeroes(hll, elem, size, &m);

	// upgrade?10

  /*
   * When updating a sparse representation, we may need to enlarge
   * the buffer by 3 bytes in the worst case (XZERO split into XZERO-VAL-XZERO)
   */
	hll = repalloc(hll, HLL_SIZE(hll) + 3);

  /*
   * Step 1:
   *
   * Locate the opcode we need to modify to check
   * if a value update is actually needed.
   */
	sparse = hll->M;
	pos = sparse;
	end = sparse + hll->mlen;

  first = 0;
  next = NULL; /* points to the next opcode at the end of the loop. */
  span = 0;

  while (pos < end)
  {
		int oplen;

		/* set span to the number of registers covered by this opcode */
		oplen = 1;
		if (HLL_SPARSE_IS_ZERO(pos))
		{
			span = HLL_SPARSE_ZERO_LEN(pos);
		}
		else if (HLL_SPARSE_IS_VAL(pos))
		{
			span = HLL_SPARSE_VAL_LEN(pos);
		}
		else
		{
			/* XZERO */
			span = HLL_SPARSE_XZERO_LEN(pos);
			oplen = 2;
		}

		/* break if this opcode covers the register m */
		if (m <= first + span - 1)
			break;

		pos += oplen;
		first += span;
  }

  next = HLL_SPARSE_IS_XZERO(pos) ? pos + 2 : pos + 1;
  if (next >= end)
		next = NULL;

  /*
   * Cache the current opcode type to avoid using the macro
   * extraneous for something that will not change.
   * Also cache the run-length of the opcode.
   */
  if (HLL_SPARSE_IS_ZERO(pos))
  {
		iszero = true;
		runlen = HLL_SPARSE_ZERO_LEN(pos);
  }
  else if (HLL_SPARSE_IS_XZERO(pos))
  {
		isxzero = true;
		runlen = HLL_SPARSE_XZERO_LEN(pos);
  }
  else
  {
		isval = true;
		runlen = HLL_SPARSE_VAL_LEN(pos);
  }

  /*
   * Step 2: After the loop:
   *
   * first stores the index of the first register covered
   * by the current opcode, which is pointed by pos.
   *
   * next and prev store respectively the next and previous opcode,
   * or NULL if the opcode at pos is respectively the last or first.
   *
   * span is set to the number of registers covered by the current
   * opcode.
   *
   * There are different cases in order to update the data structure
   * in place without generating it from scratch:
   *
   * A) If it is a VAL opcode already set to a value >= our leading,
   *    no update is needed, regardless of the VAL run-length field.
   *    In this case PFADD returns 0 since no changes are performed.
   *
   * B) If it is a VAL opcode with len = 1 (representing only our
   *    register) and the value is less than leading, we just update it
   *    since this is a trivial case. */
  if (isval)
  {
		oldleading = HLL_SPARSE_VAL_VALUE(pos);

		/* case A */
		if (oldleading >= leading)
		{
			*result = 0;

			return hll;
		}

		/* case B */
		if (runlen == 1)
		{
			HLL_SPARSE_VAL_SET(pos, leading, 1);
			hll->encoding = HLL_SPARSE_DIRTY;
		}

		*result = 1;

		return hll;
  }

  /*
   * C) Another trivial case is a ZERO opcode with a len of 1.
   * We can just replace it with a VAL opcode with our value and len of 1.
   */
  if (iszero && runlen == 1)
  {
		HLL_SPARSE_VAL_SET(pos, leading, 1);
		hll->encoding = HLL_SPARSE_DIRTY;

		*result = 1;

		return hll;
  }

  /* D) General case.
   *
   * The other cases are more complex: our register must be updated
   * and is either currently represented by a VAL opcode with len > 1,
   * by a ZERO opcode with len > 1, or by an XZERO opcode.
   *
   * In these cases the original opcode must be split into multiple
   * opcodes. The worst case is an XZERO split in the middle resulting in
   * XZERO | VAL | XZERO, so the resulting sequence max length is
   * 2 + 1 + 2 = 5 bytes.
   *
   * We perform the split writing the new sequence into the 'new' buffer
   * with 'newlen' as length. Later the new sequence is inserted in place
   * of the old one, possibly moving what is on the right a few bytes
   * if the new sequence is longer than the older one. */

  last = first + span - 1; /* Last register covered by the sequence. */
  if (iszero || isxzero)
  {
		/* handle splitting of ZERO / XZERO */
		if (m != first)
		{
			len = m - first;
			if (len > 64)
			{
				HLL_SPARSE_XZERO_SET(n, len);
				n += 2;
			}
			else
			{
				HLL_SPARSE_ZERO_SET(n, len);
				n++;
			}
		}

		HLL_SPARSE_VAL_SET(n, leading, 1);
		n++;
		if (m != last)
		{
			len = last - m;
			if (len > 64)
			{
				HLL_SPARSE_XZERO_SET(n, len);
				n += 2;
			}
			else
			{
				HLL_SPARSE_ZERO_SET(n, len);
				n++;
			}
		}
  }
  else
  {
		/* handle splitting of VAL */
		int curval = HLL_SPARSE_VAL_VALUE(pos);

		if (m != first)
		{
			len = m - first;
			HLL_SPARSE_VAL_SET(n, curval, len);
			n++;
		}

		HLL_SPARSE_VAL_SET(n, leading, 1);
		n++;

		if (m != last)
		{
			len = last - m;
			HLL_SPARSE_VAL_SET(n, curval, len);
			n++;
		}
  }

  /*
   * Step 3:
   *
   * Store the new registers in the HLL
   */
  seqlen = n - seq;
  oldlen = isxzero ? 2 : 1;
  deltalen = seqlen - oldlen;

//   if (deltalen > 0 &&
//       sdslen(o->ptr)+deltalen > server.hll_sparse_max_bytes) goto promote;

  /* we already repalloc'd enough space to do this */
	if (deltalen && next)
	 memmove(next + deltalen, next, end - next);

	hll = repalloc(hll, HLL_SIZE(hll) + deltalen);

	memcpy(pos, seq, seqlen);

	hll->encoding = HLL_SPARSE_DIRTY;
	hll->mlen += deltalen;

	*result = 1;

	return hll;
}

static double
hll_sparse_sum(HyperLogLog *hll, double *PE, int *ezp)
{
  double E = 0;
  int ez = 0;
  int idx = 0;
  int runlen;
  int regval;
  uint8 *pos = hll->M;
  uint8 *end = pos + hll->mlen;

  while (pos < end)
  {
		if (HLL_SPARSE_IS_ZERO(pos))
		{
			runlen = HLL_SPARSE_ZERO_LEN(pos);
			idx += runlen;
			ez += runlen;
			/* Increment E at the end of the loop. */
			pos++;
		}
		else if (HLL_SPARSE_IS_XZERO(pos))
		{
			runlen = HLL_SPARSE_XZERO_LEN(pos);
			idx += runlen;
			ez += runlen;
			/* Increment E at the end of the loop. */
			pos += 2;
		}
		else
		{
			runlen = HLL_SPARSE_VAL_LEN(pos);
			regval = HLL_SPARSE_VAL_VALUE(pos);
			idx += runlen;
			E += PE[regval]*runlen;
			pos++;
		}
  }

  E += ez;
  *ezp = ez;

  return E;
}

/*
 * MurmurHash - 64-bit version
 */
uint64
MurmurHash64A(const void *key, Size keysize)
{
  static const uint64 m = 0xc6a4a7935bd1e995;
  static const int r = 47;

  const uint64 *data = (const uint64 *) key;
  const uint64 *end = data + (keysize / 8);
  const uint8 *data2;
  uint64 h = MURMUR_SEED ^ keysize;

  while(data != end)
  {
		uint64 k = *data++;

		k *= m;
		k ^= k >> r;
		k *= m;

		h ^= k;
		h *= m;
  }

  data2 = (const uint8*) data;

  switch(keysize & 7)
  {
		case 7: h ^= (uint64) data2[6] << 48;
		case 6: h ^= (uint64) data2[5] << 40;
		case 5: h ^= (uint64) data2[4] << 32;
		case 4: h ^= (uint64) data2[3] << 24;
		case 3: h ^= (uint64) data2[2] << 16;
		case 2: h ^= (uint64) data2[1] << 8;
		case 1: h ^= (uint64) data2[0];
						h *= m;
  };

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

/*
 * HLLCreate
 *
 * Create an empty HyperLogLog structure with the given parameters
 */
HyperLogLog *
HLLCreate(int p)
{
	/* m = number of registers */
	int m = 1 << p;
	int remaining = m;
	Size size;
	HyperLogLog *hll;
	uint8 *pos;
	int mlen;

	/*
	 * We can represent 1 << 14 registers with every two bytes
	 * of the sparse representation.
	 */
	mlen = Max(2, 2 * m / HLL_SPARSE_XZERO_MAX_LEN);
	size = sizeof(HyperLogLog) + mlen;

	hll = palloc0(size);
	hll->p = p;
	hll->encoding = HLL_SPARSE_CLEAN;
	hll->mlen = mlen;

	pos = hll->M;

	while (remaining)
	{
		int xzero = Min(m, remaining);
		HLL_SPARSE_XZERO_SET(pos, xzero);
		pos += 2;
		remaining -= xzero;
	}

	return hll;
}

/*
 * HLLAdd
 *
 * Adds an element to the given HLL
 */
HyperLogLog *
HLLAdd(HyperLogLog *hll, void *elem, Size len, int *result)
{
	if (HLL_IS_SPARSE(hll))
		return hll_sparse_add(hll, elem, len, result);
	else
		return NULL;
}

/*
 * HLLSize
 *
 * Returns the cardinality of the given HLL
 */
uint64
HLLSize(HyperLogLog *hll)
{
  double m = 1 << hll->p;
  double E;
  double alpha = 0.7213 / (1 + 1.079 / m);

  int j;
  int ez; /* Number of registers equal to 0. */

  /*
   * Precompute 2^(-reg[j]) in order to speedup the
   * computation of SUM(2^-register[0..i])
   */
  static bool initialized = false;
  static double PE[64];

  if (!initialized)
  {
		PE[0] = 1; /* 2^(-reg[j]) is 1 when m is 0. */
		for (j = 1; j < 64; j++)
		{
			/* 2^(-reg[j]) is the same as 1/2^reg[j]. */
			PE[j] = 1.0/(1ULL << j);
		}
		initialized = true;
  }

  E = hll_sparse_sum(hll, PE, &ez);
  E = (1 / E) * alpha * m * m;

  /*
   * Use the LINEARCOUNTING algorithm for small cardinalities.
   * For larger values but up to 72000 HyperLogLog raw approximation is
   * used since linear counting error starts to increase. However HyperLogLog
   * shows a strong bias in the range 2.5*16384 - 72000, so we try to
   * compensate for it.
   */
  if (E < m * 2.5 && ez != 0)
  {
		E = m * log(m / ez); /* LINEARCOUNTING() */
  }
  else if (m == 16384 && E < 72000)
  {
		/*
		 * We did polynomial regression of the bias for this range, this
		 * way we can compute the bias for a given cardinality and correct
		 * according to it. Only apply the correction for P=14 that's what
		 * we use and the value the correction was verified with.
		 */
		double bias = 5.9119 * 1.0e-18 * (E * E * E *E) -
									1.4253 * 1.0e-12 * (E * E * E) +
									1.2940 * 1.0e-7 * (E * E) -
									5.2921 * 1.0e-3 * E +
									83.3216;
		E -= E * (bias / 100);
  }

  /*
   * We don't apply the correction for E > 1/30 of 2^32 since we use
   * a 64 bit function and 6 bit counters. To apply the correction for
   * 1/30 of 2^64 is not needed since it would require a huge set
   * to approach such a value.
   */
  return (uint64) E;
}
