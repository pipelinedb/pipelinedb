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
 * hll_sparse_to_dense
 *
 * Promote a sparse representation to a dense representation
 */
static HyperLogLog *
hll_sparse_to_dense(HyperLogLog *sparse)
{
	HyperLogLog *dense;
  int idx = 0;
  int runlen;
  int regval;
  uint8 *pos = (uint8 *) sparse->M;
  uint8 *end = pos + sparse->mlen;
  Size size;
  int m = (((1 << sparse->p) * HLL_BITS_PER_REGISTER) / 8);

  if (HLL_IS_DENSE(sparse))
		return sparse;

  size = sizeof(HyperLogLog) + m + 1;
  dense = palloc0(size);
  dense->card = sparse->card;
  dense->p = sparse->p;
  dense->encoding = HLL_DENSE_CLEAN;
  dense->mlen = m;

  /*
   * Read the sparse representation and set non-zero registers
   * accordingly.
   */
  while(pos < end)
  {
		if (HLL_SPARSE_IS_ZERO(pos))
		{
			runlen = HLL_SPARSE_ZERO_LEN(pos);
			idx += runlen;
			pos++;
		}
		else if (HLL_SPARSE_IS_XZERO(pos))
		{
			runlen = HLL_SPARSE_XZERO_LEN(pos);
			idx += runlen;
			pos += 2;
		}
		else
		{
			runlen = HLL_SPARSE_VAL_LEN(pos);
			regval = HLL_SPARSE_VAL_VALUE(pos);

			while (runlen--)
			{
				// why is regval 32 here?
				// that shouldn't be possible it's too big
				// seems like an alignment issue...32 is never added in sparse_add
				HLL_DENSE_SET_REGISTER(dense->M, idx, regval);
				idx++;
			}
			pos++;
		}
  }

  pfree(sparse);

  return dense;
}

/*
 * Returns the number of leading zeroes for the hash code of the
 * given element. The value of m is set to the register
 */
static uint8
num_leading_zeroes(HyperLogLog *hll, void *elem, Size size, int *m)
{
	uint64 h = MurmurHash64A(elem, size);
	uint64 index;
	uint64 bit;
	uint8 count = 0;
	int numregs = (1 << hll->p);
	int mask = (numregs - 1);

	/* register index is the first p bits of the hash */
//	*m = (h >> (64 - hll->p));
//
//	/* now determine how many leading zeroes follow the first p bits */
//	h <<= hll->p;
//
//	while ((h & MSB_64) == 0)
//	{
//		h <<= 1;
//		count ++;
//	}

  index = h & mask;
  h |= ((uint64) 1 << 63);
  bit = numregs;
  count = 1;

  while((h & bit) == 0) {
		count++;
		bit <<= 1;
  }

  *m = (int) index;

	return count;
}

static HyperLogLog *
hll_dense_add(HyperLogLog *hll, void *elem, Size size, int *result)
{
  uint8 oldleading;
  uint8 leading;
  int index;

  /* update the register if this element produced a longer run of zeroes */
  leading = num_leading_zeroes(hll, elem, size, &index);

  HLL_DENSE_GET_REGISTER(oldleading, hll->M, index);
  if (leading > oldleading)
  {
		HLL_DENSE_SET_REGISTER(hll->M, index, leading);

		*result = 1;
  }
  else
  {
		*result = 0;
  }

  return hll;
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

	if (leading > 32)
	{
		/* the sparse representation can only represent 32-bit cardinalities */
		hll = hll_sparse_to_dense(hll);

		return hll_dense_add(hll, elem, size, result);
	}

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
			if (leading == 32)
				elog(LOG, "LEADING=%d", leading);
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

	if (deltalen > 0 && hll->mlen + deltalen > HLL_MAX_SPARSE_BYTES)
	{
		hll = hll_sparse_to_dense(hll);

		return hll_dense_add(hll, elem, size, result);
	}

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
			E += PE[regval] * runlen;
			pos++;
		}
  }

  E += ez;
  *ezp = ez;

  return E;
}

/*
 * hll_dense_sum
 */
static double
hll_dense_sum(HyperLogLog *hll, double *PE, int *ezp)
{
  double E = 0;
  int j;
  int ez = 0;
  int m = 1 << hll->p;

  if (m == (1 << 14) && HLL_BITS == 6)
  {
		/* fast path when p == 14 */
		uint8 *r = hll->M;
		unsigned long r0, r1, r2, r3, r4, r5, r6, r7, r8, r9,
									r10, r11, r12, r13, r14, r15;
		for (j = 0; j < 1024; j++)
		{
			/* handle 16 registers per iteration.*/
			r0 = r[0] & 63; if (r0 == 0) ez++;
			r1 = (r[0] >> 6 | r[1] << 2) & 63; if (r1 == 0) ez++;
			r2 = (r[1] >> 4 | r[2] << 4) & 63; if (r2 == 0) ez++;
			r3 = (r[2] >> 2) & 63; if (r3 == 0) ez++;

			r4 = r[3] & 63; if (r4 == 0) ez++;
			r5 = (r[3] >> 6 | r[4] << 2) & 63; if (r5 == 0) ez++;
			r6 = (r[4] >> 4 | r[5] << 4) & 63; if (r6 == 0) ez++;
			r7 = (r[5] >> 2) & 63; if (r7 == 0) ez++;

			r8 = r[6] & 63; if (r8 == 0) ez++;
			r9 = (r[6] >> 6 | r[7] << 2) & 63; if (r9 == 0) ez++;
			r10 = (r[7] >> 4 | r[8] << 4) & 63; if (r10 == 0) ez++;
			r11 = (r[8] >> 2) & 63; if (r11 == 0) ez++;

			r12 = r[9] & 63; if (r12 == 0) ez++;
			r13 = (r[9] >> 6 | r[10] << 2) & 63; if (r13 == 0) ez++;
			r14 = (r[10] >> 4 | r[11] << 4) & 63; if (r14 == 0) ez++;
			r15 = (r[11] >> 2) & 63; if (r15 == 0) ez++;

			E += (PE[r0] + PE[r1]) + (PE[r2] + PE[r3]) + (PE[r4] + PE[r5]) +
					 (PE[r6] + PE[r7]) + (PE[r8] + PE[r9]) + (PE[r10] + PE[r11]) +
					 (PE[r12] + PE[r13]) + (PE[r14] + PE[r15]);

			r += 12;
		}
  }
  else
  {
		for (j = 0; j < m; j++) {
			unsigned long reg;

			HLL_DENSE_GET_REGISTER(reg, hll->M, j);
			if (reg == 0)
				ez++;
			else
			{
//				elog(LOG, "M[%d] = %d, E += %.3f", j, reg, PE[reg]);
				E += PE[reg]; /* precomputed 2^(-reg[j]) */
			}
		}
		E += ez;
  }

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

  const uint8 *data = (const uint8 *) key;
  const uint8 *end = data + (keysize - (keysize & 7));
  uint64 h = MURMUR_SEED ^ (keysize * m);

  while(data != end)
  {
		uint64 k = *((uint64 *) data);

		k *= m;
		k ^= k >> r;
		k *= m;

		h ^= k;
		h *= m;
		data += 8;
  }

  switch(keysize & 7)
  {
		case 7: h ^= (uint64) data[6] << 48;
		case 6: h ^= (uint64) data[5] << 40;
		case 5: h ^= (uint64) data[4] << 32;
		case 4: h ^= (uint64) data[3] << 24;
		case 3: h ^= (uint64) data[2] << 16;
		case 2: h ^= (uint64) data[1] << 8;
		case 1: h ^= (uint64) data[0];
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
		return hll_dense_add(hll, elem, len, result);
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
			PE[j] = 1.0 / (1ULL << j);
		}
		initialized = true;
  }

  if (HLL_IS_DENSE(hll))
		E = hll_dense_sum(hll, PE, &ez);
  else
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
  else if (m == (1 << 14) && E < 72000)
  {
		/*
		 * We did polynomial regression of the bias for this range, this
		 * way we can compute the bias for a given cardinality and correct
		 * according to it. Only apply the correction for P=14 that's what
		 * we use and the value the correction was verified with.
		 */
		double bias = 5.9119 * 1.0e-18 * (E * E * E * E) -
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
