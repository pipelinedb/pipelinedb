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
 *--
 *
 * Portions Copyright (c) 2014, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *-------------------------------------------------------------------------
 *
 * Portions Copyright (c) 2013-2015, PipelineDB
 *
 */
#include <math.h>

#include "pipeline/hll.h"
#include "pipeline/miscutils.h"
#include "utils/elog.h"
#include "utils/palloc.h"

#include "utils/memutils.h"

#define HLL_USE_EXPLICIT 1

#define HLL_DEFAULT_P 14
#define HLL_BITS_PER_REGISTER 6
#define HLL_REGISTER_MAX ((1 << HLL_BITS_PER_REGISTER) - 1)
#define HLL_SPARSE_VAL_MAX_LEN 4

/* Sparse representation
 * ===
 *
 * The sparse representation encodes registers using a run length
 * encoding composed of three opcodes, two using one byte, and one using
 * of two bytes. The opcodes are called ZERO, XZERO and VAL.
 *
 * ZERO opcode is represented as 00xxxxxx. The 6-bit integer represented
 * by the six bits 'xxxxxx', plus 1, means that there are N registers set
 * to 0. This opcode can represent from 1 to 64 contiguous registers set
 * to the value of 0.
 *
 * XZERO opcode is represented by two bytes 01xxxxxx yyyyyyyy. The 14-bit
 * integer represented by the bits 'xxxxxx' as most significant bits and
 * 'yyyyyyyy' as least significant bits, plus 1, means that there are N
 * registers set to 0. This opcode can represent from 0 to 16384 contiguous
 * registers set to the value of 0.
 *
 * VAL opcode is represented as 1vvvvvxx. It contains a 5-bit integer
 * representing the value of a register, and a 2-bit integer representing
 * the number of contiguous registers set to that value 'vvvvv'.
 * To obtain the value and run length, the integers vvvvv and xx must be
 * incremented by one. This opcode can represent values from 1 to 32,
 * repeated from 1 to 4 times.
 *
 * The sparse representation can't represent registers with a value greater
 * than 32, however it is very unlikely that we find such a register in an
 * HLL with a cardinality where the sparse representation is still more
 * memory efficient than the dense representation. When this happens the
 * HLL is converted to the dense representation.
 *
 * The sparse representation is purely positional. For example a sparse
 * representation of an empty HLL is just: XZERO:16384.
 *
 * An HLL having only 3 non-zero registers at position 1000, 1020, 1021
 * respectively set to 2, 3, 3, is represented by the following three
 * opcodes:
 *
 * XZERO:1000 (Registers 0-999 are set to 0)
 * VAL:2,1    (1 register set to value 2, that is register 1000)
 * ZERO:19    (Registers 1001-1019 set to 0)
 * VAL:3,2    (2 registers set to value 3, that is registers 1020,1021)
 * XZERO:15362 (Registers 1022-16383 set to 0)
 *
 * --
 *
 * Dense representation
 * ===
 *
 * The dense representation is as follows:
 *
 * +--------+--------+--------+------//      //--+
 * |11000000|22221111|33333322|55444444 ....     |
 * +--------+--------+--------+------//      //--+
 *
 * The 6 bit registers are encoded one after the other starting from the
 * LSB to the MSB, and using the next bytes as needed.
 *
 *
 * =========================== Low level bit macros =========================
 *
 *
 * Macros to access the dense representation.
 *
 * We need to get and set 6 bit counters in an array of 8 bit bytes.
 * We use macros to make sure the code is inlined since speed is critical
 * especially in order to compute the approximated cardinality in
 * HLLCOUNT where we need to access all the registers at once.
 * For the same reason we also want to avoid conditionals in this code path.
 *
 * +--------+--------+--------+------//
 * |11000000|22221111|33333322|55444444
 * +--------+--------+--------+------//
 *
 * Note: in the above representation the most significant bit (MSB)
 * of every byte is on the left. We start using bits from the LSB to MSB,
 * and so forth passing to the next byte.
 *
 * Example, we want to access to counter at pos = 1 ("111111" in the
 * illustration above).
 *
 * The index of the first byte b0 containing our data is:
 *
 *  b0 = 6 * pos / 8 = 0
 *
 *   +--------+
 *   |11000000|  <- Our byte at b0
 *   +--------+
 *
 * The position of the first bit (counting from the LSB = 0) in the byte
 * is given by:
 *
 *  fb = 6 * pos % 8 -> 6
 *
 * Right shift b0 of 'fb' bits.
 *
 *   +--------+
 *   |11000000|  <- Initial value of b0
 *   |00000011|  <- After right shift of 6 pos.
 *   +--------+
 *
 * Left shift b1 of bits 8-fb bits (2 bits)
 *
 *   +--------+
 *   |22221111|  <- Initial value of b1
 *   |22111100|  <- After left shift of 2 bits.
 *   +--------+
 *
 * OR the two bits, and finally AND with 111111 (63 in decimal) to
 * clean the higher order bits we are not interested in:
 *
 *   +--------+
 *   |00000011|  <- b0 right shifted
 *   |22111100|  <- b1 left shifted
 *   |22111111|  <- b0 OR b1
 *   |  111111|  <- (b0 OR b1) AND 63, our value.
 *   +--------+
 *
 * We can try with a different example, like pos = 0. In this case
 * the 6-bit counter is actually contained in a single byte.
 *
 *  b0 = 6 * pos / 8 = 0
 *
 *   +--------+
 *   |11000000|  <- Our byte at b0
 *   +--------+
 *
 *  fb = 6 * pos % 8 = 0
 *
 *  So we right shift of 0 bits (no shift in practice) and
 *  left shift the next byte of 8 bits, even if we don't use it,
 *  but this has the effect of clearing the bits so the result
 *  will not be affacted after the OR.
 *
 * --
 *
 * Read the value of the register at position regnum into variable target.
 * regs is an array of unsigned bytes.
 */

#define HLL_DENSE_GET_REGISTER(target, regs, regnum) do { \
	uint8 *_p = (uint8 *) regs; \
	unsigned long _byte = regnum * HLL_BITS_PER_REGISTER / 8; \
	unsigned long _fb = regnum * HLL_BITS_PER_REGISTER & 7; \
	unsigned long _fb8 = 8 - _fb; \
	unsigned long b0 = _p[_byte]; \
	unsigned long b1 = _p[_byte + 1]; \
	target = ((b0 >> _fb) | (b1 << _fb8)) & HLL_REGISTER_MAX; \
} while(0)

/* -------------------------------------------------------------------------
 *
 * Setting the register is a bit more complex, let's assume that 'val'
 * is the value we want to set, already in the right range.
 *
 * We need two steps, in one we need to clear the bits, and in the other
 * we need to bitwise-OR the new bits.
 *
 * Let's try with 'pos' = 1, so our first byte at 'b' is 0,
 *
 * "fb" is 6 in this case.
 *
 *   +--------+
 *   |11000000|  <- Our byte at b0
 *   +--------+
 *
 * To create a AND-mask to clear the bits about this position, we just
 * initialize the mask with the value 63, left shift it of "fs" bits,
 * and finally invert the result.
 *
 *   +--------+
 *   |00111111|  <- "mask" starts at 63
 *   |11000000|  <- "mask" after left shift of "ls" bits.
 *   |00111111|  <- "mask" after invert.
 *   +--------+
 *
 * Now we can bitwise-AND the byte at "b" with the mask, and bitwise-OR
 * it with "val" left-shifted of "ls" bits to set the new bits.
 *
 * Now let's focus on the next byte b1:
 *
 *   +--------+
 *   |22221111|  <- Initial value of b1
 *   +--------+
 *
 * To build the AND mask we start again with the 63 value, right shift
 * it by 8-fb bits, and invert it.
 *
 *   +--------+
 *   |00111111|  <- "mask" set at 2&6-1
 *   |00001111|  <- "mask" after the right shift by 8-fb = 2 bits
 *   |11110000|  <- "mask" after bitwise not.
 *   +--------+
 *
 * Now we can mask it with b+1 to clear the old bits, and bitwise-OR
 * with "val" left-shifted by "rs" bits to set the new value.
 *
 * --
 *
 * Set the value of the register at position regnum to val.
 * regs is an array of unsigned bytes.
 */
#define HLL_DENSE_SET_REGISTER(regs, regnum, val) do { \
	uint8 *_p = (uint8 *) regs; \
	unsigned long _byte = regnum * HLL_BITS_PER_REGISTER / 8; \
	unsigned long _fb = regnum * HLL_BITS_PER_REGISTER & 7; \
	unsigned long _fb8 = 8 - _fb; \
	unsigned long _v = val; \
	_p[_byte] &= ~(HLL_REGISTER_MAX << _fb); \
	_p[_byte] |= _v << _fb; \
	_p[_byte + 1] &= ~(HLL_REGISTER_MAX >> _fb8); \
	_p[_byte + 1] |= _v >> _fb8; \
} while(0)


#define HLL_SPARSE_XZERO_BIT 0x40 /* 01xxxxxx */
#define HLL_SPARSE_IS_XZERO(p) (((*(p)) & 0xc0) == HLL_SPARSE_XZERO_BIT)
#define HLL_SPARSE_XZERO_MAX_LEN (1 << 14) /* 16384 */
#define HLL_SPARSE_XZERO_LEN(p) (((((*(p)) & 0x3f) << 8) | (*((p) + 1))) + 1)
#define HLL_SPARSE_XZERO_SET(p, len) do { \
	int _l = (len) - 1; \
	*(p) = (_l >> 8) | HLL_SPARSE_XZERO_BIT; \
	*((p) + 1) = (_l & 0xff); \
} while(0)

#define HLL_SPARSE_IS_ZERO(p) (((*(p)) & 0xc0) == 0) /* 00xxxxxx */
#define HLL_SPARSE_ZERO_LEN(p) (((*(p)) & 0x3f) + 1)
#define HLL_SPARSE_ZERO_SET(p, len) do { \
	*(p) = (len)-1; \
} while(0)

#define HLL_SPARSE_VAL_BIT 0x80 /* 1vvvvvxx */
#define HLL_SPARSE_IS_VAL(p) ((*(p)) & HLL_SPARSE_VAL_BIT)
#define HLL_SPARSE_VAL_LEN(p) (((*(p)) & 0x3) + 1)
#define HLL_SPARSE_VAL_VALUE(p) ((((*(p)) >> 2) & 0x1f) + 1)
#define HLL_SPARSE_VAL_SET(p,val,len) do { \
	*(p) = (((val) - 1) << 2 | ((len) - 1)) | HLL_SPARSE_VAL_BIT; \
} while(0)

/*
 * Explicit representation
 * ===
 *
 * The explicit representations encodes the registers explicitly in a sorted array. The array consists
 * on contiguous 32-bit structures, where the most significant 24 bits encode the register number and
 * the least significant 8 bits encode the number of leading zeros in that register.
 *
 */
#define HLL_EXPLICIT_GET_NUM_LEADING(p) (*(uint32 *) (p) & 0x000000ff)
#define HLL_EXPLICIT_GET_REGISTER(p) ((*(uint32 *) (p) >> 8) & 0x00ffffff)
#define HLL_EXPLICIT_ENTRY_SIZE sizeof(uint32)
#define HLL_EXPLICIT_SET_NUM_LEADING(p, val) do { \
	*(uint32 *) (p) = (*(uint32 *) (p) & 0xffffff00) | ((val) & 0x000000ff); \
} while(0)
#define HLL_EXPLICIT_SET_REGISTER(p, reg) do { \
	*((uint32 *) (p)) |= ((reg) << 8) & 0xffffff00; \
} while(0)

#define HLL_IS_DENSE(hll) ((hll)->encoding == HLL_DENSE_DIRTY || (hll)->encoding == HLL_DENSE_CLEAN)
#define HLL_IS_CLEAN(hll) ((hll)->encoding == HLL_DENSE_CLEAN || (hll)->encoding == HLL_SPARSE_CLEAN || (hll)->encoding == HLL_EXPLICIT_CLEAN)

#define MURMUR_SEED 0xbee5bf4112801383L

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
  dense->encoding = HLL_IS_CLEAN(sparse) ? HLL_DENSE_CLEAN : HLL_DENSE_DIRTY;
  dense->mlen = m;

  /*
   * Read the sparse representation and set non-zero registers
   * accordingly.
   */
  while (pos < end)
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
				HLL_DENSE_SET_REGISTER(dense->M, idx, regval);
				idx++;
			}
			pos++;
		}
  }

  return dense;
}

/*
 * Returns the number of leading zeroes for the hash code of the
 * given element. The value of m is set to the register
 */
static uint8
num_leading_zeroes(HyperLogLog *hll, void *elem, Size size, int *m)
{
	uint64 h = MurmurHash3_64(elem, size, MURMUR_SEED);
	uint64 index;
	uint64 bit;
	uint8 count = 0;
	int numregs = (1 << hll->p);
	int mask = (numregs - 1);

	/* register index is the first p bits of the hash */
  index = h & mask;
  h |= ((uint64) 1 << 63);
  bit = numregs;
  count = 1;

  while ((h & bit) == 0) {
		count++;
		bit <<= 1;
  }

  *m = (int) index;

	return count;
}

static HyperLogLog *
hll_dense_add_internal(HyperLogLog *hll, int m, uint8 leading, int *result)
{
  uint8 oldleading;

  HLL_DENSE_GET_REGISTER(oldleading, hll->M, m);
  if (leading > oldleading)
  {
		HLL_DENSE_SET_REGISTER(hll->M, m, leading);
		*result = 1;
  }
  else
		*result = 0;

  return hll;
}

static HyperLogLog *
hll_dense_add(HyperLogLog *hll, void *elem, Size size, int *result)
{
	int m;
	uint8 leading = num_leading_zeroes(hll, elem, size, &m);
	return hll_dense_add_internal(hll, m, leading, result);
}

static HyperLogLog *
hll_sparse_add_internal(HyperLogLog *hll, int m, uint8 leading, int *result, bool realloc)
{
	uint8 oldleading;
	uint8 *sparse;
	uint8 *end;
	uint8 *pos;
	uint8 *next;
	uint8 *prev;
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
  int scanlen;

	if (leading > 32)
	{
		/* the sparse representation can only represent 32-bit cardinalities */
		hll = hll_sparse_to_dense(hll);
		return hll_dense_add_internal(hll, m, leading, result);
	}


	/*
	 * When updating a sparse representation, we may need to enlarge
	 * the buffer by 3 bytes in the worst case (XZERO split into XZERO-VAL-XZERO)
	 *
	 * In case of upgrading an explicit to sparse representation, we already preallocate enough space that
	 * we never need to perform this reallocation.
	 */
	if (realloc)
		hll = repalloc(hll, HLLSize(hll) + 3);

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
  prev = NULL; /* points to previous opcode at the end of the loop. */
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

		prev = pos;
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
			goto updated;
		}
  }

  /*
   * C) Another trivial case is a ZERO opcode with a len of 1.
   * We can just replace it with a VAL opcode with our value and len of 1.
   */
  if (iszero && runlen == 1)
  {
		HLL_SPARSE_VAL_SET(pos, leading, 1);
		goto updated;
  }

  /*
   * D) General case
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
		return hll_dense_add_internal(hll, m, leading, result);
	}

  /* we already repalloc'd enough space to do this */
	if (deltalen && next)
	 memmove(next + deltalen, next, end - next);

	memcpy(pos, seq, seqlen);
	hll->mlen += deltalen;
	end += deltalen;

updated:
	/*
	 * Step 4: Merge adjacent values if possible.
	 *
	 * The representation was updated, however the resulting representation
	 * may not be optimal: adjacent VAL opcodes can sometimes be merged into
	 * a single one.
	 */
	pos = prev ? prev : sparse;
	scanlen = 5; /* Scan up to 5 upcodes starting from prev. */
	while (pos < end && scanlen--)
	{
		if (HLL_SPARSE_IS_XZERO(pos))
		{
			pos += 2;
			continue;
		}
		else if (HLL_SPARSE_IS_ZERO(pos))
		{
			pos++;
			continue;
		}

		/*
		 * We need two adjacent VAL opcodes to try a merge, having
		 * the same value, and a len that fits the VAL opcode max len.
		 */
		if (pos + 1 < end && HLL_SPARSE_IS_VAL(pos + 1))
		{
			int v1 = HLL_SPARSE_VAL_VALUE(pos);
			int v2 = HLL_SPARSE_VAL_VALUE(pos + 1);

			if (v1 == v2)
			{
				int len = HLL_SPARSE_VAL_LEN(pos) + HLL_SPARSE_VAL_LEN(pos + 1);

				if (len <= HLL_SPARSE_VAL_MAX_LEN)
				{
					HLL_SPARSE_VAL_SET(pos + 1, v1, len);
					memmove(pos, pos + 1, end - pos);
					hll->mlen--;
					end--;

					/*
					 * After a merge we reiterate without incrementing 'p'
					 * in order to try to merge the just merged value with
					 * a value on its right.
					 */
					continue;
				}
			}
		}

		pos++;
	}

	hll->encoding = HLL_SPARSE_DIRTY;
	*result = 1;
	return hll;
}

/*
 * hll_sparse_add
 *
 * Adds an element to the given HLL using the sparse representation
 */
static HyperLogLog *
hll_sparse_add(HyperLogLog *hll, void *elem, Size size, int *result)
{
	int m;
	uint8 leading = num_leading_zeroes(hll, elem, size, &m);
	return hll_sparse_add_internal(hll, m, leading, result, true);
}

static HyperLogLog *
hll_explicit_to_sparse(HyperLogLog *hll)
{
	uint8 *pos, *end;
	int m = 1 << hll->p;
	int result;
	int mlen = Max(2, 2 * m / HLL_SPARSE_XZERO_MAX_LEN);
	/*
	 * In worst case we'll need 1 XZERO and 1 VAL opcodes for each register and a terminal XZERO
	 * for each register we're going to insert.
	 */
	HyperLogLog *sparse = palloc(sizeof(HyperLogLog) + (3 * HLL_EXPLICIT_GET_NUM_REGISTERS(hll) + 2));

	/* Initialize an *empty* sparse HLL */
	sparse->p = hll->p;
	sparse->encoding = HLL_SPARSE_CLEAN;
	sparse->mlen = mlen;

	pos = sparse->M;

	while (m)
	{
		int xzero = Min(HLL_SPARSE_XZERO_MAX_LEN, m);
		HLL_SPARSE_XZERO_SET(pos, xzero);
		pos += 2;
		m -= xzero;
	}

	/* Insert all non-zero registers into the sparse HLL */
	pos = hll->M;
	end = hll->M + hll->mlen;

	while (pos < end)
	{
		/*
		 * Pass the realloc flag as false, so we don't needlessly keep on reallocating
		 * on each addition
		 */
		sparse = hll_sparse_add_internal(sparse, HLL_EXPLICIT_GET_REGISTER(pos),
				HLL_EXPLICIT_GET_NUM_LEADING(pos), &result, false);
		pos += HLL_EXPLICIT_ENTRY_SIZE;
	}

	return sparse;
}

static HyperLogLog *
hll_explicit_add_internal(HyperLogLog *hll, int reg, uint8 leading, int *result)
{
	uint8 *pos = hll->M;
	uint8 *end = hll->M + hll->mlen;
	bool found = false;
	int skipped = 0;

	Assert(hll->mlen % 4 == 0);

	if (HLL_EXPLICIT_GET_NUM_REGISTERS(hll) >= HLL_MAX_EXPLICIT_REGISTERS)
	{
		hll = hll_explicit_to_sparse(hll);
		return hll_sparse_add_internal(hll, reg, leading, result, true);
	}

	/*
	 * We store explicit registers in sorted order, so use an insertion sort like procedure to find the
	 * register we're looking for.
	 */
	while (pos < end)
	{
		int curr_reg = HLL_EXPLICIT_GET_REGISTER(pos);

		if (curr_reg < reg)
		{
			pos += HLL_EXPLICIT_ENTRY_SIZE;
			skipped++;
			continue;
		}

		if (curr_reg == reg)
		{
			if (leading <= HLL_EXPLICIT_GET_NUM_LEADING(pos))
			{
				*result = 0;
				return hll;
			}

			HLL_EXPLICIT_SET_NUM_LEADING(pos, leading);
			found = true;
			break;
		}

		/* Register needs to be added. */
		if (MemoryContextContains(CurrentMemoryContext, hll))
			hll = repalloc(hll, HLLSize(hll) + 4);
		else
		{
			HyperLogLog *new = palloc(HLLSize(hll) + 4);
			memcpy(new, hll, HLLSize(hll));
			hll = new;
		}
		pos = hll->M + (HLL_EXPLICIT_ENTRY_SIZE * skipped);
		end = hll->M + hll->mlen;
		memmove(pos + HLL_EXPLICIT_ENTRY_SIZE, pos, end - pos);
		*(uint32 *) pos = 0;
		HLL_EXPLICIT_SET_REGISTER(pos, reg);
		HLL_EXPLICIT_SET_NUM_LEADING(pos, leading);
		hll->mlen += HLL_EXPLICIT_ENTRY_SIZE;
		found = true;
		break;
	}

	/* Didn't find a place to put the register? Stick it at the end. */
	if (!found)
	{
		if (MemoryContextContains(CurrentMemoryContext, hll))
			hll = repalloc(hll, HLLSize(hll) + HLL_EXPLICIT_ENTRY_SIZE);
		else
		{
			HyperLogLog *new = palloc(HLLSize(hll) + HLL_EXPLICIT_ENTRY_SIZE);
			memcpy(new, hll, HLLSize(hll));
			hll = new;
		}
		end = hll->M + hll->mlen;
		*(uint32 *) end = 0;
		HLL_EXPLICIT_SET_REGISTER(end, reg);
		HLL_EXPLICIT_SET_NUM_LEADING(end, leading);
		hll->mlen += HLL_EXPLICIT_ENTRY_SIZE;
	}

	hll->encoding = HLL_EXPLICIT_DIRTY;
	*result = 1;

	return hll;
}

static HyperLogLog *
hll_explicit_add(HyperLogLog *hll, void *elem, Size size, int *result)
{
	int m;
	uint8 leading = num_leading_zeroes(hll, elem, size, &m);
	return hll_explicit_add_internal(hll, m, leading, result);
}

static double
hll_explicit_sum(HyperLogLog *hll, double *PE, int *ezp)
{
	int m = 1 << hll->p;
	int ez = m - HLL_EXPLICIT_GET_NUM_REGISTERS(hll);
	double E = 0;
	uint8 *pos = hll->M;
	uint8 *end = hll->M + hll->mlen;

	while (pos < end)
	{
		E += PE[HLL_EXPLICIT_GET_NUM_LEADING(pos)];
		pos += HLL_EXPLICIT_ENTRY_SIZE;
	}

	E += ez;
	*ezp = ez;

	return E;
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

  if (m == (1 << 14) && HLL_BITS_PER_REGISTER == 6)
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
				E += PE[reg]; /* precomputed 2^(-reg[j]) */
		}
		E += ez;
  }

  *ezp = ez;

  return E;
}

/*
 * HLLCreate
 *
 * Create an empty HyperLogLog structure with the given p
 */
HyperLogLog *
HLLCreateWithP(int p)
{
	/* m = number of registers */
	int m = 1 << p;
	int remaining = m;
	Size size;
	HyperLogLog *hll;
	uint8 *pos;
	int mlen = 0;

	/*
	 * If HLL_USE_EXPLICIT is on, we initialize an empty HLL in the EXPLICIT
	 * representation, otherwise we initialize an empty HLL in the SPARSE representation.
	 */
	if (!HLL_USE_EXPLICIT)
	{
		/*
		 * We can represent 1 << 14 registers with every two bytes
		 * of the sparse representation.
		 */
		mlen = Max(2, 2 * m / HLL_SPARSE_XZERO_MAX_LEN);
	}


	size = sizeof(HyperLogLog) + mlen;

	hll = palloc0(size);
	hll->p = p;
	hll->encoding = HLL_SPARSE_CLEAN;
	hll->mlen = mlen;

	if (HLL_USE_EXPLICIT)
		hll->encoding = HLL_EXPLICIT_CLEAN;
	else
	{
		hll->encoding = HLL_SPARSE_CLEAN;
		pos = hll->M;

		while (remaining)
		{
			int xzero = Min(HLL_SPARSE_XZERO_MAX_LEN, remaining);
			HLL_SPARSE_XZERO_SET(pos, xzero);
			pos += 2;
			remaining -= xzero;
		}
	}

	SET_VARSIZE(hll, HLLSize(hll));

	return hll;
}

/*
 * HLLCreate
 *
 * Create an empty HyperLogLog structure with a default p
 */
HyperLogLog *
HLLCreate(void)
{
	return HLLCreateWithP(HLL_DEFAULT_P);
}

/*
 * HLLCopy
 *
 * Creates a copy of the given HLL
 */
HyperLogLog *
HLLCopy(HyperLogLog *src)
{
	Size size = HLLSize(src);
	HyperLogLog *new = palloc(size);
	memcpy((char *) new, (char *) src, size);
	return new;
}

/*
 * HLLAdd
 *
 * Adds an element to the given HLL
 */
HyperLogLog *
HLLAdd(HyperLogLog *hll, void *elem, Size len, int *result)
{
	HyperLogLog *ret;

	if (HLL_IS_EXPLICIT(hll))
		ret = hll_explicit_add(hll, elem, len, result);
	else if (HLL_IS_SPARSE(hll))
		ret = hll_sparse_add(hll, elem, len, result);
	else
		ret = hll_dense_add(hll, elem, len, result);

	/* if the cardinality changed, invalidate the cached cardinality */
	if (*result)
		ret->encoding =	HLL_IS_SPARSE(ret) ? HLL_SPARSE_DIRTY :
				(HLL_IS_DENSE(ret) ? HLL_DENSE_DIRTY : HLL_EXPLICIT_DIRTY);

	SET_VARSIZE(hll, HLLSize(hll));

	return ret;
}

/*
 * HLLCardinality
 *
 * Returns the cardinality of the given HLL
 */
uint64
HLLCardinality(HyperLogLog *hll)
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

  /*
   * XXX(derekjn) This function should accept a (HyperLogLog **) that we can store
   * this new pointer in for the caller, since it will likely want to use the new HLL.
   */
  if (HLL_IS_UNPACKED(hll))
		hll = HLLPack(hll);

  /*
   * If nothing has changed since the last cardinality computation,
   * we can just use the last result
   */
  if (HLL_IS_CLEAN(hll))
		return hll->card;

  if (HLL_IS_DENSE(hll))
  {
		E = hll_dense_sum(hll, PE, &ez);
		hll->encoding = HLL_DENSE_CLEAN;
  }
  else if (HLL_IS_SPARSE(hll))
  {
		E = hll_sparse_sum(hll, PE, &ez);
		hll->encoding = HLL_SPARSE_CLEAN;
  }
  else
  {
		E = hll_explicit_sum(hll, PE, &ez);
		hll->encoding = HLL_EXPLICIT_CLEAN;
  }

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
  hll->card = (uint64) E;

  return hll->card;
}

static HyperLogLog *
hll_dense_union(HyperLogLog *hllu, HyperLogLog *incoming)
{
	int reg;
	int m = (1 << hllu->p);

	Assert(HLL_IS_UNPACKED(hllu));

	if (HLL_IS_UNPACKED(incoming))
	{
		/* easy, just take the max of each HLL's registers */
		for (reg=0; reg<m; reg++)
		{
			uint8 v = incoming->M[reg];

			if (v > hllu->M[reg])
				hllu->M[reg] = v;
		}
	}
	else if (HLL_IS_DENSE(incoming))
	{
		/* easy, just take the max of each HLL's registers */
		for (reg=0; reg<m; reg++)
		{
			uint8 v;

			HLL_DENSE_GET_REGISTER(v, incoming->M, reg);
			if (v > hllu->M[reg])
				hllu->M[reg] = v;
		}
	}
	else if (HLL_IS_SPARSE(incoming))
	{
		/* run-length encoded, read every non-zero register value */
		uint8 *pos = incoming->M;
		uint8 *end = pos + incoming->mlen;
		uint8 regval;
		long runlen;

		reg = 0;
		while (pos < end)
		{
			if (HLL_SPARSE_IS_ZERO(pos))
			{
				runlen = HLL_SPARSE_ZERO_LEN(pos);
				reg += runlen;
				pos++;
			}
			else if (HLL_SPARSE_IS_XZERO(pos))
			{
				runlen = HLL_SPARSE_XZERO_LEN(pos);
				reg += runlen;
				pos += 2;
			}
			else
			{
				runlen = HLL_SPARSE_VAL_LEN(pos);
				regval = HLL_SPARSE_VAL_VALUE(pos);
				while (runlen--)
				{
					if (regval > hllu->M[reg])
						hllu->M[reg] = regval;
					reg++;
				}
				pos++;
			}
		}
	}
	else
	{
		/* explicit encoding, iterate through all registers */
		uint8 *pos = incoming->M;
		uint8 *end = incoming->M + incoming->mlen;

		while (pos < end)
		{
			int reg = HLL_EXPLICIT_GET_REGISTER(pos);
			uint8 r0 = HLL_EXPLICIT_GET_NUM_LEADING(pos);

			if (r0 > hllu->M[reg])
				hllu->M[reg] = r0;

			pos += HLL_EXPLICIT_ENTRY_SIZE;
		}
	}

	SET_VARSIZE(hllu, HLLSize(hllu));

	return hllu;
}

static HyperLogLog *
hll_explicit_union(HyperLogLog *result, HyperLogLog *incoming)
{
	uint8 *pos = incoming->M;
	uint8 *end = incoming->M + incoming->mlen;
	int r;

	Assert(HLL_IS_EXPLICIT(result) && HLL_IS_EXPLICIT(incoming));

	while (pos < end)
	{
		int reg = HLL_EXPLICIT_GET_REGISTER(pos);
		uint8 leading = HLL_EXPLICIT_GET_NUM_LEADING(pos);

		/*
		 * The result HLL can upgrade during these additions so we need to consider
		 * all denser representations.
		  */
		if (HLL_IS_EXPLICIT(result))
			result = hll_explicit_add_internal(result, reg, leading, &r);
		else if (HLL_IS_SPARSE(result))
			result = hll_sparse_add_internal(result, reg, leading, &r, true);
		else
			result = hll_dense_add_internal(result, reg, leading, &r);

		pos += HLL_EXPLICIT_ENTRY_SIZE;
	}

	return result;
}

static HyperLogLog *
hll_sparse_union(HyperLogLog *result, HyperLogLog *incoming)
{
	uint8 *pos = incoming->M;
	uint8 *end = incoming->M + incoming->mlen;
	int r;

	Assert(HLL_IS_SPARSE(result) && HLL_IS_EXPLICIT(incoming));

	while (pos < end)
	{
		int reg = HLL_EXPLICIT_GET_REGISTER(pos);
		uint8 leading = HLL_EXPLICIT_GET_NUM_LEADING(pos);

		/*
		 * The result HLL can upgrade during these additions so we need to consider
		 * all denser representations.
		  */
		if (HLL_IS_SPARSE(result))
			result = hll_sparse_add_internal(result, reg, leading, &r, true);
		else
			result = hll_dense_add_internal(result, reg, leading, &r);

		pos += HLL_EXPLICIT_ENTRY_SIZE;
	}

	return result;
}

/*
 * HLLUnion
 *
 * Returns the lossless union of multiple HyperLogLogs
 *
 * This function can be potentially slow, but is optimized to run fast
 * for continuous queries and will try to upgrade to denser representations
 * as lazily as possible.
 */
HyperLogLog *
HLLUnion(HyperLogLog *result, HyperLogLog *incoming)
{
	HyperLogLog *hllu;

	/* EXPLICIT + EXPLICIT */
	if (HLL_IS_EXPLICIT(result) && HLL_IS_EXPLICIT(incoming))
	{
		result = hll_explicit_union(result, incoming);
		SET_VARSIZE(result, HLLSize(result));

		return result;
	}

	/* SPARSE + EXPLICIT */
	/* TODO(usmanm): Add SPARSE || SPARSE support */
	if (HLL_IS_SPARSE(result) && HLL_IS_EXPLICIT(incoming))
	{
		result = hll_sparse_union(result, incoming);
		SET_VARSIZE(result, HLLSize(result));

		return result;
	}

	hllu = HLLUnpack(result);

	/* DENSE + (DENSE | SPARSE | EXPLICIT) */
	hllu = HLLUnionAdd(hllu, incoming);
	result = HLLPack(hllu);

	return result;
}

/*
 * HLLUnpack
 */
HyperLogLog *
HLLUnpack(HyperLogLog *initial)
{
	int reg;
	int m = (1 << initial->p);
	HyperLogLog *result = palloc0(sizeof(HyperLogLog) + m);

	/*
	 * We don't support any other unions for now, so just upgrade result
	 * to the DENSE representation.
	 */
	if (HLL_IS_UNPACKED(initial))
		return initial;

	if (HLL_IS_EXPLICIT(initial))
		initial = hll_explicit_to_sparse(initial);
	if (HLL_IS_SPARSE(initial))
		initial = hll_sparse_to_dense(initial);

	result->encoding = HLL_UNPACKED;
	result->mlen = m;
	result->p = initial->p;

	for (reg = 0; reg < m; reg++)
	{
		uint8 v;
		HLL_DENSE_GET_REGISTER(v, initial->M, reg);
		result->M[reg] = v;
	}

	SET_VARSIZE(result, HLLSize(result));

	return result;
}

/*
 * HLLPack
 */
HyperLogLog *
HLLPack(HyperLogLog *hllu)
{
	int reg;
	int m = (((1 << hllu->p) * HLL_BITS_PER_REGISTER) / 8);
	HyperLogLog *result = palloc0(sizeof(HyperLogLog) + m);

	result->encoding = HLL_DENSE_DIRTY;
	result->p = hllu->p;
	result->mlen = m;

	for (reg = 0; reg < hllu->mlen; reg++)
	{
		HLL_DENSE_SET_REGISTER(result->M, reg, hllu->M[reg]);
	}

	SET_VARSIZE(result, HLLSize(result));

	return result;
}

/*
 * HLLUnionAdd
 */
HyperLogLog *
HLLUnionAdd(HyperLogLog *hllu, HyperLogLog *incoming)
{
	if (!HLL_IS_UNPACKED(hllu))
		hllu = HLLUnpack(hllu);

	return hll_dense_union(hllu, incoming);
}
