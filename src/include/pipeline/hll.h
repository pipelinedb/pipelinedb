/*-------------------------------------------------------------------------
 *
 * hll.h
 *	  Interface for HyperLogLog support
 *
 *
 * src/include/pipeline/hll.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_HLL_H
#define PIPELINE_HLL_H

#include "c.h"

#define MSB_64 (1UL << 63)
#define HLL_MAX_SPARSE_BYTES 11000
#define HLL_BITS 6 /* Enough to count up to 63 leading zeroes. */
#define HLL_REGISTER_MAX ((1 << HLL_BITS) - 1)

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
	unsigned long _byte = regnum * HLL_BITS / 8; \
	unsigned long _fb = regnum * HLL_BITS & 7; \
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
	unsigned long _byte = regnum * HLL_BITS / 8; \
	unsigned long _fb = regnum * HLL_BITS & 7; \
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

#define HLL_SPARSE_DIRTY 's'
#define HLL_SPARSE_CLEAN 'S'
#define HLL_DENSE_DIRTY 'd'
#define HLL_DENSE_CLEAN 'D'

#define HLL_IS_SPARSE(hll) ((hll)->encoding == HLL_SPARSE_DIRTY || (hll)->encoding == HLL_SPARSE_CLEAN)
#define HLL_IS_DENSE(hll) ((hll)->encoding == HLL_DENSE_DIRTY || (hll)->encoding == HLL_DENSE_CLEAN)

#define MURMUR_SEED 0xadc83b19ULL
#define HLL_SIZE(hll) (sizeof(HyperLogLog) + (hll)->mlen)

#define HLL_BITS_PER_REGISTER 6

typedef struct HyperLogLog {
	/* Dense or sparse, dirty or clean? See above */
  char encoding;
  /*
   * Last computed cardinality, can be reused until new data is added.
   * That is, if the encoding is *_CLEAN.
   */
  long card;
  /* number of leading bits of hash values to use for determining register */
  uint8 p;
  /* number of bytes allocated for M */
  int mlen;
  /* substream registers */
  uint8 M[1];
} HyperLogLog;

uint64 MurmurHash64A(const void *key, Size keysize);
uint64 HLLSize(HyperLogLog *hll);
HyperLogLog *HLLAdd(HyperLogLog *hll, void *elem, Size len, int *result);
HyperLogLog *HLLCreate(int p);

#endif
