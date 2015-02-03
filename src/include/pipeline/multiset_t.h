/*-------------------------------------------------------------------------
 *
 * multiset_t.h
 *	  Declarations for HyperLogLog data type support.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/pipeline/multiset_t.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef MULTISET_T_H
#define MULTISET_T_H

#include "executor/tuptable.h"

void MurmurHash3_128 ( const void * key, int len, uint32_t seed, void * out );

typedef struct
{
    size_t		mse_nelem;
    uint64_t	mse_elems[0];

} ms_explicit_t;

// Defines the *unpacked* register.
typedef uint8_t compreg_t;

typedef struct
{
    compreg_t	msc_regs[0];

} ms_compressed_t;

// Size of the compressed or explicit data.
#define MS_MAXDATA		(128 * 1024)

typedef struct
{
    size_t		ms_nbits;
    size_t		ms_nregs;
    size_t		ms_log2nregs;
    int64		ms_expthresh;
    bool		ms_sparseon;

	uint64_t	ms_type;	// size is only for alignment.

    union
    {
        // MST_EMPTY and MST_UNDEFINED don't need data.
        // MST_SPARSE is only used in the packed format.
        //
        ms_explicit_t	as_expl;	// MST_EXPLICIT
        ms_compressed_t	as_comp;	// MST_COMPRESSED
        uint8_t			as_size[MS_MAXDATA];	// sizes the union.

    }		ms_data;

} multiset_t;

uint8_t multiset_unpack(multiset_t * o_msp,
		uint8_t const * i_bitp,
		size_t i_size,
		uint8_t * o_encoded_type);
void multiset_pack(multiset_t const * i_msp, uint8_t * o_bitp, size_t i_size);
size_t multiset_packed_size(multiset_t const * i_msp);
void multiset_init(multiset_t * o_msp);
void multiset_add(multiset_t * o_msp, uint64_t element);
double multiset_card(multiset_t const * i_msp);

uint64_t hash_tuple(TupleTableSlot *slot, int num_attrs, AttrNumber *attrs);

#endif   /* MULTISET_T_H */
