/*-------------------------------------------------------------------------
 *
 * Miscellaneous utilities
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef MISCUTILS_H
#define MISCUTILS_H

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "lib/stringinfo.h"
#include "signal.h"
#include "storage/dsm.h"
#include "utils/typcache.h"

extern volatile sig_atomic_t pipeline_got_SIGTERM;

typedef struct tagged_ref_t
{
	void *ptr;
	uint64 tag;
} tagged_ref_t;

#define set_sigterm_flag() \
	do \
	{ \
		pipeline_got_SIGTERM = true; \
	} while (0)
#define get_sigterm_flag() (pipeline_got_SIGTERM)

#define ptr_difference(begin, end) ((void *) (((char *) end) - ((char *) begin)))
#define ptr_offset(begin, offset) ((void *) (((char *) begin) + ((uintptr_t) offset)))

#define foreach_tuple(slot, store) \
 while (tuplestore_gettupleslot(store, true, false, slot))

#define BITMAPSET_SIZE(nwords)	\
	(offsetof(Bitmapset, words) + (nwords) * sizeof(bitmapword))

extern void append_suffix(char *str, char *suffix, int max_len);
extern int skip_token(const char *str, char* substr, int start);
extern char *random_hex(int len);

/* hash functions */
extern void MurmurHash3_128(const void *key, const Size len, const uint64_t seed, void *out);
extern uint64_t MurmurHash3_64(const void *key, const Size len, const uint64_t seed);
extern void SlotAttrsToBytes(TupleTableSlot *slot, int num_attrs, AttrNumber *attrs, StringInfo buf);
extern void DatumToBytes(Datum d, TypeCacheEntry *typ, StringInfo buf);

extern Oid GetTypeOid(char *name);

extern bool equalTupleDescsWeak(TupleDesc tupdesc1, TupleDesc tupdesc2, bool check_names);

extern void print_tupledesc(TupleDesc desc);

#endif   /* MISCUTILS_H */
