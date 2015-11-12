/*
 * Miscellaneous utilities
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/include/pipeline/miscutils.h
 */
#ifndef MISCUTILS_H
#define MISCUTILS_H

#include "c.h"
#include "executor/tuptable.h"
#include "lib/stringinfo.h"
#include "storage/dsm.h"
#include "utils/typcache.h"

#define ptr_difference(begin, end) ((void *) (((char *) end) - ((char *) begin)))
#define ptr_offset(begin, offset) ((void *) (((char *) begin) + ((uintptr_t) offset)))

extern void append_suffix(char *str, char *suffix, int max_len);
extern int skip_token(const char *str, char* substr, int start);
extern char *random_hex(int len);

/* hash functions */
extern void MurmurHash3_128(const void *key, const Size len, const uint64_t seed, void *out);
extern uint64_t MurmurHash3_64(const void *key, const Size len, const uint64_t seed);
extern void SlotAttrsToBytes(TupleTableSlot *slot, int num_attrs, AttrNumber *attrs, StringInfo buf);
extern void DatumToBytes(Datum d, TypeCacheEntry *typ, StringInfo buf);

/* for backends / bg workers to yield cpu */
extern int SetNicePriority(void);
extern int SetDefaultPriority(void);

extern dsm_segment *dsm_find_or_attach(dsm_handle handle);

#endif   /* MISCUTILS_H */
