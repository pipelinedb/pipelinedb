/*
 * Miscellaneous utilities
 *
 * src/include/pipeline/miscutils.h
 */
#ifndef MISCUTILS_H
#define MISCUTILS_H

#include "c.h"

extern void append_suffix(char *str, char *suffix, int max_len);
extern int skip_substring(char *str, char* substr, int start);
extern char *random_hex(int len);

/* hash functions */
extern void MurmurHash3_128(const void *key, const Size len, const uint64_t seed, void *out);
extern uint64_t MurmurHash3_64(const void *key, const Size len, const uint64_t seed);
extern int32_t JumpConsistentHash(uint64_t key, int32_t num_buckets);

#endif   /* MISCUTILS_H */
