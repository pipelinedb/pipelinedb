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

uint64 MurmurHash64A(const void *key, Size keysize);

#endif   /* MISCUTILS_H */
