/*
 * Miscellaneous utilities
 *
 * src/include/pipeline/miscutils.h
 */
#ifndef MISCUTILS_H
#define MISCUTILS_H

extern void append_suffix(char *str, char *suffix, int max_len);
extern int skip_substring(char *str, char* substr, int start);
extern char *random_hex(int len);

#endif   /* MISCUTILS_H */
