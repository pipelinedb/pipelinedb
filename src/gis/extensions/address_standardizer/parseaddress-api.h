/*
parseaddres-api.h - utility to crack a string into address, city st zip

Copyright 2006-2010 Stephen Woodbridge.

woodbri@swoodbridge.com
woodbr@imaptools.com

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


 */

#ifndef PARSEADDRESS_API_H
#define PARSEADDRESS_API_H

#include "postgres.h"

#define OVECCOUNT 30

#ifdef USE_HSEARCH

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <search.h>

typedef struct hsearch_data  HHash;

#else

#include "hash.h"

typedef hash_t HHash;

#endif

typedef struct address_struct {
    char *num;
    char *street;
    char *street2;
    char *address1;
    char *city;
    char *st;
    char *zip;
    char *zipplus;
    char *cc;
    double lat;
    double lon;
} ADDRESS;

int clean_trailing_punct(char *s);
void strtoupper(char *s);
int match(char *pattern, char *s, int *ovect, int options);
ADDRESS *parseaddress(HHash *stH, char *s, int *err);
int load_state_hash(HHash *stH);
void free_state_hash(HHash *stH);
void free_address(ADDRESS *a);

/*
 *  ERRORS
 *
 *  1000 general memory allocation error
 *  1001 failed to create hash table structure
 *  1002 failed to find state abbreviation
 *  1003 hash table is full, failled to add new entry
 *
*/

#endif
