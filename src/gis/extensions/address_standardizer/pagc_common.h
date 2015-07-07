/*-- pagc_common.h -- 

Certain common definitions used both by the pagc library and its clients

Prototype 20H10 (This file was written by Walter Sinclair).

This file is part of PAGC.

Copyright (c) 2010 Walter Bruce Sinclair

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

*/

/* For pagc-0.4.0 : last revised 2010-11-08 */
 

#ifndef PAGC_COM_H
#define PAGC_COM_H

#ifdef MAXPATHLEN
#define PATHNAME_LEN MAXPATHLEN
#else
#define PATHNAME_LEN 1024
#endif

/* -- 2006-04-25 : structure added to index arc endpoints -- */
typedef struct pagc_point {
   double X ;
   double Y ;
} PAGC_POINT ;


typedef int SYMB ;


#define ERR_FAIL -2
#define FAIL -1
#define NULL_READ 0
#define MATCH_READ 2
#define BOTH 2


/*------------------------------------
strategy types
------------------------------------*/
#define ADDRESS_SCORING 0
#define INTERSECTION_SCORING 1
#define LANDMARK_SCORING 3

#define SITE_MATCH 0
#define SITE_INTERPOLATE 1
#define INTERSECTION 2
#define ADDRESS_RANGE_2 3
#define ADDRESS_RANGE_4 4 
#define REVERSE_SITE 5
#define REVERSE_INTERSECTION 6 
#define INTERSECTION_B 7 
#define CONCAT 8
#define LANDMARK_NAME 9

/*----------------------------------
response format types :
------------------------------------*/
#define CSV 0
#define JSON 1
#define XML 2

/* -- build flags -- */
#define STATISTICS 2 /* -- output statistics on rules used. FLSTATS in schema-- */
#define PRINT_PROGRESS 128 /* output 10% completion points */
#define LOG_COMPLETE 2048 /* log certain initializations when complete */
#define ZERO_IS_BLANK 512 /* schema: FLZBLNK */
#define RNF_PRETYPE_REDIRECT 4096 /* schema: FLRNFRE */

#define SENTINEL '\0'
#define BLANK_STRING(STR) *STR = SENTINEL
#define MAXSTRLEN 256

/* -- boolean -- */
#ifndef TRUE
#define TRUE 1
#define FALSE 0
#endif

#define READ_ONLY_MODE 0
#define WRITE_CREATE_MODE 1
#define WRITE_APPEND_MODE 2

#define PAGE_SIZE 4096
#define MAX_REF_CANDS 100

#ifdef ENABLE_THREADED
#define MAX_CONTEXTS 20
#else
#define MAX_CONTEXTS 1
#endif

#define BACK_SLASH 0x5c
#define FORE_SLASH '/'
#define IS_DOT(CH) ( CH == '.' )
#define IS_DIR_SEP(CH) ( CH == global_path_separator )
#define IS_COLON(CH) ( CH == ':' )
#define NOT_PATH_DELIMITOR(CH) \
   ( CH != global_path_separator ) && \
   ( !IS_COLON(CH) )
#define IS_PATH_DELIMITOR(CH) \
   ( IS_DIR_SEP(CH) || \
   IS_COLON(CH) )
#define COMMA_APPEND_WITH_LEN( D , S , L ) \
   char_append( "," , D , S , L )

#endif
