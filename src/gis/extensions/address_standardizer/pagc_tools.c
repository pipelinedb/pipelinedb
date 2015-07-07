/* -- pagc_tools.c 

Various and miscellaneous functions.

Prototype 20H10 (This file was written by Walter Sinclair).

This file is part of PAGC.

Copyright (c) 2010 Walter Bruce Sinclair

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

*/

/* For pagc-0.4.0 : last revised 2010-11-25 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include "pagc_common.h"
#include "pagc_tools.h"

#ifndef MAXOUTSYM
#define MAXOUTSYM 18
#endif

#ifdef MSYS_POSIX
static void conform_directory_separator( char * ) ;
#endif


static const char *OutSymbNames[] = {
"BLDNG",
"HOUSE",
"PREDIR",
"QUALIF",
"PRETYP",
"STREET",
"SUFTYP",
"SUFDIR",
"RR",
"UNKNWN",
"CITY",
"PROV",
"NATION",
"POSTAL",
"BOXH",
"BOXT",
"UNITH",
"UNITT"
} ;

static const char *InSymbNames[] = {
   "NUMBER",
   "WORD",
   "TYPE",
   "QUALIF",
   "PRETYP",
   "STREET",
   "ROAD",
   "STOPWORD",
   "RR",
   "DASH",
   "CITY",
   "PROV",
   "NATION",
   "AMPERS",
   "BOXH",
   "ORD",
   "UNITH",
   "UNITT",
   "SINGLE",
   "BUILDH",
   "MILE",
   "DOUBLE",
   "DIRECT",
   "MIXED",
   "BUILDT",
   "FRACT",
   "PCT",
   "PCH",
   "QUINT",
   "QUAD",
} ;

/* ------------------------------------------------------------ 
   ISO 8859 character set may pop up in some files. After 1998
   TigerLine will use them. 
------------------------------------------------------------- */
void convert_latin_one ( char *inp ) {
   unsigned char *str ;

   for ( str = ( unsigned char * ) inp ; 
         *str != SENTINEL ; 
         str++ ) {
      unsigned char ch ;
      ch = *str ;
      /* ------------------------------------------- 
         if bit 7 is set, reset bit 5 so both upper
         and lower case can be done together 
      --------------------------------------------- */
      if ( ch & 0x80 ) {
         ch &= 0xDF ;
         /* ----------------------------------------- 
            reduce letters with diacritical marks to
            their unmarked base letters 
         ------------------------------------------ */
         if ( ch >= 0xC0 &&
              ch <= 0xC6 )
            ch = 'A' ;
         else if ( ch == 0xc7 )
            ch = 'C' ;
         else if ( ch >= 0xc8 && ch <= 0xcb )
            ch = 'E' ;
         else if ( ch >= 0xcc && ch <= 0xcf )
            ch = 'I' ;
         else if ( ch == 0xd0 )
            ch = 'D' ;
         else if ( ch == 0xd1 )
            ch = 'N' ;
         else if ( ch >= 0xd2 && ch <= 0xd6 )
            ch = 'O' ;
         else if ( ch >= 0xd9 && ch <= 0xdc )
            ch = 'U' ;
         else if ( ch >= 0xdd && ch < 0xdf )
            ch = 'Y' ;
         else
            /* ------------------------------- 
               just clear the top bit so it
               won't gum up the edit distance
               machinery 
            -------------------------------- */
            ch &= 0x7f ;
      }
      *str = ch ;
   }

   /* ---------------------------------------------- 
   while we're at it, add a newline to the end
      because the lexical scanner likes it like that 
   ----------------------------------------------- */
   *str++ = '\n' ;
   *str = SENTINEL ;
}

void char_append( const char *div ,
                  char *dest ,
                  const char *src ,
                  int max_wid ) {
   if ( *src == SENTINEL )
      return ;
   /* -- skip the delimitor if dest is empty -- */
   if ( *dest == SENTINEL ) {
      append_string_to_max( dest , 
                            ( char * ) src ,
                            max_wid ) ;
      return ;
   }
   append_string_to_max( dest , ( char * ) div , max_wid ) ;
   append_string_to_max( dest , ( char * ) src , max_wid ) ;
}

const char *out_symb_name( int i ) {
   return ( OutSymbNames[ i ] ) ;
}

const char *in_symb_name( int i ) {
   return ( InSymbNames[ i ] ) ;
}

int out_symb_value( const char *src ) {
   int i ;

   /* -- linear search -- */
   for ( i = 0 ;
         i < MAXOUTSYM ;
         i++ ) {
      if ( strcmp( src , 
                   OutSymbNames[ i ] ) == 0 )
         return i ;
   }
   return FAIL ;
}

/*-------------------------------------------
util.c (get_input_line)
called by initial.c (restore_build_state)
--------------------------------------------*/
int get_input_line( char *buf ,
                    FILE *fp ) {
   int i ;

   BLANK_STRING(buf) ;
   if ( ( fgets( buf ,
                 MAXSTRLEN ,
                 fp ) ) == NULL )
      return FALSE ;
   for ( i = strlen( buf ) ;
         i > 0 ;
         i-- ) {
      if ( strchr( "\n\r",
                   buf[ i - 1 ] ) ) {
         buf[ i - 1 ] = SENTINEL ;
      } else
         break ;
   }
   return TRUE ;
}


/*-------------------------------------------------------
pagc_tools.c (parse_file_name)
called by open_aux_file, main.c (main)
copies the file name to the output_tail and the path to
the output_head
--------------------------------------------------------*/
void parse_file_name( const char *input_path_name ,
                      char global_path_separator ,
                      char *output_tail ,
                      char *output_head ) {
	const char *end_ptr , *src ;
	char *dest ;
   /* -- find the file name part first -- */
   /* -- move to end of the pathname -- */
	for ( end_ptr = input_path_name ; *end_ptr != SENTINEL ; end_ptr++ ) ;
	/* -- find the last directory delimitor -- */
	while ( ( end_ptr > input_path_name ) && NOT_PATH_DELIMITOR(*end_ptr) ) {
		end_ptr -- ;
	}
	/* --------------------------------------------------------------- 
	either end_ptr has the last delimitor or it is at string start.
		If the first case, we need to increment to get the filename and
		need to copy everything up to and including for the path. 
	-----------------------------------------------------------------*/
	/* -- copy from beg to endptr to output path -- */
	dest = output_head ;
	src = input_path_name ;
	/* if end_ptr points to a path delimitor, copy everything up but not
	including it into the output_head (if output_head isn't NULL) */
	if ( IS_PATH_DELIMITOR( *end_ptr ) ) {
		while ( src < end_ptr ) {
			if ( dest != NULL ) {
				*dest++ = *src ;
			}
			src++ ;
		}
		src++ ;
	}
	/* -- copy from endptr to end to output file name -- */
	if ( dest != NULL ) {
		BLANK_STRING(dest) ;
	}
	/* copy everything after the delimitor up to the sentinel
	into the output_tail */
	if ( ( dest = output_tail ) != NULL ) {
		while ( TRUE ) {
			if ( ( *dest++ = *src++ ) == SENTINEL ) {
				break ;
			}
		}
	}
}


/*--------------------------------------------------
pagc_tools.c (combine_path_file)
called by util.c (open_aux_file)
calls char_append
--------------------------------------------------*/
void combine_path_file( char global_path_separator ,
                        char *input_head ,
                        char *input_tail ,
                        char *output_path_name ) {
   char combine_buf[ 2 ] ;

   combine_buf[ 0 ] = global_path_separator ;
   combine_buf[ 1 ] = SENTINEL ;

   if ( ( input_head != NULL ) && 
        ( input_head[ 0 ] != SENTINEL ) ) {
      append_string_to_max( output_path_name ,
                            input_head ,
                            PATHNAME_LEN ) ;

      char_append( combine_buf ,
                   output_path_name ,
                   input_tail ,
                   PATHNAME_LEN ) ;
      return ;
   }
   append_string_to_max( output_path_name ,
                         input_tail ,
                         PATHNAME_LEN ) ;
}


void upper_case( char *d ,
                 const char *s ) {
   /* -- make an uppercase copy in d of string in s -- */
   for ( ; 
         *s != SENTINEL ;
         s++ ) {
      *d++ = ( islower( *s )? toupper( *s ) : *s ) ;
   }
   BLANK_STRING(d) ;
}

/* 2010-10-22 : new routine */
int upper_case_compare( char *str1 , char* str2 ) {
	char upper_buf1[ MAXSTRLEN ] ;
	char upper_buf2[ MAXSTRLEN ] ;
	upper_case( upper_buf1 , str1 ) ;
	upper_case( upper_buf2 , str2 ) ;
	return ( strcmp( upper_buf1 , upper_buf2 ) ) ;
}

/* 2010-10-30 : moved here for use in ds */
void fast_reverse_endian( char *location_to_reverse , int bytes_to_reverse ) {
	char *start_byte_ptr , *end_byte_ptr ;

	for ( start_byte_ptr = location_to_reverse , end_byte_ptr = location_to_reverse + bytes_to_reverse - 1 ; start_byte_ptr < end_byte_ptr ; start_byte_ptr++ , end_byte_ptr-- ) {
		char a  = *start_byte_ptr ;
		*start_byte_ptr = *end_byte_ptr ;
		*end_byte_ptr = a ;
	}                     
}

/*=================================================================
pagc_tools.c (append_string_to_max ) = format.c (format_ncat)
=================================================================*/
void append_string_to_max( char *dest_buf_start ,
                           char *src_str_start ,
                           int buf_size ) {

   char a ;
   char *d_ptr , *s_ptr , *buf_end ;

   /* -- move to end of current contents of buffer -- */
   d_ptr = dest_buf_start ; 
   while ( ( a = *d_ptr ) != SENTINEL ) {
      d_ptr ++ ;
   }
   buf_end = dest_buf_start + buf_size - 1 ;

   if ( d_ptr >= buf_end ) {
#ifndef BUILD_API
#ifndef NO_STDERR_OUTPUT
      fprintf( stderr , "format_strncat: fatal buffer overflow of %s\n" , dest_buf_start ) ;
      fprintf( stderr , "No room for %s\n" , src_str_start ) ;
#endif
      exit( 1 ) ;
#else
      /* TODO if postgresql we can throw and error or notice 
         but for now we will just truncate the string */
      *d_ptr = SENTINEL ;
      return;
#endif
   }
   s_ptr = src_str_start ;
   while ( ( ( a = *s_ptr++ ) != SENTINEL ) && 
           ( d_ptr != buf_end ) ) {
      *d_ptr++ = a ;
   }
   *d_ptr = SENTINEL ;
}



/* ========================================================
pagc_tools.c (establish_directory)
Determine the current working directory and path_separator
========================================================= */
int establish_directory( char * c_w_d ,
                         char * p_s ) {
   char *c_w_d_ptr ;

   c_w_d_ptr = getcwd( c_w_d , 
                       ( PATHNAME_LEN - 1 ) ) ;
   if ( c_w_d_ptr  == NULL ) {
      return FALSE ;
   }

   *p_s = FORE_SLASH ;

#ifdef MSYS_POSIX

   /* ..... transform cwd's non-POSIX directory separators to conform  ..... */

   conform_directory_separator( c_w_d ) ;

#endif

   if ( isalpha( c_w_d[ 0 ] ) ) {

      /* ..... drive letter, colon, dir_sep ..... */

      if ( IS_COLON( c_w_d[ 1 ] ) ) {
         *p_s = c_w_d[ 2 ] ;
         if ( ( *p_s != FORE_SLASH ) &&
              ( *p_s != BACK_SLASH ) ) {
            return FALSE ;
         }
      } else {
         return FALSE ;
      }
   }
   return TRUE ;
}

#ifdef MSYS_POSIX
/*------------------------------------------------------------------
pagc_tools.c (conform_directory_separator)
-- called only if compiled with MSYS_POSIX defined ..... 
-- transform non-POSIX directory separators to conform with POSIX --
called by init_global
string.h (strlen)
-------------------------------------------------------------------*/
static void conform_directory_separator( char * path_name ) {
   int i , 
       pn_len ;   

   pn_len = strlen( path_name ) ;
   for ( i = 0 ; 
         i < pn_len ; 
         i++ ) {
      if ( path_name[ i ] == BACK_SLASH ) {
         path_name[ i ] = FORE_SLASH ;
      }
   }
}
/* ..... END OF IFDEF MSYS_POSIX ..... */
#endif


