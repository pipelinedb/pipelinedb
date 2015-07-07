/*=================================================================
  -- pagc_tools.h -- 

Certain common tools used both by the pagc library and its clients

Prototype 20H10 (This file was written by Walter Sinclair).

This file is part of PAGC.

Copyright (c) 2010 Walter Bruce Sinclair

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

*/

/* For pagc-0.4.0 : last revised 2010-11-25 */

#ifndef PGC_T_H
#define PGC_T_H

void convert_latin_one ( char * ) ;
void char_append( const char * , char * , const char * , int ) ;
void append_string_to_max( char * , char * , int ) ;
const char *out_symb_name( int ) ;
const char *in_symb_name( int ) ;
int out_symb_value( const char * ) ;
int get_input_line( char * , FILE * ) ;
void combine_path_file( char , char * , char * , char * ) ;
int upper_case_compare( char * , char* ) ; /* 2010-10-22 */
void fast_reverse_endian( char * , int ) ; /* 2010-10-30 */
void upper_case( char * , const char * ) ;
void parse_file_name( const char * , char , char * , char * ) ;
int establish_directory( char * , char * ) ;

#endif
