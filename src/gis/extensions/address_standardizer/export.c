/* -- export.c 

This file contains the routines for extracting the sequence of
postal attributes and definitions produced by the standardizer
into strings of text (in __standard_fields__).

Prototype 7H08 (This file was written by Walter Sinclair).

Copyright (c) 2009 Walter Bruce Sinclair

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

*/

/* For pagc-0.4.0 : last revised 2009-10-03 */

#include <stdio.h>
#include <string.h>
#include <stddef.h>
#include "pagc_api.h"
#include "pagc_tools.h"

#define ORDER_DISPLACEMENT 2

/* -- local prototypes -- */
static void _copy_standard_( STAND_PARAM * , SYMB , int , int  ) ; 
static void _scan_target_( STAND_PARAM * , SYMB , int  ) ; 
static char *_get_standard_( STAND_PARAM * , int , int ) ;
static char *_get_definition_text_( STAND_PARAM * , int ) ;
 
//#ifndef BUILD_API

/* -- local storage -- */
static const char *__field_start_tag__[][3] = {
   { "    <Build>",  "\"", "Building:         " },
   { "    <Civic>",  "\"", "House Address:    " },
   { "    <PreDir>", "\"", "Prefix Direction: " },
   { "    <Qualif>", "\"", "Qualifier:        " },
   { "    <PreTyp>", "\"", "Prefix Type:      " },
   { "    <Street>", "\"", "Street Name:      " },
   { "    <SufTyp>", "\"", "Suffix Type:      " },
   { "    <SufDir>", "\"", "Suffix Direction: " },
   { "    <Rural>",  "\"", "Rural Route:      " },
   { "    <Extra>",  "\"", "Additional Info:  " },
   { "    <City>",   "\"", "Municipal:        " },
   { "    <Prov>",   "\"", "Province/State:   " },
   { "    <Nation>", "\"", "Country:          " },
   { "    <Postal>", "\"", "Postal/Zip Code:  " },
   { "    <Box>",    "\"", "Box:              " },
   { "    <Unit>",   "\"", "Unit:             " }
} ;
static const char *__land_field_start_tag__[][3] = {
   { "<FeatureName>",  "\"", "FeatureName       " },
   { "<FeatureType>",  "\"", "FeatureType       " },
   { "<FeatureArea>", "\"", "FeatureArea       " }
} ;
static const char *__land_field_tag_end__[][3] = {
   { "</FeatureName>\n",  "\",", "\n" },
   { "</FeatureType>\n",  "\",", "\n" },
   { "</FeatureArea>\n", "\",", "\n" }
} ;
static const char *__field_tag_end__[][3] = {
   { "</Build>\n",  "\",", "\n" },
   { "</Civic>\n",  "\",", "\n" },
   { "</PreDir>\n", "\",", "\n" },
   { "</Qualif>\n", "\",", "\n" },
   { "</PreTyp>\n", "\",", "\n" },
   { "</Street>\n", "\",", "\n" },
   { "</SufTyp>\n", "\",", "\n" },
   { "</SufDir>\n", "\",", "\n" },
   { "</Rural>\n",  "\",", "\n" },
   { "</Extra>\n",  "\",", "\n" },
   { "</City>\n",   "\",", "\n" },
   { "</Prov>\n",   "\",", "\n" },
   { "</Nation>\n", "\",", "\n" },
   { "</Postal>\n", "\",", "\n" },
   { "</Box>\n",    "\",", "\n" },
   { "</Unit>\n",   "\",", "\n" }
} ;
static const char *__record_start_tag__[ ] = {
   "   <address>\n" , "\n", "\n"
} ;
static const char *__landmark_record_start_tag__[ ] = {
   "   <landmark>\n" , "\n", "\n"
} ;
static const char *__record_end_tag__[ ] = {
  "   </address>\n", "\n", "\n"
} ;
static const char *__landmark_record_end_tag__[ ] = {
   "   </landmark>\n" , "\n", "\n"
} ;

//#endif

static SYMB __ord_list__[] = { ORD, FAIL } ;

/*----------------------------------------------------------------
export.c (init_output_fields)
----------------------------------------------------------------*/
void init_output_fields( STAND_PARAM *__stand_param__ , int which_fields )
{
	/* -- called with BOTH to erase both the micro and macro fields
		called with RIGHT to erase only the macro fields, and
		LEFT to erase only the micro fields -- */
	int i  ;
	char **__standard_fields__ = __stand_param__->standard_fields ;
	/*-- Decide which set of fields to initialize --*/
	if ( which_fields == BOTH )
	{
		for ( i = 0 ; i < MAXOUTSYM ; i++ )
		{
			__standard_fields__[i][0] = SENTINEL ;
		}
	} 
	else 
	{
		/*-- Clean only one set --*/
		if ( which_fields == RIGHT )
		{
			/*-- Erase the macro fields only --*/
			for ( i = CITY ; i < NEEDHEAD ; i++ )
			{
				__standard_fields__[i][0] = SENTINEL ;
			}
		} 
		else 
		{
			/*-- Erase the micro fields only --*/
			for ( i = BLDNG ; i < CITY ; i++ )
			{
				__standard_fields__[i][0] = SENTINEL ;
			}
			for ( i = NEEDHEAD ; i < MAXOUTSYM ; i++ )
			{
				__standard_fields__[i][0] = SENTINEL ;
			}
		}
	}
}

/*-----------------------------------------
export.c (sym_to_field)
-------------------------------------------*/
int sym_to_field( SYMB sym )
{
	int fld = NEEDHEAD ;
	if ( sym == BOXH || sym == BOXT ) return fld ;
	fld++ ;
	if ( sym == UNITH || sym == UNITT ) return fld ;
	if ( sym >= BLDNG && sym < MAXOUTSYM ) return sym ;
	return FAIL ;
}

/*--------------------------------------------------
export.c (_get_definition_text_)
-- called by export.c (_get_standard_)
---------------------------------------------------*/
static char *_get_definition_text_( STAND_PARAM *__stand_param__ , int lex_pos )
{
	DEF *__best_DEF__ = __stand_param__->best_defs[lex_pos] ;
	if (!( __best_DEF__->Protect ))
	{
		return ( __best_DEF__->Standard ) ;
	}
	return ( __stand_param__->lex_vector[lex_pos].Text ) ;
}

/*-----------------------------------------
export.c (stuff_fields)
--calls export.c (_scan_target_)
-------------------------------------------*/
void stuff_fields( STAND_PARAM *__stand_param__ ) 
{
	int fld ;
	/*-- Translate the symbols and definitions of the standardization into
		the __standard_fields__ for output --*/
	for (fld = 0 ;fld < NEEDHEAD ;fld++) 
	{
		/*-- Fields that correspond one to one with the symbols --*/
		_scan_target_(__stand_param__ ,fld,fld) ;
	}
	/*-- These two fields have two tokens for each field --*/
	_scan_target_( __stand_param__ , BOXH, NEEDHEAD ) ;
	_scan_target_( __stand_param__ , BOXT, NEEDHEAD ) ;
	_scan_target_( __stand_param__ , UNITH, NEEDHEAD+1 ) ;
	_scan_target_( __stand_param__ , UNITT, NEEDHEAD+1 ) ;
}

//#ifndef BUILD_API

/*---------------------------------------------------------------------
export.c (send_fields_to_stream)
uses BLANK_STRING
2009-09-27 modify to display landmark fields
----------------------------------------------------------------------*/
#define STREAM_BUF_SIZE MAXSTRLEN
void send_fields_to_stream( char **__standard_fields__ , FILE *__dest_file__ , int opt , int is_landmark)
{
	int output_order ;
	if (opt < NO_FORMAT)
	{
		if (__dest_file__ != NULL)
		{
			fprintf(__dest_file__,"%s\n",(is_landmark? __landmark_record_start_tag__[opt] : __record_start_tag__[opt])) ;
		} 
		else 
		{
			printf("%s\n",(is_landmark? __landmark_record_start_tag__[opt] : __record_start_tag__[opt])) ;
		}
	}
	/*-- We want to rearrange so that unit and box come first --*/
	for (output_order = 0; output_order < (NEEDHEAD + ORDER_DISPLACEMENT); output_order++)
	{
		char __line_buf__[STREAM_BUF_SIZE] ;
		int loc = ((output_order < ORDER_DISPLACEMENT)? (NEEDHEAD + output_order) : (output_order - ORDER_DISPLACEMENT)) ;
		char *__field_string__ = __standard_fields__[loc] ;
		BLANK_STRING(__line_buf__) ;
		if (*__field_string__ != SENTINEL)
		{
			if (opt < NO_FORMAT)
			{
				char * __source_start_tag__ ;
				if (is_landmark)
				{
					switch (loc)
					{
			        case FEATNAME :
						__source_start_tag__ = ( char *) __land_field_start_tag__[0][opt] ;
     				    break ;
                    case FEATTYPE :
                       __source_start_tag__ = ( char *) __land_field_start_tag__[1][opt] ;
                        break ;
                    case FEATAREA :
						__source_start_tag__ = ( char *) __land_field_start_tag__[2][opt] ;
						break ;
					default :
						__source_start_tag__ = ( char * ) __field_start_tag__[loc][opt] ;
					}
				} 
				else 
				{
					__source_start_tag__ = (char *) __field_start_tag__[loc][opt] ;
				}
				append_string_to_max(__line_buf__, __source_start_tag__ , STREAM_BUF_SIZE) ;
			}
			append_string_to_max( __line_buf__,  __field_string__ , STREAM_BUF_SIZE ) ;
			if (opt < NO_FORMAT)
			{
				char * __source_end_tag__ ;
				if (is_landmark)
				{
 					switch (loc)
					{
					case FEATNAME :
						__source_end_tag__ = ( char *) __land_field_tag_end__[ 0 ][ opt ] ;
						break ;
					case FEATTYPE :
						__source_end_tag__ = ( char *) __land_field_tag_end__[ 1 ][ opt ] ;
						break ;
					case FEATAREA :
						__source_end_tag__ = ( char *) __land_field_tag_end__[ 2 ][ opt ] ;
						break ;
					default :
						__source_end_tag__ = ( char * ) __field_tag_end__[ loc ][ opt ] ;
					}
				}
				else 
				{
					__source_end_tag__ = ( char * ) __field_tag_end__[ loc ][ opt ] ;
				}
				append_string_to_max( __line_buf__ , __source_end_tag__ , STREAM_BUF_SIZE ) ;
			}
			if ( __dest_file__ != NULL )
			{
				fprintf( __dest_file__ , "%s" , __line_buf__ ) ;
			} 
			else 
			{
				printf( "%s" , __line_buf__ ) ;
			}
		}
	}
	if ( opt < NO_FORMAT ) 
	{
		if ( __dest_file__ != NULL ) 
		{
			fprintf( __dest_file__ , "%s\n", ( is_landmark? __landmark_record_end_tag__[ opt ] : __record_end_tag__[ opt ]));
		} 
		else 
		{
			printf( "%s\n" , ( is_landmark? __landmark_record_end_tag__[ opt ] :  __record_end_tag__[ opt ] ) );
		}
	}
	if ( __dest_file__ != NULL )
	{
		fflush( __dest_file__ ) ;
	} 
	else 
	{
		fflush( stdout ) ;
	}
}

//#endif

/*-----------------------------------------
export.c (_get_standard_)
-- called by export.c (_copy_standard_)
-- calls _get_definition_text_ , find_def_type
uses MACRO BLANK_STRING
-------------------------------------------*/
static char *_get_standard_(STAND_PARAM *__stand_param__ ,int lex_pos, int output_sym)
{
	char *__selected_standardization__ ;
	DEF *__best_DEF__ = __stand_param__->best_defs[lex_pos] ;
	if ((output_sym == STREET) && (find_def_type(__best_DEF__,__ord_list__)) && (__best_DEF__->Type == WORD))
	{
		/*-- <remarks> If the best definition is a streetname typed as a word, but also
			including an ordinal type, then substitute the ordinal
			standardization - however, the lexicon should take care of most
			cases of this. </remarks> --*/

		DEF *__scan_DEF__ ;

		for (__scan_DEF__ = __stand_param__->lex_vector[lex_pos].DefList;__scan_DEF__ != NULL;__scan_DEF__ = __scan_DEF__->Next)
		{
			if (__scan_DEF__->Type == ORD)
			{
				if ((__selected_standardization__ = __scan_DEF__->Standard) != NULL) 
				{
					return (__selected_standardization__) ;
				}
				break ;
			}
		}
	}

	/*-- If it is in the lexicon, use the standardization there, otherwise
		use the form that emerged from tokenization --*/

	__selected_standardization__ = _get_definition_text_(__stand_param__,lex_pos) ;
	if ((output_sym == HOUSE) && (*__selected_standardization__ == '0'))
	{
		/*-- Remove leading zeroes to simplify match comparisons
			on the house number that use strings rather than integers -
			we won't do this on zip codes. There may arise some need to
			do it for unit and box numbers in the future. --*/
		char *__zero_pointer__ ;
		char *__buffer_pointer__ = __zero_pointer__ = __selected_standardization__ ;
		while ( *__zero_pointer__ == '0' ) __zero_pointer__++ ; /*-- Move to first nonzero character --*/
		while ( *__zero_pointer__ != SENTINEL ) *__buffer_pointer__++ = *__zero_pointer__++ ; /*-- Move down in buffer --*/ 
		/*-- Trim down all-zeroes to a single zero: if deleting all
			the zeros leaves an empty buffer, put a zero back --*/
		if ( __buffer_pointer__ == __selected_standardization__ ) *__buffer_pointer__++ = '0' ; 
		BLANK_STRING( __buffer_pointer__ ) ;
	}
	return ( __selected_standardization__ ) ;
}

/*-----------------------------------------
export.c (_scan_target_ )
-- calls export.c (_copy_standard_) 
-- called by export.c (stuff_fields)
-------------------------------------------*/
static void _scan_target_(STAND_PARAM *__stand_param__,SYMB sym , int dest)
{
	int i ;

	int n = __stand_param__->LexNum ;
	SYMB *__output_syms__ = __stand_param__->best_output ;
	/*-- <remarks> Probe the array of output symbols in the best output and find
      the position of a matching symbol and send it to be copied to
      the output string fields. The order of the words in each field
      will therefore follow the order that they appear in the input </remarks> --*/
	for (i = FIRST_LEX_POS;i < n;i++)
	{
		if (__output_syms__[i] == sym)
		{
			_copy_standard_(__stand_param__,sym,dest,i) ;
		}
	}
}

/*-----------------------------------------
export.c (_copy_standard_) 
-- called by export.c (_scan_target_) -- 
--calls export.c (_get_standard_, 
strlen, strcpy 
uses macro SPACE_APPEND_WITH_LEN 
-------------------------------------------*/
static void _copy_standard_( STAND_PARAM *__stand_param__ , SYMB output_sym , int fld , int lex_pos )
{

	/*-- Retrieve the standardized string --*/
	char *__stan_str__ = _get_standard_( __stand_param__ , lex_pos , output_sym ) ;
	char *__dest_buf__ = __stand_param__->standard_fields[fld] ;
	if (( strlen( __stan_str__ ) + strlen( __dest_buf__ )) > MAXFLDLEN )
	{
		/*-- Truncate without warning --*/
		return ;
	}
	if ( *__dest_buf__ != SENTINEL )
	{
		SPACE_APPEND_WITH_LEN( __dest_buf__ , __stan_str__ , MAXFLDLEN ) ;
	} 
	else if ( output_sym == UNITT )
	{
		/*-- If the unit id type is missing, one needs to be provided.
         This might result in a mismatch, when the type is implicit
         in one of the compared addresses, and explicit in the
         other. Not much you can do with implicit. Better a generic
         identifier than nothing at all --*/

		strcpy( __dest_buf__ , "# " ) ; /* -- reconsider this -- */
		append_string_to_max( __dest_buf__ , __stan_str__ , MAXFLDLEN ) ;
	} 
	else if ( output_sym == BOXT )
	{
		strcpy( __dest_buf__, "BOX " ) ;
		append_string_to_max( __dest_buf__ , __stan_str__ ,MAXFLDLEN ) ;
	} 
	else 
	{
		strcpy( __dest_buf__ , __stan_str__ ) ;
	}
}

