/* analyze.c 


This file contains the routines for finding the rules that
best fit the input address and assigns each element of the
input to the appropriate output field. The process is
essentially one of pattern-matching. The Aho-Corasick algorithm
is used to match rules that map input symbols found by the tokenizer
to output symbols. In the general case a clause tree is built left to 
right, matching rules of a particular class, depending on the state.

Prototype 7H08 (This file was written by Walter Sinclair).

Copyright (c) 2009 Walter Bruce Sinclair

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

*/

/* For pagc-0.3.0 : last revised 2010-11-18 */

//#define OCCUPANCY_DEBUG
#define USE_FORCE_MACRO

#include <stdlib.h>
#include <stdio.h>
#include <stddef.h>
#include <string.h>
#include "pagc_api.h"


/* ------------------------------------------------------------ 
A lookup string with a particular standardization is prevented
   from becoming associated with a particular output symbol 
------------------------------------------------------------- */
typedef struct def_blocker 
{
   char *lookup ; 
   char *standard ;
   SYMB output_symbol ;
   DEF *definition ;
} DEF_BLOCKER ;

#define NUM_DEF_BLOCKERS 2

/* --------------------------------------------------------------- 
When adding to this list, increment NUM_DEF_BLOCKERS for each new
   entry. This list blocks the use of the lookup string (first entry)
   as the standardization (second entry) as an output symbol (third)
   binding to the definition (fourth entry). The fourth entry is
   added at initialization after the lexicon is read into memory.
   Thus ST is blocked as STREET as a pretype. This occurs if the
   rule attempts to move ST (as SAINT) left from STREET into PRETYP.
---------------------------------------------------------------- */
static DEF_BLOCKER __def_block_table__[NUM_DEF_BLOCKERS] = 
{
   {"ST", "STREET", PRETYP, NULL } ,
   {"ST", "STREET", CITY, NULL }
} ;

/* -- local prototypes -- */

static int check_def_block( STAND_PARAM * , int ) ;
static void delete_stz( STZ_PARAM * , int ) ;
static int delete_duplicate_stz( STZ_PARAM * , int ) ;
static void first_composition( STAND_PARAM * ) ;
static int prepare_target_pattern( STAND_PARAM * ) ;
static int no_break( STAND_PARAM *__stand_param__ , int ) ;
static int do_left_combine( STAND_PARAM * , int , int ) ;
static int need_compression( STAND_PARAM *, SYMB , int , int  ) ;
static int select_next_composition( STAND_PARAM * ) ;
static int copy_best( STAND_PARAM * , int * , SYMB , int , SYMB * ) ;
static void save_current_composition( STAND_PARAM * , SEG * , int , SYMB * , DEF ** ) ;
static void scan_clause_tree(  STAND_PARAM * , int, int ) ;
static void shallow_clause_scan( STAND_PARAM * , int , int ) ;
static void deposit_stz( STAND_PARAM *, double , int ) ; 
static STZ *copy_stz( STAND_PARAM * , double ) ;
static void make_singleton( SEG * , SYMB , int , int , double ) ;
static int lex_has_def( STAND_PARAM * , int , SYMB ) ;
static void _force_deposit_( STAND_PARAM * , int ) ;
static int have_schema_symbol( int * , SYMB ) ;
static void default_seg_val( int * , int , SEG * , int , SYMB , double ) ;
static int _modify_position_( STAND_PARAM *, SEG * , int , int , SYMB , SYMB ) ;
static int schema_modify_position( STAND_PARAM * , SEG * , int , int , SYMB , SYMB ) ;
static void force_arc_clause( STAND_PARAM * ) ;
#ifdef USE_FORCE_MACRO
static void _force_macro_clause_( STAND_PARAM * ) ;
#endif
static int non_geocode_address( STAND_PARAM * ) ;
static int evaluate_micro_l(STAND_PARAM *) ;

/* -- Guide to the transition table:
         MACRO_C MICRO_C ARC_C   CIVIC_C EXTRA_C
MICRO_B  FAIL    FAIL    EXIT    FAIL    FAIL
MICRO_M  FAIL    EXIT    PREFIX  FAIL    MICR0_M
MACRO    EXIT    FAIL    FAIL    FAIL    FAIL
PREFIX   FAIL    FAIL    FAIL    EXIT    FAIL
EXIT     FAIL    FAIL    FAIL    FAIL    EXIT

-- */

static int __tran_table__[MAX_CL][MAX_CL] = {
   { FAIL, FAIL, EXIT, FAIL, FAIL } ,
   { FAIL, EXIT, PREFIX, FAIL, MICRO_M } ,
   { EXIT, FAIL, FAIL, FAIL, FAIL } , 
   { FAIL, FAIL, FAIL, EXIT, FAIL } , 
   { FAIL, FAIL, FAIL, FAIL, EXIT } 
} ; 

/* -- skew weights for each rule class -- */
static double __weight_table__[MAX_CL] = 
{
  1.0, 0.95, 0.95, 0.8 , 0.85
} ;


#define TARG_START 0
#define FIRST_STZ 0
#define INITIAL_STZ_CUTOFF .05
#define VERY_LOW_WEIGHT .15
#define START_DEPTH 0

static double __load_value__[ NUMBER_OF_WEIGHTS ] = 
{
   0.00, 0.325, 0.35 , 0.375 , 0.4 , 
   0.475 , 0.55, 0.6 , 0.65 , 0.675 , 
   0.7 , 0.75 , 0.8 , 0.825 , 0.85 , 
   0.9 , 0.95 , 1.00 
} ;

#ifdef OCCUPANCY_DEBUG
static const char *__rule_type_names__[] = 
{
   "MACRO" , "MICRO" , "ARC" , "CIVIC" , "EXTRA"
} ;
#endif


/* ====================================================================
analyze.c (install_def_block_table)
process level initialization - called by standard.l (init_stand_process)
calls lexicon.c (find_entry)
returns FALSE if error encountered.
string.h (strcmp)
uses macro RET_ERR1, LOG_MESS, CLIENT_ERR
=======================================================================*/
int install_def_block_table( ENTRY **__hash_table__, ERR_PARAM *__err_param__ )
{
	int i ;
	for ( i = 0 ; i < NUM_DEF_BLOCKERS ; i++ )
	{
		DEF * __standard_def__ ;
		ENTRY *__lookup_entry__ = find_entry( __hash_table__ , __def_block_table__[i].lookup ) ;
		if (__lookup_entry__ == NULL)
		{
			RET_ERR1( "install_def_block_table: Could not find def_block for %s\n", __def_block_table__[i].lookup , __err_param__ , FALSE ) ;
		}
		for ( __standard_def__ = __lookup_entry__->DefList ; __standard_def__ != NULL ; __standard_def__ = __standard_def__->Next ) 
		{
			if ( strcmp( __standard_def__->Standard , __def_block_table__[i].standard ) == 0 )
			{
	            __def_block_table__[i].definition = __standard_def__ ;
			}
            break ;
		}
		if ( __def_block_table__[i].definition == NULL ) 
		{
			RET_ERR1( "install_def_block_table: Could not find def_block definition for %s\n" , __def_block_table__[i].standard , __err_param__ , FALSE ) ;
		}
	}
	return TRUE ;
}

/* ====================================================================
analyze.c (create_segments)
context level initialization -- must come after the lexicon
      is read - called by init_stand_context
Null on error.
=======================================================================*/
STZ_PARAM *create_segments( ERR_PARAM *__err_param__ )
{
	STZ_PARAM *__stz_info__ ;
	int i ;
	/* -- we're going to be re-sorting these pointers -- */
    PAGC_ALLOC_STRUC(__stz_info__,STZ_PARAM,__err_param__,NULL) ;
	PAGC_CALLOC_STRUC(__stz_info__->stz_array,STZ *,MAX_STZ,__err_param__,NULL) ;
	for ( i = FIRST_STZ ; i < MAX_STZ ; i++ ) 
	{
		PAGC_ALLOC_STRUC(__stz_info__->stz_array[i],STZ,__err_param__,NULL) ;
	}
	PAGC_CALLOC_STRUC(__stz_info__->segs,SEG,MAXLEX,__err_param__,NULL) ;
	return __stz_info__ ; 
}

/* ====================================================================
analyze.c (destroy_segments)
context level cleanup 
- called by (standard.l) close_stand_context 
uses macros PAGC_DESTROY_2D_ARRAY, FREE_AND_NULL
=======================================================================*/
void destroy_segments( STZ_PARAM *__stz_info__ ) 
{
	if ( __stz_info__ == NULL ) 
	{
		return ;
	}
	PAGC_DESTROY_2D_ARRAY( __stz_info__->stz_array , STZ,MAX_STZ )
	FREE_AND_NULL( __stz_info__->segs ) ;
	FREE_AND_NULL( __stz_info__ ) ; 
}
/* ====================================================================
analyze.c (get_stz_downgrade)
2008-03-13 : lower grade standardizations should not produce the same
matching score as higher grade. This can be critical when a lower grade
standardization produces a perfect match on the wrong reference record
=======================================================================*/
double get_stz_downgrade( STAND_PARAM *__stand_param__ , int request_stz ) 
{
	double numerator, denominator ;
	STZ_PARAM *__stz_info__ = __stand_param__->stz_info ;
	if (( __stz_info__->stz_list_size - 1 ) < request_stz )
	{
		return 0. ;
	}
	if ( request_stz == 0 ) 
	{
		return 1.0 ;
	}
	if (( denominator = __stz_info__->stz_array[0]->score ) == 0. )
	{
		return denominator ;
	}
	numerator = __stz_info__->stz_array[request_stz]->score ;
	return ( numerator / denominator ) ;
}

/* ====================================================================
analyze.c (get_next_stz)
called by analyze.c (evaluator) , build.c (Build)
  build.c (transform_rows) match.c (match_records),
calls analyze.c (check_def_block, delete_duplicate_stz) 
export.c (init_output_fields, stuff_fields)
<remarks>
      return FALSE if the requested stz is not there - this allows 
      termination to a request loop when there are fewer than the maximum 
      number on the list and also reports, on request of 0, that none were 
      found. If the request_stz is the same as the last one done (since 
      evaluator last initialized the last_stz_output variable) we just 
      return rather than redo the same work. When matching we need 
      to know the correct standardization for positioning the point along 
      the arc 
      2008-04-06 : This function needs to return to the 0 stz when selecting
      the best standardization for the build. To indicate that an override
      is required, we'll take FAIL as a proxy for 0.
</remarks>
=======================================================================*/
int get_next_stz( STAND_PARAM *__stand_param__ , int request_stz_in )
{
	int i ;

	DEF **__best_defs__ = __stand_param__->best_defs ;
	SYMB *__best_output__ = __stand_param__->best_output ;
	STZ_PARAM *__stz_info__ = __stand_param__->stz_info ;
	int n = __stand_param__->LexNum ;
	int request_stz = request_stz_in ;
    STZ * __cur_stz__ ;
	if (request_stz_in != FAIL)
	{
		if ((( __stz_info__->stz_list_size - 1 ) < request_stz ) || ( __stz_info__->last_stz_output == request_stz ))
		{
			/*-- Indicate that this is the last one : don't call
				get_next_stz with 0 unless you want a FALSE --*/
			return FALSE ;
		}
		/*-- Delete standardizations that contain blocked definitions --*/
		while (( check_def_block( __stand_param__ , request_stz )) && ( __stz_info__->stz_list_size > request_stz )) ;
		/*-- Have we reached the end of the list? --*/
		if ( __stz_info__->stz_list_size == request_stz ) 
		{
			return FALSE ;
		}
		/* -----------------------------------------------------------------------
			<remarks> A clause tree analysis may produce identical output to a MICRO_C by
			combining an ARC_C and CIVIC_C pair. We want only the first one in any
			situation where we ask for lower scoring candidates </remarks>
		------------------------------------------------------------------------ */
		if ( request_stz > FIRST_STZ )
		{
			while (( delete_duplicate_stz( __stz_info__, request_stz )) && ( __stz_info__->stz_list_size > request_stz )) ;
			if ( __stz_info__->stz_list_size == request_stz )
			{
				return FALSE ;
			}
		}
	} 
	else 
	{
		request_stz = FIRST_STZ ;
	}
	/*-- Reload the best defs and output from the new stz --*/
	__cur_stz__ = __stz_info__->stz_array[request_stz] ;
	for ( i = FIRST_LEX_POS ; i < n ; i++ )
	{
		__best_defs__[i] = __cur_stz__->definitions[i] ;
		__best_output__[i] = __cur_stz__->output[i] ;
	}
	__best_defs__[i] = NULL ;
	__best_output__[i] = FAIL ;
	/* -------------------------------------------------------------------
		Because this function is called with values greater than 0 only to
		redo a MICRO
	-------------------------------------------------------------------- */
	if (request_stz > FIRST_STZ || request_stz_in == FAIL)
	{
		/*-- LEFT : just MICRO here --*/
		init_output_fields(__stand_param__,LEFT) ;
		stuff_fields( __stand_param__ ) ;
	}
	__stz_info__->last_stz_output = request_stz ;
	return TRUE ;
}

/* ====================================================================
analyze.c (check_def_block)
called by analyze.c (get_next_stz)
calls analyze.c (delete_stz)
=======================================================================*/
static int check_def_block( STAND_PARAM *__stand_param__ , int request_stz )
{
	int i, j ;
	STZ_PARAM *__stz_info__ = __stand_param__->stz_info ; 
	SYMB *__cur_sym_ptr__ = __stz_info__->stz_array[ request_stz ]->output ; 
	DEF **__stz_definitions__ = __stz_info__->stz_array[ request_stz ]->definitions ; 
	int n = __stand_param__->LexNum ; 
	for (i = FIRST_LEX_POS ; i < n ; i++) 
	{
		for (j = 0 ; j < NUM_DEF_BLOCKERS ; j ++) 
		{
			if (__cur_sym_ptr__[i] == __def_block_table__[j].output_symbol) 
			{
				if (__stz_definitions__[i] == __def_block_table__[j].definition) 
				{
					delete_stz(__stz_info__ , request_stz) ;
					return TRUE ;
				}
			}
		}
	}
	return FALSE ;
}

/* ====================================================================
analyze.c (delete_stz)
called by analyze.c (check_def_block), analyze.c (delete_duplicate_stz)
=======================================================================*/
static void delete_stz( STZ_PARAM *__stz_info__ , int request_stz )
{
	int i, n ;
    STZ **__stz_list__ ;
    STZ *__stz_ptr__ ;

	__stz_info__->stz_list_size -- ; /* -- change list count for deletion -- */
	n = __stz_info__->stz_list_size ;
	__stz_list__ = __stz_info__->stz_array ; 

	/*-- last on list? - it just becomes inactive --*/
	if ( request_stz == __stz_info__->stz_list_size ) 
	{
		return ;
	}
	/*-- we don't want to lose this pointer --*/
	__stz_ptr__ = __stz_list__[request_stz] ;
	/* ----------------------------------------------------------
		move the rest of the list down to eliminate the duplicate.
		The replacement entry will become the new, requested stz 
		
		if there are, for instance, n stz pointers active, stz_list_size
		will be n. So the ordinal n-1 is the last active stz. In this
		function, after the first instruction, n will point to the last
		active stz. When we move the stz pointers down, when i = n-1,
		the nth is moved into n-1. So, in order not to lose the pointer,
		the deleted stz goes into the vacated nth spot
		 0       req       n-1  n  inactive  MAX_STZ - 1
		[ ] [ ] [ ] ...   [ ] [ ] [ ] ...   [ ]
	----------------------------------------------------------- */
	for ( i = request_stz ; i < n ; i ++ ) 
	{
		__stz_list__[i] = __stz_list__[i+1] ;
	}
	/* -- save the pointer, now inactive, for reuse -- */
	__stz_list__[n] = __stz_ptr__ ;
}

/* ====================================================================
analyze.c (delete_duplicate_stz)
calls analyze.c (delete_stz)
called by analyze.c (get_next_stz)
=======================================================================*/
static int delete_duplicate_stz(STZ_PARAM *__stz_info__, int request_stz)
{
	/* --------------------------------------------------------------------- 
      if the requested_stz is identical to any earlier ones on the list, 
      both for definition and output symbol, or if it contains a blocked
      definition , eliminate this entry and move the rest of the list down  
      one. Return TRUE if this happens, otherwise FALSE
	  -----------------------------------------------------------------------*/ 

	int i ;
	STZ **__stz_list__ = __stz_info__->stz_array ;
	for (i = FIRST_STZ; i < request_stz; i ++)
	{
		SYMB a ;
		SYMB *__cur_sym_ptr__ = __stz_list__[request_stz]->output ;
		DEF **__stz_definitions__ = __stz_list__[request_stz]->definitions ;
		SYMB *__prev_sym_ptr__ = __stz_list__[i]->output ;
		DEF **__prev_stz_definitions__ = __stz_list__[i]->definitions ;

		while (( a = *__prev_sym_ptr__++ ) == *__cur_sym_ptr__++ )
		{
			/* -------------------------------------------------------------
				A differing definition, even if the output token is the same
			could lead to a different result
			-------------------------------------------------------------- */
			if (*__prev_stz_definitions__++ != *__stz_definitions__++)
			{
				return FALSE ;
			}
			/*-- FAIL terminates output , so they're identical --*/
			if (a == FAIL)
			{
				delete_stz(__stz_info__, request_stz) ;
				return TRUE ;
			}
		}
	}
	return FALSE ;
}

/* ====================================================================
analyze.c (evaluate_micro_l)
called by evaluator
2009-08-09 : special routine for MICRO_L state : landmark words
<revision date='2012-07-22'> Keep track of start_state </revision>
=======================================================================*/

static int evaluate_micro_l( STAND_PARAM *__stand_param__ )
{
	int i , desired_type , output_field ;
	int __def_marked__[MAXLEX][MAXDEF] ;
	int *__orig_pos__ = __stand_param__->orig_str_pos ;
	int *__sym_sel__ = __stand_param__->cur_sym_sel ;
	int *__num_defs__ = __stand_param__->def_cnt ;
	LEXEME *__lexeme__ = __stand_param__->lex_vector ;
	int n = __stand_param__->LexNum ;
	/* 2009-08-15 : use lexicon types */
	switch ( __stand_param__->start_state )
	{
	case FEAT_L :
		desired_type = 1 ;
		output_field = FEATNAME ;
		break ;
	case FEAT_T :
		desired_type = 2 ;
		output_field = FEATTYPE ;
		break ;
	case FEAT_A :
		desired_type = 1 ;
		output_field = FEATAREA ;
		break ;
	default :
		return FALSE ;
	}
	/* -- read the symbols from the definitions into the lex_sym array -- */
	for (i = FIRST_LEX_POS ; i < n ; i++)
	{
		int j ;
		DEF *__def__ ;
		__orig_pos__[i] = i ; /* we won't use compression here */
		__sym_sel__[i] = 0 ; /* -- start at 0 for each Lexeme -- */
		/* -- walk the def chain, counting the symbs and putting them
		into the array -- */
		for (j = 0, __def__ = __lexeme__[i].DefList; __def__ != NULL; __def__ = __def__->Next, j++)
		{
			__stand_param__->comp_lex_sym[i][j] = __def__->Type ;
			__stand_param__->def_array[i][j] = __def__ ;
			/* 2009-08-30 : filter out non-default non-desired */
			if ((__def__->Type == desired_type) || (__def__->Protect))
			{
				__def_marked__[i][j] = TRUE ;
			} 
			else __def_marked__[i][j] = FALSE ;
		}
		__num_defs__[i] = j ;
	}
	/*-- Now go through all the compositions, looking for those consisting
		only of unduplicated defs --*/
	do 
	{
		int marked ;
		double seg_score ;
		/* one duplicated def disqualifies this composition */
		for (i = n-1 , marked = TRUE; i >= FIRST_LEX_POS; i --)
		{
			if (!__def_marked__[i][__sym_sel__[i]])
			{
				marked = FALSE ;
				break ;
			}
		}
		/* 2009-10-16 : accept other types */
		seg_score = (marked ? EXCELLENT : LOW) ;
		default_seg_val(__sym_sel__, n, __stand_param__->stz_info->segs, FALSE, output_field, seg_score) ;
		_force_deposit_(__stand_param__, ( n - 1)) ;
	} while ( select_next_composition(__stand_param__)) ;
	return ( get_next_stz(__stand_param__, FIRST_STZ)) ; /* -- in case nothing was found -- */
}


/* ====================================================================
analyze.c (evaluator)
called by standard.l (close_stand_field)
calls analyze.c (first_composition) , analyze.c (shallow_clause_scan) ,
  analyze.c (scan_clause_tree) , analyze.c (select_next_composition) ,
  analyze.c(force_arc_clause) , analyze.c (_force_macro_clause_) ,
  analyze.c(non_geocode_address) , analyze.c (get_next_stz)
analyze.c (prepare_target_pattern)
<revision date='2006-11-02'> add STAND_PARAM arg and change calls </revision>
<revision date='2012-07-22'> Keep track of start_state </revision>
=======================================================================*/
int evaluator(STAND_PARAM *__stand_param__) 
{

    int state ;
	STZ_PARAM *__stz_info__ = __stand_param__->stz_info ;
	__stz_info__->stz_list_cutoff = INITIAL_STZ_CUTOFF ; 
	state = __stand_param__->start_state ;

#ifdef OCCUPANCY_DEBUG
	if (state == EXTRA_STATE)
	{
		__stz_info__->stz_list_cutoff = 0.00 ;
	}
#endif
	__stz_info__->stz_list_size = FIRST_STZ ;
	__stz_info__->last_stz_output = FAIL ;
   
	/*-- <revision date='2009-08-09'> Special evaluation for landmarks </revision> --*/
	if (state > EXTRA_STATE) 
	{
		return (evaluate_micro_l(__stand_param__)) ;
	}
	while (TRUE)
	{
		first_composition(__stand_param__) ; /* 2007-08-09 */
		/* -- cycle through all the possible compositions -- */
		do 
		{
			int target_len ;
			if ((target_len = prepare_target_pattern(__stand_param__)) == TARG_START)
			{
				continue ;
			}
			/* -------------------------------------------------------------- 
            We don't need to build a clause tree for each composition for
            MICRO_B and MACRO start states since we only want one 
            segment.
			----------------------------------------------------------------*/
			switch (state)
			{
            case MACRO :
				shallow_clause_scan(__stand_param__, MACRO_C, target_len) ;
				break ;
            case MICRO_B :
				shallow_clause_scan(__stand_param__, ARC_C, target_len) ;
               break ;
            case EXTRA_STATE :
				/* -- 2008-04-19 : scan for occupancy only -- */
				shallow_clause_scan(__stand_param__, EXTRA_C, target_len) ;
				break ;
			default :
				scan_clause_tree(__stand_param__, state, target_len) ;
			}
			/* ---------------------------------------------------------------- 
            If we don't check the list size, we may be checking the score
            of some previous result in the case where no standardization is
            found
			----------------------------------------------------------------- */
			if ((__stz_info__->stz_list_size > FIRST_STZ) && (!__stand_param__->analyze_complete) && (__stz_info__->stz_array[FIRST_STZ]->score >= __load_value__[EXCELLENT]))
			{
				break ;
			}
		} while (select_next_composition(__stand_param__)) ;
		if ((__stz_info__->stz_list_size > FIRST_STZ) && (__stz_info__->stz_array[FIRST_STZ]->score >= __load_value__[1]))
		{
			break ;
		}
		/* -- force a segment -- */
		if (state == MICRO_B)
		{
			force_arc_clause(__stand_param__) ;
			break ;
		}
#ifdef USE_FORCE_MACRO
		if (state == MACRO)
		{
			_force_macro_clause_(__stand_param__) ;
			break ;
		}
#endif
		if (state != MICRO_M) 
		{
			break ;
		}
		if (!non_geocode_address(__stand_param__))
		{
			break ;
		}
		state = EXIT ;
	} /*-- end of while TRUE --*/
	return (get_next_stz(__stand_param__, FIRST_STZ)) ; /* -- in case nothing was found -- */
}


/* ====================================================================
<summary>
<function name='analyze.c (first_composition)'/>
<called-by> <functionref='analyze.c (evaluator)'/>
<remarks> Called by Evaluator to intialize __best_output__ and __sym_sel__ - 
	also sets up lex_sym, save_defs and __num_defs__ from the 
    definitions in the LexVector  </remarks>
</summary>
=======================================================================*/
static void first_composition( STAND_PARAM *__stand_param__ )
{
	int i ;

	int *__sym_sel__ = __stand_param__->cur_sym_sel ;
	int *__num_defs__ = __stand_param__->def_cnt ;
	LEXEME *__lexemes__ = __stand_param__->lex_vector ;
	int n = __stand_param__->LexNum ;
	/*-- <remarks> Read the symbols from the definitions into the lex_sym array </remarks> --*/
	for (i = FIRST_LEX_POS; i < n; i++)
	{
		int j ;
		DEF *__def__ ;
		__sym_sel__[i] = 0 ; /* -- start at 0 for each Lexeme -- */
		/*-- <remarks> Walk the def chain, counting the symbs and putting them
			into the array </remarks> --*/
		for (j = 0, __def__ = __lexemes__[i].DefList; __def__ != NULL; __def__ = __def__->Next, j++)
		{
			__stand_param__->comp_lex_sym[i][j] = __def__->Type ;
			__stand_param__->def_array[i][j] = __def__ ;
		}
		__num_defs__[i] = j ;
	}     
}

/* ============================================================
analyze.c (prepare_target_pattern)
called by analyze.c (evaluator)
calls analyze.c (need_compression) gamma.c (refresh_transducer)
2006-10-31 : add STAND_PARAM parameter and change calls
==============================================================*/
static int prepare_target_pattern(STAND_PARAM *__stand_param__)
{
	int lex_pos, target_pos;
	int *__sym_sel__ = __stand_param__->cur_sym_sel ;
	SYMB *__p_target__ = __stand_param__->target ;
	int *__orig_pos__ = __stand_param__->orig_str_pos ;
	int n = __stand_param__->LexNum ;
	NODE **__g_function__ = __stand_param__->rules->gamma_matrix ;
	for ( lex_pos = FIRST_LEX_POS , target_pos = TARG_START ; lex_pos < n ;lex_pos++ )
	{
		SYMB in_symb = __stand_param__->comp_lex_sym[lex_pos][__sym_sel__[lex_pos]] ;
		/* ------------------------------------------------------------
			compress multiple words and stopwords - the idea is that
			any combination of LEFT and RIGHT compression tokens (words
			and stopwords, compress as a single word
		------------------------------------------------------------- */
		if ( !need_compression( __stand_param__ , in_symb , lex_pos , target_pos ))
		{
			/* --------------------------------------------------------- 
            If no compression, associate this lex_pos with the
            target_pos, put the symbol into the target and increment
            the target_pos. Otherwise, keep the same target_pos and
            discard symbol
			---------------------------------------------------------- */
			__orig_pos__[lex_pos] = target_pos ;
			__p_target__[target_pos++] = in_symb ;
		}
	}
	/*-- Terminate symb lists --*/
	__p_target__[target_pos] = FAIL ;
	/*-- But suppose we only have one symbol, and it is a stopword --*/
	if ( target_pos > TARG_START )
	{
		/*-- Set up the Aho-Corasick registry of output links --*/
		refresh_transducer( __stand_param__->registry , __p_target__ , __g_function__ ) ;
	}
	return target_pos ; /* -- return cardinal number of target symbols -- */
}

/* ============================================================
analyze.c (no_break)
called by analyze.c (do_left_combine)
-- moved from tokenize.c to analyze.c
==============================================================*/
static int no_break( STAND_PARAM *__stand_param__ , int n ) 
{
	int k = __stand_param__->lex_vector[n].EndMorph ;
	/* 0 is no break
	1 is set for semicolons, tabs and commas,
	2 for spaces */
	return (( __stand_param__->morph_array[k].Term  == 1 )? FALSE : TRUE ) ;
}


/* ============================================================
analyze.c (do_left_combine)
calls analyze.c (no_break) called by analyze.c (need_compression)
==============================================================*/
static int do_left_combine( STAND_PARAM *__stand_param__ , int lex_pos , int target_pos )
{
	/*-- A LEFT_COMPRESS left compresses only if a LEFT_COMPRESS there to
		combine with --*/
	if (( target_pos == TARG_START ) || ( __stand_param__->target[target_pos - 1] != LEFT_COMPRESS )) 
	{
		/*-- A RIGHT_COMPRESS also returns FALSE if it is at the start or
			if the previous token isn't a LEFT_COMPRESS. need_compression will
			deal with this --*/
		return FALSE ;
	}
	/*-- A break in the lex sequence suggests these two words don't
		belong together --*/
	if ( !no_break( __stand_param__ , lex_pos - 1 ))
	{
		return FALSE ;
	}
	/*-- Okay, left compress it by giving it the same target position as the
		previous symbol --*/
	__stand_param__->orig_str_pos[lex_pos] = target_pos - 1 ; /* -- need to associate lex_pos
                            and target_pos for later decompression --*/
	return TRUE ; /*-- Indicate compression was done --*/
}


/* ============================================================
analyze.c (need_compression)
called by analyze.c (prepare_target_pattern)
calls analyze.c (do_left_combine)
==============================================================*/
static int need_compression( STAND_PARAM *__stand_param__ , SYMB a , int lex_pos , int target_pos ) 
{
	/*-- No stopwords are accepted, no matter what --*/
	if ( a == RIGHT_COMPRESS ) 
	{
		/*-- Does it combine with the last target symbol or the next? --*/
		if ( !do_left_combine( __stand_param__ , lex_pos , target_pos ))
		{
			/* --------------------------------------------------------------- 
            do a right combine by giving it the next position. Note that 
            this allows the possibility of a STOPWORD with combining with 
            TYPE or DIR tokens, but this is what we want in cases like EL 
            CAMINO RD -- a RIGHT_COMPRESS may stray into the wrong field --
            deal with this when decompressing
			---------------------------------------------------------------- */
			__stand_param__->orig_str_pos[lex_pos] = target_pos ; /* -- target_pos does not
                                              advance if returning TRUE --*/
		}
		return TRUE ;
	}
	/* -----------------------------------------------------------------------
	everything that isn't a WORD must be accepted - we don't want to
	combine words that are used in parsing, - two direction words, for
	instance, one of which may be used as part of a street name, the other
	perhaps as a suffix direction.
	-------------------------------------------------------------------------*/
	if ( a != LEFT_COMPRESS )
	{
		return FALSE ;
	}
	/*-- compress the WORD --*/
	return ( do_left_combine( __stand_param__ , lex_pos , target_pos )) ;
}

/*========================================================================
analyze.c (scan_clause_tree)
Called by analyze.c (Evaluator)
Calls analyze.c (deposit_stz)
2006-11-02 : add KW *** arg, change call to GetOutputLink to direct access
=========================================================================*/
static void scan_clause_tree(STAND_PARAM *__stand_param__,int start_state,int start_pos)
{
	int next_state ;

	RULE_PARAM *__rules__ = __stand_param__->rules ;
	KW ***__output_link__ = __rules__->output_link ;
	SEG *__segments__ = __stand_param__->stz_info->segs ;
	double sum = 0.00 ; /* -- running total for score calculation --*/
	int pos = start_pos ; /* -- one beyond the last symbol -- */
	int state = start_state ; /* --for the __tran_table__ -- */
	int depth = START_DEPTH ; /* --how deep in the clause tree -- */
	int cl = 0 ; 
	KW *__keyw__ = NULL ;

	while (TRUE)
	{
        SEG *__outer_seg__ ;
		while (TRUE)
		{
            SEG *__inner_seg__ ;
			if (__keyw__ == NULL)
			{
				/*-- when we're out of keys for this class, get next class --*/
				if (++cl == MAX_CL)
				{
					/* -- no more states to transition to, so go up clause tree
					- unless there's nowhere to go -- */
					if (depth == START_DEPTH) return ; /* -- the exit -- */
					depth -- ;
					break ;
				}
				if ((next_state = __tran_table__[state][cl]) == FAIL)
				{
					/*-- no transition, try next clause --*/
					continue ;
				}
				/*-- recall that the registry is shifted right one node to
				account for the node that corresponds to total failure --*/
				/*-- <revision date='2006-11-02'> Substitute for GetOutputLink </revision> --*/
				if ((__keyw__ = __output_link__[__stand_param__->registry[pos]][cl]) == NULL)
				{
					continue ;
				}
			} /* end of if keyword is NULL */

			/* -- skip pointless rules -- */
			if ((__keyw__->Length == pos) && (next_state != EXIT))
			{
				__keyw__ = __keyw__->OutputNext ; /* -- the next key to check -- */
				continue ;
			}
			/* -- fill in this definition for output if it forms part of a
            completed stz -- */
			__inner_seg__ = __segments__ + depth ;
			__inner_seg__->End = pos - 1 ; /* -- ordinal numb of last sym in target -- */
			__inner_seg__->Key = __keyw__ ;
			__inner_seg__->State = state ;
			__inner_seg__->Output = __keyw__->Output ;
			if (__rules__->collect_statistics)
			{
				__keyw__->hits ++ ;
				__rules__->total_key_hits ++ ;
			}
			/* -- running total in sum, segment total in Segment -- */
			sum += (__inner_seg__->Value = __load_value__[__keyw__->Weight] * __weight_table__[__keyw__->Type]) ;
			if ((__inner_seg__->Start = pos - (__keyw__->Length)) == 0)
			{
				/* -- all definitions have been matched: if this is a valid
				state, save the standardization , then head back up
				the tree -- */
				if (next_state == EXIT)
				{
					deposit_stz(__stand_param__,sum,depth) ;
				}
				/* -- keep the same cl,  state , depth and pos -- */
				sum -= __inner_seg__->Value ; /* -- restore the previous sum -- */
				__keyw__ = __keyw__->OutputNext ; /* -- and get the next rule on the 
                                              linked list -- */
				continue ;
			}
			/* -- begin a subtree at the new depth -- */
			pos = __inner_seg__->Start ;
			state = __tran_table__[state][cl] ;
			depth ++ ;
			cl = 0 ;
			__keyw__ = NULL ; /* -- new start -- */
		} /* -- end of inner loop -- */
		/* -- restore the previous state from the seg before overwrite -- */
		__outer_seg__ = __segments__ + depth ;
		state = __outer_seg__->State ;
		if (depth != START_DEPTH)
		{
			sum -= __outer_seg__->Value ;
			pos = __outer_seg__->End + 1 ;
		} 
		else 
		{
			sum = 0.00 ;
			pos = start_pos ;
		}
		__keyw__ = __outer_seg__->Key ;
		cl = __keyw__->Type ; /* -- the clause we were working on -- */
		__keyw__ = __keyw__->OutputNext ; /* -- the next key to check -- */
	} /* -- end of outer loop -- */
}

/*========================================================================
analyze.c (shallow_clause_scan)
Called by analyze.c (evaluator)
Calls analyze.c (deposit_stz)
<remarks>Called by Evaluator to get a complete rule for this class. If we
		can't get a complete rule we don't want one at all. If no composition
		can up with one, force_standardization will activate</remarks>
2006-11-02 : add KW *** arg, change call to GetOutputLink to direct access
=========================================================================*/
static void shallow_clause_scan(STAND_PARAM *__stand_param__ , int cl, int pos)
{
	KW *__kw__ ;

	RULE_PARAM *__rules__ = __stand_param__->rules ;
	KW ***__output_link__ = __rules__->output_link ;
	SEG * __seg__ = __stand_param__->stz_info->segs ;
	__seg__->End = pos - 1 ;
	__seg__->Start = 0 ;
	/*-- <revision date='2006-11-02'> Substitute for GetOutputLink </revision> --*/
	for (__kw__ = __output_link__[__stand_param__->registry[pos]][cl] ; __kw__ != NULL; __kw__ = __kw__->OutputNext)
	{
		/*-- once we get a short keyword, depart --*/
		if (__kw__->Length < pos) return ;
		/*-- fill in the rest of this definition for output if it forms part 
         of a completed stz --*/
		__seg__->Output = __kw__->Output ;
		if (__rules__->collect_statistics)
		{
			__seg__->Key = __kw__ ;
			__kw__->hits ++ ;
			__rules__->total_key_hits ++ ;
		}
#ifdef OCCUPANCY_DEBUG
		if (cl == EXTRA_C)
		{
			SYMB *__ol__ ;
			printf( "\nRule is type %d (%s)\n: " , __kw__->Type , __rule_type_names__[__kw__->Type] ) ;
			printf( "Input : " ) ;
			for ( __ol__ = __kw__->Input ; *__ol__ != FAIL ; __ol__++ )
			{
				printf( "|%d (%s)|", *__ol__ , in_symb_name( *__ol__ )) ;
			}
			printf("\nOutput: ") ;
			/*-- output the output symbols --*/
			for (__ol__ = __kw__->Output;*__ol__ != FAIL;__ol__++)
			{
				printf("|%d (%s)|",*__ol__,out_symb_name(*__ol__)) ;
			}
			printf ("\nrank %d ( %f)\n",__kw__->Weight,__load_value__[__kw__->Weight]) ;
		}
#endif
		/* -- don't skew weights with these start states - so the cutoff is 
         easier -- */
		deposit_stz(__stand_param__,__load_value__[__kw__->Weight],START_DEPTH) ;
	}
}

/* ====================================================================
analyze.c (select_next_composition)
called by analyze.c (evaluator)
=======================================================================*/
static int select_next_composition( STAND_PARAM *__stand_param__ )
{
	int pos ;
	int *__sym_sel__ = __stand_param__->cur_sym_sel ;
	int *__num_defs__ = __stand_param__->def_cnt ;

	for ( pos = __stand_param__->LexNum - 1 ; pos >= FIRST_LEX_POS ; pos-- )
	{
		__sym_sel__[pos]++ ; /*-- Increase selector --*/
		if ( __sym_sel__[pos] < __num_defs__[pos] )
		{
			/*-- Not ready yet for turnover --*/
			return TRUE ;
		}
		__sym_sel__[pos] = 0 ; /*-- Reset selector --*/
	}
	return FALSE ;
}

/* ====================================================================
<summary>
	<function name='analyze.c (make_singleton)'>
	<remarks> Called to make a segment with a putative single position output.
		Don't really need a KW. as long as copy_best knows how to handle
		it. </remarks>
	<called-by><functionref='analyze.c (default_seg_val)'/></called-by>
	<revision date='2009-08-09'> Eliminate cl arg to make_singleton. </revision>
</summary>
=======================================================================*/
static void make_singleton( SEG *__segments__, SYMB sym , int pos, int depth, double score )
{

	/*-- <remarks> Since the __segments__ go left to right and the positions go right to
		left, the depth and position will usually be different. </remarks> --*/
	SEG *__seg__ = __segments__ + depth ;
	__seg__->Start = pos ;
	__seg__->End = pos ;
	__seg__->Value = score ;
	__seg__->Output = NULL ;
	__seg__->sub_sym = sym ;
}

/* ====================================================================
analyze.c (deposit_stz)
calls analyze.c (copy_stz, save_current_composition)
called by analyze.c (_force_deposit_, shallow_clause_scan,scan_clause_tree)
=======================================================================*/
static void deposit_stz( STAND_PARAM *__stand_param__ , double sum , int depth )
{
	STZ_PARAM * __stz_info__ = __stand_param__->stz_info ;
    STZ *__cur_stz__ ;

	/*-- calculate the score here --*/
	double cur_score = (sum / (double) (depth + 1)) ;

	/*-- and apply the cutoff before doing all the work of putting it into
		the list --*/
	if ( cur_score < __stz_info__->stz_list_cutoff ) return ;

	/*-- need the score to get the pointer, need the pointer to copy the
		content --*/
	__cur_stz__ = copy_stz( __stand_param__ , cur_score ) ;

	/*-- Then add the content, once we have a pointer -- */
	if (( __stand_param__->rules->collect_statistics ) && ( depth == START_DEPTH ))
	{
		SEG *__seg__ = __stz_info__->segs + START_DEPTH ;
		if (__seg__->Key != NULL)
		{
			__cur_stz__->build_key = __seg__->Key ;
		}
	}
	save_current_composition( __stand_param__ , __stz_info__->segs,depth , __cur_stz__->output , __cur_stz__-> definitions ) ;
}

#define DUP_DECREMENT .0025

/* ====================================================================
analyze.c (copy_stz)
called by analyze.c (deposit_stz)
=======================================================================*/
static STZ * copy_stz(STAND_PARAM *__stand_param__ ,double current_score)
{
	/* -- sort it into the list and knock the last one off the list 
      if it is MAX_STZ -- */
	/* -- Take the Score of the last remaining item as the new cutoff, 
      if it is greater than the current cutoff -- */
	int i ;
    int last_on_list ;
    STZ *__cur_stz__ ;


	STZ_PARAM *__stz_info__ = __stand_param__->stz_info ;
	STZ **__stz_list__ = __stz_info__->stz_array ; 

	/* -- Increase the list size only if it isn't full. If it is full, take
		the score of the last on the list (which we're going to knock off the
		list) as the new cutoff -- */
	
	if (__stz_info__->stz_list_size != MAX_STZ)
	{
		__stz_info__->stz_list_size++ ;
	}
	
	/* -- Get the pointer of the last on the list if the list is full (to be 
      knocked off, or one beyond the previous last item (with undefined 
      content) if the list isn't full. -- */
	last_on_list = __stz_info__->stz_list_size - 1 ;
	__cur_stz__ = __stz_list__[last_on_list] ; /* -- implicitly discard contents -- */
	__cur_stz__->score = current_score ;
	__cur_stz__->raw_score = current_score ;

	/*-- Initialize the output vector - but is this necessary ? --*/
	for (i = FIRST_LEX_POS;i <= __stand_param__->LexNum;i++)
	{
		__cur_stz__->output[i] = FAIL ;
	}
	/* -- boundary condition : last-1   last
                               [ ]     [ ] 
      suppose the last - 1 has a score less than the current score - then
        it isn't copied into last, so __cur_stz__ goes back into the slot
        from which it was just removed - nothing moves  -- */
	for (i = last_on_list;i > FIRST_STZ;i --)
	{
		/* -- Get the next pointer on the list and move it back if it has a 
         lesser score. Otherwise we put the pointer to the new stz in the 
         present position -- */
		STZ *__next_stz__ = __stz_list__[i-1] ;
		if (current_score > __next_stz__->raw_score)
		{
			__stz_list__[i] = __next_stz__ ;
		}
		else
		{
			if (current_score == __next_stz__->raw_score)
			{
				/* -- 2008-03-14: first come, first served -- */
				__cur_stz__->score = __next_stz__->score - DUP_DECREMENT ;
			}
			break ;
		}
	}
	__stz_list__[i] = __cur_stz__ ;
	if (__stz_info__->stz_list_size == MAX_STZ)
	{
		__stz_info__->stz_list_cutoff = __stz_list__[last_on_list]->score ;
	}
	return __cur_stz__ ; /* -- tell the caller where we put it -- */
}

/* ====================================================================
analyze.c (save_current_composition)
called by analyze.c (deposit_stz)
calls analyze.c (copy_best)
<remarks>called by deposit_stz to align the current standardization output 
      symbols to the LEXEME input symbols - it depends on the correct 
      LEXEMES being present and the __sym_sel__ reflecting the last composition. 
      Consequently it must be done at the time of deposit </remarks>
=======================================================================*/
static void save_current_composition(STAND_PARAM *__stand_param__,SEG *__segments__, int depth, SYMB *__best_output__ , DEF **__best_defs__) 
{
	
	int lex_pos ;
	SEG *__seg__ ;
	int *__sym_sel__ = __stand_param__->cur_sym_sel ;
	
	/*-- <remarks> Get the definitions selected from save_defs - needed for outputing
		the lexemes. Different definitions may give a different
		standardization for the same input - the letter W may be standardized
		as W if a SINGLE or WEST if a DIRECT </remarks> --*/
	
	/* -- use the whole target -- */
	for ( lex_pos = FIRST_LEX_POS ; lex_pos < __stand_param__->LexNum ; lex_pos++ ) 
	{
		__best_defs__[lex_pos] = __stand_param__->def_array[lex_pos][__sym_sel__[lex_pos]] ;
	}
	__best_defs__[lex_pos] = NULL ;
	
	/*-- <remarks> Segments go backwards (right to left) , but the content for
      each segment goes left to right </remarks> --*/
	
	for ( __seg__ = __segments__ + depth, lex_pos = FIRST_LEX_POS ; __seg__ >= __segments__ ; __seg__-- )
	{
		SYMB *__sym_ptr__ ;
		if (( __sym_ptr__ = __seg__->Output ) == NULL)
		{
			lex_pos = copy_best( __stand_param__ , __sym_sel__ , __seg__->sub_sym , lex_pos , __best_output__ ) ;
			continue ;
		}
		for ( ; *__sym_ptr__ != FAIL ; __sym_ptr__ ++ )
		{
			lex_pos = copy_best( __stand_param__ , __sym_sel__ , *__sym_ptr__ , lex_pos , __best_output__ ) ;
		}
   }
}

/* ====================================================================
analyze.c (copy_best)
called by analyze.c (save_current_composition)
<remarks> Called by save_current_composition to decompress stopword and word
      sequences </remarks>
=======================================================================*/
static int copy_best( STAND_PARAM *__stand_param__ , int *__sym_sel__ , SYMB output_symb , int beg , SYMB *__best_output__ )
{
	int lex_pos ; 
	int *__orig_pos__ = __stand_param__->orig_str_pos ;
	
	/*-- <remarks> <code>orig_pos</code> has the (multiple) LEXEME positions to which the 
      (single) output symbol corresponds - so we add that symbol to each of 
      the positions </remarks> --*/
	
	int next_target_pos = __orig_pos__[beg] + 1 ;
	for ( lex_pos = beg ; __orig_pos__[lex_pos] < next_target_pos ; lex_pos ++ )
	{
		if ( lex_pos == __stand_param__->LexNum ) break ;

		/*-- <remarks> Check for errant RIGHT_COMPRESS - put it back into STREET
			if possible </remarks> --*/

		if (( lex_pos > FIRST_LEX_POS ) && ( output_symb != STREET ) && ( __stand_param__->comp_lex_sym[lex_pos][__sym_sel__[lex_pos]] == RIGHT_COMPRESS ) && ( __best_output__[lex_pos - 1] == STREET ))
		{
			__best_output__[lex_pos] = STREET ;
		} 
		else 
		{
			__best_output__[lex_pos] = output_symb ;
		}
	}
	return lex_pos ;
}

/* ====================================================================
analyze.c (lex_has_def)
called by analyze.c (non_geocode_address, _modify_position_)
scan the ith row of comp_lex_sym for the symbol sym
returns the matching cell j
=======================================================================*/
static int lex_has_def(STAND_PARAM *__stand_param__, int i, SYMB sym)
{
	int j ;
	int *__num_defs__ = __stand_param__->def_cnt ;
	for (j = 0; j < __num_defs__[i]; j ++)
	{
		if (__stand_param__->comp_lex_sym[i][j] == sym)
		{
			return j ;
		}
	}
	return FAIL ;
}

/* ====================================================================
analyze.c (have_schema_symbol)
called by analyze.c (schema_modify_position)
=======================================================================*/
static int have_schema_symbol(int *__check_dir__,SYMB sym)
{
	if (__check_dir__ != NULL)
	{
		if (__check_dir__[sym])
		{
			return TRUE ;
		}
	}
	return FALSE ;
}

/* ====================================================================
<summary>
	<function name='analyze.c (default_seg_val)'/>
	<calls> <functionref='analyze.c (make_singleton)'/> </calls>
	<called-by> <functionref='analyze.c (force_arc_clause, 
		_force_macro_clause_)'/> </called-by>
	<revision date='2009-08-09'> Fourth arg now used to determine if
		the __sym_sel__ should be initialized to the first definition :
		save_composition uses the value. We will do that when we
		have no idea at all which the right one is -- and there is
		always at least one. </revision>
</summary>
=======================================================================*/
#define DEPTH_POS ( num_lexes - 1 ) - depth

static void default_seg_val( int *__sym_sel__, int num_lexes, SEG *__segments__, int use_default_sym, SYMB sym, double score )
{
	int depth ;
	for (depth = FIRST_LEX_POS ;depth < num_lexes;depth ++)
	{
		if (use_default_sym)
		{
			/*-- <revision date='2009-08-09'> Set default only if told to do so </revision> --*/
			__sym_sel__[DEPTH_POS] = 0 ; /* -- default value -- */
		}
		/*-- <revision date='2009-08-09'> Eliminate cl arg to make_singleton. </revision> --*/
		make_singleton(__segments__,sym,DEPTH_POS,depth,score) ;
	}
}

/* ====================================================================
analyze.c (_modify_position_)
called by analyze.c (schema_modify_position,_force_macro_clause_)
calls analyze.c (lex_has_def)
<remarks>If the input symbol is found at pos, then we put the out_sym as the sub_sym
at depth in __seg__
=======================================================================*/
static int _modify_position_(STAND_PARAM *__stand_param__, SEG *__seg__, int depth, int pos, SYMB in_sym, SYMB out_sym)
{
	int sel ;
	if ((sel = lex_has_def(__stand_param__, pos, in_sym)) != FAIL)
	{
		__seg__[depth].sub_sym = out_sym ;
		__stand_param__->cur_sym_sel[pos] = sel ;
		return TRUE ;
	}
	return FALSE ;   
}

/* ====================================================================
analyze.c (schema_modify_position)
- called by analyze.c (force_arc_clause)
calls analyze.c (have_schema_symbol, _modify_position_)
=======================================================================*/
static int schema_modify_position( STAND_PARAM  *__stand_param__ , SEG *__segments__ , int depth , int lex_pos , SYMB in_sym , SYMB out_sym )
{
	/* -- note: this requires that attributes are present. It
	only works if we're working within a particular
	reference dataset. -- */
	if (have_schema_symbol(__stand_param__->have_ref_att, out_sym))
	{
		return (_modify_position_(__stand_param__,__segments__, depth , lex_pos , in_sym , out_sym)) ;
	}
	return FALSE ;
}   



/* ====================================================================
analyze.c (force_arc_clause)
called by analyze.c (evaluator)
calls analyze.c (default_seg_val, schema_modify_position and _force_deposit_) 
<remarks>We're going to force standardization on an Arc clause without
      much computation. first_composition has already done its work,
      so we go through the lex_sym looking for likely constructions , using
      the schema read as a guide </remarks>
=======================================================================*/
static void force_arc_clause( STAND_PARAM *__stand_param__ )
{
	int lex_start, lex_end, depth ;
	STZ_PARAM * __stz_info__ = __stand_param__->stz_info ;
	int num_lexes = __stand_param__->LexNum ;
	default_seg_val( __stand_param__->cur_sym_sel , num_lexes , __stz_info__->segs , ARC_C , STREET , VERY_LOW_WEIGHT ) ;
	depth = lex_start = 0 ;
	lex_end = num_lexes -1 ;
	/*-- look for a SUFDIR in the last position --*/
	if (lex_start < lex_end -1)
	{
		if (schema_modify_position( __stand_param__ , __stz_info__->segs , depth , lex_end , DIRECT , SUFDIR )) 
		{
			lex_end-- ;
			depth ++ ;
		}
	}
	/*-- look for a SUFTYP --*/
	if (lex_start < (lex_end -1))
	{
		if (schema_modify_position( __stand_param__ , __stz_info__->segs , depth , lex_end , TYPE , SUFTYP ))
		{
			lex_end-- ;
		}
	}
	depth = num_lexes - 1 ;
	if (lex_start < (lex_end -1))
	{
		if (schema_modify_position(__stand_param__, __stz_info__->segs, depth, lex_start, DIRECT, PREDIR))
		{
			lex_start++ ;
			depth -- ;
		}
	}
	if (lex_start < (lex_end-1))
	{
		if (schema_modify_position(__stand_param__, __stz_info__->segs, depth, lex_start, TYPE, PRETYP))
		{
			lex_start++ ;
		}
	}
	_force_deposit_(__stand_param__, (__stand_param__->LexNum-1)) ;
}

#define MODIFY_SEG_POS(_IN_SYM_VAL_,_OUT_SYM_VAL_)\
if ( _modify_position_( __stand_param__ , __segments__ , depth , lex_sym_pos , _IN_SYM_VAL_ , _OUT_SYM_VAL_ ) ) { continue ; }


/* ====================================================================
<summary>
	<function name='analyze.c (_force_macro_clause_)'/>
	<called-by> <functionref='analyze.c (evaluator)'/> </called-by>
	<calls> <functionref='analyze.c (default_seg_val,_modify_position_,_force_deposit_)'/> </calls>
</summary>
=======================================================================*/
#ifdef USE_FORCE_MACRO
static void _force_macro_clause_( STAND_PARAM *__stand_param__ )
{
	int lex_sym_pos, depth ;
	int n = __stand_param__->LexNum ;
	int end = n -1 ;
	SEG *__segments__ = __stand_param__->stz_info->segs ;

	default_seg_val( __stand_param__->cur_sym_sel , n , __segments__ , MACRO_C , POSTAL , VERY_LOW_WEIGHT ) ;
	for ( lex_sym_pos = 0 , depth = end ; lex_sym_pos <= end ; lex_sym_pos ++ , depth -- )
	{
		MODIFY_SEG_POS(PCH,POSTAL);
		MODIFY_SEG_POS(PCT,POSTAL);
		MODIFY_SEG_POS(QUINT,POSTAL);
		MODIFY_SEG_POS(QUAD,POSTAL);
		MODIFY_SEG_POS(NUMBER,POSTAL);
		MODIFY_SEG_POS(MIXED,POSTAL);
		MODIFY_SEG_POS(NATION,NATION);
		MODIFY_SEG_POS(PROV,PROV);
		MODIFY_SEG_POS(CITY,CITY);
		MODIFY_SEG_POS(WORD,CITY);
	}
	_force_deposit_(__stand_param__,n-1) ;
}
#endif
/* ====================================================================
<summary>
	<function name='analyze.c (_force_deposit_)'/>
	<called-by> <function ref='analyze.c (force_arc_clause,_force_macro_clause_)'/> </called-by>
	<calls> <function ref='analyze.c (deposit_stz)'/> </calls>
</summary>
=======================================================================*/
static void _force_deposit_( STAND_PARAM *__stand_param__ , int depth ) 
{
	/*-- <remarks> Worst case scenario: we have a string of unknowns. It'll score
		really low, but not zero. </remarks> --*/
	double sum = 0.00 ;
	SEG *__seg__ ;
	SEG *__segments__ = __stand_param__->stz_info->segs ;
	for (__seg__ = __segments__ + depth; __seg__ >= __segments__; __seg__--)
	{
		sum += __seg__->Value ;
	}
	deposit_stz( __stand_param__ , sum , depth ) ;
}

/* ====================================================================
analyze.c (non_geocode_address)
called by analyze.c (evaluator)
calls analyze.c (lex_has_def)
=======================================================================*/
static int non_geocode_address( STAND_PARAM *__stand_param__ ) 
{
	/* -- scan through each position looking for an RR or BOXH token. -- */
	int lex_sym_pos ;
	int n = __stand_param__->LexNum ;
	for ( lex_sym_pos = FIRST_LEX_POS ; lex_sym_pos < n ; lex_sym_pos ++ )
	{
		int result = lex_has_def( __stand_param__ , lex_sym_pos , RR ) ;
		if ( result != FAIL ) 
		{
			return TRUE ;
		}
		if ((result = lex_has_def( __stand_param__ , lex_sym_pos , BOXH )) != FAIL)
		{
			return TRUE ;
		}
	}
	return FALSE ;
}

/* ====================================================================
analyze.c (output_raw_elements)
print out the raw elements of the tokens
=======================================================================*/
void output_raw_elements( STAND_PARAM * __stand_param__ , ERR_PARAM *__err_param__ )
{
	int stz_no , n ;
	int lex_pos ;
	DEF *__def__ ;
    STZ **__stz_list__;

	STZ_PARAM *__stz_info__ = __stand_param__->stz_info ;
	if (__err_param__ == NULL) 
	{
		printf("Input tokenization candidates:\n") ;
	} 
	else 
	{
		LOG_MESS("Input tokenization candidates:",__err_param__) ;
	}
	for (lex_pos = FIRST_LEX_POS;lex_pos < __stand_param__->LexNum;lex_pos ++) 
	{
		for ( __def__ = __stand_param__->lex_vector[lex_pos].DefList; __def__ != NULL; __def__ = __def__->Next)
		{
			if (__err_param__ == NULL) 
			{
				printf("\t(%d) std: %s, tok: %d (%s)\n",lex_pos,((__def__->Protect )? __stand_param__->lex_vector[lex_pos].Text : __def__->Standard),__def__->Type,in_symb_name(__def__->Type));
			} 
			else 
			{
				sprintf( __err_param__->error_buf , "\t(%d) std: %s, tok: %d (%s)\n" , lex_pos , (( __def__->Protect )? __stand_param__->lex_vector[lex_pos].Text : __def__->Standard) , __def__->Type , in_symb_name( __def__->Type ));
				register_error( __err_param__ ) ;
			}
		}
	}
	n = __stz_info__->stz_list_size ; 
	__stz_list__ = __stz_info__->stz_array ;
	for ( stz_no = FIRST_STZ ; stz_no < n ; stz_no ++ )
	{
		STZ *__cur_stz__ = __stz_list__[stz_no] ;
		if ( __err_param__ == NULL ) 
		{
			printf( "Raw standardization %d with score %f:\n" , ( stz_no  ) , __cur_stz__->score ) ;
		} 
		else 
		{
			LOG_MESS2( "Raw standardization %d with score %f:\n" , ( stz_no  ) , __cur_stz__->score , __err_param__ ) ;
		}
		for ( lex_pos = FIRST_LEX_POS ; lex_pos < __stand_param__->LexNum ; lex_pos ++ ) 
		{
            SYMB k;
			__def__ = __cur_stz__->definitions[lex_pos] ;
			/*-- 2010-11-18 : handle end STOPWORD --*/
			k = __cur_stz__->output[lex_pos] ;
			if ( __err_param__ == NULL ) 
			{
				printf( "\t(%d) Input %d (%s) text %s mapped to output %d (%s)\n" , lex_pos , __def__->Type , in_symb_name( __def__->Type ) , (( __def__->Protect )? __stand_param__->lex_vector[lex_pos].Text : __def__->Standard ) , k , (( k == FAIL )? "NONE" : out_symb_name( k ))) ;
			} 
			else 
			{
				sprintf( __err_param__->error_buf , "\t(%d) Input %d (%s) text %s mapped to output %d (%s)\n" , lex_pos , __def__->Type , in_symb_name( __def__->Type ) , (( __def__->Protect )? __stand_param__->lex_vector[lex_pos].Text : __def__->Standard ) , k , (( k == FAIL )? "NONE" : out_symb_name( k ))) ;
				register_error( __err_param__ ) ;
			}
			if ( k == FAIL ) break ;
		}
	}
	fflush( stdout ) ;
}

