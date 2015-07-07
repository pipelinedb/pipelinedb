/* -- standard.c 

interface for the standardizer

Prototype 7H08 (This file was written by Walter Sinclair).

This file is part of PAGC.

Copyright (c) 2009 Walter Bruce Sinclair

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

*/

/*-- For pagc-0.4.2 : last revised 2012-07-18 --*/

#undef DEBUG
//#define DEBUG 1

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include "pagc_api.h"
#ifdef BUILD_API
#include "pagc_std_api.h"
#endif

#define GAZ_LEXICON

/* -- local prototypes -- */
/*-- <revision date='2012-07-22'> Keep track of start_state </revision> --*/
static int _Close_Stand_Field_(STAND_PARAM *) ;
static int _Scan_String_(STAND_PARAM *, char *) ;
static char * _Scan_Next_(STAND_PARAM *, char *) ;

static char __spacer__[] = " \\-.)}>_" ;

#define TERM_AND_LENGTH \
	*__dest__ = SENTINEL ; \
	n = strlen(__scan_buf__) 

#define RETURN_NEW_MORPH(TOKEN_ARG) \
	if (!new_morph(__stand_param__,TOKEN_ARG,__scan_buf__,n))\
	{\
		return NULL ; \
	} \
	return __src__

#define COLLECT_LOOKAHEAD \
	*__dest__++ = a ; __src__++ ; *__dest__++ = b ; __src__++ 

#define COLLECT_WHILE(COND) \
	do { *__dest__++ = a ; __src__++ ; a = *__src__ ; } while (COND)

#define NO_COLLECT_WHILE(COND) \
	do { __dest__++ ; __src__++ ; a = *__src__ ; } while (COND)

#define TEST_FOR_ORD_DIGIT(N,NEXT_LOW,NEXT_UP) \
	if ((b == NEXT_LOW) || (b == NEXT_UP)) \
	{ \
		if (last_digit == N)\
		{ \
			if ((n < 2 ) || (*(__dest__-2) != '1')) \
			{ \
				COLLECT_LOOKAHEAD ; \
				TERM_AND_LENGTH ; \
				RETURN_NEW_MORPH(DORD) ; \
			} \
		} \
	} \
	break


/*========================================================================
<summary>
	<function name='standard.c (standardize_field)'/>
		<remarks>This function is called with a pointer to the
			str to standardize and a start state indicating
			the kind of standardization to perform. It invokes
			the scanner to start the creation of the morphemes
		<calls><functionref='tokenize.c (initialize_morphs)'/></calls>
		<calls><functionref='_Close_Stand_Field_s'/></calls>
		<calls><functionref='_Scan_String_'/></calls>
</summary>
=========================================================================*/
int standardize_field(STAND_PARAM *__stand_param__ ,char *__in_str__ , int client_start_state )
{
	/*-- <revision date='2009-08-13'> Support multiple lexicons </revision> --*/
	/*-- <revision date='2012-06-01'> Add gaz_lexicon to be triggered on start_state= MACRO </revision> --*/
	__stand_param__->lexicon = __stand_param__->address_lexicon ;
	if (client_start_state > EXTRA_STATE)
	{
		__stand_param__->lexicon = __stand_param__->poi_lexicon ;
	}
#ifdef GAZ_LEXICON
	else
	{
		if (client_start_state == MACRO)
		{
		   __stand_param__->lexicon = __stand_param__->gaz_lexicon ;
		}
	}
#endif
	/*-- <revision date='2012-07-22'> Keep track of start_state </revision> --*/
	__stand_param__->start_state = client_start_state ;
	initialize_morphs(__stand_param__) ;
	if (!_Scan_String_(__stand_param__,__in_str__))
	{
		return FALSE ;
	}
	/*-- <revision date='2012-07-22'> Keep track of start_state </revision> --*/
	return (_Close_Stand_Field_(__stand_param__)) ;
}

static int _Scan_String_(STAND_PARAM *__stand_param__ ,char *__in_str__ )
{
	char *__src__ = __in_str__ ;
	while (TRUE)
	{
		char a = *__src__ ;
		/*-- <remarks> If we're done, process the tokens: </remarks> --*/
		if ((a == '\n') || (a == SENTINEL))
		{
			return (process_input(__stand_param__)) ;
		}
		/*-- <remarks> Gather sequences into tokens: </remarks> --*/
		__src__ = _Scan_Next_(__stand_param__,__src__) ;
		/*-- <remarks> Check for errors: </remarks> --*/
		if (__src__ == NULL)
		{
			break ;
		}
	}
	return FALSE ;
}  

static char * _Scan_Next_( STAND_PARAM *__stand_param__,char * __in_ptr__) 
{
	int n ;
	char __scan_buf__[MAXSTRLEN] ;

/*-- <remarks> Collect a sequence of characters into the scan_buf </remarks> --*/

	char *__src__ = __in_ptr__ ;
	char a = *__src__ ;
	char *__dest__ = __scan_buf__ ;
	*__dest__ = SENTINEL ;

	/*-- <remarks> Type one terminators </remarks> --*/
	if ((a == ',') || (a == '\t') || (a == ';'))
	{
		*__dest__++ = a ;
		*__dest__ = SENTINEL;
		set_term(__stand_param__,1,__scan_buf__) ;
		/*-- <remarks> Point to next input char </remarks> --*/
		return (__src__ + 1) ;
	}
	/*-- <remarks> Numeric sequences : ordinals, fractions and numbers </remarks> --*/
	if (isdigit(a))
	{
        char b ;
        char last_digit ;

		COLLECT_WHILE(isdigit(a)) ;
		/*-- <remarks> Get a character of lookahead and one of lookbehind </remarks> --*/
		b = *(__src__ + 1 ) ;
		last_digit = *(__dest__ - 1 ) ; /*-- last digit collected --*/
		n = __dest__ - __scan_buf__ ;
		switch (a)
		{
			/*-- <remarks> Fractions </remarks> --*/
		case '/' :
			/*-- <remarks> Collect the rest of the fraction </remarks> --*/
			if (isdigit(b))
			{
				switch (b)
				{
				case '2' :
					if (last_digit == '1')
					{
						COLLECT_LOOKAHEAD ;
						TERM_AND_LENGTH ;
						RETURN_NEW_MORPH(DFRACT) ;
					}
					break ;
				case '3' :
					if ((last_digit == '1') || (last_digit == '2'))
					{
						COLLECT_LOOKAHEAD ;
						TERM_AND_LENGTH ;
						RETURN_NEW_MORPH(DFRACT) ;
					}
					break ;
				case '4' :
					if ((last_digit == '1') || (last_digit == '3'))
					{
						COLLECT_LOOKAHEAD ;
						TERM_AND_LENGTH ;
						RETURN_NEW_MORPH(DFRACT) ;
					}
					break ;
				} /*-- <remarks> end of switch on lookahead </remarks> --*/
			}
			break ;
			/*-- <remarks> ordinals -- */
		case 's' : case 'S' :
			/*-- <remarks> 1st, 21st, 31st, -- for 1 </remarks> --*/
			/*-- <remarks> Point to next input char </remarks> --*/
			TEST_FOR_ORD_DIGIT('1','t','T') ;
		case 'r' : case 'R' :
			/*-- <remarks> 3rd, 23rd, 33rd, -- for 3 </remarks> --*/
			/*-- <remarks> Point to next input char </remarks> --*/
			TEST_FOR_ORD_DIGIT('3','d','D') ;
		case 'n' : case 'N' :
			/*-- <remarks> 2nd, 22nd, 32nd, -- for 2 </remarks> --*/
			/*-- <remarks> Point to next input char </remarks> --*/
			TEST_FOR_ORD_DIGIT('2','d','D') ;
		case 't' : case 'T' :
			if ((b == 'h') || (b == 'H'))
			{
				switch (last_digit)
				{
				case '1' : case '2' : case '3' :
					/*-- <remarks> 11th, 111th, 211th etc -- for 11-13 </remarks> --*/
					if ((n > 1) && (*(__dest__ - 2) == '1'))
					{
						COLLECT_LOOKAHEAD ;
						TERM_AND_LENGTH ;
						/*-- <remarks> Point to next input char </remarks> --*/
						RETURN_NEW_MORPH(DORD) ;
					}
					break ;
				default :
					/*-- <remarks> 4th, 14th, 24th etc -- for 0, 4-9 </remarks> --*/
					COLLECT_LOOKAHEAD ;
					TERM_AND_LENGTH ;
					/*-- <remarks> Point to next input char </remarks> --*/
					RETURN_NEW_MORPH(DORD) ;
				}
			}
			break ;
		}
		/*-- <remarks> ordinary numeric sequence </remarks> --*/
		TERM_AND_LENGTH ;
		/*-- <remarks> Retain position </remarks> --*/
		RETURN_NEW_MORPH(DNUMBER) ;
	}
	/*-- <revision date='2009-08-15'> Fix ampersand : P&R --> P & R </remarks> --*/
	if (a == '&')
	{
		COLLECT_WHILE(a == '&') ;
		TERM_AND_LENGTH ;
		RETURN_NEW_MORPH(DSINGLE) ;
	}
	/*-- <remarks> Alphabetic sequence </remarks> --*/
	if ((isalpha(a)) || (a == '\'') || (a == '#'))
	{
		COLLECT_WHILE((isalpha(a)) || (a == '\'')) ;
		TERM_AND_LENGTH ;
		/*-- <remarks> Retain position </remarks> --*/
		switch (n)
		{
		case 1 :
			RETURN_NEW_MORPH(DSINGLE) ;
		case 2 :
			RETURN_NEW_MORPH(DDOUBLE) ;
		default :
			RETURN_NEW_MORPH( DWORDT ) ;
		}
		/*-- <remarks> Retain position </remarks> --*/
		return __src__ ;
	}
	/*-- <remarks> Type 2 terminators ( spacing ) </remarks> --*/
	if (strchr(__spacer__,a) != NULL)
	{
		NO_COLLECT_WHILE(strchr(__spacer__,a) != NULL) ;
		set_term(__stand_param__,2,__scan_buf__) ;
		/*-- <remarks> Retain position </remarks> --*/
		return (__src__) ;
	}
	/*-- <remarks> Ignore everything not specified. Point to next input char. </remarks> --*/
	return (__src__ + 1) ;
}

#ifdef BUILD_API

/*
typedef struct STANDARDIZER_s {
    int data;
    char *err_msg;
} STANDARDIZER;

typedef struct STDADDR_s {  // define as required
   char *house_num;
   char *prequal;
   char *pretype;
   char *predir;
   char *name;
   char *suftype;
   char *sufdir;
   char *sufqual;
   char *extra;
   char *city;
   char *state;
   char *postcode;
   char *country;
} STDADDR;

*/

STANDARDIZER *std_init()
{
    STANDARDIZER *std;

    std = (STANDARDIZER *) calloc(1,sizeof(STANDARDIZER)) ;
    if ( std == NULL ) return NULL ;

    std -> pagc_p = (PAGC_GLOBAL *) calloc(1,sizeof(PAGC_GLOBAL)) ;
    if ( std -> pagc_p == NULL ) {
        free( std ) ;
        return NULL ;
    }

    std -> pagc_p -> process_errors = init_errors(std -> pagc_p, NULL) ;
    std -> err_p = std -> pagc_p -> process_errors ;

    return std;
}


int std_use_lex(STANDARDIZER *std, LEXICON *lex)
{
    std -> pagc_p -> addr_lexicon = lex -> hash_table ;
    lex -> hash_table = NULL;
    lex_free(lex);
    if (!setup_default_defs(std -> pagc_p)) return FALSE ;
    return (install_def_block_table(std -> pagc_p -> addr_lexicon, std -> pagc_p -> process_errors)) ;
}


int std_use_gaz(STANDARDIZER *std, LEXICON *gaz)
{
    std -> pagc_p -> gaz_lexicon = gaz -> hash_table ;
    gaz -> hash_table = NULL;
    lex_free(gaz);
    return 0;
}


int std_use_rules(STANDARDIZER *std, RULES *rules)
{
    if ( ! rules -> ready ) {
        RET_ERR("std_use_rules: Rules have not been readied!", std -> err_p, 1);
    }
    std -> pagc_p -> rules = rules -> r_p ;
    rules -> r_p = NULL;
    rules_free(rules);
    return 0;
}

int std_ready_standardizer(STANDARDIZER *std)
{
    std -> misc_stand = 
        init_stand_context(std -> pagc_p, std -> err_p, 1);

    if (std -> misc_stand == NULL)
        return 1;
    return 0;
}


void std_free(STANDARDIZER *std)
{
    if ( std == NULL ) return;
    DBG("Calling close_stand_process");
    if ( std -> pagc_p != NULL ) close_stand_process( std -> pagc_p ) ;
    if ( std -> pagc_p -> process_errors != NULL ) {
        DBG("Calling close_errors");
        close_errors( std -> pagc_p -> process_errors );
        DBG("Calling FREE_AND_NULL");
        FREE_AND_NULL( std -> pagc_p ) ;
    }
    DBG("Calling close_stand_context");
    close_stand_context( std -> misc_stand );
    DBG("Calling free");
    free( std );
}


void stdaddr_free(STDADDR *stdaddr)
{
    if (!stdaddr) return;
    if (stdaddr->building)   free(stdaddr->building);
    if (stdaddr->house_num)  free(stdaddr->house_num);
    if (stdaddr->predir)     free(stdaddr->predir);
    if (stdaddr->qual)       free(stdaddr->qual);
    if (stdaddr->pretype)    free(stdaddr->pretype);
    if (stdaddr->name)       free(stdaddr->name);
    if (stdaddr->suftype)    free(stdaddr->suftype);
    if (stdaddr->sufdir)     free(stdaddr->sufdir);
    if (stdaddr->ruralroute) free(stdaddr->ruralroute);
    if (stdaddr->extra)      free(stdaddr->extra);
    if (stdaddr->city)       free(stdaddr->city);
    if (stdaddr->state)      free(stdaddr->state);
    if (stdaddr->country)    free(stdaddr->country);
    if (stdaddr->postcode)   free(stdaddr->postcode);
    if (stdaddr->box)        free(stdaddr->box);
    if (stdaddr->unit)       free(stdaddr->unit);
    free(stdaddr);
    stdaddr = NULL;
}

static char *coalesce( char *a, char *b )
{
    return a?a:b;
}

void print_stdaddr( STDADDR *result )
{
    if (result) {
        printf("  building: %s\n", coalesce(result -> building, ""));
        printf(" house_num: %s\n", coalesce(result -> house_num, ""));
        printf("    predir: %s\n", coalesce(result -> predir, ""));
        printf("      qual: %s\n", coalesce(result -> qual, ""));
        printf("   pretype: %s\n", coalesce(result -> pretype, ""));
        printf("      name: %s\n", coalesce(result -> name, ""));
        printf("   suftype: %s\n", coalesce(result -> suftype, ""));
        printf("    sufdir: %s\n", coalesce(result -> sufdir, ""));
        printf("ruralroute: %s\n", coalesce(result -> ruralroute, ""));
        printf("     extra: %s\n", coalesce(result -> extra, ""));
        printf("      city: %s\n", coalesce(result -> city, ""));
        printf("     state: %s\n", coalesce(result -> state, ""));
        printf("   country: %s\n", coalesce(result -> country, ""));
        printf("  postcode: %s\n", coalesce(result -> postcode, ""));
        printf("       box: %s\n", coalesce(result -> box, ""));
        printf("      unit: %s\n", coalesce(result -> unit, ""));
    }
}

/*
STDADDR *std_standardize_one(STANDARDIZER *std, char *address_one_line, int options)
{
    return NULL;
}
*/

STDADDR *std_standardize_mm(STANDARDIZER *std, char *micro, char *macro, int options)
{
    STAND_PARAM *stand_address;
    STDADDR *stdaddr;
    int err;

    stand_address = std -> misc_stand ;
    if (stand_address == NULL)
        return NULL;

    if (!micro || ( IS_BLANK( micro ))) {
        RET_ERR("std_standardize_mm: micro attribute to standardize!", std -> err_p, NULL);
    }

    init_output_fields( stand_address, BOTH );
    if (macro && macro[0] != '\0') {
        err = standardize_field( stand_address, macro, MACRO );
        if (!err) {
            RET_ERR1("std_standardize_mm: No standardization of %s!",
                     macro, std -> err_p, NULL);
        }

        if (options & 1) {
            printf("After standardize_field for macro:\n");
            output_raw_elements( stand_address , NULL ) ;
            send_fields_to_stream(stand_address->standard_fields , NULL, 0, 0);
        }
    }

    err = standardize_field( stand_address, micro, MICRO_M );
    if (!err) {
        RET_ERR1("std_standardize_mm: No standardization of %s!",
                 micro, std -> err_p, NULL);
    }

    if (options & 1) {
        printf("After standardize_field for micro:\n");
        send_fields_to_stream(stand_address->standard_fields , NULL, 0, 0);
    }

    PAGC_CALLOC_STRUC(stdaddr,STDADDR,1,std -> err_p,NULL);

    if (strlen(stand_address -> standard_fields[0]))
        stdaddr->building   = strdup(stand_address -> standard_fields[0]);
    if (strlen(stand_address -> standard_fields[1]))
        stdaddr->house_num  = strdup(stand_address -> standard_fields[1]);
    if (strlen(stand_address -> standard_fields[2]))
        stdaddr->predir     = strdup(stand_address -> standard_fields[2]);
    if (strlen(stand_address -> standard_fields[3]))
        stdaddr->qual       = strdup(stand_address -> standard_fields[3]);
    if (strlen(stand_address -> standard_fields[4]))
        stdaddr->pretype    = strdup(stand_address -> standard_fields[4]);
    if (strlen(stand_address -> standard_fields[5]))
        stdaddr->name       = strdup(stand_address -> standard_fields[5]);
    if (strlen(stand_address -> standard_fields[6]))
        stdaddr->suftype    = strdup(stand_address -> standard_fields[6]);
    if (strlen(stand_address -> standard_fields[7]))
        stdaddr->sufdir     = strdup(stand_address -> standard_fields[7]);
    if (strlen(stand_address -> standard_fields[8]))
        stdaddr->ruralroute = strdup(stand_address -> standard_fields[8]);
    if (strlen(stand_address -> standard_fields[9]))
        stdaddr->extra      = strdup(stand_address -> standard_fields[9]);
    if (strlen(stand_address -> standard_fields[10]))
        stdaddr->city       = strdup(stand_address -> standard_fields[10]);
    if (strlen(stand_address -> standard_fields[11]))
        stdaddr->state      = strdup(stand_address -> standard_fields[11]);
    if (strlen(stand_address -> standard_fields[12]))
        stdaddr->country    = strdup(stand_address -> standard_fields[12]);
    if (strlen(stand_address -> standard_fields[13]))
        stdaddr->postcode   = strdup(stand_address -> standard_fields[13]);
    if (strlen(stand_address -> standard_fields[14]))
        stdaddr->box        = strdup(stand_address -> standard_fields[14]);
    if (strlen(stand_address -> standard_fields[15]))
        stdaddr->unit       = strdup(stand_address -> standard_fields[15]);

    return stdaddr;
}


STDADDR *std_standardize(STANDARDIZER *std, char *address, char *city, char *state, char *postcode, char *country, int options)
{
    return NULL;
}

#else

/*========================================================================
<summary>
	<function name='standard.c (init_stand_process)'/>
	<remarks>set up process level, opens the lexicons and rules
		and default definitions for the tokenizer</remarks>
	<calls><functionref='(gamma.c) create_rules'/>, <functionref='(lexicon.c) create_lexicon'/>,
		<functionref='(tokenize.c) setup_default_defs'/> and 
		<functionref='(analyze.c) install_def_block_table'/></calls>
</summary>
=========================================================================*/
int init_stand_process(PAGC_GLOBAL *__pagc_global__ ,const char *__rule_name__, const char *__lexicon_name__ , const char *__gazetteer_name__ , const char *__featword_name__)
{
	if ((__pagc_global__->rules = create_rules(__rule_name__,__pagc_global__)) == NULL)
	{
		return FALSE ;
	}
	/*-- <revision date='2009-08-13'> Support multiple lexicons </revision> --*/
	if ((__pagc_global__->addr_lexicon = create_lexicon(__pagc_global__ ,__lexicon_name__ , __gazetteer_name__)) == NULL)
	{
		return FALSE ;
	}
	if ((__pagc_global__->poi_lexicon = create_lexicon(__pagc_global__ ,__featword_name__ ,NULL)) == NULL) 
	{
		return FALSE ;
	}
#ifdef GAZ_LEXICON
	/*-- <revision date='2012-06-01'> Add gaz_lexicon to be triggered on _start_state_ = MACRO </revision> --*/
	if ((__pagc_global__->gaz_lexicon = create_lexicon(__pagc_global__,__gazetteer_name__,NULL)) == NULL) 
	{
		return FALSE ;
	}
#endif
	if (!setup_default_defs(__pagc_global__))
	{
		return FALSE ;
	}
	return (install_def_block_table(__pagc_global__->addr_lexicon ,__pagc_global__->process_errors)) ;
}

#endif

/*========================================================================
<summary>
	<function name='standard.c (close_stand_process)'/>
	<remarks> Called on exit to close down standardizer </remarks>
	<calls> <functionref='(tokenize.c) remove_default_defs'/>,
		<functionref='(gamma.c) destroy_rules'/> and 
		<functionref='lexicon.c (destroy_lexicon)'/></calls>
</summary>
=========================================================================*/
void close_stand_process(PAGC_GLOBAL * __pagc_global__)
{
	if (__pagc_global__ == NULL)
	{
		return ;
	}
    DBG("remove_default_defs(__pagc_global__)");
	remove_default_defs(__pagc_global__) ;
    DBG("destroy_rules(__pagc_global__->rules) ;");
	destroy_rules(__pagc_global__->rules) ;
	/*-- <revision date='2009-08-13'> Support multiple lexicons </revision> --*/
    DBG("destroy_lexicon(__pagc_global__->addr_lexicon)");
	destroy_lexicon(__pagc_global__->addr_lexicon) ;
    DBG("destroy_lexicon(__pagc_global__->poi_lexicon)");
	destroy_lexicon(__pagc_global__->poi_lexicon) ;
	/*-- <revision date='2012-06-01'> Add gaz_lexicon to be triggered on _start_state_ = MACRO </revision> --*/
#ifdef GAZ_LEXICON
    DBG("destroy_lexicon(__pagc_global__->gaz_lexicon)");
	destroy_lexicon(__pagc_global__->gaz_lexicon) ; 
#endif
}

/*========================================================================
<summary>
	<function name='standard.c (init_stand_context)'/>
	<param name='__err_param__'>belongs to the dataset context.</param>
	<calls><functionref='analyze.c (create_segments)'/>
	<returns>NULL returned on error - if so, call <functionref='close_stand_context'/></returns>
</summary>
=========================================================================*/
STAND_PARAM *init_stand_context(PAGC_GLOBAL *__pagc_global__,ERR_PARAM *__err_param__,int exhaustive_flag)
{
	STAND_PARAM *__stand_param__ ;
	/*-- <remarks> Initialization-time allocation </remarks> --*/
	PAGC_CALLOC_STRUC(__stand_param__,STAND_PARAM,1,__err_param__,NULL) ;
	if ((__stand_param__->stz_info = create_segments(__err_param__)) == NULL)
	{
		return NULL ;
	}
	PAGC_CALLOC_2D_ARRAY(__stand_param__->standard_fields, char, MAXOUTSYM, MAXFLDLEN, __err_param__, NULL) ;
	__stand_param__->analyze_complete = exhaustive_flag ;
	__stand_param__->errors = __err_param__ ;
	__stand_param__->have_ref_att = NULL  ;
	/*-- <remarks> Transfer from global </remarks> --*/
	__stand_param__->rules = __pagc_global__->rules ;
	/*-- <revision date='2009-08-13'> Support multiple lexicons </revision> --*/
	/*-- <remarks> Transfer from global </remarks> --*/
	__stand_param__->address_lexicon = __pagc_global__->addr_lexicon ;
	/*-- <remarks> Transfer from global </remarks> --*/
	__stand_param__->poi_lexicon = __pagc_global__->poi_lexicon ;
	/*-- <revision date='2012-06-01'> Add gaz_lexicon to be triggered on _start_state_ = MACRO </revision> --*/
#ifdef GAZ_LEXICON
	__stand_param__->gaz_lexicon = __pagc_global__->gaz_lexicon ;
#endif
	__stand_param__->default_def = __pagc_global__->default_def ;
	return __stand_param__ ;
}


/*========================================================================
<summary>
	<function name='standard.c (close_stand_context)'/>
	<remarks> Closes the <code>STAND_PARAM</code> record </remarks>
	<calls> <functionref='analyze.c (destroy_segments)'/>,
		<macroref='FREE_AND_NULL'/></calls>
<summary>
=========================================================================*/
void close_stand_context( STAND_PARAM *__stand_param__ ) 
{
	if (__stand_param__ == NULL) 
	{
		return ;
	}
	destroy_segments(__stand_param__->stz_info) ;
	if (__stand_param__->standard_fields != NULL)
	{
		PAGC_DESTROY_2D_ARRAY(__stand_param__->standard_fields,char,MAXOUTSYM) ;
	}
	/*-- <remarks> Cleanup time memory release </remarks> --*/
	FREE_AND_NULL(__stand_param__) ;
}

/*========================================================================
<summary>
	<function name='standard.c (_Close_Stand_Field_)'/>
	<remarks> Sends the scanned and processed input to the evaluator </remarks>
	<called-by> <functionref='standard.c (standardize_field)'/></called-by>
	<calls> <functionref='analyze.c (evaluator)'/> , <functionref='export.c (stuff_fields)'/></calls>
	<returns>FALSE on error</returns>
	<revision date='2012-07-22'> Keep track of start_state </revision>
</summary>
=========================================================================*/
static int _Close_Stand_Field_(STAND_PARAM *__stand_param__)
{
	/*-- <revision date='2012-07-22'> Keep track of start_state </revision> --*/
	if (evaluator(__stand_param__))
	{
		/*-- <remarks> Write the output into the fields. </remarks> --*/
		stuff_fields(__stand_param__) ;
		return TRUE ;
	}
	RET_ERR("_Close_Stand_Field_: Address failed to standardize",__stand_param__->errors,FALSE) ;
}

