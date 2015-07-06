/* -- tokenize.c 

This module takes the lexical scanner's output, possibly recombining
it, looking it up in the lexicon for possible definitions, preparing
the input for application to the set of rules.

Prototype 7H08 (This file was written by Walter Sinclair).

This file is part of PAGC.

Copyright (c) 2008 Walter Bruce Sinclair

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

*/


#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <stddef.h>
#include "pagc_api.h"

static SYMB precedes_identifier_list[] = { BOXT , ROAD , UNITH , PRETYP , BUILDH , RR , FAIL } ;
static SYMB precedes_route_list[] = { TYPE , QUALIF , PROV , FAIL } ;
#ifdef COMBINE_FRACTS_WITH_NUMBS
static SYMB FractL[] = { FRACT , FAIL } ;
#endif
static SYMB NumberL[] = { NUMBER , FAIL } ;
static SYMB RoadL[] = { ROAD , FAIL } ;
static SYMB mixed_components[] = { NUMBER , WORD , SINGLE , FAIL } ;
static SYMB RouteL[] = { RR , ROAD , FAIL } ;
static SYMB MixedL[] = { MIXED , FAIL } ;
static SYMB ProvL[] = { PROV , FAIL } ;
static SYMB PostalL[] = { PCT, PCH, FAIL } ;


/* -- local prototypes -- */
static int next_morph( STAND_PARAM * ) ;
static int no_space( LEXEME * , struct morph * ) ;
static int process_lexeme( STAND_PARAM * , int , int ) ;
static int is_route( ENTRY * ) ;
static int is_direction_letter( LEXEME * , LEXEME *, struct morph * ,DEF **, char * ) ; 
#ifdef EXPRESS_ORDINALS
static int is_ordinal_suffix( LEXEME * , LEXEME *, struct morph * ,DEF **, char * ) ;
#endif
static int is_zip( STAND_PARAM * , DEF ** , struct morph * ) ; 
static void fix_mixed( STAND_PARAM * , DEF **, struct morph * ) ; 
static void reunite_mixed( STAND_PARAM * , DEF ** , struct morph * , char * ) ; 
static void mark_hyphen_unit( int , LEXEME * , struct morph * , DEF ** ) ; 
static void numeric_tail( STAND_PARAM * , DEF ** , struct morph *, char * ) ; 
static DEF *new_defs( struct morph * , DEF ** , ENTRY * , int , char * ) ; 
static int set_lexeme( STAND_PARAM *, int , int , DEF *, char * ) ; 
static void reset_lexeme( LEXEME * ) ; 
static void combine_lexemes( STAND_PARAM * , struct morph * , DEF * ) ;
static int phrase_from_morphs( struct morph * , char * , int  , int ) ; 

#define MAKE_DEFAULT_DEF_FIRST(DTOKEN,TOKEN) \
   if ( ( glo_p -> default_def[DTOKEN] = create_def(TOKEN,NULL,0,TRUE,glo_p->process_errors) ) == NULL) \
      return FALSE

#define MAKE_DEFAULT_DEF_NEXT(DTOKEN,TOKEN) \
   if ( ( glo_p->default_def[DTOKEN]->Next = create_def(TOKEN,NULL,1,TRUE,glo_p->process_errors)) == NULL) \
      return FALSE

/* --------------------------------------------------------------
tokenize.c (setup_default_defs)
called by standard.l (init_stand_process) when process is started
calls lexicon.c (create_def)
uses MACROS MAKE_DEFAULT_DEF_FIRST, MAKE_DEFAULT_DEF_NEXT,
PAGC_CALLOC_STRUC
--------------------------------------------------------------- */
int setup_default_defs( PAGC_GLOBAL *glo_p ) {

   /* -- initialization-time allocation -- */
   PAGC_CALLOC_STRUC(glo_p->default_def,DEF*,DUNIT+1,glo_p->process_errors,FALSE) ;
   MAKE_DEFAULT_DEF_FIRST(DFRACT,FRACT);
   MAKE_DEFAULT_DEF_FIRST(DSINGLE,SINGLE);
   MAKE_DEFAULT_DEF_FIRST(DDOUBLE,DOUBLE);
   MAKE_DEFAULT_DEF_FIRST(DWORDT,WORD);
   MAKE_DEFAULT_DEF_FIRST(DNUMBER,NUMBER);
   MAKE_DEFAULT_DEF_FIRST(DMIXED,MIXED);
   MAKE_DEFAULT_DEF_FIRST(DPOSTH,PCH);
   MAKE_DEFAULT_DEF_FIRST(DPOSTT,PCT);
   MAKE_DEFAULT_DEF_FIRST(DZIPH,NUMBER);
   MAKE_DEFAULT_DEF_FIRST(DZIPT,NUMBER);
   MAKE_DEFAULT_DEF_FIRST(DDIRLET,SINGLE);
   MAKE_DEFAULT_DEF_FIRST(DORD,WORD);
   MAKE_DEFAULT_DEF_FIRST(DUNIT,NUMBER);

   /* second elements of the list */
   MAKE_DEFAULT_DEF_NEXT(DPOSTH,MIXED) ;
   MAKE_DEFAULT_DEF_NEXT(DPOSTT,MIXED) ;
   MAKE_DEFAULT_DEF_NEXT(DZIPH,QUINT) ;
   MAKE_DEFAULT_DEF_NEXT(DZIPT,QUAD) ;

   /* -- consider making DIRECT first, then SINGLE? -- */
   MAKE_DEFAULT_DEF_NEXT(DDIRLET,DIRECT) ;
   MAKE_DEFAULT_DEF_NEXT(DORD,ORD) ;

   /* -- prefixed occupancy : 101-1750 Main St. -- */
   MAKE_DEFAULT_DEF_NEXT(DUNIT,UNITT) ;
   return TRUE ;
}

/* ----------------------------------------------------------------
tokenize.c (remove_default_defs)
called by standard.l (close_stand_process) when process is finished
calls lexicon.c (destroy_def_list)
----------------------------------------------------------------- */
void remove_default_defs( PAGC_GLOBAL *glo_p ) {
   DEFDEF i ;

   if ( glo_p -> default_def != NULL ) {
      for ( i = 0 ;
            i < DUNIT+1 ;
            i++ ) {
         destroy_def_list ( glo_p -> default_def[ i ] ) ;
      }
   }
   /* -- cleanup time memory release -- */
   FREE_AND_NULL ( glo_p -> default_def ) ;
}

/* ---------------------------------------------------------
tokenize.c (process_input)
return FALSE on error (too many lexemes)
calls tokenize.c (process_lexeme)
--------------------------------------------------------- */
int process_input( STAND_PARAM *s_p ) { 
  /* -- process all the morphs not yet made into lexemes 
     -- called by scanner -- */



  s_p -> cur_morph--  ; /* -- back it down - no more morphs coming -- */
  while ( s_p -> base_morph <= s_p -> cur_morph  ) {
     s_p -> base_morph = process_lexeme( s_p ,
                                         s_p -> cur_morph ,
                                         s_p -> base_morph ) ;
     if ( s_p -> base_morph == ERR_FAIL ) {
        return FALSE ;
     }
     s_p -> LexNum++ ; 
  }
  return TRUE ;
}

/* ---------------------------------------------------------
tokenize.c (new_morph)
return FALSE if the the text is too long or too many lexemes
are created
called by standard.l (yylex)
calls tokenize.c (process_lexeme) and util.c (upper_case)
calls tokenize.c (next_morph)
uses macros CLIENT_ERR, RET_ERR1
--------------------------------------------------------- */
int new_morph( STAND_PARAM *s_p , 
               DEFDEF t ,
               const char *s ,
               int length ) {
   struct morph *morph_vector; 
   int i , j ; 

   morph_vector = s_p -> morph_array ; 
   /* -- called by scanner to do one morpheme -- */
   i = s_p -> cur_morph ; 
   j = s_p -> base_morph ;

   if ( length >= MAXTEXT ) {
      CLIENT_ERR( s_p -> errors ) ;
      RET_ERR1( "new_morph: %s is way too long",
                s,
                s_p->errors,
                FALSE) ;
   }

   morph_vector[ i ]. Term = 0 ; 
   morph_vector[ i ] . Sym = t ; 
   /* -- Lexicon is in upper case - we need to match. -- */
   upper_case( morph_vector[ i ] . Text , 
               s ) ;
   morph_vector[ i ] . TextLen = length ; 

   /* -- Is it time to look for a phrase? -- */
   
   if ( i == ( j + MAXPHRASE - 1 ) ) {
      if ( ( s_p -> base_morph = process_lexeme( s_p ,
                                                 i ,
                                                 j ) ) == ERR_FAIL ) {

         return FALSE ;
      }
      s_p -> LexNum++ ;
   }
   return ( next_morph( s_p ) ) ; 
}

/*-----------------------------------------------------
tokenize.c (next_morph)
increment the morph count
called by new_morph
return FALSE if too many
uses macros CLIENT_ERR, RET_ERR
------------------------------------------------------*/
static int next_morph( STAND_PARAM *s_p ) { 
   if ( s_p -> cur_morph++ > MAXMORPHS ) {
      CLIENT_ERR( s_p -> errors ) ;
      RET_ERR( "next_morph: Too many morphemes in input",
               s_p->errors,
               FALSE) ;
   }
   return TRUE ;
}

/*-----------------------------------------------------
tokenize.c (set_term)
called by standard.l (yylex)
adds a terminator to a morph
------------------------------------------------------*/
void set_term( STAND_PARAM *s_p , 
               int c ,
               const char *s ) {

   int i ;


   /* 0 is no break
      1 is set for semicolons, tabs and commas,
      2 for spaces */

   i = s_p -> cur_morph ;
   if ( *s == '-' ) {
      c++ ;
   }

   /* -- only add a space if there isn't already a break there -- */
   if ( ( i > 0 ) &&
        ( s_p -> morph_array[ i - 1 ] . Term == 0 ) )
      s_p -> morph_array[ i - 1 ] . Term = c ;
}

/*-------------------------------------------------------
tokenize.c (no_space)
called by tokenize.c (reunite_mixed, is_direction_letter)
--------------------------------------------------------*/
static int no_space( LEXEME *lex_p , 
                     struct morph *morph_p  ) {
   int k ;

   k = lex_p -> EndMorph ; 
   return ( ( ( morph_p + k ) -> Term == 0 )? 
              TRUE : 
              FALSE ) ;
}

/*-----------------------------------------------------
tokenize.c (initialize_morphs)
called by standard.l (standardize_field)
calls tokenize.c (reset_lexeme)
------------------------------------------------------*/
void initialize_morphs( STAND_PARAM *s_p ) {
   int i ;

   s_p -> cur_morph = 0 ;
   s_p -> base_morph = 0 ;
   s_p -> LexNum = 0 ;
   for ( i = FIRST_LEX_POS ; 
         i < MAXLEX ;
         i++ ) {
      reset_lexeme( s_p -> lex_vector + i ) ;
   }
}

/*---------------------------------------------------------
tokenize.c (process_lexeme)
return ERR_FAIL on error (too many lexemes)
called by tokenize.c (process_input, new_morph)
calls tokenize.c (phrase_from_morphs, set_lexeme, is_route,
find_def_type, reunite_mixed, mark_hyphen_unit)
calls (lexicon.c) find_entry
MACROS: BLANK_STRING
string.h (strncmp)
-----------------------------------------------------------*/
static int process_lexeme( STAND_PARAM *s_p , 
                           int cur_m, 
                           int base_m ) {
   int Ceiling ;
   ENTRY *cur_entry ;
   char LTarget[ MAXSTRLEN ] ;
   struct morph *morph_ptr ; 
   DEF **d_p ; 
   LEXEME *lex_p ;

   d_p = s_p -> default_def ; 
   morph_ptr = s_p -> morph_array ; 
   BLANK_STRING(LTarget) ;
   cur_entry = NULL ;
   for ( Ceiling = cur_m ; 
         Ceiling >= base_m ; 
         Ceiling-- ) {
      /* -- Combine the morphs into a phrase from cur_morph to Ceiling -- */
      Ceiling = phrase_from_morphs( morph_ptr , 
                                    LTarget, 
                                    base_m, 
                                    Ceiling ) ;

      if ( ( cur_entry = find_entry( s_p -> lexicon , /* 2007-11-20 hash table */
                                     LTarget ) ) != NULL ) {
         /* -- Before accepting an entry from the lexicon, it may be
            necessary to establish that the entry does not subsume a
            more appropriate entry. -- */

         lex_p = s_p -> lex_vector + s_p -> LexNum - 1 ; 
         if ( ( Ceiling > base_m ) &&
              ( base_m > 0 ) &&
              ( !strncmp( LTarget, 
                          "ST ", 
                          3 ) ) ) {
            /* -- have we preempted street or saint by state? -- */
            /* -- and what about at the end of the address? -- */
            if ( is_route( cur_entry ) ) {
               if ( find_def_type( lex_p -> DefList, 
                                   precedes_route_list ) ) {
                 /* -- if the previous lexeme is any of the categories
                    on precedes_route_list, we're okay -- */
                  break ;
               }
               /* -- reject if preceded by a number -- */
               if ( find_def_type( lex_p -> DefList , 
                                   NumberL ) )

                  continue ;
            }
         }
         break ;
      }
      if ( Ceiling == 0 )
         break ;
   }
   if ( Ceiling < base_m ) {
      Ceiling = base_m ; /* -- make a singleton lexeme -- */
   }
   /* -- set up either the lexicon or default definitions and
      add the new lexeme to the list -- */

   /* -- pass LTarget to new_defs -- */
   if ( !set_lexeme( s_p , 
                     base_m,
                     Ceiling,
                     new_defs( morph_ptr , 
                               d_p , 
                               cur_entry,
                               base_m ,
                               LTarget ),
                      LTarget ) ) {
       return ERR_FAIL ;
   }
   /* -- Handle reactants and reunite broken alphanumeric strings -- */
   reunite_mixed( s_p , 
                  d_p , 
                  morph_ptr , 
                  LTarget ) ;

   mark_hyphen_unit( s_p -> LexNum , 
                     s_p -> lex_vector , 
                     morph_ptr , 
                     d_p  ) ;
   /* -- return position of next unprocessed morpheme -- */
   return ( Ceiling + 1 ) ;
}  

/*-----------------------------------------------------
tokenize.c (is_route)
called by process_lexeme
calls tokenize.c (is_symb_on_list)
------------------------------------------------------*/
static int is_route( ENTRY *E ) {
   DEF *D ;

   for ( D = E -> DefList ;
         D != NULL ;
         D = D -> Next ) {
      if ( is_symb_on_list( D -> Type ,
                            RouteL ) ) {
         return TRUE ;
      }
   }
   return FALSE ;
}

/*-----------------------------------------------------
tokenize.c (is_direction_letter)
called by tokenize.c (numeric_tail)
call tokenize.c (no_space)
string.h (strlen)
------------------------------------------------------*/
static int is_direction_letter( LEXEME *cur_lex_p , 
                                LEXEME *prev_lex_p , 
                                struct morph *morph_p ,
                                DEF **d_p , 
                                char *LT ) {
   char c ;

   if ( ( strlen( LT ) == 1 ) &&
        ( no_space( prev_lex_p, 
                    morph_p   ) ) ) {
       c = *LT ;
       switch ( c ) {
          case 'N':
          case 'S':
          case 'W':
          case 'E':
             cur_lex_p -> DefList = d_p[ DDIRLET ]  ;
             return TRUE ;
          default :
             return FALSE ;
       }
   }
   return FALSE ;
}

#ifdef EXPRESS_ORDINALS
static int is_ordinal_suffix( LEXEME *cur_lex_p , 
                              LEXEME *prev_lex_p ,
                              struct morph *morph_p ,
                              DEF **d_p ,
                              char *LT ) {
   int prev_len ;
   char Ult, 
        Penult ;

   if ( ( strlen( LT ) != 2 ) ||
        ( no_space( prev_lex_p, 
                    morph_p   ) ) ) {
      return FALSE ;
   }
   prev_len = strlen( prev_lex_p -> Text ) ;
   Ult = prev_lex_p -> Text[ prev_len - 1 ] ;
   Penult = ( ( prev_len < 2 )?
              SENTINEL :
              prev_lex_p -> Text[ prev_len - 2 ] ) ;
   if ( ( !strcmp( LT,
                   "ST" ) ) &&
        ( Ult == '1' ) &&
        ( Penult != '1' ) ) {
      return TRUE ;
   } else if ( ( !strcmp( LT,
                          "ND" ) ) &&
               ( Ult == '2' ) &&
               ( Penult != '1' ) ) {
      return TRUE ;
   } else if ( ( !strcmp( LT,
                          "RD" ) ) &&
               ( Ult == '3' ) &&
               ( Penult != '1' ) ) {
      return TRUE ;
   } else if ( ( !strcmp( LT,
                        "TH" ) ) &&
             ( isdigit( Ult ) ) ) {
      if ( Ult == '1' ||
           Ult == '2' ||
           Ult == '3' ) {
         if ( Penult == '1' ) {
            return TRUE ;
         } else {
            return FALSE ;
         }
      } else {
         return TRUE ;
      }
   }
   return FALSE ;
}
#endif


/*----------------------------------------------------------
tokenize.c (is_zip)
called by tokenize.c (reunite_mixed)
calls tokenize.c (combine_lexemes, no_space, find_def_type)
string.h (strlen) ctype.h (isalpha,isdigit)
-----------------------------------------------------------*/
static int is_zip( STAND_PARAM *s_p , 
                   DEF **d_p , 
                   struct morph *morph_p  ) {
   /* -- Canadian Postal Code and US zip code -
      called by reunite_mixed -- */
   DEFDEF d ;
   char *cur_txt ;
   int alt_state ;
   int tl ;
   LEXEME *cur_lex_p ; 

   cur_lex_p = s_p -> lex_vector + s_p -> LexNum ; 
   cur_txt = cur_lex_p -> Text ; 
   tl = strlen( cur_txt ) ;
   if ( ( find_def_type( cur_lex_p -> DefList, 
                         NumberL ) ) &&
        ( tl > 3 ) ) {
      /* -- US Zip code -- */
      if ( tl > 6 ) {
         return FALSE ;
      }
      if ( isalpha( *cur_txt ) ) {
 
         return FALSE ;
      }
      d = ( ( tl == 4 )? DZIPT : 
                         DZIPH ) ;
      cur_lex_p -> DefList = d_p[ d ] ; 
      return TRUE ;
   }

   /* -- Canadian postal codes -- */
   if ( s_p -> LexNum < 2 ) { 
      return FALSE ;
   }
   if ( tl != 1 ) {
      return FALSE ;
   }
   if ( isdigit( *cur_txt ) ) {
      alt_state = TRUE ;
   } else {
      if ( isalpha( *cur_txt ) ) {
         alt_state = FALSE ;
      } else {
         return FALSE;
      }
   }
   cur_lex_p-- ;
   cur_txt = cur_lex_p -> Text ; 
   if ( !no_space( cur_lex_p , 
                   morph_p  ) ) {
      return FALSE ;
   }
   /* -- First check if lexeme created for Mixed, with a
      length of 2 on the last pass -- */
   if ( find_def_type( cur_lex_p -> DefList , 
                       MixedL ) ) {

      if ( strlen( cur_txt ) != 2 ) {
         return FALSE ;
      }
      /* -- Will the pattern correspond? -- */
      if ( alt_state ) {
         /* -- if the current character is a number, then the
            previous string must be number + letter -- */
         if ( !isdigit( *cur_txt ) ) {
            return FALSE ;
         }
         if ( !isalpha( *( cur_txt + 1 ) ) ) {
            return FALSE ;
         }
      } else {
         /* -- The FSA: if the current character is a letter,
            then the previous string must be letter + number -- */
         if ( !isalpha( *cur_txt ) ) {
            return FALSE ;
         }
         if ( !isdigit( *( cur_txt + 1 ) ) ) {
            return FALSE ;
         }
      }
      /* -- if it ends with a digit, it's the tail -- */
      d = ( ( alt_state ) ? DPOSTT : 
                            DPOSTH ) ;
      combine_lexemes( s_p , 
                       morph_p ,
                       d_p[ d ]  ) ;
      return TRUE;
   }
   /* -- Prior strings of length 2 have been dealt with, leaving only
      the prior strings of length 1 to consider -- */
   if ( strlen( cur_txt ) != 1 ) {
      return FALSE  ;
   }
   /* -- If the current character is a letter, then the previous one must
      be a number, and vice versa -- */
   if ( alt_state ) {
      if ( !isalpha( *cur_txt ) ) {
         return  FALSE;
      }
   }  else {
      if ( !isdigit( *cur_txt ) ) {
         return  FALSE;
      }
   }

   cur_lex_p -- ; 
   cur_txt = cur_lex_p -> Text ; 

   /* -- Now look for a character, not followed by a space, which must be
      a number if the current character is a number, and a letter if the
      current character is a letter. -- */
   if ( strlen( cur_txt ) != 1 ) {
      return FALSE;
   }
   if ( !no_space( cur_lex_p , 
                   morph_p  ) ) {
      return FALSE;
   }
   if ( !alt_state ) {
      if ( !isalpha( *cur_txt ) ) {
         return FALSE;
      }
   } else if ( !isdigit( *cur_txt ) ) {
      return FALSE;
   }

   /* -- if it ends with a digit, it's the tail -- */

   d = ( ( alt_state ) ? DPOSTT : 
                         DPOSTH ) ;
   combine_lexemes( s_p , 
                    morph_p , 
                    d_p[ d ]  ) ;
   combine_lexemes( s_p , 
                    morph_p  ,
                    d_p[ d ]  ) ;
   return TRUE ;
}

/*----------------------------------------------------------
tokenize.c (fix_mixed)
called by tokenize.c (reunite_mixed)
calls tokenize.c (combine_lexemes, no_space, find_def_type)
----------------------------------------------------------*/
static void fix_mixed( STAND_PARAM *s_p , 
                       DEF **d_p , 
                       struct morph *morph_p  ) {
   /* -- recombine alphabet sequences and numeric sequences split apart by
      the lexical scanner - but only if they form an identifier. -- */
   LEXEME *cur_lex_p, *prev_lex_p ; 


   cur_lex_p = s_p -> lex_vector + s_p -> LexNum ; 
   prev_lex_p = cur_lex_p - 1 ; 

   if ( s_p -> LexNum < 2 ) 
      return ;
   if ( !no_space( prev_lex_p , 
                   morph_p  ) ) {
      return ;
   }
   if ( !find_def_type( cur_lex_p -> DefList ,  
                        mixed_components ) ) {
      return ;
   }

   /* -- We have an item that can make up part of a mixed string and no space
      preceding it. If the previous item was mixed and not a postal code,
      then we'll just merge this one in right away. -- */

   if ( find_def_type( prev_lex_p -> DefList , 
                       MixedL ) &&
        !find_def_type( prev_lex_p -> DefList , 
                        PostalL ) ) {
      /* -- if the previous item is mixed and not a postal code -- */

      combine_lexemes( s_p , 
                       morph_p , 
                       d_p[ DMIXED ]  ) ;
      return ;
   }

   /* -- The previous lexeme must be of the right kind to do a mix -- */
   if ( !find_def_type( prev_lex_p -> DefList , 
                      mixed_components ) ) {
      return ;
   }
   /* -- If a road comes before a mixed, it might also be a PROV -- */
   if ( find_def_type( prev_lex_p -> DefList ,  
                     RoadL ) &&
        !find_def_type( prev_lex_p -> DefList , 
                      ProvL ) ) {
      return ;
   }

   /* -- a mixed identifier only follows certain types -- */

   prev_lex_p -- ; 
   if ( !find_def_type( prev_lex_p -> DefList , 
                        precedes_identifier_list ) ) {
      return ;
   }

   combine_lexemes( s_p , 
                    morph_p , 
                    d_p[ DMIXED ]  ) ;
   return ;
}

/*-----------------------------------------------------
tokenize.c (reunite_mixed)
called by tokenize.c (process_lexeme)
calls tokenize.c (is_zip, numeric_tail, fix_mixed)
------------------------------------------------------*/
static void reunite_mixed( STAND_PARAM *s_p , 
                           DEF **d_p , 
                           struct morph *morph_p , 
                           char *LT ) {
   /* -- called by process_lexeme -- */

   if ( is_zip( s_p , 
                d_p ,
                morph_p  ) ) {
      return ; /* -- handle postal and zip codes -- */
   }
   
   numeric_tail( s_p , 
                 d_p , 
                 morph_p , 
                 LT ) ;


   fix_mixed( s_p , 
              d_p , 
              morph_p ) ; /* -- handle mixed identifiers -- */
}

/*-----------------------------------------------------
tokenize.c (mark_hyphen_unit)
called by tokenize.c (process_lexeme)
calls tokenize.c (find_def_type)
------------------------------------------------------*/
static void mark_hyphen_unit( int n , 
                              LEXEME *lex_p , 
                              struct morph *morph_p , 
                              DEF **def_ptr  ) {

   /* -- if the current lexeme is the second and the previous is terminated
      by a hyphen and both are numbers, redefine the previous lexeme to
      be a unittail. -- */

   LEXEME *cur_lex_p ; 

   cur_lex_p = lex_p + n ; 
   if ( ( n != 1 ) || 
        ( !find_def_type( ( cur_lex_p ) -> DefList , 
                          NumberL ) ) ||
        ( !find_def_type( ( cur_lex_p - 1 ) -> DefList, 
                          NumberL ) ) ) {
      return ;
   }
   
   cur_lex_p -- ; 
   if ( ( morph_p + ( cur_lex_p  -> EndMorph  ) ) -> Term == 3  ) {
      /* -- overwrite the old deflist -- */
      cur_lex_p -> DefList = def_ptr[DUNIT] ;  
   }
}


/*---------------------------------------------------------------------
tokenize.c (numeric_tail)
called by tokenize.c (reunite_mixed )
calls tokenize.c (combine_lexemes, find_def_type, is_direction_letter)
----------------------------------------------------------------------*/
static void numeric_tail( STAND_PARAM *s_p , 
                          DEF **d_p , 
                          struct morph *morph_p , 
                          char *LT ) {

   /* -- all subsequent items follow a number -- */
   int n ;
   LEXEME *prev_lex_p , *cur_lex_p ; 


   n = s_p -> LexNum ; 
   if ( n < 1 )
      return  ;
   cur_lex_p = s_p -> lex_vector + n ; 
   prev_lex_p = cur_lex_p - 1 ; 
   if ( !find_def_type( prev_lex_p -> DefList , 
                        NumberL ) ) {
      return ;
   }
   if ( is_direction_letter( cur_lex_p , 
                             prev_lex_p , 
                             morph_p , 
                             d_p , 
                             LT ) ) {
      return ;
   }

#ifdef COMBINE_FRACTS_WITH_NUMBS
   if ( find_def_type( cur_lex_p -> DefList , 
                       FractL ) ) {

      combine_lexemes( s_p , 
                       morph_p , 
                       d_p[ DNUMBER ] )  ;
      return ;
   }
#endif

#ifdef EXPRESS_ORDINALS
   if ( is_ordinal_suffix( cur_lex_p , 
                           prev_lex_p ,
                           morph_p ,
                           d_p ,
                           LT ) ) {
      combine_lexemes( s_p , 
                       morph_p , 
                       d_p[ DORD ] ) ;
      return ;
   }
#endif

}

/*-----------------------------------------------------
tokenize.c (new_defs)
called by tokenize.c (process_lexemes)
MACROS: BLANK_STRING
------------------------------------------------------*/
static DEF *new_defs( struct morph *morph_p , 
                      DEF **d_p , 
                      ENTRY *Cur ,
                      int pos ,
                      char *LTarget ) {
   DEFDEF s ;

   /* -- A single or double letter sequence, even if found in the lexicon,
      may be only that - the two letter abbreviation for Alaska may be
      part of a unit number. A more sophisticated solution might be to
      check context - but implementation has all sorts of pitfalls - maybe
      later -- */

   s = ( morph_p + pos ) -> Sym ; 

   if ( Cur != NULL ) {
      return( Cur -> DefList );
   }
   /* -- standardize ordinals as numbers -- do this before
      the Target is copied into the lexeme -- */

#ifndef EXPRESS_ORDINALS
   if ( s == DORD ) { 
      /* -- remove the suffix -- */
      BLANK_STRING((LTarget + strlen(LTarget) - 2)) ;
   }
#endif

   /* -- if no entry was found, just use the default list -- */
   return ( d_p[ s ]  ) ; 
}

/*-----------------------------------------------------
tokenize.c (is_symb_on_list)
called by tokenize.c (find_def_type, is_route)
------------------------------------------------------*/
int is_symb_on_list( SYMB a , 
                     SYMB *List ) {
   SYMB *s ;
   for ( s = List ; 
         *s != FAIL ; 
         s++ )
      if ( *s == a )
         return TRUE ;
   return FALSE ;
}

/*-----------------------------------------------------
tokenize.c (find_def_type)
searches a definition list looking for one that matches
the type
calls tokenize.c (is_symb_on_list)
called by tokenize.c (process_lexeme etc)
return TRUE if found
------------------------------------------------------*/
int find_def_type( DEF *df , 
                   SYMB *slist ) {
   DEF *d ;

   for ( d = df ; 
         d != NULL ;
         d = d -> Next )
     if ( is_symb_on_list( d -> Type ,
                           slist ) )
        return TRUE ;
   return FALSE ;
}

/*-----------------------------------------------------
tokenize.c (set_lexeme)
called by tokenize.c (process_lexeme)
MACROS: CLIENT_ERR, RET_ERR1 
string.h (strcpy)
------------------------------------------------------*/
static int set_lexeme( STAND_PARAM *s_p , 
                       int Start ,
                       int End , 
                       DEF *start_def ,
                       char *text ) {
   LEXEME *L ;
   int n ; 

   /* -- we need a limit -- */
   if ( ( n = s_p -> LexNum ) >= MAXLEX ) { 
      CLIENT_ERR( s_p -> errors ) ;
      RET_ERR1( "set_lexeme: %s is one too many lexemes" ,
                text ,
                s_p -> errors ,
                FALSE ) ;
   }
   L = s_p -> lex_vector + n ; 
   strcpy( L -> Text , 
           text ) ;
   L -> DefList = start_def ;
   L -> StartMorph = Start ;
   L -> EndMorph = End ;
   return TRUE ;
}

/*-------------------------------------------------------
tokenize.c (reset_lexeme)
called by tokenize.c (combine_lexemes, initialize_morphs)
NULL out the lexeme's text buffer
--------------------------------------------------------*/
static void reset_lexeme( LEXEME *lex_p ) { 
   int i ;
   char *s ;

   lex_p -> DefList = NULL ; 
   s = lex_p -> Text ; 
   for ( i = 0 ; 
         i < MAXTEXT ; 
         i++ ) {
      *( s + i ) = SENTINEL ;
   }
}

/*-----------------------------------------------------
tokenize.c (combine_lexemes)
called by tokenize.c (is_zip, fix_mixed , numeric tail)
calls tokenize.c (phrase_from_morphs, reset_lexeme)
------------------------------------------------------*/
static void combine_lexemes( STAND_PARAM *s_p , 
                             struct morph * morph_p , 
                             DEF *d ) {
   /* -- combine the current Lexeme with the previous one -- */
    int n ;
    LEXEME *CurLexVector, *PrevLexVector ; 

    /* -- find the two lexemes to combine -- */
    CurLexVector = s_p -> lex_vector + s_p -> LexNum ; 
    PrevLexVector = CurLexVector - 1 ; 
    PrevLexVector -> EndMorph = CurLexVector -> EndMorph ; /* the new end */ 
    PrevLexVector -> Text[ 0 ] = SENTINEL ; 

    n = phrase_from_morphs( morph_p , 
                            PrevLexVector -> Text , 
                            PrevLexVector -> StartMorph , 
                            PrevLexVector -> EndMorph ) ; 
    
    PrevLexVector -> DefList = d ; /* - overwrite old deflist -- */
    reset_lexeme( CurLexVector ) ; 
    s_p -> LexNum-- ; 
}

/*--------------------------------------------------------
tokenize.c (phrase_from_morphs)
called by tokenize.c (process_lexemes , combine_lexemes)
concatenate the morph strings into a single string
uses macro BLANK_STRING
---------------------------------------------------------*/
static int phrase_from_morphs( struct morph *morph_vector , 
                               char *Dest , 
                               int beg , 
                               int end ) {
   int i , 
       a ;

   BLANK_STRING(Dest) ; 
   strcpy( Dest ,
           morph_vector[ beg ] . Text ) ;
   for ( i = beg + 1 ; 
         i <= end ; 
         i++ ) {
      /* -- No breaks in the middle of a phrase -- */
      
      a = morph_vector[ i - 1 ] . Term ;
      if ( a == 1 )
         return ( i - 1 ) ; /* -- indicate last morph used -- */
      if ( a > 1 ) {
         append_string_to_max( Dest ,
                               " " ,
                               MAXSTRLEN ) ; 
      }
      append_string_to_max( Dest,
                            morph_vector[ i ] . Text ,
                            MAXSTRLEN ) ;

   }
   return end ;
}

