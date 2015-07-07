/* -- lexicon.c 

This file reads the lexicon definitions into a chained
hash table and handles the lookups of words in the hash table,
returning definitions in the form of an input symbol and a
standardized text.

Prototype 7H08 (This file was written by Walter Sinclair).

This file is part of pagc.

Copyright (c) 2008 Walter Bruce Sinclair

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

*/
/* For pagc-0.4.2 : last revised 2012-05-23 */

#undef DEBUG
//#define DEBUG

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <ctype.h>
#include "pagc_api.h"

/* -- Hash table size should be a prime number -- */
/* 5581, 5953, 6337, 6733, 7561, 7993, 8893, 10333, 10837, 11353, 12421, 12973, 13537, 15913, 18481  */
#define LEXICON_HTABSIZE 7561

#ifdef BUILD_API
#include "pagc_std_api.h"
#endif

/* -- local prototypes -- */
static unsigned calc_hash( char * ) ;
static ENTRY **create_hash_table( ERR_PARAM * ) ; 
static int add_dict_entry( ERR_PARAM *, ENTRY ** , char * , int , SYMB , char * ) ;

#ifndef BUILD_API
static char *convert_field( char * , char * ) ;
static int read_lexicon( ERR_PARAM *, ENTRY ** , FILE * ) ;
#endif

LEXICON *lex_init( ERR_PARAM *err_p ) ;
static int append_new_def( ERR_PARAM *, ENTRY * , SYMB , char * , int ) ; 
static unsigned elf_hash( char * ) ;
void print_lexicon( ENTRY ** hash_table ) ;

#ifdef BUILD_API

/*
typedef struct LEXICON_s {
    ENTRY **hash_table;
    ERR_PARAM *err_p;
} LEXICON;

*/

LEXICON *lex_init( ERR_PARAM *err_p )
{
    LEXICON *lex;

    PAGC_CALLOC_STRUC(lex,LEXICON,1,err_p,NULL);

    lex->hash_table = create_hash_table( err_p );
    if (lex->hash_table == NULL) {
        lex_free(lex);
        return NULL;
    }

    lex->err_p = err_p;

    return lex;
}

int lex_add_entry(LEXICON *lex, int seq, char *word, char *stdword, SYMB token)
{
    return add_dict_entry( lex->err_p, lex->hash_table, word, seq-1, token, stdword);
}

void lex_free(LEXICON *lex)
{
    if (lex == NULL) return;
    destroy_lexicon(lex->hash_table);
    free(lex);
    lex = NULL;
}



#else

/* ---------------------------------------------------------------------
lexicon.c (create_lexicon) -
read the lexicon file into memory, chaining off a hash table
returns a pointer to the hash table, or NULL if error.
called by standard.l (init_stand_process)
calls util.c (open_aux_file) lexicon.c (read_lexicon, create_hash_table)
uses macro LOG_MESS
stdio.h (fclose)
-----------------------------------------------------------------------*/
ENTRY **create_lexicon( PAGC_GLOBAL *glo_p ,
                        const char *lex_name ,
                        const char *gaz_name ) {
   /* -- called by init_stand_process to read in the Lexicon and set up the
      definitions in memory for hash table access -- */
   FILE *gaz_file , 
        *dict_file ;
   ENTRY **hash_table ; 

   if ( (hash_table = create_hash_table( glo_p -> process_errors ) ) == NULL ) {
       return NULL ;
   }
   /* 2009-08-13 : support multiple lexicons */
   if ( gaz_name != NULL ) {
      if ( ( gaz_file = open_aux_file( glo_p ,
                                       gaz_name ) ) == NULL )
         return NULL ; 
      if ( !read_lexicon( glo_p -> process_errors ,
                          hash_table , 
                          gaz_file ) ) {
          fclose( gaz_file ) ;
          return NULL ;
      }
      fclose( gaz_file ) ;
   }

   if ( ( dict_file = open_aux_file( glo_p ,
                                     lex_name ) ) == NULL )
      return NULL  ;
   if ( !read_lexicon( glo_p -> process_errors ,
                       hash_table , 
                       dict_file ) ) {
       fclose( dict_file ) ;
       return NULL ;
   }
   fclose( dict_file ) ;
   return hash_table ;
}

/* ----------------------------------------------------
lexicon.c (read_lexicon) -
called by lexicon.c (create_lexicon) for each file
calls convert_field, add_dict_entry  
returns FALSE if error encountered
stdio.h (fgets,feof,sscanf)
uses macro BLANK_STRING
-------------------------------------------------------*/
static int read_lexicon( ERR_PARAM *err_p ,
                         ENTRY **hash_table , 
                         FILE *CFile ) {
   char record_buffer[ MAXSTRLEN ] ;
   char lookup_str[ MAXTEXT ] ;
   char num_str[ MAXTEXT ] ;
   int cur_token ;
   int num_def ;
   char standard_str[ MAXTEXT ] ;
   char *next_str ;

   while ( !feof( CFile ) ) {
      /* -- read in each line of the csv file and add to hash table -- */
      BLANK_STRING(record_buffer) ;
      fgets( record_buffer , 
             MAXSTRLEN , 
             CFile ) ;

#ifdef SEW_NOT_SURE_IF_WE_NEED_THIS
      /* -- check for and skip over blank lines -- */
      if (strspn(record_buffer, " \t\r\n") == strlen(record_buffer))
         continue;
#endif

      /* -- comma-separated values are handled only as well as necessary
         in the present context -- */
      if ( ( next_str =
                convert_field( num_str ,
                               record_buffer ) ) == NULL ) {
         break ;
      }
      sscanf( num_str ,
              "%d" ,
              &num_def ) ;
      next_str = convert_field( lookup_str ,
                                next_str ) ;
      next_str = convert_field( num_str ,
                                next_str ) ;
      sscanf( num_str ,
              "%d" ,
              &cur_token ) ;
      next_str = convert_field( standard_str ,
                                next_str ) ;
      if ( add_dict_entry( err_p ,
                           hash_table , 
                           lookup_str ,
                           ( num_def - 1 ) ,
                           cur_token ,
                           standard_str ) == ERR_FAIL ) {
         return FALSE ;
      }
   }
   return TRUE ;
}

/* ----------------------------------------------------
lexicon.c (convert_field)
called by lexicon.c (read_lexicon)
ctype.h (isspace)
uses macro BLANK_STRING
-------------------------------------------------------*/
static char *convert_field( char *buf ,
                            char *inp ) {
   char c ;
   char *d  = buf;
   char *s = inp ;

   BLANK_STRING(d) ;
   /* -- space at the beginning of a line will stop the read -- */
   if ( isspace( *s ) )
      return NULL ;
   while ( ( c = *s++ ) != SENTINEL ) {
      if ( c == '\"' ||
           c == '\r' ) 
         continue ; /* -- ignore quotes and carriage returns -- */
      /* -- zero terminate field and record delimiters -- */
      if ( c == '\n' ||
           c == ',' ) {
          BLANK_STRING(d) ;
          return s ;
      }
      *d++ = c ; /* -- copy it -- */
   }
   return NULL ;
}

#endif

/* ----------------------------------------------------
lexicon.c (destroy_lexicon)
called by standard.l (close_stand_process)
calls lexicon.c (destroy_def_list)
uses macro FREE_AND_NULL
-------------------------------------------------------*/
void destroy_lexicon(ENTRY ** hash_table)
{
	/* -- called by Clean-Up - */
	unsigned __i__ ;
	ENTRY *__E__,*__F__ ;
	if (hash_table == NULL)
	{
		return ;
	}
	for (__i__ = 0 ;__i__ < LEXICON_HTABSIZE ;__i__++ ) 
	{
		for (__E__ = hash_table[__i__] ;__E__ != NULL ;__E__ = __F__)
		{
			destroy_def_list(__E__->DefList) ;
			__F__ = __E__->Next ;
			FREE_AND_NULL(__E__->Lookup) ;
			FREE_AND_NULL(__E__) ;
		}
	}
    DBG("destroy_lexicon: i=%d", __i__);
	/* <revision date='2012-05-23'>free hash table</revision> */
	FREE_AND_NULL(hash_table);
    DBG("leaving destroy_lexicon");
}


/* ----------------------------------------------------------
lexicon.c (destroy_def_list)
called by destroy_lexicon and tokenize.c (remove_default_defs)
uses macro FREE_AND_NULL
------------------------------------------------------------*/
void destroy_def_list( DEF *start_def ) {
   DEF *cur_def ;
   DEF *next_def = NULL ;



   for ( cur_def = start_def ;
         cur_def != NULL ;
         cur_def = next_def ) {
      next_def = cur_def -> Next ;
      /* -- Default definitions have no associated text -- */
      if ( cur_def -> Protect == 0 ) {
         FREE_AND_NULL( cur_def -> Standard ) ;
      }
      FREE_AND_NULL( cur_def ) ;
   }
}

/* ----------------------------------------------------
lexicon.c (find_entry)
called by lexicon.c (add_dict_entry)
calls lexicon.c (calc_hash)
string.h (strcmp)
-------------------------------------------------------*/
ENTRY *find_entry(ENTRY **hash_table,char *lookup_str) 
{
	/* -- called to create a lexeme -- */
	ENTRY *__E__ ;
	unsigned __hash_index__ ; /* -- 2006-11-20 : to return hash table pointer -- */

	__hash_index__ = calc_hash(lookup_str) ;
	for (__E__ = hash_table[__hash_index__] ; __E__ != NULL ; __E__ = __E__->Next)
	{
		if (strcmp(lookup_str,__E__->Lookup) == 0)
		{
			return __E__ ;
		}
	}
	return __E__ ;
}

#define US sizeof( unsigned )
/* ----------------------------------------------------
lexicon.c (elf_hash)
called by lexicon.c (calc_hash)
-------------------------------------------------------*/
static unsigned elf_hash( char *key_str ) {
  unsigned h , 
           g , 
           c ;

  h = 0 ;
  while ( ( c = ( unsigned ) *key_str ) != '\0' ) {
     h = ( h << US ) + c  ;
     if ( ( g = h & ( ~ ( ( unsigned )( ~0 ) >> US ) ) ) )
        h ^= g >> ( US * 6 ) ;
     h &= ~g ;
     key_str++ ;
  }
  return h ;
}


/* ----------------------------------------------------
lexicon.c (calc_hash)
called by lexicon.c (find_entry, add_dict_entry)
calls lexicon.c (elf_hash)
-------------------------------------------------------*/

static unsigned calc_hash( char *key_str ) {
  unsigned h ;

  h = elf_hash( key_str ) ;
  return ( h  % LEXICON_HTABSIZE ) ;
}

/* ----------------------------------------------------
lexicon.c (create_hash_table)
allocate and initialize hash table in memory
return NULL if error
called by create_lexicon
uses macro PAGC_CALLOC_STRUC
-------------------------------------------------------*/
static ENTRY **create_hash_table(ERR_PARAM *err_p)
{
	unsigned __i__ ;
	ENTRY **__hash_table__ ;
	PAGC_CALLOC_STRUC(__hash_table__,ENTRY *,LEXICON_HTABSIZE,err_p,NULL) ;
	for (__i__ = 0 ;__i__ < LEXICON_HTABSIZE ;__i__++ ) 
	{
		__hash_table__[__i__] = NULL ;
	}
	return __hash_table__ ;
}

/* ----------------------------------------------------
lexicon.c (add_dict_entry)
called by lexicon.c (read_lexicon)
calls lexicon.c (calc_hash, create_def, append_new_def)
uses macro PAGC_ALLOC_STRUC , PAGC_STORE_STR, RET_ERR
return ERR_FAIL if error
-------------------------------------------------------*/
static int add_dict_entry( ERR_PARAM *err_p ,
                           ENTRY **hash_table , 
                           char *lookup_str ,
                           int def_num ,
                           SYMB t ,
                           char *standard_str ) {
   ENTRY *E ;

   E = find_entry( hash_table ,
                   lookup_str ) ;
   if ( E == NULL ) {
      unsigned hash_index ; 

      PAGC_ALLOC_STRUC(E,ENTRY,err_p,ERR_FAIL);
      /* -- add the Lookup string to the record -- */
      PAGC_STORE_STR(E->Lookup,lookup_str,err_p,ERR_FAIL) ;
      /* -- add new entry to beginning of table -- */
      hash_index = calc_hash( lookup_str ) ; 

      E -> Next = hash_table[ hash_index ] ; /* -- collision chain -- */
      hash_table[ hash_index ] = E ;
      if ( ( E -> DefList = create_def( t ,
                                        standard_str ,
                                        def_num ,
                                        FALSE ,
                                        err_p ) ) == NULL ) {
          return ERR_FAIL ;
      }  
  } else {
      int err_stat ;
      if ( E -> DefList == NULL ) {
         RET_ERR("add_dict_entry: Lexical entry lacks definition" ,
                 err_p ,
                 ERR_FAIL ) ;
      }
      if ( ( err_stat = append_new_def( err_p ,
                                        E ,
                                        t ,
                                        standard_str ,
                                        def_num ) ) != TRUE ) {
         return err_stat ;
      }
   }
   return TRUE ;
}

/* ----------------------------------------------------
lexicon.c (append_new_def)
called by lexicon.c (add_dict_entry)
calls lexicon.c (create_def)
returns FALSE if entry is already there
returns ERR_FAIL on allocation error
-------------------------------------------------------*/
static int append_new_def( ERR_PARAM *err_p ,
                           ENTRY *E ,
                           SYMB t ,
                           char *text ,
                           int def_num ) { 

   DEF *D, 
       *pd, 
       *cd ;
   for ( cd = E -> DefList , pd = NULL ;
         cd != NULL ;
         cd = cd -> Next ) {
      pd = cd ;
      /* -- avoid duplication except for local entries -- */
      if ( cd -> Type == t ) {
         return FALSE ;
      }
   }
   if ( ( D = create_def( t ,
                          text ,
                          def_num ,
                          FALSE ,
                          err_p ) ) == NULL ) {
       return ERR_FAIL ;
   }
   if ( pd == NULL ) {
      E -> DefList = D ;
   } else {
      D -> Next = pd -> Next ;
      pd -> Next = D ;
   }
   return TRUE ;
}

/*--------------------------------------------------------------------
lexicon.c (create_def) 
called by lexicon.c (append_new_def) tokenize.c (setup_default_defs)
allocate memory for lexicon entry.
Pflag is TRUE for default entries 
returns NULL for allocation error
uses macro PAGC_ALLOC_STRUC, PAGC_STORE_STR
-------------------------------------------------------------------- */
DEF *create_def ( SYMB s ,
                  char *standard_str ,
                  int def_num ,
                  int PFlag ,
                  ERR_PARAM *err_p ) {
   /* -- allocate the memory and set up the definition structure with the
      standard form -- */
   DEF *cur_def ;

   /* -- initialization-time allocation -- */
   PAGC_ALLOC_STRUC(cur_def,DEF,err_p,NULL) ;
   cur_def -> Type = s ;
   cur_def -> Protect = PFlag ; /* -- False for definitions from lexicon
                                   true for default definitions -- */
   if ( !PFlag ) {
      /* -- initialization-time allocation -- */
      PAGC_STORE_STR(cur_def->Standard,standard_str,err_p,NULL) ;
   } else
      cur_def -> Standard = NULL ;
   cur_def -> Order = def_num ;
   cur_def -> Next = NULL ;
   return cur_def ;
}

/*--------------------------------------------------------------------
lexicon.c (print_lexicon)
not called by useful for debugging. It will print out the lexicon.
--------------------------------------------------------------------*/
void print_lexicon( ENTRY ** hash_table )
{
    unsigned i;
    ENTRY *E;

    if (!hash_table) return;

    for (i=0; i< LEXICON_HTABSIZE; i++)
    {
        E = hash_table[i];
        while (E)
        {
            DEF *D = E->DefList;
            printf("'%s'\n", E->Lookup);
            while (D)
            {
                printf("    %d, %d, %d, '%s'\n", D->Order, D->Type, D->Protect, D->Standard);
                D = D->Next;
            }
            E = E->Next;
        }
    }
}

