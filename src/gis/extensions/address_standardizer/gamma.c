/* -- gamma.c 

This file reads the rules file into memory and sets up the rule
lookup structures. These are based on the optimized Aho-Corasick
algorithms in Watson (1994).

Copyright (c) 2008 Walter Bruce Sinclair

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

*/
/* For pagc-0.4.0 : last revised 2010-11-01 */

#undef DEBUG
//#define DEBUG

#include <stdlib.h>
#include <stdio.h>
#include <stddef.h>
#include "pagc_api.h"
#include "gamma.h"

#ifdef BUILD_API
#include "pagc_std_api.h"
RULES *rules_init( ERR_PARAM *err_p ) ;
#endif

/* -- local prototypes -- */
static int initialize_link( ERR_PARAM *, KW *** , NODE ) ; 
static void classify_link( RULE_PARAM * , KW ***, KW *, NODE , SYMB , SYMB  ) ; 
static void add_failure_linkage( KW ***, NODE , NODE  ) ; 
static NODE **precompute_gamma_function( ERR_PARAM *, NODE ** , KW ***, NODE  ) ; 

static double load_value[ NUMBER_OF_WEIGHTS ] = {
   0.00, 0.325, 0.35 , 0.375 , 0.4 , 
   0.475 , 0.55, 0.6 , 0.65 , 0.675 , 
   0.7 , 0.75 , 0.8 , 0.825 , 0.85 , 
   0.9 , 0.95 , 1.00 } ;

/*--------------------------------------------------------------------------- 
gamma.c (refresh_transducer)
called by analyze.c (prepare_target_pattern) 
The registry of matching keywords is regenerated with the use of the 
precomputed Gamma function, Output Links and the current target.
----------------------------------------------------------------------------*/
void refresh_transducer( NODE *r , 
                         SYMB *S ,
                         NODE **gamma_function ) {
   NODE q ;
   int i ;

   i = 0 ;
   q = r[ i ] = EPSILON ;
   while ( S[ i ] != FAIL ) {
      q = gamma_function[ q ][ S[ i ] ] ; 
      i++ ;
      r[ i ] = q ;
   }
}

/*--------------------------------------------------------------------------- 
gamma.c (is_input_symbol)
called by gamma.c (create_rules)
----------------------------------------------------------------------------*/
int is_input_symbol( SYMB sym ) {

   if ( sym > MAXINSYM || 
        sym < 0 )
      return FALSE ;
   return TRUE ;
}

/*--------------------------------------------------------------------------- 
gamma.c (is_output_symbol)
called by gamma.c (create_rules)
----------------------------------------------------------------------------*/
int is_output_symbol( SYMB sym ) {
   if ( sym > MAXOUTSYM || 
        sym < 0 )
      return FALSE ;
   return TRUE ;
}

#ifdef BUILD_API

/*
typedef struct RULES_s {
    int ready;
    int rule_number;
    int last_node;
    RULE_PARAM *r_p;
    ERR_PARAM *err_p;
    NODE **Trie;
    SYMB *rule_end ;
    SYMB *r ;
} RULES;
*/

/*---------------------------------------------------------------------------
gamma.c (rules_init)
api interface to replace (create_rules)
---------------------------------------------------------------------------*/
RULES *rules_init( ERR_PARAM *err_p ) {
    RULES *rules;
    /* -- returns size of Gamma Function Matrix -- */
    SYMB a ;
    KW *k_s ;
    KW ***o_l ;
    NODE **Trie ;
    SYMB *r_s ;
    RULE_PARAM *r_p ;


    PAGC_CALLOC_STRUC(rules,RULES,1,err_p,NULL);
    rules->err_p = err_p;
    rules->ready = 0;
    rules->rule_number = 0;
    rules->last_node = EPSILON;

    PAGC_ALLOC_STRUC(r_p,RULE_PARAM,err_p,NULL) ;
    rules->r_p = r_p;

    /* -- initialize the statistics record -- */
    r_p -> collect_statistics = FALSE ;
    r_p -> total_best_keys = 0 ;
    r_p -> total_key_hits = 0 ;

    /* -- storage for input and output records -- */
    PAGC_CALLOC_STRUC(r_s,SYMB,RULESPACESIZE,err_p,NULL);

    /* -- storage for temporary trie for rules -- */
    PAGC_CALLOC_STRUC(Trie,NODE *,MAXNODES,err_p,NULL);

    /* -- initialize the first( EPSILON ) node of the trie -- */
    PAGC_CALLOC_STRUC(Trie[EPSILON],NODE,MAXINSYM,err_p,NULL);

    for ( a = 0 ;
          a < MAXINSYM ;
          a++ ) {
       Trie[ EPSILON ][ a ] = FAIL ;
    }

    /* -- storage for global output_link -- */
    PAGC_CALLOC_STRUC(o_l,KW **,MAXNODES,err_p,NULL);
    PAGC_CALLOC_STRUC(k_s,KW,MAXRULES,err_p,NULL);

    if ( !initialize_link( err_p ,
                           o_l ,
                           EPSILON ) ) {
       return NULL ;
    }

    rules -> r_p -> rule_space = r_s ;
    rules -> r_p -> key_space = k_s ;
    rules -> r_p -> output_link = o_l ;

    rules -> Trie = Trie ;
    rules -> rule_end = r_s + RULESPACESIZE ;

    rules -> r = r_s ;

    return rules;
}


int rules_add_rule(RULES *rules, int num, int *rule) {
    int i ,
        w ;
    SYMB a ,
         t ;
    SYMB *rule_start ,
         *r ,
         *r_s ;
    NODE u ;
    NODE **Trie ;
    KW *keyw ,
       *k_s ;
    KW ***o_l ;

    if ( !rules ) return 1;       /* error rules obj not initialized */
    if ( !rules -> r_p ) return 2;  /* RULE_PARAM not allocated */
    if ( rules -> ready ) return 3; /* rules have already be readied */
    if ( rules -> rule_number >= MAXRULES ) {
        RET_ERR( "rules_add_rule: Too many rules are being added.",
                 rules -> err_p, 4);
    }

    /* get local copies of stuff saved in RULES */
    o_l = rules -> r_p -> output_link ;
    k_s = rules -> r_p -> key_space ;
    r_s = rules -> r_p -> rule_space ;
    Trie = rules -> Trie ;
    r = rules -> r ;

    keyw = k_s + rules -> rule_number ;
    MEM_ERR(keyw, rules -> err_p, 5);

    u = EPSILON ;
    rule_start = r ; /* save rule start for inclusion in the record */
    if ( rule_start > rules -> rule_end ) {
        RET_ERR( "rules_add_rule: Too many rules for allocated memory.",
                 rules -> err_p, 5);
    }

    for (i=0; ; i++, r++ ) {
        if (i >= num) {
            RET_ERR( "rules_add_rule: invalid rule structure.",
                     rules -> err_p, 6);
        }

        *r = rule[i] ;
        /* -- a fail at the beginning of a field indicates end of record
           unless it's at the beginning of the record, in which case
           it's the end of file -- */
        if ( *r == FAIL ) {
            if ( i == 0 ) return 0;
            break;
        }

        /* -- check the input -- */
        if ( !is_input_symbol( *r ) ) {
            RET_ERR2( "rules_add_rule: Bad Input Token %d at rule %d",
                      *r,
                      rules -> rule_number ,
                      rules -> err_p,
                      7 ) ;
        }

        /* -- build the trie structure -- */
        if ( Trie[ u ][ *r ] == FAIL ) {
            if ( ++rules -> last_node >= MAXNODES ) {
                RET_ERR( "rules_add_rule: Too many nodes in gamma function",
                        rules -> err_p,
                        8 ) ;
            }
            Trie[ u ][ *r ] = rules -> last_node ;
            PAGC_CALLOC_STRUC(Trie[rules -> last_node],NODE,MAXINSYM,rules -> err_p,9) ;
            for ( a = 0 ;
                  a < MAXINSYM ;
                  a++ ) {
                Trie[ rules -> last_node ][ a ] = FAIL ;
            }
            if ( !initialize_link( rules -> err_p ,
                                   o_l ,
                                   rules -> last_node ) ) {
                return 10;
            }
        }
        u = Trie[ u ][ *r ] ;
    } /* end of for loop */

    keyw -> Input = rule_start ;
    if ( ( keyw -> Length = i ) == 0 ) {
        RET_ERR1( "rules_add_rule: Error 0 length rule #%d",
                  rules -> rule_number,
                  rules -> err_p,
                  11 ) ;
    }

    /* -- read the output tokens into the rule_space -- */
    r++ ; /* -- move to beginning of the output tokens -- */
    rule_start = r ; /* -- remember the beginning -- */
    while ( TRUE ) {
        i++;
        if ( i >= num ) {
            RET_ERR( "rules_add_rule: invalid rule structure.",
                     rules -> err_p, 6);
        }
        *r = rule[i] ;
        if ( *r == FAIL ) break;
        if ( !is_output_symbol( *r ) ) {
            RET_ERR2( "rules_add_rule: Rule File: Non-Token %d in Rule #%d\n",
                      *r ,
                      rules -> rule_number,
                      rules -> err_p,
                      7 ) ;
        }
        r++ ;
    }
    keyw -> Output = rule_start ;

    /* -- classify the output -- */
    i++ ;
    t = rule[i] ;
    i++ ;
    w = rule[i] ; 

    classify_link( rules -> r_p ,
                   o_l ,
                   keyw ,
                   u ,
                   w ,
                   t ) ;

    rules -> rule_number++ ;
    rules -> r = ++r ; ;
    return 0;
}


int rules_ready(RULES *rules) {
    SYMB a;

    if (!rules) return 1;       /* error rules obj not initialized */
    if (!rules->r_p) return 2;  /* RULE_PARAM not allocated */
    if (rules->ready) return 3; /* rules have already be readied */

    rules -> r_p -> rules_read = rules->rule_number ;

    if ( ++rules -> last_node >= MAXNODES ) {
        RET_ERR( "rules_ready: Too many nodes in gamma function" ,
                 rules -> err_p, 4) ;
    }

    /* -- change the EPSILON node transitions in preparation for Gamma -- */
    for ( a = 0 ;
          a < MAXINSYM ;
          a++ ) {
       if ( rules -> Trie[ EPSILON ][ a ] == FAIL ) {
          rules -> Trie[ EPSILON ][ a ] = EPSILON ;
       }
    }

    /* -- create the global Gamma function matrix -- */
    if ( ( rules -> r_p -> gamma_matrix =
            precompute_gamma_function( rules -> err_p,
                                       rules -> Trie ,
                                       rules -> r_p -> output_link ,
                                       rules -> last_node ) ) == NULL ) {
       return 5 ;
    }

    /* -- no longer need the Trie -- */
    PAGC_DESTROY_2D_ARRAY(rules -> Trie,NODE,rules -> last_node) ;
    rules -> Trie = NULL ;

    rules -> r_p -> num_nodes = rules -> last_node ;

/*
    if ( glo_p -> log_init ) {
       CLIENT_ERR( err_p ) ;
       LOG_MESS2( "create_rules: Rules installed with %d nodes and %d rules",
                  rules -> last_node ,
                  rules->rule_number ,
                  err_p ) ;
    }
*/

    rules -> ready = 1 ;

    return 0;
}

void rules_free(RULES *rules) {

    if (!rules) return;
    if (rules->r_p) destroy_rules(rules->r_p);
    free(rules);
    rules = NULL;
}

#else

/*--------------------------------------------------------------------------- 
gamma.c (create_rules)
called by standard.l (init_stand_process)
calls util.c (open_aux_file)
calls gamma.c (initialize_link, is_input_symbol, is_output_symbol,
classify_link,precompute_gamma_function) 
----------------------------------------------------------------------------*/
RULE_PARAM *create_rules( const char *rule_name ,
                          PAGC_GLOBAL *glo_p ) {
   /* -- returns size of Gamma Function Matrix -- */
   SYMB a , 
        t ;
   NODE u ;
   int i , 
       w ;
   int is_eof = FALSE ;
   int rule_number = 0 ;
   int last_node = EPSILON ;
   FILE *rule_file ;
   SYMB *rule_start ,
        *rule_end ,
        *r ;
   KW *keyw , *k_s ;
   KW ***o_l ; 
   NODE **Trie ; 
   SYMB *r_s ;
   RULE_PARAM *r_p ; 
   ERR_PARAM *err_p ;

   err_p = glo_p -> process_errors ;

   PAGC_ALLOC_STRUC(r_p,RULE_PARAM,err_p,NULL) ; 

   /* -- initialize the statistics record -- */
   r_p -> collect_statistics = FALSE ;
   r_p -> total_best_keys = 0 ;
   r_p -> total_key_hits = 0 ;


   /* -- open the rule file, if possible -- */
   if ( ( rule_file = open_aux_file( glo_p ,
                                     rule_name ) ) == NULL ) {
      return NULL ;
   }
   /* -- rule file has the format of i i ... i -1 o o ... o -1 t f -- */


   /* -- storage for input and output records -- */
   PAGC_CALLOC_STRUC(r_s,SYMB,RULESPACESIZE,err_p,NULL); 

   /* -- storage for temporary trie for rules -- */
   PAGC_CALLOC_STRUC(Trie,NODE *,MAXNODES,err_p,NULL); 

   /* -- initialize the first( EPSILON ) node of the trie -- */
   PAGC_CALLOC_STRUC(Trie[EPSILON],NODE,MAXINSYM,err_p,NULL); 

   for ( a = 0 ; 
         a < MAXINSYM ; 
         a++ ) {
      Trie[ EPSILON ][ a ] = FAIL ;
   }

   /* -- storage for global output_link -- */
   PAGC_CALLOC_STRUC(o_l,KW **,MAXNODES,err_p,NULL); 
   PAGC_CALLOC_STRUC(k_s,KW,MAXRULES,err_p,NULL); 

   rule_end = r_s + RULESPACESIZE ; 
   if ( !initialize_link( err_p ,
                          o_l , 
                          EPSILON ) ) {
      return NULL ;
   }
   for ( r = r_s ; 
         !feof( rule_file ) ; 
         r++, rule_number++ ) {
      if ( rule_number >= MAXRULES ) {
         CLIENT_ERR( err_p ) ;
         RET_ERR( "create_rules: Too many rules in file",
                  err_p,
                  NULL) ; 
      }
      keyw = k_s + rule_number ; 
      MEM_ERR(keyw,err_p,NULL);
      /* -- get input record -- */

      u = EPSILON ;
      rule_start = r ; /* -- save rule start for inclusion in record -- */
      if ( rule_start > rule_end ) {
         RET_ERR( "create_rules: Too many rules for allocated memory",
                  err_p,
                  NULL ) ; 
      }
      for ( i = 0 ; 
            ; 
            i++, r++  ) {

         /* -- read the first integer -- */
         fscanf( rule_file, 
                 "%d", 
                 r ) ;
         /* -- a fail at the beginning of a field indicates end of record
            unless it's at the beginning of the record, in which case
            it's the end of file -- */
         if ( *r == FAIL ) {
            if ( i == 0 ) {
               is_eof = TRUE ;
            }
            break ;
         }
         /* -- check the input -- */
         if ( !is_input_symbol( *r ) ) {
            CLIENT_ERR( err_p ) ;
            RET_ERR2( "create_rules: Rule file: Bad Input Token %d at rule %d", 
                      *r, 
                      rule_number , 
                      err_p, 
                      NULL ) ;
         }

         /* -- build the trie structure -- */
         if ( Trie[ u ][ *r ] == FAIL ) {
            if ( ++last_node >= MAXNODES ) { 
               RET_ERR( "create_rules: Too many nodes in gamma function",
                        err_p,
                        NULL ) ; 
            }
            Trie[ u ][ *r ] = last_node ;
            PAGC_CALLOC_STRUC(Trie[last_node],NODE,MAXINSYM,err_p,NULL) ;
            for ( a = 0 ; 
                  a < MAXINSYM ; 
                  a++ ) {
               Trie[ last_node ][ a ] = FAIL ;
            }        
            if ( !initialize_link( err_p ,
                                   o_l , 
                                   last_node ) ) {
               return NULL ;
            }
         }
         u = Trie[ u ][ *r ] ;
      }
      if ( is_eof )
         break ;
      keyw -> Input = rule_start ;
      if ( ( keyw -> Length = i ) == 0 ) {
         CLIENT_ERR( err_p ) ;
         RET_ERR1( "create_rules: Error Rule File: 0 length rule #%d",
                   rule_number,
                   err_p,
                   NULL ) ;
      }

      /* -- read the output tokens into the rule_space -- */
      r++ ; /* -- move to beginning of the output tokens -- */
      rule_start = r ; /* -- remember the beginning -- */
      while ( TRUE ) {
         fscanf( rule_file, 
                 "%d", 
                 r ) ;
         if ( *r == FAIL )
            break ;
         if ( !is_output_symbol( *r ) ) {
            RET_ERR2( "create_rules: Rule File: Non-Token %d in Rule #%d\n", 
                      *r , 
                      rule_number,
                      err_p,
                      NULL ) ;
         }
         r++ ;
      }
      keyw -> Output = rule_start ;

      /* -- classify the output -- */
      fscanf( rule_file , 
              "%d" , 
              &t ) ;
      fscanf( rule_file , 
              "%d" , 
              &w ) ;

      classify_link( r_p ,
                     o_l , 
                     keyw , 
                     u , 
                     w , 
                     t ) ;
   } /* -- end of file read -- */


   r_p -> rule_space = r_s ;
   r_p -> key_space = k_s ;
   r_p -> output_link = o_l ;
   r_p -> rules_read = rule_number ;

   fclose( rule_file ) ;


   if ( ++last_node >= MAXNODES ) { 
      RET_ERR( "create_rules: Too many nodes in gamma function" ,
               err_p,
               NULL) ; 
   }
   /* -- change the EPSILON node transitions in preparation for Gamma -- */
   for ( a = 0 ; 
         a < MAXINSYM ; 
         a++ ) {
      if ( Trie[ EPSILON ][ a ] == FAIL ) {
         Trie[ EPSILON ][ a ] = EPSILON ;
      }
   }

   /* -- create the global Gamma function matrix -- */
   if ( ( r_p -> gamma_matrix = precompute_gamma_function( err_p, 
                                                           Trie , 
                                                           o_l , 
                                                           last_node ) ) == NULL ) {
      return NULL ;
   }

   /* -- no longer need the Trie -- */
   PAGC_DESTROY_2D_ARRAY(Trie,NODE,last_node) ; 


   r_p -> num_nodes = last_node ; 

   if ( glo_p -> log_init ) {
      CLIENT_ERR( err_p ) ;
      LOG_MESS2( "create_rules: Rules installed with %d nodes and %d rules",
                 last_node ,
                 rule_number ,
                 err_p ) ;
   }

   return r_p ; 
}

#endif

/*--------------------------------------------------------------------------- 
gamma.c (destroy_rules)
----------------------------------------------------------------------------*/
void destroy_rules( RULE_PARAM * r_p ) { 
   if ( r_p != NULL ) {
      DBG("destroy_rules 1");
      FREE_AND_NULL( r_p -> rule_space ) ;
      DBG("destroy_rules 2");
      FREE_AND_NULL( r_p -> key_space ) ;
      DBG("destroy_rules 3");
      PAGC_DESTROY_2D_ARRAY(r_p->output_link,KW*,r_p->num_nodes) ;
      DBG("destroy_rules 4");
      PAGC_DESTROY_2D_ARRAY(r_p->gamma_matrix,NODE,r_p->num_nodes) ;
      DBG(" destroy_rules 5");
      FREE_AND_NULL( r_p ) ;
   }
}

/* ========================= Output Links ========================= */

/*--------------------------------------------------------------------------- 
gamma.c (initalize_link)
called by gamma.c (create_rules)
----------------------------------------------------------------------------*/
static int initialize_link( ERR_PARAM *err_p ,
                            KW ***o_l , 
                            NODE u ) {
   int cl ;

   /* -- classification by clause type -- */

   PAGC_CALLOC_STRUC(o_l[u],KW *,MAX_CL,err_p,FALSE); 
   for ( cl = 0 ; 
         cl < MAX_CL ; 
         cl++ ) {

      o_l[ u ][ cl ] = NULL ; 
   }
   return TRUE ;
}

/*--------------------------------------------------------------------------- 
gamma.c (classify_link)
called by gamma.c (create_rules)
----------------------------------------------------------------------------*/
static void classify_link( RULE_PARAM *r_p ,
                           KW ***o_l , /* -- 2006-11-02 : arg -- */
                           KW *k ,
                           NODE u ,
                           SYMB w ,
                           SYMB c ) {

   /* -- classification by clause type -- */
   KW * last_key , 
      * penult ;

   k -> hits = 0 ;
   k -> best = 0 ;
   k -> Type = c ;
   k -> Weight = w ;
   last_key = o_l[ u ][ c ] ; /* -- 2006-11-02 : arg -- */
   if ( last_key == NULL ) {
      o_l[ u ][ c ] = k ; /* -- 2006-11-02 : arg -- */

   } else {
      /* -- if the same input symbols are used... -- */
      while ( ( penult = last_key -> OutputNext ) != NULL )
          last_key = penult ;
      last_key -> OutputNext = k ;
   }
   /* -- initialize in anticipation of failure extensions -- */
   k -> OutputNext = NULL ;

}

/*--------------------------------------------------------------------------- 
gamma.c (add_failure_linkage)
called by gamma.c (precompute_gamma_function)
----------------------------------------------------------------------------*/
static void add_failure_linkage( KW ***o_l ,
                                 NODE x ,
                                 NODE u ) {
   /* -- called by precompute_gamma_function
      -- x is the node in the failure function of the node u
      -- classification by clause type -- */
   KW *k ,
      *fk ;
   int cl ;

   for ( cl = 0 ; 
         cl < MAX_CL ; 
         cl++ ) {
      /* -- append the failure keys for each class to the end of the
         appropriate chain -- */ 
      fk = o_l[ x ][ cl ] ; 
      k = o_l[ u ][ cl ] ; 
      if ( k == NULL ) {
         o_l[ u ][ cl ] = fk ; 
      } else {
         /* -- since the chain will be already null-terminated, we only find
            the end of the chain if fk is non-null -- */
         if ( fk != NULL ) {
            /* -- append to the end of the list and make sure that the longer
               lengths go first - this is probably redundant. -- */
            while ( k -> OutputNext != NULL ) {
               k = k -> OutputNext ;
            }
            k -> OutputNext = fk ;
         }
      }
   }
}

/*--------------------------------------------------------------------------- 
gamma.c (precompute_gamma_function)
called by gamma.c (create_rules)
calls gamma.c (add_failure_linkage)
----------------------------------------------------------------------------*/
static NODE **precompute_gamma_function( ERR_PARAM *err_p ,
                                         NODE **Trie , 
                                         KW ***o_l , 
                                         NODE n ) {
   NODE u , 
        ua , 
        x ;
   SYMB a ;
   int i , 
       j ;
   NODE **Gamma ;
   NODE *Failure ,
        *Queue ;

   /* -- Storage for Failure Function -- */
   PAGC_CALLOC_STRUC(Failure,NODE,n,err_p,NULL) ;
   /* -- Storage for Breadth First Search Queue -- */
   PAGC_CALLOC_STRUC(Queue,NODE,n,err_p,NULL) ;

   PAGC_CALLOC_2D_ARRAY(Gamma,NODE,n,MAXINSYM,err_p,NULL) ; 

   u = EPSILON ;
   i = 0 ;
   for ( a = 0 ;
         a < MAXINSYM ; 
         a++ ) {
      x = Trie[ EPSILON ][ a ] ;
      Gamma[ EPSILON ][ a ] = x ;
      Failure[ x ] = EPSILON ;
      /* -- add to Queue for breadth-first search -- */
      if ( x != EPSILON ) {
         Queue[ i++ ] = x ;
      }
   }
   Queue[ i ] = FAIL ; /* -- terminate the list of nodes to process -- */

   for ( j = 0 ; 
         Queue[ j ] != FAIL ; 
         j++ ) {
      u = Queue[ j ] ;
      /* -- get non-Fail transitions from Trie onto queue -- */
      for ( a = 0 ; 
            a < MAXINSYM ;
            a++ ) {
         if ( ( x = Trie[ u ][ a ] ) != FAIL ) {
           Queue[ i++ ] = x ;
         }
      }
      Queue[ i ] = FAIL ; /* -- mark end of list -- */
      x = Failure[ u ] ;
      add_failure_linkage( o_l , 
                           x , 
                           u ) ;
      for ( a = 0 ; 
            a < MAXINSYM ; 
            a ++ ) {
         ua = Trie[ u ][ a ] ;
         if ( ua != FAIL ) {
            Gamma[ u ][ a ] = ua ;
            Failure[ ua ] = Gamma[ x ][ a ] ;
         } else {
            Gamma[ u ][ a ] = Gamma[ x ][ a ] ;
         }
      }
   }
   FREE_AND_NULL( Failure ) ;
   FREE_AND_NULL( Queue ) ;
   return Gamma ; 
}



static const char *rule_type_names[] = {
   "MACRO" , "MICRO" , "ARC" , "CIVIC" , "EXTRA"
} ;

/* =========================================
gamma.c (output_rule_statistics)
uses macro OPEN_ALLOCATED_NAME
stdio.h (printf,fprintf,fflush,fclose)
===========================================*/
#ifdef BUILD_API
int output_rule_statistics( RULE_PARAM *r_p, ERR_PARAM *err_p ) {
#else
int output_rule_statistics( RULE_PARAM *r_p , 
                            ERR_PARAM *err_p ,
                            char *name ,
                            DS_Handle _file_sys_p ) {
#endif
   int i ,
       found_count ,
       n ;
   SYMB *OL ;
   char *sts_name = NULL ;
   FILE *sts_file = NULL ;
   KW * k ; 
   KW * k_s ; 
   double hit_frequency ,
          best_frequency ;

   if ( !r_p -> collect_statistics ) {
      printf( "Statistics were not collected\n" ) ;
      return FALSE ;
   }      

#ifndef BUILD_API
   if ( name != NULL && name[ 0 ] != SENTINEL ) {
      OPEN_ALLOCATED_NAME(sts_name,"sts",sts_file,name,"wb+",_file_sys_p,err_p,FALSE) ;
   } 
#endif

   /* -- cycle through the keys -- */
   n = r_p -> rules_read ; 
   k_s = r_p -> key_space ; 
   for ( i = 0 , found_count = 0 ;
         i < n ;
         i++ ) {
      k = k_s + i ; 
      if ( k -> hits == 0 ) {
         continue ;
      }
 
      found_count++ ;
      if ( sts_file == NULL ) {
         printf( "\nRule %d is of type %d (%s)\n: " ,  
                 i ,
                 k -> Type ,
                 rule_type_names[ k -> Type ] ) ;
         printf( "Input : " ) ;
      } else {
         fprintf( sts_file ,
                  "\nRule %d is of type %d (%s)\n: " ,  
                  i ,
                  k -> Type ,
                  rule_type_names[ k -> Type ]  ) ;
         fprintf( sts_file ,
                  "Input : " ) ;
      }
      for ( OL = k -> Input ;
            *OL != FAIL ;
            OL++ ) {
         if ( sts_file == NULL ) {
             printf( "|%d (%s)|" ,
                     *OL ,
                     in_symb_name( *OL ) ) ;
         } else {
             fprintf( sts_file ,
                      "|%d (%s)|" ,
                      *OL ,
                      in_symb_name( *OL ) ) ;
         }
      }
      if ( sts_file == NULL ) {
         printf( "\nOutput: " ) ;

      } else {
         fprintf( sts_file ,
                  "\nOutput: " ) ;
      }
      /* -- output the output symbols -- */
      for ( OL = k -> Output ;
            *OL != FAIL ;
            OL++ ) {
         if ( sts_file == NULL ) {
            printf( "|%d (%s)|" ,
                    *OL ,
                    out_symb_name( *OL ) ) ;
         } else {
            fprintf( sts_file ,
                     "|%d (%s)|" ,
                     *OL ,
                     out_symb_name( *OL ) ) ;
         }
      }
      if ( sts_file == NULL ) {
         printf ( "\nrank %d ( %f): hits %d out of %d\n" ,
                  k -> Weight ,
                  load_value[ k -> Weight ] ,
                  k->hits, 
                  r_p -> total_key_hits ) ;
      } else {
         hit_frequency = ( ( double ) k -> hits ) / ( ( double ) r_p -> total_key_hits ) ;
         best_frequency = ( ( double )  k -> best ) / ( ( double ) r_p -> total_best_keys ) ;
         fprintf( sts_file ,
                  "\nrank %d ( %f): hit frequency: %f, best frequency: %f" ,
                  k -> Weight ,
                  load_value[ k -> Weight ] ,
                  hit_frequency ,
                  best_frequency ) ;
         fprintf ( sts_file ,
                   "\n%d hits out of %d, best %d out of %d\n" ,
                   k->hits, r_p -> total_key_hits, k-> best, r_p -> total_best_keys ) ;
      }
      k -> hits = 0 ;
      k -> best = 0 ;
   }
   if ( sts_file == NULL ) {
      printf( "Found %d rules hit\n" , found_count ) ;
   } else {
      fprintf( sts_file ,
               "Found %d rules hit\n" , 
               found_count ) ;
   }          
   /* -- start over -- */
   r_p -> total_key_hits = 0 ;
   r_p -> total_best_keys = 0 ;
   if ( sts_file != NULL ) {
      fflush( sts_file ) ;
      fclose( sts_file ) ;
      FREE_AND_NULL( sts_name ) ; 
   } else {
      fflush( stdout ) ;
   }
   return TRUE ;
}

