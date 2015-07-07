
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>

#include "pagc_api.h"
#include "pagc_std_api.h"

#define RULESIZE 40

#define LEXIN "lexicon.csv"
#define GAZIN "gazeteer.csv"
#define RULESIN "rules.txt"

static int standardize_command_line( STANDARDIZER *std ,
                                     char *input_str ,
                                     int option ) ;

void print_lexicon( ENTRY ** hash_table ) ;

/*
parse_csv() parses the following file format into fields

"1","#",16,"#"
"2","#",7,"#"
"1","&",13,"AND"
"2","&",1,"AND"
"3","&",7,"AND"
"1","-","9","-"

*/

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

   *d = '\0' ;
   /* -- space at the beginning of a line will stop the read -- */
   if ( isspace( *s ) )
      return NULL ;
   while ( ( c = *s++ ) != '\0' ) {
      if ( c == '\"' ||
           c == '\r' )
         continue ; /* -- ignore quotes and carriage returns -- */
      /* -- zero terminate field and record delimiters -- */
      if ( c == '\n' ||
           c == ',' ) {
          *d = '\0' ;
          return s ;
      }
      *d++ = c ; /* -- copy it -- */
   }
   return NULL ;
}

static int parse_csv(char *buf, int *seq, char *word, char *stdword, int *token)
{
    char *next_str ;
    char num_str[512];

    if ( ( next_str = convert_field( num_str , buf) ) == NULL ) return 0;
    sscanf( num_str, "%d", seq );
    next_str = convert_field( word, next_str);
    next_str = convert_field( num_str, next_str);
    sscanf( num_str, "%d", token );
    next_str = convert_field( stdword, next_str);

    return 1;
}


/*
parse_rule() reads lines the following and loads them into int[] and
returns the number of items read.

1 2 11 28 -1 10 10 11 13 -1 0 16
1 2 11 28 12 -1 10 10 11 13 12 -1 0 17
1 2 11 28 29 -1 10 10 11 13 13 -1 0 16
1 2 11 28 29 12 -1 10 10 11 13 13 12 -1 0 17
-1
*/

int parse_rule(char *buf, int *rule)
{
    int nr = 0;
    int *r = rule;
    char *p = buf;
    char *q;


    while (1) {
        *r = strtol( p, &q, 10 );
        if (p == q) break;
        p = q;
        nr++;
        r++;
    }

    return nr;
}

void Usage()
{
        printf("Usage: test_main [-o n] \n");
        printf("       -o n = options bit flag\n");
        printf("          1 = print lexicon\n");
        printf("          2 = print gazeteer\n");
        printf("          4 = print standardized fields\n");
        printf("          8 = print rule statistics\n");
        exit(1);
}

int main(int argc, char *argv[])
{
    STANDARDIZER *std;
    LEXICON *lex;
    LEXICON *gaz;
    RULES *rules;

    char buf[1024];

    int seq;
    char input_str[ 4096 ] ;
    char word[512];
    char stdword[512];
    int token;
    int nr;
    int rule[RULESIZE];
    int err;
    int cnt;
    int option = 0;

    FILE *in;

    if (argc == 3 && !strcmp(argv[1], "-o")) {
        option = strtol(argv[2], NULL, 10);
        argc -= 2;
        argv += 2;
    }
    else if (argc != 1) 
        Usage();

    std = std_init();
    assert(std);

    lex = lex_init(std->err_p);
    assert(lex);

    in = fopen(LEXIN, "rb");
    assert(in);

    cnt = 0;
    while (!feof(in) && fgets(buf, 1024, in)) {
        cnt++;
        /* parse into fields */
        if (parse_csv(buf, &seq, word, stdword, &token)) {
            /* add the record to the lexicon */
            err = lex_add_entry(lex, seq, word, stdword, token);
            if (err != 1)
                printf("lex: Failed: %d: %s", cnt, buf);
        }
        else {
            printf("lex: Skipping: %d: %s", cnt, buf);
        }
    }
    fclose(in);

    if (option & 1) {
        printf("------------ address lexicon --------------\n");
        print_lexicon(lex->hash_table);
        printf("\n");
    }

    gaz = lex_init(std->err_p);
    assert(gaz);

    in = fopen(GAZIN, "rb");
    assert(in);

    cnt = 0;
    while (!feof(in) && fgets(buf, 1024, in)) {
        cnt++;
        /* parse into fields */
        if (parse_csv(buf, &seq, word, stdword, &token)) {
            /* add the record to the lexicon */
            err = lex_add_entry(gaz, seq, word, stdword, token);
            if (err != 1)
                printf("gaz: Failed: %d: %s", cnt, buf);
        }
        else {
            printf("gaz: Skipping: %d: %s", cnt, buf);
        }
    }
    fclose(in);

    if (option & 2) {
        printf("------------ gazeteer lexicon --------------\n");
        print_lexicon(gaz->hash_table);
        printf("\n");
    }

    rules = rules_init(std->err_p);
    assert(rules);
    rules -> r_p -> collect_statistics = TRUE ;

    /* ************ RULES **************** */

    in = fopen(RULESIN, "rb");
    assert(in);

    cnt = 0;
    while (!feof(in) && fgets(buf, 1024, in)) {
        cnt++;
        /* parse into fields */
        nr = parse_rule(buf, rule);

        /* add the record to the rules */
        err = rules_add_rule(rules, nr, rule);
        if (err != 0)
            printf("rules: Failed: %d (%d): %s", cnt, err, buf);
    }
    err = rules_ready(rules);
    if (err != 0)
        printf("rules: Failed: err=%d\n", err);
    fclose(in);

    std_use_lex(std, lex);
    std_use_gaz(std, gaz);
    std_use_rules(std, rules);
    std_ready_standardizer(std);

    printf( "Standardization test. Type \"exit\" to quit:\n" ) ;
    fflush( stdout ) ;
    while ( TRUE ) {
        err = standardize_command_line( std, input_str, option ) ;
        if ( err == FAIL ) {
            break ;
        }
    }
    printf( "OK\n" ) ;
    fflush( stdout ) ;

    std_free(std);
/* these were freed when we bound them with std_use_*()
    rules_free(rules);
    lex_free(gaz);
    lex_free(lex);
*/

    return 0;
}



static int standardize_command_line( STANDARDIZER *std ,
                                     char *input_str ,
                                     int option ) {
   STDADDR *result;
   int fld_num ,
       have_user_macros ,
       num_prompts ;
   char unstandard_mic[ MAXSTRLEN ] ;
   char unstandard_mac_left[ MAXSTRLEN ] ;

   num_prompts = 3 ;

   unstandard_mic[ 0 ] = SENTINEL ;
   unstandard_mac_left[ 0 ] = SENTINEL ; ;
   have_user_macros = FALSE ;
   for ( fld_num = 1 ;
         fld_num < num_prompts ;
         fld_num++ ) {
      /* -- print prompt -- */
      if ( fld_num == 1 )
         printf( "MICRO:" ) ;
      else
         printf( "MACRO:" ) ;
      fflush( stdout ) ; /* -- to ensure prompt goes out --*/
      memset( input_str ,
              0 ,
              MAXSTRLEN ) ;
      input_str[ 0 ] = SENTINEL ;
      /* -- get user's input -- */
      if ( ( !get_input_line( input_str , stdin ) ) || 
           ( strncmp( input_str , "exit" , 4 ) == 0 ) ||
           ( strncmp( input_str , "quit" , 4 ) == 0 ) ||
           ( strncmp( input_str , "done" , 4 ) == 0 )
         ) {
         return FAIL ; /* -- indicate exit -- */
      }
      /* -- get input first, then standardize -- */
      if ( fld_num == 1 ) {
         strcpy( unstandard_mic ,
                 input_str ) ;
         if ( *unstandard_mic == SENTINEL ) {
            printf( "No MICRO input\n" ) ;
            return FALSE ; /* -- indicate no standardization -- */
         }
         convert_latin_one ( unstandard_mic ) ;
      } else {
         strcpy( unstandard_mac_left ,
                 input_str ) ;
         if ( *unstandard_mac_left != SENTINEL ) {
            have_user_macros = TRUE ;
            convert_latin_one ( unstandard_mac_left ) ;
         }
      }
   }

   result = std_standardize_mm( std,
                                unstandard_mic,
                                unstandard_mac_left,
                                (option & 4)?1:0 ) ;

   print_stdaddr( result );

   if (option & 8)
      output_rule_statistics( std->pagc_p->rules, std->err_p ) ;

   stdaddr_free(result);

   return 1;
}


