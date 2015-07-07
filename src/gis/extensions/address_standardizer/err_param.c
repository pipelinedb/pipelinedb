/* -- err_param.c 

This file handles the buffering and output of errors

Prototype 7H08 (This file was written by Walter Sinclair).

Copyright (c) 2009 Walter Bruce Sinclair

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


*/

/* For pagc-0.4.0 : last revised 2010-11-01 */

#undef DEBUG
//#define DEBUG

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "pagc_api.h"

static FILE *open_error_log ( const char *, DS_Handle , ERR_PARAM * ) ;
static int turn_off_error_log( ERR_PARAM * ) ;

#define PRINT_ERROR( TEMP , MSG ) \
      DBG( TEMP, MSG  ) ;

#define RESET_ERR_P \
   err_p -> first_err = 0 ; \
   err_p -> last_err = 0 ; \
   err_p -> next_fatal = TRUE ; \
   err_mem = err_p -> err_array  ; \
   err_p -> error_buf = err_mem -> content_buf ; \
   err_mem -> is_fatal = TRUE ; \
   BLANK_STRING( err_mem -> content_buf ) 



/* ------------------------------------------------------------
err_param.c (init_errors) :
calls : err_param.c (open_error_log), stdlib.h (malloc, free) ,
stdio.h (fprintf, fflush) stdlib.h (malloc,free)
--------------------------------------------------------------*/
ERR_PARAM *init_errors( PAGC_GLOBAL *pagc_glo_p ,
                        const char *log_name ) {
   ERR_PARAM *err_p ;
   ERR_REC *err_mem ;

   err_p = ( ERR_PARAM * ) malloc( sizeof( ERR_PARAM ) ) ;
   if ( err_p == NULL ) {
#ifndef NO_STDERR_OUTPUT
      PRINT_ERROR( "%s\n" ,
                   "FATAL ERROR : Could not allocate memory for pagc_init_errors" ) ;
#endif
      return NULL ;
   }

   /* -- set up first record -- */
   RESET_ERR_P ;
   /* -- a null log_name means we don't log , but collect -- */
   if ( log_name == NULL ) {
      err_p -> stream = NULL ;
   }
   else {
      err_p -> stream = open_error_log( log_name ,
                                        pagc_glo_p -> _file_sys ,
                                        err_p ) ;
      if ( err_p -> stream == NULL ) {
         FREE_AND_NULL( err_p ) ;
#ifndef NO_STDERR_OUTPUT
         PRINT_ERROR( "Could not create error log for pathname: %s\n" ,
                       log_name ) ;
#endif
         return NULL ;
      }                  
   }
   return err_p ;
}   


/* ------------------------------------------------------------
err_param.c (close_errors)
uses macros BLANK_STRING, FREE_AND_NULL, and PRINT_ERROR
--------------------------------------------------------------*/
void close_errors( ERR_PARAM *err_p ) {
   int is_fatal_error ;
   char err_out_buf[ MAXSTRLEN ] ;


   if ( err_p == NULL ) {
      return ;
   }

   BLANK_STRING( err_out_buf ) ;

   /* -- read each error into the buffer and then
      output it as a single line -- */
   while ( empty_errors( err_p ,
                         &is_fatal_error ,
                         err_out_buf ) ) {
      if ( is_fatal_error ) {
#ifndef NO_STDERR_OUTPUT
         PRINT_ERROR( "ERROR: %s\n" ,
                      err_out_buf ) ;
      } else {
         PRINT_ERROR( "%s\n" ,
                      err_out_buf ) ;
#endif
      }
      BLANK_STRING( err_out_buf ) ;
   }    
   FREE_AND_NULL( err_p ) ;
}

/* ------------------------------------------------------------
err_param.c (turn_off_error_log)
called by err_param.c (empty_errors)
stdio.h (fclose)
--------------------------------------------------------------*/
static int turn_off_error_log( ERR_PARAM *err_p ) {
   ERR_REC *err_mem ;

   if ( ( err_p == NULL ) || 
        ( err_p -> stream == NULL ) ) { 
      return FALSE ;
   }
   fclose( err_p -> stream ) ;
   err_p -> stream = NULL ;
   RESET_ERR_P ;
   return TRUE ;
}

/* ----------------------------------------------------------
err_param.c (empty_errors)
calls : err_param.c (turn_off_error_log)
returns FALSE when all errors have been reported. 
TRUE otherwise
------------------------------------------------------------*/
int empty_errors( ERR_PARAM *err_p ,
                  int *is_fatal ,
                  char *err_dest ) {

   ERR_REC *err_mem ;
	
   if ( err_p == NULL ) {
      return FALSE ;
   }

   if ( err_p -> first_err >= err_p -> last_err ) {
      /* -- reset the counters -- */
      RESET_ERR_P ;
      return FALSE ; /* -- indicate empty -- */
   }

   /* -- if logging, turn it off and indicate empty -- */
   if ( turn_off_error_log( err_p ) ) {
      return FALSE ;
   }

   /* -- output the current lowest record -- */
   err_mem = err_p -> err_array + err_p -> first_err ;
   append_string_to_max( err_dest ,
                         err_mem -> content_buf ,
                         MAXSTRLEN ) ;   
   *is_fatal = err_mem -> is_fatal ;

   /* -- update the low mark -- */
   err_p -> first_err ++ ;
   return TRUE ; /* indicate error there */
}

/* ------------------------------------------------
err_param.c (open_error_log) :
called by init_errors
calls : stdlib.h (free) stdio.h (fopen)
uses macros OPEN_ALLOCATED_NAME, FREE_AND_NULL
--------------------------------------------------- */
static FILE *open_error_log( const char *client_log_name ,
                             DS_Handle _file_sys_p ,
                             ERR_PARAM *err_p ) {
#ifdef BUILD_API
   return NULL;
#else
   char *alloc_log_name ;
   FILE *error_file ;

   if ( client_log_name != NULL ) {
      /* -- will overwrite previous log in same location -- */
     OPEN_ALLOCATED_NAME(alloc_log_name,"err",error_file,client_log_name,"wb+",_file_sys_p,err_p,NULL) ;
   }
   FREE_AND_NULL( alloc_log_name ) ;
   return error_file ;
#endif
}



/* -----------------------------------------------------------
err_param.c (register_error)
called after the error is written to the error_buf
stdlib.h (malloc) stdio.h (fprintf,fflush) string.h (strcpy)
------------------------------------------------------------ */
void register_error( ERR_PARAM *err_p ) {
   int i ;
   ERR_REC *err_mem ;


   /* -- check if there is anything in the error_buf -- */
   if ( err_p -> error_buf[ 0 ] == SENTINEL ) {
      return ;
   }
   if ( strlen( err_p -> error_buf ) > MAXSTRLEN ) {
#ifndef NO_STDERR_OUTPUT
      PRINT_ERROR( "Error message %s is too long" ,
                    err_p -> error_buf ) ; 
#endif
      return ;
   }
   /* -- print it out immediately, if we're logging -- */
   if ( err_p -> stream != NULL ) {
      fprintf( err_p -> stream ,
               "%s\n" ,
               err_p -> error_buf ) ;
      fflush( err_p -> stream ) ;
      /* -- set up for next error -- */
      BLANK_STRING( err_p -> error_buf ) ;
      return ;
   }
   /* -- update the current error record -- */
   err_mem = err_p -> err_array + err_p -> last_err ;
   err_mem -> is_fatal = err_p -> next_fatal ;

   if ( err_p -> last_err == ( MAX_ERRORS - 1 ) ) {
#ifndef NO_STDERR_OUTPUT
      PRINT_ERROR( "%s is too many errors - losing old ones" ,
                   err_p -> error_buf ) ;
#endif
      /* -- move the whole array down a slot to make room for
         the next error. The first in the array disappears -- */
      for ( i = err_p -> first_err ;
            i < err_p -> last_err ;
            i++ ) {
         err_p -> err_array[ i ] . is_fatal = err_p -> err_array[ i + 1 ] . is_fatal ;
         strcpy( err_p -> err_array[ i ] . content_buf ,
                 err_p -> err_array[ i + 1 ] . content_buf ) ;
      }
   } else {
      /* -- last_err points to the next one to fill -- */
      err_p -> last_err ++ ; 
      err_mem = err_p -> err_array + err_p -> last_err  ;
   }

   /* -- reset error_buf to the new content_buf -- */
   err_p -> error_buf = err_mem -> content_buf ; 
   BLANK_STRING( err_mem -> content_buf ) ;
   err_p -> next_fatal = TRUE ;
   return ;
} 


/*==========================================
2006-11-02 add new arg
===========================================*/
void send_fields_to_error( ERR_PARAM *err_p ,
                           char **s_fields ) {

   send_fields_to_stream( s_fields , /* 2006-11-02 */
                          err_p -> stream ,
                          SCREEN , FALSE ) ;
}

