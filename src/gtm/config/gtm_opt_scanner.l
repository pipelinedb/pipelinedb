/* -*-pgsql-c-*- */
/*
 * Scanner for the configuration file
 *
 * Copyright (c) 2000-2011, PostgreSQL Global Development Group
 *
 * src/backend/utils/misc/guc-file.l
 */

%{

#include "gtm/gtm.h"

#include <ctype.h>
#include <unistd.h>
#include <stdlib.h>

#include "mb/pg_wchar.h"
#include "gtm/assert.h"
#include "gtm/gtm_opt.h"
#include "gtm/elog.h"


/* Avoid exit() on fatal scanner errors (a bit ugly -- see yy_fatal_error) */
#undef fprintf
#define fprintf(file, fmt, msg)  ereport(ERROR, (errmsg_internal("%s", msg)))

enum {
	GTMOPT_ID = 1,
	GTMOPT_STRING = 2,
	GTMOPT_INTEGER = 3,
	GTMOPT_REAL = 4,
	GTMOPT_EQUALS = 5,
	GTMOPT_UNQUOTED_STRING = 6,
	GTMOPT_QUALIFIED_ID = 7,
	GTMOPT_EOL = 99,
	GTMOPT_ERROR = 100
};

static unsigned int ConfigFileLineno;

/* flex fails to supply a prototype for yylex, so provide one */
int GTMOPT_yylex(void);

%}

%option 8bit
%option never-interactive
%option nodefault
%option noinput
%option nounput
%option noyywrap
%option prefix="GTMOPT_yy"


SIGN            ("-"|"+")
DIGIT           [0-9]
HEXDIGIT        [0-9a-fA-F]

UNIT_LETTER     [a-zA-Z]

INTEGER         {SIGN}?({DIGIT}+|0x{HEXDIGIT}+){UNIT_LETTER}*

EXPONENT        [Ee]{SIGN}?{DIGIT}+
REAL            {SIGN}?{DIGIT}*"."{DIGIT}*{EXPONENT}?

LETTER          [A-Za-z_\200-\377]
LETTER_OR_DIGIT [A-Za-z_0-9\200-\377]

ID              {LETTER}{LETTER_OR_DIGIT}*
QUALIFIED_ID    {ID}"."{ID}

UNQUOTED_STRING {LETTER}({LETTER_OR_DIGIT}|[-._:/])*
STRING          \'([^'\\\n]|\\.|\'\')*\'

%%

\n              ConfigFileLineno++; return GTMOPT_EOL;
[ \t\r]+        /* eat whitespace */
#.*             /* eat comment (.* matches anything until newline) */

{ID}            return GTMOPT_ID;
{QUALIFIED_ID}  return GTMOPT_QUALIFIED_ID;
{STRING}        return GTMOPT_STRING;
{UNQUOTED_STRING} return GTMOPT_UNQUOTED_STRING;
{INTEGER}       return GTMOPT_INTEGER;
{REAL}          return GTMOPT_REAL;
=               return GTMOPT_EQUALS;

.               return GTMOPT_ERROR;

%%
