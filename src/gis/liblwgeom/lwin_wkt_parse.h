/* A Bison parser, made by GNU Bison 3.0.4.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015 Free Software Foundation, Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

#ifndef YY_WKT_YY_LWIN_WKT_PARSE_H_INCLUDED
# define YY_WKT_YY_LWIN_WKT_PARSE_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int wkt_yydebug;
#endif

/* Token type.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    POINT_TOK = 258,
    LINESTRING_TOK = 259,
    POLYGON_TOK = 260,
    MPOINT_TOK = 261,
    MLINESTRING_TOK = 262,
    MPOLYGON_TOK = 263,
    MSURFACE_TOK = 264,
    MCURVE_TOK = 265,
    CURVEPOLYGON_TOK = 266,
    COMPOUNDCURVE_TOK = 267,
    CIRCULARSTRING_TOK = 268,
    COLLECTION_TOK = 269,
    RBRACKET_TOK = 270,
    LBRACKET_TOK = 271,
    COMMA_TOK = 272,
    EMPTY_TOK = 273,
    SEMICOLON_TOK = 274,
    TRIANGLE_TOK = 275,
    TIN_TOK = 276,
    POLYHEDRALSURFACE_TOK = 277,
    DOUBLE_TOK = 278,
    DIMENSIONALITY_TOK = 279,
    SRID_TOK = 280
  };
#endif
/* Tokens.  */
#define POINT_TOK 258
#define LINESTRING_TOK 259
#define POLYGON_TOK 260
#define MPOINT_TOK 261
#define MLINESTRING_TOK 262
#define MPOLYGON_TOK 263
#define MSURFACE_TOK 264
#define MCURVE_TOK 265
#define CURVEPOLYGON_TOK 266
#define COMPOUNDCURVE_TOK 267
#define CIRCULARSTRING_TOK 268
#define COLLECTION_TOK 269
#define RBRACKET_TOK 270
#define LBRACKET_TOK 271
#define COMMA_TOK 272
#define EMPTY_TOK 273
#define SEMICOLON_TOK 274
#define TRIANGLE_TOK 275
#define TIN_TOK 276
#define POLYHEDRALSURFACE_TOK 277
#define DOUBLE_TOK 278
#define DIMENSIONALITY_TOK 279
#define SRID_TOK 280

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED

union YYSTYPE
{
#line 106 "lwin_wkt_parse.y" /* yacc.c:1909  */

	int integervalue;
	double doublevalue;
	char *stringvalue;
	LWGEOM *geometryvalue;
	POINT coordinatevalue;
	POINTARRAY *ptarrayvalue;

#line 113 "lwin_wkt_parse.h" /* yacc.c:1909  */
};

typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif

/* Location type.  */
#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE YYLTYPE;
struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
};
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif


extern YYSTYPE wkt_yylval;
extern YYLTYPE wkt_yylloc;
int wkt_yyparse (void);

#endif /* !YY_WKT_YY_LWIN_WKT_PARSE_H_INCLUDED  */
