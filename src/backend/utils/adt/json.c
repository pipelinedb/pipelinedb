/*-------------------------------------------------------------------------
 *
 * json.c
 *		JSON data type support.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/json.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/transam.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "parser/parse_coerce.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/json.h"
#include "utils/jsonapi.h"
#include "utils/typcache.h"
#include "utils/syscache.h"

/*
 * The context of the parser is maintained by the recursive descent
 * mechanism, but is passed explicitly to the error reporting routine
 * for better diagnostics.
 */
typedef enum					/* contexts of JSON parser */
{
	JSON_PARSE_VALUE,			/* expecting a value */
	JSON_PARSE_STRING,			/* expecting a string (for a field name) */
	JSON_PARSE_ARRAY_START,		/* saw '[', expecting value or ']' */
	JSON_PARSE_ARRAY_NEXT,		/* saw array element, expecting ',' or ']' */
	JSON_PARSE_OBJECT_START,	/* saw '{', expecting label or '}' */
	JSON_PARSE_OBJECT_LABEL,	/* saw object label, expecting ':' */
	JSON_PARSE_OBJECT_NEXT,		/* saw object value, expecting ',' or '}' */
	JSON_PARSE_OBJECT_COMMA,	/* saw object ',', expecting next label */
	JSON_PARSE_END				/* saw the end of a document, expect nothing */
} JsonParseContext;

typedef enum					/* type categories for datum_to_json */
{
	JSONTYPE_NULL,				/* null, so we didn't bother to identify */
	JSONTYPE_BOOL,				/* boolean (built-in types only) */
	JSONTYPE_NUMERIC,			/* numeric (ditto) */
	JSONTYPE_TIMESTAMP,         /* we use special formatting for timestamp */
	JSONTYPE_TIMESTAMPTZ,       /* ... and timestamptz */
	JSONTYPE_JSON,				/* JSON itself (and JSONB) */
	JSONTYPE_ARRAY,				/* array */
	JSONTYPE_COMPOSITE,			/* composite */
	JSONTYPE_CAST,				/* something with an explicit cast to JSON */
	JSONTYPE_OTHER				/* all else */
} JsonTypeCategory;

static inline void json_lex(JsonLexContext *lex);
static inline void json_lex_string(JsonLexContext *lex);
static inline void json_lex_number(JsonLexContext *lex, char *s, bool *num_err);
static inline void parse_scalar(JsonLexContext *lex, JsonSemAction *sem);
static void parse_object_field(JsonLexContext *lex, JsonSemAction *sem);
static void parse_object(JsonLexContext *lex, JsonSemAction *sem);
static void parse_array_element(JsonLexContext *lex, JsonSemAction *sem);
static void parse_array(JsonLexContext *lex, JsonSemAction *sem);
static void report_parse_error(JsonParseContext ctx, JsonLexContext *lex);
static void report_invalid_token(JsonLexContext *lex);
static int	report_json_context(JsonLexContext *lex);
static char *extract_mb_char(char *s);
static void composite_to_json(Datum composite, StringInfo result,
				  bool use_line_feeds);
static void array_dim_to_json(StringInfo result, int dim, int ndims, int *dims,
				  Datum *vals, bool *nulls, int *valcount,
				  JsonTypeCategory tcategory, Oid outfuncoid,
				  bool use_line_feeds);
static void array_to_json_internal(Datum array, StringInfo result,
					   bool use_line_feeds);
static void json_categorize_type(Oid typoid,
					 JsonTypeCategory *tcategory,
					 Oid *outfuncoid);
static void datum_to_json(Datum val, bool is_null, StringInfo result,
			  JsonTypeCategory tcategory, Oid outfuncoid,
			  bool key_scalar);
static void add_json(Datum val, bool is_null, StringInfo result,
		 Oid val_type, bool key_scalar);

/* the null action object used for pure validation */
static JsonSemAction nullSemAction =
{
	NULL, NULL, NULL, NULL, NULL,
	NULL, NULL, NULL, NULL, NULL
};

/* Recursive Descent parser support routines */

/*
 * lex_peek
 *
 * what is the current look_ahead token?
*/
static inline JsonTokenType
lex_peek(JsonLexContext *lex)
{
	return lex->token_type;
}

/*
 * lex_accept
 *
 * accept the look_ahead token and move the lexer to the next token if the
 * look_ahead token matches the token parameter. In that case, and if required,
 * also hand back the de-escaped lexeme.
 *
 * returns true if the token matched, false otherwise.
 */
static inline bool
lex_accept(JsonLexContext *lex, JsonTokenType token, char **lexeme)
{
	if (lex->token_type == token)
	{
		if (lexeme != NULL)
		{
			if (lex->token_type == JSON_TOKEN_STRING)
			{
				if (lex->strval != NULL)
					*lexeme = pstrdup(lex->strval->data);
			}
			else
			{
				int			len = (lex->token_terminator - lex->token_start);
				char	   *tokstr = palloc(len + 1);

				memcpy(tokstr, lex->token_start, len);
				tokstr[len] = '\0';
				*lexeme = tokstr;
			}
		}
		json_lex(lex);
		return true;
	}
	return false;
}

/*
 * lex_accept
 *
 * move the lexer to the next token if the current look_ahead token matches
 * the parameter token. Otherwise, report an error.
 */
static inline void
lex_expect(JsonParseContext ctx, JsonLexContext *lex, JsonTokenType token)
{
	if (!lex_accept(lex, token, NULL))
		report_parse_error(ctx, lex);;
}

/* chars to consider as part of an alphanumeric token */
#define JSON_ALPHANUMERIC_CHAR(c)  \
	(((c) >= 'a' && (c) <= 'z') || \
	 ((c) >= 'A' && (c) <= 'Z') || \
	 ((c) >= '0' && (c) <= '9') || \
	 (c) == '_' || \
	 IS_HIGHBIT_SET(c))

/*
 * Input.
 */
Datum
json_in(PG_FUNCTION_ARGS)
{
	char	   *json = PG_GETARG_CSTRING(0);
	text	   *result = cstring_to_text(json);
	JsonLexContext *lex;

	/* validate it */
	lex = makeJsonLexContext(result, false);
	pg_parse_json(lex, &nullSemAction);

	/* Internal representation is the same as text, for now */
	PG_RETURN_TEXT_P(result);
}

/*
 * Output.
 */
Datum
json_out(PG_FUNCTION_ARGS)
{
	/* we needn't detoast because text_to_cstring will handle that */
	Datum		txt = PG_GETARG_DATUM(0);

	PG_RETURN_CSTRING(TextDatumGetCString(txt));
}

/*
 * Binary send.
 */
Datum
json_send(PG_FUNCTION_ARGS)
{
	text	   *t = PG_GETARG_TEXT_PP(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendtext(&buf, VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t));
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * Binary receive.
 */
Datum
json_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	char	   *str;
	int			nbytes;
	JsonLexContext *lex;

	str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);

	/* Validate it. */
	lex = makeJsonLexContextCstringLen(str, nbytes, false);
	pg_parse_json(lex, &nullSemAction);

	PG_RETURN_TEXT_P(cstring_to_text_with_len(str, nbytes));
}

/*
 * makeJsonLexContext
 *
 * lex constructor, with or without StringInfo object
 * for de-escaped lexemes.
 *
 * Without is better as it makes the processing faster, so only make one
 * if really required.
 *
 * If you already have the json as a text* value, use the first of these
 * functions, otherwise use  makeJsonLexContextCstringLen().
 */
JsonLexContext *
makeJsonLexContext(text *json, bool need_escapes)
{
	return makeJsonLexContextCstringLen(VARDATA(json),
										VARSIZE(json) - VARHDRSZ,
										need_escapes);
}

JsonLexContext *
makeJsonLexContextCstringLen(char *json, int len, bool need_escapes)
{
	JsonLexContext *lex = palloc0(sizeof(JsonLexContext));

	lex->input = lex->token_terminator = lex->line_start = json;
	lex->line_number = 1;
	lex->input_length = len;
	if (need_escapes)
		lex->strval = makeStringInfo();
	return lex;
}

/*
 * pg_parse_json
 *
 * Publicly visible entry point for the JSON parser.
 *
 * lex is a lexing context, set up for the json to be processed by calling
 * makeJsonLexContext(). sem is a strucure of function pointers to semantic
 * action routines to be called at appropriate spots during parsing, and a
 * pointer to a state object to be passed to those routines.
 */
void
pg_parse_json(JsonLexContext *lex, JsonSemAction *sem)
{
	JsonTokenType tok;

	/* get the initial token */
	json_lex(lex);

	tok = lex_peek(lex);

	/* parse by recursive descent */
	switch (tok)
	{
		case JSON_TOKEN_OBJECT_START:
			parse_object(lex, sem);
			break;
		case JSON_TOKEN_ARRAY_START:
			parse_array(lex, sem);
			break;
		default:
			parse_scalar(lex, sem);		/* json can be a bare scalar */
	}

	lex_expect(JSON_PARSE_END, lex, JSON_TOKEN_END);

}

/*
 *	Recursive Descent parse routines. There is one for each structural
 *	element in a json document:
 *	  - scalar (string, number, true, false, null)
 *	  - array  ( [ ] )
 *	  - array element
 *	  - object ( { } )
 *	  - object field
 */
static inline void
parse_scalar(JsonLexContext *lex, JsonSemAction *sem)
{
	char	   *val = NULL;
	json_scalar_action sfunc = sem->scalar;
	char	  **valaddr;
	JsonTokenType tok = lex_peek(lex);

	valaddr = sfunc == NULL ? NULL : &val;

	/* a scalar must be a string, a number, true, false, or null */
	switch (tok)
	{
		case JSON_TOKEN_TRUE:
			lex_accept(lex, JSON_TOKEN_TRUE, valaddr);
			break;
		case JSON_TOKEN_FALSE:
			lex_accept(lex, JSON_TOKEN_FALSE, valaddr);
			break;
		case JSON_TOKEN_NULL:
			lex_accept(lex, JSON_TOKEN_NULL, valaddr);
			break;
		case JSON_TOKEN_NUMBER:
			lex_accept(lex, JSON_TOKEN_NUMBER, valaddr);
			break;
		case JSON_TOKEN_STRING:
			lex_accept(lex, JSON_TOKEN_STRING, valaddr);
			break;
		default:
			report_parse_error(JSON_PARSE_VALUE, lex);
	}

	if (sfunc != NULL)
		(*sfunc) (sem->semstate, val, tok);
}

static void
parse_object_field(JsonLexContext *lex, JsonSemAction *sem)
{
	/*
	 * an object field is "fieldname" : value where value can be a scalar,
	 * object or array
	 */

	char	   *fname = NULL;	/* keep compiler quiet */
	json_ofield_action ostart = sem->object_field_start;
	json_ofield_action oend = sem->object_field_end;
	bool		isnull;
	char	  **fnameaddr = NULL;
	JsonTokenType tok;

	if (ostart != NULL || oend != NULL)
		fnameaddr = &fname;

	if (!lex_accept(lex, JSON_TOKEN_STRING, fnameaddr))
		report_parse_error(JSON_PARSE_STRING, lex);

	lex_expect(JSON_PARSE_OBJECT_LABEL, lex, JSON_TOKEN_COLON);

	tok = lex_peek(lex);
	isnull = tok == JSON_TOKEN_NULL;

	if (ostart != NULL)
		(*ostart) (sem->semstate, fname, isnull);

	switch (tok)
	{
		case JSON_TOKEN_OBJECT_START:
			parse_object(lex, sem);
			break;
		case JSON_TOKEN_ARRAY_START:
			parse_array(lex, sem);
			break;
		default:
			parse_scalar(lex, sem);
	}

	if (oend != NULL)
		(*oend) (sem->semstate, fname, isnull);
}

static void
parse_object(JsonLexContext *lex, JsonSemAction *sem)
{
	/*
	 * an object is a possibly empty sequence of object fields, separated by
	 * commas and surrounde by curly braces.
	 */
	json_struct_action ostart = sem->object_start;
	json_struct_action oend = sem->object_end;
	JsonTokenType tok;

	if (ostart != NULL)
		(*ostart) (sem->semstate);

	/*
	 * Data inside an object is at a higher nesting level than the object
	 * itself. Note that we increment this after we call the semantic routine
	 * for the object start and restore it before we call the routine for the
	 * object end.
	 */
	lex->lex_level++;

	/* we know this will succeeed, just clearing the token */
	lex_expect(JSON_PARSE_OBJECT_START, lex, JSON_TOKEN_OBJECT_START);

	tok = lex_peek(lex);
	switch (tok)
	{
		case JSON_TOKEN_STRING:
			parse_object_field(lex, sem);
			while (lex_accept(lex, JSON_TOKEN_COMMA, NULL))
				parse_object_field(lex, sem);
			break;
		case JSON_TOKEN_OBJECT_END:
			break;
		default:
			/* case of an invalid initial token inside the object */
			report_parse_error(JSON_PARSE_OBJECT_START, lex);
	}

	lex_expect(JSON_PARSE_OBJECT_NEXT, lex, JSON_TOKEN_OBJECT_END);

	lex->lex_level--;

	if (oend != NULL)
		(*oend) (sem->semstate);
}

static void
parse_array_element(JsonLexContext *lex, JsonSemAction *sem)
{
	json_aelem_action astart = sem->array_element_start;
	json_aelem_action aend = sem->array_element_end;
	JsonTokenType tok = lex_peek(lex);

	bool		isnull;

	isnull = tok == JSON_TOKEN_NULL;

	if (astart != NULL)
		(*astart) (sem->semstate, isnull);

	/* an array element is any object, array or scalar */
	switch (tok)
	{
		case JSON_TOKEN_OBJECT_START:
			parse_object(lex, sem);
			break;
		case JSON_TOKEN_ARRAY_START:
			parse_array(lex, sem);
			break;
		default:
			parse_scalar(lex, sem);
	}

	if (aend != NULL)
		(*aend) (sem->semstate, isnull);
}

static void
parse_array(JsonLexContext *lex, JsonSemAction *sem)
{
	/*
	 * an array is a possibly empty sequence of array elements, separated by
	 * commas and surrounded by square brackets.
	 */
	json_struct_action astart = sem->array_start;
	json_struct_action aend = sem->array_end;

	if (astart != NULL)
		(*astart) (sem->semstate);

	/*
	 * Data inside an array is at a higher nesting level than the array
	 * itself. Note that we increment this after we call the semantic routine
	 * for the array start and restore it before we call the routine for the
	 * array end.
	 */
	lex->lex_level++;

	lex_expect(JSON_PARSE_ARRAY_START, lex, JSON_TOKEN_ARRAY_START);
	if (lex_peek(lex) != JSON_TOKEN_ARRAY_END)
	{

		parse_array_element(lex, sem);

		while (lex_accept(lex, JSON_TOKEN_COMMA, NULL))
			parse_array_element(lex, sem);
	}

	lex_expect(JSON_PARSE_ARRAY_NEXT, lex, JSON_TOKEN_ARRAY_END);

	lex->lex_level--;

	if (aend != NULL)
		(*aend) (sem->semstate);
}

/*
 * Lex one token from the input stream.
 */
static inline void
json_lex(JsonLexContext *lex)
{
	char	   *s;
	int			len;

	/* Skip leading whitespace. */
	s = lex->token_terminator;
	len = s - lex->input;
	while (len < lex->input_length &&
		   (*s == ' ' || *s == '\t' || *s == '\n' || *s == '\r'))
	{
		if (*s == '\n')
			++lex->line_number;
		++s;
		++len;
	}
	lex->token_start = s;

	/* Determine token type. */
	if (len >= lex->input_length)
	{
		lex->token_start = NULL;
		lex->prev_token_terminator = lex->token_terminator;
		lex->token_terminator = s;
		lex->token_type = JSON_TOKEN_END;
	}
	else
		switch (*s)
		{
				/* Single-character token, some kind of punctuation mark. */
			case '{':
				lex->prev_token_terminator = lex->token_terminator;
				lex->token_terminator = s + 1;
				lex->token_type = JSON_TOKEN_OBJECT_START;
				break;
			case '}':
				lex->prev_token_terminator = lex->token_terminator;
				lex->token_terminator = s + 1;
				lex->token_type = JSON_TOKEN_OBJECT_END;
				break;
			case '[':
				lex->prev_token_terminator = lex->token_terminator;
				lex->token_terminator = s + 1;
				lex->token_type = JSON_TOKEN_ARRAY_START;
				break;
			case ']':
				lex->prev_token_terminator = lex->token_terminator;
				lex->token_terminator = s + 1;
				lex->token_type = JSON_TOKEN_ARRAY_END;
				break;
			case ',':
				lex->prev_token_terminator = lex->token_terminator;
				lex->token_terminator = s + 1;
				lex->token_type = JSON_TOKEN_COMMA;
				break;
			case ':':
				lex->prev_token_terminator = lex->token_terminator;
				lex->token_terminator = s + 1;
				lex->token_type = JSON_TOKEN_COLON;
				break;
			case '"':
				/* string */
				json_lex_string(lex);
				lex->token_type = JSON_TOKEN_STRING;
				break;
			case '-':
				/* Negative number. */
				json_lex_number(lex, s + 1, NULL);
				lex->token_type = JSON_TOKEN_NUMBER;
				break;
			case '0':
			case '1':
			case '2':
			case '3':
			case '4':
			case '5':
			case '6':
			case '7':
			case '8':
			case '9':
				/* Positive number. */
				json_lex_number(lex, s, NULL);
				lex->token_type = JSON_TOKEN_NUMBER;
				break;
			default:
				{
					char	   *p;

					/*
					 * We're not dealing with a string, number, legal
					 * punctuation mark, or end of string.  The only legal
					 * tokens we might find here are true, false, and null,
					 * but for error reporting purposes we scan until we see a
					 * non-alphanumeric character.  That way, we can report
					 * the whole word as an unexpected token, rather than just
					 * some unintuitive prefix thereof.
					 */
					for (p = s; p - s < lex->input_length - len && JSON_ALPHANUMERIC_CHAR(*p); p++)
						 /* skip */ ;

					/*
					 * We got some sort of unexpected punctuation or an
					 * otherwise unexpected character, so just complain about
					 * that one character.
					 */
					if (p == s)
					{
						lex->prev_token_terminator = lex->token_terminator;
						lex->token_terminator = s + 1;
						report_invalid_token(lex);
					}

					/*
					 * We've got a real alphanumeric token here.  If it
					 * happens to be true, false, or null, all is well.  If
					 * not, error out.
					 */
					lex->prev_token_terminator = lex->token_terminator;
					lex->token_terminator = p;
					if (p - s == 4)
					{
						if (memcmp(s, "true", 4) == 0)
							lex->token_type = JSON_TOKEN_TRUE;
						else if (memcmp(s, "null", 4) == 0)
							lex->token_type = JSON_TOKEN_NULL;
						else
							report_invalid_token(lex);
					}
					else if (p - s == 5 && memcmp(s, "false", 5) == 0)
						lex->token_type = JSON_TOKEN_FALSE;
					else
						report_invalid_token(lex);

				}
		}						/* end of switch */
}

/*
 * The next token in the input stream is known to be a string; lex it.
 */
static inline void
json_lex_string(JsonLexContext *lex)
{
	char	   *s;
	int			len;
	int			hi_surrogate = -1;

	if (lex->strval != NULL)
		resetStringInfo(lex->strval);

	Assert(lex->input_length > 0);
	s = lex->token_start;
	len = lex->token_start - lex->input;
	for (;;)
	{
		s++;
		len++;
		/* Premature end of the string. */
		if (len >= lex->input_length)
		{
			lex->token_terminator = s;
			report_invalid_token(lex);
		}
		else if (*s == '"')
			break;
		else if ((unsigned char) *s < 32)
		{
			/* Per RFC4627, these characters MUST be escaped. */
			/* Since *s isn't printable, exclude it from the context string */
			lex->token_terminator = s;
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type json"),
					 errdetail("Character with value 0x%02x must be escaped.",
							   (unsigned char) *s),
					 report_json_context(lex)));
		}
		else if (*s == '\\')
		{
			/* OK, we have an escape character. */
			s++;
			len++;
			if (len >= lex->input_length)
			{
				lex->token_terminator = s;
				report_invalid_token(lex);
			}
			else if (*s == 'u')
			{
				int			i;
				int			ch = 0;

				for (i = 1; i <= 4; i++)
				{
					s++;
					len++;
					if (len >= lex->input_length)
					{
						lex->token_terminator = s;
						report_invalid_token(lex);
					}
					else if (*s >= '0' && *s <= '9')
						ch = (ch * 16) + (*s - '0');
					else if (*s >= 'a' && *s <= 'f')
						ch = (ch * 16) + (*s - 'a') + 10;
					else if (*s >= 'A' && *s <= 'F')
						ch = (ch * 16) + (*s - 'A') + 10;
					else
					{
						lex->token_terminator = s + pg_mblen(s);
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
								 errmsg("invalid input syntax for type json"),
								 errdetail("\"\\u\" must be followed by four hexadecimal digits."),
								 report_json_context(lex)));
					}
				}
				if (lex->strval != NULL)
				{
					char		utf8str[5];
					int			utf8len;

					if (ch >= 0xd800 && ch <= 0xdbff)
					{
						if (hi_surrogate != -1)
							ereport(ERROR,
							   (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
								errmsg("invalid input syntax for type json"),
								errdetail("Unicode high surrogate must not follow a high surrogate."),
								report_json_context(lex)));
						hi_surrogate = (ch & 0x3ff) << 10;
						continue;
					}
					else if (ch >= 0xdc00 && ch <= 0xdfff)
					{
						if (hi_surrogate == -1)
							ereport(ERROR,
							   (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
								errmsg("invalid input syntax for type json"),
								errdetail("Unicode low surrogate must follow a high surrogate."),
								report_json_context(lex)));
						ch = 0x10000 + hi_surrogate + (ch & 0x3ff);
						hi_surrogate = -1;
					}

					if (hi_surrogate != -1)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
								 errmsg("invalid input syntax for type json"),
								 errdetail("Unicode low surrogate must follow a high surrogate."),
								 report_json_context(lex)));

					/*
					 * For UTF8, replace the escape sequence by the actual
					 * utf8 character in lex->strval. Do this also for other
					 * encodings if the escape designates an ASCII character,
					 * otherwise raise an error. We don't ever unescape a
					 * \u0000, since that would result in an impermissible nul
					 * byte.
					 */

					if (ch == 0)
					{
						appendStringInfoString(lex->strval, "\\u0000");
					}
					else if (GetDatabaseEncoding() == PG_UTF8)
					{
						unicode_to_utf8(ch, (unsigned char *) utf8str);
						utf8len = pg_utf_mblen((unsigned char *) utf8str);
						appendBinaryStringInfo(lex->strval, utf8str, utf8len);
					}
					else if (ch <= 0x007f)
					{
						/*
						 * This is the only way to designate things like a
						 * form feed character in JSON, so it's useful in all
						 * encodings.
						 */
						appendStringInfoChar(lex->strval, (char) ch);
					}
					else
					{
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
								 errmsg("invalid input syntax for type json"),
								 errdetail("Unicode escape values cannot be used for code point values above 007F when the server encoding is not UTF8."),
								 report_json_context(lex)));
					}

				}
			}
			else if (lex->strval != NULL)
			{
				if (hi_surrogate != -1)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
							 errmsg("invalid input syntax for type json"),
							 errdetail("Unicode low surrogate must follow a high surrogate."),
							 report_json_context(lex)));

				switch (*s)
				{
					case '"':
					case '\\':
					case '/':
						appendStringInfoChar(lex->strval, *s);
						break;
					case 'b':
						appendStringInfoChar(lex->strval, '\b');
						break;
					case 'f':
						appendStringInfoChar(lex->strval, '\f');
						break;
					case 'n':
						appendStringInfoChar(lex->strval, '\n');
						break;
					case 'r':
						appendStringInfoChar(lex->strval, '\r');
						break;
					case 't':
						appendStringInfoChar(lex->strval, '\t');
						break;
					default:
						/* Not a valid string escape, so error out. */
						lex->token_terminator = s + pg_mblen(s);
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
								 errmsg("invalid input syntax for type json"),
							errdetail("Escape sequence \"\\%s\" is invalid.",
									  extract_mb_char(s)),
								 report_json_context(lex)));
				}
			}
			else if (strchr("\"\\/bfnrt", *s) == NULL)
			{
				/*
				 * Simpler processing if we're not bothered about de-escaping
				 *
				 * It's very tempting to remove the strchr() call here and
				 * replace it with a switch statement, but testing so far has
				 * shown it's not a performance win.
				 */
				lex->token_terminator = s + pg_mblen(s);
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("invalid input syntax for type json"),
						 errdetail("Escape sequence \"\\%s\" is invalid.",
								   extract_mb_char(s)),
						 report_json_context(lex)));
			}

		}
		else if (lex->strval != NULL)
		{
			if (hi_surrogate != -1)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("invalid input syntax for type json"),
						 errdetail("Unicode low surrogate must follow a high surrogate."),
						 report_json_context(lex)));

			appendStringInfoChar(lex->strval, *s);
		}

	}

	if (hi_surrogate != -1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type json"),
			errdetail("Unicode low surrogate must follow a high surrogate."),
				 report_json_context(lex)));

	/* Hooray, we found the end of the string! */
	lex->prev_token_terminator = lex->token_terminator;
	lex->token_terminator = s + 1;
}

/*-------------------------------------------------------------------------
 * The next token in the input stream is known to be a number; lex it.
 *
 * In JSON, a number consists of four parts:
 *
 * (1) An optional minus sign ('-').
 *
 * (2) Either a single '0', or a string of one or more digits that does not
 *	   begin with a '0'.
 *
 * (3) An optional decimal part, consisting of a period ('.') followed by
 *	   one or more digits.  (Note: While this part can be omitted
 *	   completely, it's not OK to have only the decimal point without
 *	   any digits afterwards.)
 *
 * (4) An optional exponent part, consisting of 'e' or 'E', optionally
 *	   followed by '+' or '-', followed by one or more digits.  (Note:
 *	   As with the decimal part, if 'e' or 'E' is present, it must be
 *	   followed by at least one digit.)
 *
 * The 's' argument to this function points to the ostensible beginning
 * of part 2 - i.e. the character after any optional minus sign, and the
 * first character of the string if there is none.
 *
 *-------------------------------------------------------------------------
 */
static inline void
json_lex_number(JsonLexContext *lex, char *s, bool *num_err)
{
	bool		error = false;
	char	   *p;
	int			len;

	len = s - lex->input;
	/* Part (1): leading sign indicator. */
	/* Caller already did this for us; so do nothing. */

	/* Part (2): parse main digit string. */
	if (*s == '0')
	{
		s++;
		len++;
	}
	else if (*s >= '1' && *s <= '9')
	{
		do
		{
			s++;
			len++;
		} while (len < lex->input_length && *s >= '0' && *s <= '9');
	}
	else
		error = true;

	/* Part (3): parse optional decimal portion. */
	if (len < lex->input_length && *s == '.')
	{
		s++;
		len++;
		if (len == lex->input_length || *s < '0' || *s > '9')
			error = true;
		else
		{
			do
			{
				s++;
				len++;
			} while (len < lex->input_length && *s >= '0' && *s <= '9');
		}
	}

	/* Part (4): parse optional exponent. */
	if (len < lex->input_length && (*s == 'e' || *s == 'E'))
	{
		s++;
		len++;
		if (len < lex->input_length && (*s == '+' || *s == '-'))
		{
			s++;
			len++;
		}
		if (len == lex->input_length || *s < '0' || *s > '9')
			error = true;
		else
		{
			do
			{
				s++;
				len++;
			} while (len < lex->input_length && *s >= '0' && *s <= '9');
		}
	}

	/*
	 * Check for trailing garbage.  As in json_lex(), any alphanumeric stuff
	 * here should be considered part of the token for error-reporting
	 * purposes.
	 */
	for (p = s; len < lex->input_length && JSON_ALPHANUMERIC_CHAR(*p); p++, len++)
		error = true;

	if (num_err != NULL)
	{
		/* let the caller handle the error */
		*num_err = error;
	}
	else
	{
		lex->prev_token_terminator = lex->token_terminator;
		lex->token_terminator = p;
		if (error)
			report_invalid_token(lex);
	}
}

/*
 * Report a parse error.
 *
 * lex->token_start and lex->token_terminator must identify the current token.
 */
static void
report_parse_error(JsonParseContext ctx, JsonLexContext *lex)
{
	char	   *token;
	int			toklen;

	/* Handle case where the input ended prematurely. */
	if (lex->token_start == NULL || lex->token_type == JSON_TOKEN_END)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type json"),
				 errdetail("The input string ended unexpectedly."),
				 report_json_context(lex)));

	/* Separate out the current token. */
	toklen = lex->token_terminator - lex->token_start;
	token = palloc(toklen + 1);
	memcpy(token, lex->token_start, toklen);
	token[toklen] = '\0';

	/* Complain, with the appropriate detail message. */
	if (ctx == JSON_PARSE_END)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type json"),
				 errdetail("Expected end of input, but found \"%s\".",
						   token),
				 report_json_context(lex)));
	else
	{
		switch (ctx)
		{
			case JSON_PARSE_VALUE:
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("invalid input syntax for type json"),
						 errdetail("Expected JSON value, but found \"%s\".",
								   token),
						 report_json_context(lex)));
				break;
			case JSON_PARSE_STRING:
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("invalid input syntax for type json"),
						 errdetail("Expected string, but found \"%s\".",
								   token),
						 report_json_context(lex)));
				break;
			case JSON_PARSE_ARRAY_START:
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("invalid input syntax for type json"),
						 errdetail("Expected array element or \"]\", but found \"%s\".",
								   token),
						 report_json_context(lex)));
				break;
			case JSON_PARSE_ARRAY_NEXT:
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("invalid input syntax for type json"),
					  errdetail("Expected \",\" or \"]\", but found \"%s\".",
								token),
						 report_json_context(lex)));
				break;
			case JSON_PARSE_OBJECT_START:
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("invalid input syntax for type json"),
					 errdetail("Expected string or \"}\", but found \"%s\".",
							   token),
						 report_json_context(lex)));
				break;
			case JSON_PARSE_OBJECT_LABEL:
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("invalid input syntax for type json"),
						 errdetail("Expected \":\", but found \"%s\".",
								   token),
						 report_json_context(lex)));
				break;
			case JSON_PARSE_OBJECT_NEXT:
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("invalid input syntax for type json"),
					  errdetail("Expected \",\" or \"}\", but found \"%s\".",
								token),
						 report_json_context(lex)));
				break;
			case JSON_PARSE_OBJECT_COMMA:
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("invalid input syntax for type json"),
						 errdetail("Expected string, but found \"%s\".",
								   token),
						 report_json_context(lex)));
				break;
			default:
				elog(ERROR, "unexpected json parse state: %d", ctx);
		}
	}
}

/*
 * Report an invalid input token.
 *
 * lex->token_start and lex->token_terminator must identify the token.
 */
static void
report_invalid_token(JsonLexContext *lex)
{
	char	   *token;
	int			toklen;

	/* Separate out the offending token. */
	toklen = lex->token_terminator - lex->token_start;
	token = palloc(toklen + 1);
	memcpy(token, lex->token_start, toklen);
	token[toklen] = '\0';

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for type json"),
			 errdetail("Token \"%s\" is invalid.", token),
			 report_json_context(lex)));
}

/*
 * Report a CONTEXT line for bogus JSON input.
 *
 * lex->token_terminator must be set to identify the spot where we detected
 * the error.  Note that lex->token_start might be NULL, in case we recognized
 * error at EOF.
 *
 * The return value isn't meaningful, but we make it non-void so that this
 * can be invoked inside ereport().
 */
static int
report_json_context(JsonLexContext *lex)
{
	const char *context_start;
	const char *context_end;
	const char *line_start;
	int			line_number;
	char	   *ctxt;
	int			ctxtlen;
	const char *prefix;
	const char *suffix;

	/* Choose boundaries for the part of the input we will display */
	context_start = lex->input;
	context_end = lex->token_terminator;
	line_start = context_start;
	line_number = 1;
	for (;;)
	{
		/* Always advance over newlines */
		if (context_start < context_end && *context_start == '\n')
		{
			context_start++;
			line_start = context_start;
			line_number++;
			continue;
		}
		/* Otherwise, done as soon as we are close enough to context_end */
		if (context_end - context_start < 50)
			break;
		/* Advance to next multibyte character */
		if (IS_HIGHBIT_SET(*context_start))
			context_start += pg_mblen(context_start);
		else
			context_start++;
	}

	/*
	 * We add "..." to indicate that the excerpt doesn't start at the
	 * beginning of the line ... but if we're within 3 characters of the
	 * beginning of the line, we might as well just show the whole line.
	 */
	if (context_start - line_start <= 3)
		context_start = line_start;

	/* Get a null-terminated copy of the data to present */
	ctxtlen = context_end - context_start;
	ctxt = palloc(ctxtlen + 1);
	memcpy(ctxt, context_start, ctxtlen);
	ctxt[ctxtlen] = '\0';

	/*
	 * Show the context, prefixing "..." if not starting at start of line, and
	 * suffixing "..." if not ending at end of line.
	 */
	prefix = (context_start > line_start) ? "..." : "";
	suffix = (lex->token_type != JSON_TOKEN_END && context_end - lex->input < lex->input_length && *context_end != '\n' && *context_end != '\r') ? "..." : "";

	return errcontext("JSON data, line %d: %s%s%s",
					  line_number, prefix, ctxt, suffix);
}

/*
 * Extract a single, possibly multi-byte char from the input string.
 */
static char *
extract_mb_char(char *s)
{
	char	   *res;
	int			len;

	len = pg_mblen(s);
	res = palloc(len + 1);
	memcpy(res, s, len);
	res[len] = '\0';

	return res;
}

/*
 * Determine how we want to print values of a given type in datum_to_json.
 *
 * Given the datatype OID, return its JsonTypeCategory, as well as the type's
 * output function OID.  If the returned category is JSONTYPE_CAST, we
 * return the OID of the type->JSON cast function instead.
 */
static void
json_categorize_type(Oid typoid,
					 JsonTypeCategory *tcategory,
					 Oid *outfuncoid)
{
	bool		typisvarlena;

	/* Look through any domain */
	typoid = getBaseType(typoid);

	/* We'll usually need to return the type output function */
	getTypeOutputInfo(typoid, outfuncoid, &typisvarlena);

	/* Check for known types */
	switch (typoid)
	{
		case BOOLOID:
			*tcategory = JSONTYPE_BOOL;
			break;

		case INT2OID:
		case INT4OID:
		case INT8OID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			*tcategory = JSONTYPE_NUMERIC;
			break;

		case TIMESTAMPOID:
			*tcategory = JSONTYPE_TIMESTAMP;
			break;

		case TIMESTAMPTZOID:
			*tcategory = JSONTYPE_TIMESTAMPTZ;
			break;

		case JSONOID:
		case JSONBOID:
			*tcategory = JSONTYPE_JSON;
			break;

		default:
			/* Check for arrays and composites */
			if (OidIsValid(get_element_type(typoid)))
				*tcategory = JSONTYPE_ARRAY;
			else if (type_is_rowtype(typoid))
				*tcategory = JSONTYPE_COMPOSITE;
			else
			{
				/* It's probably the general case ... */
				*tcategory = JSONTYPE_OTHER;
				/* but let's look for a cast to json, if it's not built-in */
				if (typoid >= FirstNormalObjectId)
				{
					HeapTuple	tuple;

					tuple = SearchSysCache2(CASTSOURCETARGET,
											ObjectIdGetDatum(typoid),
											ObjectIdGetDatum(JSONOID));
					if (HeapTupleIsValid(tuple))
					{
						Form_pg_cast castForm = (Form_pg_cast) GETSTRUCT(tuple);

						if (castForm->castmethod == COERCION_METHOD_FUNCTION)
						{
							*tcategory = JSONTYPE_CAST;
							*outfuncoid = castForm->castfunc;
						}

						ReleaseSysCache(tuple);
					}
				}
			}
			break;
	}
}

/*
 * Turn a Datum into JSON text, appending the string to "result".
 *
 * tcategory and outfuncoid are from a previous call to json_categorize_type,
 * except that if is_null is true then they can be invalid.
 *
 * If key_scalar is true, the value is being printed as a key, so insist
 * it's of an acceptable type, and force it to be quoted.
 */
static void
datum_to_json(Datum val, bool is_null, StringInfo result,
			  JsonTypeCategory tcategory, Oid outfuncoid,
			  bool key_scalar)
{
	char	   *outputstr;
	text	   *jsontext;
	bool		numeric_error;
	JsonLexContext dummy_lex;

	if (is_null)
	{
		appendStringInfoString(result, "null");
		return;
	}

	if (key_scalar &&
		(tcategory == JSONTYPE_ARRAY ||
		 tcategory == JSONTYPE_COMPOSITE ||
		 tcategory == JSONTYPE_JSON ||
		 tcategory == JSONTYPE_CAST))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		  errmsg("key value must be scalar, not array, composite, or json")));

	switch (tcategory)
	{
		case JSONTYPE_ARRAY:
			array_to_json_internal(val, result, false);
			break;
		case JSONTYPE_COMPOSITE:
			composite_to_json(val, result, false);
			break;
		case JSONTYPE_BOOL:
			outputstr = DatumGetBool(val) ? "true" : "false";
			if (key_scalar)
				escape_json(result, outputstr);
			else
				appendStringInfoString(result, outputstr);
			break;
		case JSONTYPE_NUMERIC:
			outputstr = OidOutputFunctionCall(outfuncoid, val);
			if (key_scalar)
			{
				/* always quote keys */
				escape_json(result, outputstr);
			}
			else
			{
				/*
				 * Don't call escape_json for a non-key if it's a valid JSON
				 * number.
				 */
				dummy_lex.input = *outputstr == '-' ? outputstr + 1 : outputstr;
				dummy_lex.input_length = strlen(dummy_lex.input);
				json_lex_number(&dummy_lex, dummy_lex.input, &numeric_error);
				if (!numeric_error)
					appendStringInfoString(result, outputstr);
				else
					escape_json(result, outputstr);
			}
			pfree(outputstr);
			break;
		case JSONTYPE_TIMESTAMP:
			{
				Timestamp	timestamp;
				struct pg_tm tm;
				fsec_t		fsec;
				char		buf[MAXDATELEN + 1];

				timestamp = DatumGetTimestamp(val);

				/* XSD doesn't support infinite values */
				if (TIMESTAMP_NOT_FINITE(timestamp))
					ereport(ERROR,
							(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
							 errmsg("timestamp out of range"),
							 errdetail("JSON does not support infinite timestamp values.")));
				else if (timestamp2tm(timestamp, NULL, &tm, &fsec, NULL, NULL) == 0)
					EncodeDateTime(&tm, fsec, false, 0, NULL, USE_XSD_DATES, buf);
				else
					ereport(ERROR,
							(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
							 errmsg("timestamp out of range")));

				appendStringInfo(result,"\"%s\"",buf);
			}
			break;
		case JSONTYPE_TIMESTAMPTZ:
			{
				TimestampTz timestamp;
				struct pg_tm tm;
				int			tz;
				fsec_t		fsec;
				const char *tzn = NULL;
				char		buf[MAXDATELEN + 1];

				timestamp = DatumGetTimestamp(val);

				/* XSD doesn't support infinite values */
				if (TIMESTAMP_NOT_FINITE(timestamp))
					ereport(ERROR,
							(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
							 errmsg("timestamp out of range"),
							 errdetail("JSON does not support infinite timestamp values.")));
				else if (timestamp2tm(timestamp, &tz, &tm, &fsec, &tzn, NULL) == 0)
					EncodeDateTime(&tm, fsec, true, tz, tzn, USE_XSD_DATES, buf);
				else
					ereport(ERROR,
							(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
							 errmsg("timestamp out of range")));

				appendStringInfo(result,"\"%s\"",buf);
			}
			break;
		case JSONTYPE_JSON:
			/* JSON and JSONB output will already be escaped */
			outputstr = OidOutputFunctionCall(outfuncoid, val);
			appendStringInfoString(result, outputstr);
			pfree(outputstr);
			break;
		case JSONTYPE_CAST:
			/* outfuncoid refers to a cast function, not an output function */
			jsontext = DatumGetTextP(OidFunctionCall1(outfuncoid, val));
			outputstr = text_to_cstring(jsontext);
			appendStringInfoString(result, outputstr);
			pfree(outputstr);
			pfree(jsontext);
			break;
		default:
			outputstr = OidOutputFunctionCall(outfuncoid, val);
			if (key_scalar && *outputstr == '\0')
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("key value must not be empty")));
			escape_json(result, outputstr);
			pfree(outputstr);
			break;
	}
}

/*
 * Process a single dimension of an array.
 * If it's the innermost dimension, output the values, otherwise call
 * ourselves recursively to process the next dimension.
 */
static void
array_dim_to_json(StringInfo result, int dim, int ndims, int *dims, Datum *vals,
				  bool *nulls, int *valcount, JsonTypeCategory tcategory,
				  Oid outfuncoid, bool use_line_feeds)
{
	int			i;
	const char *sep;

	Assert(dim < ndims);

	sep = use_line_feeds ? ",\n " : ",";

	appendStringInfoChar(result, '[');

	for (i = 1; i <= dims[dim]; i++)
	{
		if (i > 1)
			appendStringInfoString(result, sep);

		if (dim + 1 == ndims)
		{
			datum_to_json(vals[*valcount], nulls[*valcount], result, tcategory,
						  outfuncoid, false);
			(*valcount)++;
		}
		else
		{
			/*
			 * Do we want line feeds on inner dimensions of arrays? For now
			 * we'll say no.
			 */
			array_dim_to_json(result, dim + 1, ndims, dims, vals, nulls,
							  valcount, tcategory, outfuncoid, false);
		}
	}

	appendStringInfoChar(result, ']');
}

/*
 * Turn an array into JSON.
 */
static void
array_to_json_internal(Datum array, StringInfo result, bool use_line_feeds)
{
	ArrayType  *v = DatumGetArrayTypeP(array);
	Oid			element_type = ARR_ELEMTYPE(v);
	int		   *dim;
	int			ndim;
	int			nitems;
	int			count = 0;
	Datum	   *elements;
	bool	   *nulls;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	JsonTypeCategory tcategory;
	Oid			outfuncoid;

	ndim = ARR_NDIM(v);
	dim = ARR_DIMS(v);
	nitems = ArrayGetNItems(ndim, dim);

	if (nitems <= 0)
	{
		appendStringInfoString(result, "[]");
		return;
	}

	get_typlenbyvalalign(element_type,
						 &typlen, &typbyval, &typalign);

	json_categorize_type(element_type,
						 &tcategory, &outfuncoid);

	deconstruct_array(v, element_type, typlen, typbyval,
					  typalign, &elements, &nulls,
					  &nitems);

	array_dim_to_json(result, 0, ndim, dim, elements, nulls, &count, tcategory,
					  outfuncoid, use_line_feeds);

	pfree(elements);
	pfree(nulls);
}

/*
 * Turn a composite / record into JSON.
 */
static void
composite_to_json(Datum composite, StringInfo result, bool use_line_feeds)
{
	HeapTupleHeader td;
	Oid			tupType;
	int32		tupTypmod;
	TupleDesc	tupdesc;
	HeapTupleData tmptup,
			   *tuple;
	int			i;
	bool		needsep = false;
	const char *sep;

	sep = use_line_feeds ? ",\n " : ",";

	td = DatumGetHeapTupleHeader(composite);

	/* Extract rowtype info and find a tupdesc */
	tupType = HeapTupleHeaderGetTypeId(td);
	tupTypmod = HeapTupleHeaderGetTypMod(td);
	tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

	/* Build a temporary HeapTuple control structure */
	tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
	tmptup.t_data = td;
	tuple = &tmptup;

	appendStringInfoChar(result, '{');

	for (i = 0; i < tupdesc->natts; i++)
	{
		Datum		val;
		bool		isnull;
		char	   *attname;
		JsonTypeCategory tcategory;
		Oid			outfuncoid;

		if (tupdesc->attrs[i]->attisdropped)
			continue;

		if (needsep)
			appendStringInfoString(result, sep);
		needsep = true;

		attname = NameStr(tupdesc->attrs[i]->attname);
		escape_json(result, attname);
		appendStringInfoChar(result, ':');

		val = heap_getattr(tuple, i + 1, tupdesc, &isnull);

		if (isnull)
		{
			tcategory = JSONTYPE_NULL;
			outfuncoid = InvalidOid;
		}
		else
			json_categorize_type(tupdesc->attrs[i]->atttypid,
								 &tcategory, &outfuncoid);

		datum_to_json(val, isnull, result, tcategory, outfuncoid, false);
	}

	appendStringInfoChar(result, '}');
	ReleaseTupleDesc(tupdesc);
}

/*
 * Append JSON text for "val" to "result".
 *
 * This is just a thin wrapper around datum_to_json.  If the same type will be
 * printed many times, avoid using this; better to do the json_categorize_type
 * lookups only once.
 */
static void
add_json(Datum val, bool is_null, StringInfo result,
		 Oid val_type, bool key_scalar)
{
	JsonTypeCategory tcategory;
	Oid			outfuncoid;

	if (val_type == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not determine input data type")));

	if (is_null)
	{
		tcategory = JSONTYPE_NULL;
		outfuncoid = InvalidOid;
	}
	else
		json_categorize_type(val_type,
							 &tcategory, &outfuncoid);

	datum_to_json(val, is_null, result, tcategory, outfuncoid, key_scalar);
}

/*
 * SQL function array_to_json(row)
 */
extern Datum
array_to_json(PG_FUNCTION_ARGS)
{
	Datum		array = PG_GETARG_DATUM(0);
	StringInfo	result;

	result = makeStringInfo();

	array_to_json_internal(array, result, false);

	PG_RETURN_TEXT_P(cstring_to_text_with_len(result->data, result->len));
}

/*
 * SQL function array_to_json(row, prettybool)
 */
extern Datum
array_to_json_pretty(PG_FUNCTION_ARGS)
{
	Datum		array = PG_GETARG_DATUM(0);
	bool		use_line_feeds = PG_GETARG_BOOL(1);
	StringInfo	result;

	result = makeStringInfo();

	array_to_json_internal(array, result, use_line_feeds);

	PG_RETURN_TEXT_P(cstring_to_text_with_len(result->data, result->len));
}

/*
 * SQL function row_to_json(row)
 */
extern Datum
row_to_json(PG_FUNCTION_ARGS)
{
	Datum		array = PG_GETARG_DATUM(0);
	StringInfo	result;

	result = makeStringInfo();

	composite_to_json(array, result, false);

	PG_RETURN_TEXT_P(cstring_to_text_with_len(result->data, result->len));
}

/*
 * SQL function row_to_json(row, prettybool)
 */
extern Datum
row_to_json_pretty(PG_FUNCTION_ARGS)
{
	Datum		array = PG_GETARG_DATUM(0);
	bool		use_line_feeds = PG_GETARG_BOOL(1);
	StringInfo	result;

	result = makeStringInfo();

	composite_to_json(array, result, use_line_feeds);

	PG_RETURN_TEXT_P(cstring_to_text_with_len(result->data, result->len));
}

/*
 * SQL function to_json(anyvalue)
 */
Datum
to_json(PG_FUNCTION_ARGS)
{
	Datum		val = PG_GETARG_DATUM(0);
	Oid			val_type = get_fn_expr_argtype(fcinfo->flinfo, 0);
	StringInfo	result;
	JsonTypeCategory tcategory;
	Oid			outfuncoid;

	if (val_type == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not determine input data type")));

	json_categorize_type(val_type,
						 &tcategory, &outfuncoid);

	result = makeStringInfo();

	datum_to_json(val, false, result, tcategory, outfuncoid, false);

	PG_RETURN_TEXT_P(cstring_to_text_with_len(result->data, result->len));
}

/*
 * json_agg transition function
 */
Datum
json_agg_transfn(PG_FUNCTION_ARGS)
{
	Oid			val_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	MemoryContext aggcontext,
				oldcontext;
	StringInfo	state;
	Datum		val;
	JsonTypeCategory tcategory;
	Oid			outfuncoid;

	if (val_type == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not determine input data type")));

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "json_agg_transfn called in non-aggregate context");
	}

	if (PG_ARGISNULL(0))
	{
		/*
		 * Make this StringInfo in a context where it will persist for the
		 * duration of the aggregate call.  MemoryContextSwitchTo is only
		 * needed the first time, as the StringInfo routines make sure they
		 * use the right context to enlarge the object if necessary.
		 */
		oldcontext = MemoryContextSwitchTo(aggcontext);
		state = makeStringInfo();
		MemoryContextSwitchTo(oldcontext);

		appendStringInfoChar(state, '[');
	}
	else
	{
		state = (StringInfo) PG_GETARG_POINTER(0);
		appendStringInfoString(state, ", ");
	}

	/* fast path for NULLs */
	if (PG_ARGISNULL(1))
	{
		datum_to_json((Datum) 0, true, state, JSONTYPE_NULL, InvalidOid, false);
		PG_RETURN_POINTER(state);
	}

	val = PG_GETARG_DATUM(1);

	/* XXX we do this every time?? */
	json_categorize_type(val_type,
						 &tcategory, &outfuncoid);

	/* add some whitespace if structured type and not first item */
	if (!PG_ARGISNULL(0) &&
		(tcategory == JSONTYPE_ARRAY || tcategory == JSONTYPE_COMPOSITE))
	{
		appendStringInfoString(state, "\n ");
	}

	datum_to_json(val, false, state, tcategory, outfuncoid, false);

	/*
	 * The transition type for array_agg() is declared to be "internal", which
	 * is a pass-by-value type the same size as a pointer.  So we can safely
	 * pass the ArrayBuildState pointer through nodeAgg.c's machinations.
	 */
	PG_RETURN_POINTER(state);
}

/*
 * json_agg final function
 */
Datum
json_agg_finalfn(PG_FUNCTION_ARGS)
{
	StringInfo	state;

	/* cannot be called directly because of internal-type argument */
	Assert(AggCheckCallContext(fcinfo, NULL));

	state = PG_ARGISNULL(0) ? NULL : (StringInfo) PG_GETARG_POINTER(0);

	if (state == NULL)
		PG_RETURN_NULL();

	appendStringInfoChar(state, ']');

	PG_RETURN_TEXT_P(cstring_to_text_with_len(state->data, state->len));
}

/*
 * json_object_agg transition function.
 *
 * aggregate two input columns as a single json value.
 */
Datum
json_object_agg_transfn(PG_FUNCTION_ARGS)
{
	Oid			val_type;
	MemoryContext aggcontext,
				oldcontext;
	StringInfo	state;
	Datum		arg;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "json_agg_transfn called in non-aggregate context");
	}

	if (PG_ARGISNULL(0))
	{
		/*
		 * Make this StringInfo in a context where it will persist for the
		 * duration off the aggregate call. It's only needed for this initial
		 * piece, as the StringInfo routines make sure they use the right
		 * context to enlarge the object if necessary.
		 */
		oldcontext = MemoryContextSwitchTo(aggcontext);
		state = makeStringInfo();
		MemoryContextSwitchTo(oldcontext);

		appendStringInfoString(state, "{ ");
	}
	else
	{
		state = (StringInfo) PG_GETARG_POINTER(0);
		appendStringInfoString(state, ", ");
	}

	if (PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("field name must not be null")));


	val_type = get_fn_expr_argtype(fcinfo->flinfo, 1);

	/*
	 * turn a constant (more or less literal) value that's of unknown type
	 * into text. Unknowns come in as a cstring pointer.
	 */
	if (val_type == UNKNOWNOID && get_fn_expr_arg_stable(fcinfo->flinfo, 1))
	{
		val_type = TEXTOID;
		arg = CStringGetTextDatum(PG_GETARG_POINTER(1));
	}
	else
	{
		arg = PG_GETARG_DATUM(1);
	}

	if (val_type == InvalidOid || val_type == UNKNOWNOID)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("arg 1: could not determine data type")));

	add_json(arg, false, state, val_type, true);

	appendStringInfoString(state, " : ");

	val_type = get_fn_expr_argtype(fcinfo->flinfo, 2);
	/* see comments above */
	if (val_type == UNKNOWNOID && get_fn_expr_arg_stable(fcinfo->flinfo, 2))
	{
		val_type = TEXTOID;
		if (PG_ARGISNULL(2))
			arg = (Datum) 0;
		else
			arg = CStringGetTextDatum(PG_GETARG_POINTER(2));
	}
	else
	{
		arg = PG_GETARG_DATUM(2);
	}

	if (val_type == InvalidOid || val_type == UNKNOWNOID)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("arg 2: could not determine data type")));

	add_json(arg, PG_ARGISNULL(2), state, val_type, false);

	PG_RETURN_POINTER(state);
}

/*
 * json_object_agg final function.
 *
 */
Datum
json_object_agg_finalfn(PG_FUNCTION_ARGS)
{
	StringInfo	state;

	/* cannot be called directly because of internal-type argument */
	Assert(AggCheckCallContext(fcinfo, NULL));

	state = PG_ARGISNULL(0) ? NULL : (StringInfo) PG_GETARG_POINTER(0);

	if (state == NULL)
		PG_RETURN_TEXT_P(cstring_to_text("{}"));

	appendStringInfoString(state, " }");

	PG_RETURN_TEXT_P(cstring_to_text_with_len(state->data, state->len));
}

/*
 * SQL function json_build_object(variadic "any")
 */
Datum
json_build_object(PG_FUNCTION_ARGS)
{
	int			nargs = PG_NARGS();
	int			i;
	Datum		arg;
	char	   *sep = "";
	StringInfo	result;
	Oid			val_type;


	if (nargs % 2 != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid number or arguments: object must be matched key value pairs")));

	result = makeStringInfo();

	appendStringInfoChar(result, '{');

	for (i = 0; i < nargs; i += 2)
	{

		/* process key */

		if (PG_ARGISNULL(i))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("arg %d: key cannot be null", i + 1)));
		val_type = get_fn_expr_argtype(fcinfo->flinfo, i);

		/*
		 * turn a constant (more or less literal) value that's of unknown type
		 * into text. Unknowns come in as a cstring pointer.
		 */
		if (val_type == UNKNOWNOID && get_fn_expr_arg_stable(fcinfo->flinfo, i))
		{
			val_type = TEXTOID;
			if (PG_ARGISNULL(i))
				arg = (Datum) 0;
			else
				arg = CStringGetTextDatum(PG_GETARG_POINTER(i));
		}
		else
		{
			arg = PG_GETARG_DATUM(i);
		}
		if (val_type == InvalidOid || val_type == UNKNOWNOID)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("arg %d: could not determine data type", i + 1)));
		appendStringInfoString(result, sep);
		sep = ", ";
		add_json(arg, false, result, val_type, true);

		appendStringInfoString(result, " : ");

		/* process value */

		val_type = get_fn_expr_argtype(fcinfo->flinfo, i + 1);
		/* see comments above */
		if (val_type == UNKNOWNOID && get_fn_expr_arg_stable(fcinfo->flinfo, i + 1))
		{
			val_type = TEXTOID;
			if (PG_ARGISNULL(i + 1))
				arg = (Datum) 0;
			else
				arg = CStringGetTextDatum(PG_GETARG_POINTER(i + 1));
		}
		else
		{
			arg = PG_GETARG_DATUM(i + 1);
		}
		if (val_type == InvalidOid || val_type == UNKNOWNOID)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("arg %d: could not determine data type", i + 2)));
		add_json(arg, PG_ARGISNULL(i + 1), result, val_type, false);

	}
	appendStringInfoChar(result, '}');

	PG_RETURN_TEXT_P(cstring_to_text_with_len(result->data, result->len));

}

/*
 * degenerate case of json_build_object where it gets 0 arguments.
 */
Datum
json_build_object_noargs(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text_with_len("{}", 2));
}

/*
 * SQL function json_build_array(variadic "any")
 */
Datum
json_build_array(PG_FUNCTION_ARGS)
{
	int			nargs = PG_NARGS();
	int			i;
	Datum		arg;
	char	   *sep = "";
	StringInfo	result;
	Oid			val_type;


	result = makeStringInfo();

	appendStringInfoChar(result, '[');

	for (i = 0; i < nargs; i++)
	{
		val_type = get_fn_expr_argtype(fcinfo->flinfo, i);
		arg = PG_GETARG_DATUM(i + 1);
		/* see comments in json_build_object above */
		if (val_type == UNKNOWNOID && get_fn_expr_arg_stable(fcinfo->flinfo, i))
		{
			val_type = TEXTOID;
			if (PG_ARGISNULL(i))
				arg = (Datum) 0;
			else
				arg = CStringGetTextDatum(PG_GETARG_POINTER(i));
		}
		else
		{
			arg = PG_GETARG_DATUM(i);
		}
		if (val_type == InvalidOid || val_type == UNKNOWNOID)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("arg %d: could not determine data type", i + 1)));
		appendStringInfoString(result, sep);
		sep = ", ";
		add_json(arg, PG_ARGISNULL(i), result, val_type, false);
	}
	appendStringInfoChar(result, ']');

	PG_RETURN_TEXT_P(cstring_to_text_with_len(result->data, result->len));

}

/*
 * degenerate case of json_build_array where it gets 0 arguments.
 */
Datum
json_build_array_noargs(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text_with_len("[]", 2));
}

/*
 * SQL function json_object(text[])
 *
 * take a one or two dimensional array of text as name vale pairs
 * for a json object.
 *
 */
Datum
json_object(PG_FUNCTION_ARGS)
{
	ArrayType  *in_array = PG_GETARG_ARRAYTYPE_P(0);
	int			ndims = ARR_NDIM(in_array);
	StringInfoData result;
	Datum	   *in_datums;
	bool	   *in_nulls;
	int			in_count,
				count,
				i;
	text	   *rval;
	char	   *v;

	switch (ndims)
	{
		case 0:
			PG_RETURN_DATUM(CStringGetTextDatum("{}"));
			break;

		case 1:
			if ((ARR_DIMS(in_array)[0]) % 2)
				ereport(ERROR,
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
						 errmsg("array must have even number of elements")));
			break;

		case 2:
			if ((ARR_DIMS(in_array)[1]) != 2)
				ereport(ERROR,
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
						 errmsg("array must have two columns")));
			break;

		default:
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("wrong number of array subscripts")));
	}

	deconstruct_array(in_array,
					  TEXTOID, -1, false, 'i',
					  &in_datums, &in_nulls, &in_count);

	count = in_count / 2;

	initStringInfo(&result);

	appendStringInfoChar(&result, '{');

	for (i = 0; i < count; ++i)
	{
		if (in_nulls[i * 2])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("null value not allowed for object key")));

		v = TextDatumGetCString(in_datums[i * 2]);
		if (v[0] == '\0')
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("empty value not allowed for object key")));
		if (i > 0)
			appendStringInfoString(&result, ", ");
		escape_json(&result, v);
		appendStringInfoString(&result, " : ");
		pfree(v);
		if (in_nulls[i * 2 + 1])
			appendStringInfoString(&result, "null");
		else
		{
			v = TextDatumGetCString(in_datums[i * 2 + 1]);
			escape_json(&result, v);
			pfree(v);
		}
	}

	appendStringInfoChar(&result, '}');

	pfree(in_datums);
	pfree(in_nulls);

	rval = cstring_to_text_with_len(result.data, result.len);
	pfree(result.data);

	PG_RETURN_TEXT_P(rval);

}

/*
 * SQL function json_object(text[], text[])
 *
 * take separate name and value arrays of text to construct a json object
 * pairwise.
 */
Datum
json_object_two_arg(PG_FUNCTION_ARGS)
{
	ArrayType  *key_array = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *val_array = PG_GETARG_ARRAYTYPE_P(1);
	int			nkdims = ARR_NDIM(key_array);
	int			nvdims = ARR_NDIM(val_array);
	StringInfoData result;
	Datum	   *key_datums,
			   *val_datums;
	bool	   *key_nulls,
			   *val_nulls;
	int			key_count,
				val_count,
				i;
	text	   *rval;
	char	   *v;

	if (nkdims > 1 || nkdims != nvdims)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("wrong number of array subscripts")));

	if (nkdims == 0)
		PG_RETURN_DATUM(CStringGetTextDatum("{}"));

	deconstruct_array(key_array,
					  TEXTOID, -1, false, 'i',
					  &key_datums, &key_nulls, &key_count);

	deconstruct_array(val_array,
					  TEXTOID, -1, false, 'i',
					  &val_datums, &val_nulls, &val_count);

	if (key_count != val_count)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("mismatched array dimensions")));

	initStringInfo(&result);

	appendStringInfoChar(&result, '{');

	for (i = 0; i < key_count; ++i)
	{
		if (key_nulls[i])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("null value not allowed for object key")));

		v = TextDatumGetCString(key_datums[i]);
		if (v[0] == '\0')
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("empty value not allowed for object key")));
		if (i > 0)
			appendStringInfoString(&result, ", ");
		escape_json(&result, v);
		appendStringInfoString(&result, " : ");
		pfree(v);
		if (val_nulls[i])
			appendStringInfoString(&result, "null");
		else
		{
			v = TextDatumGetCString(val_datums[i]);
			escape_json(&result, v);
			pfree(v);
		}
	}

	appendStringInfoChar(&result, '}');

	pfree(key_datums);
	pfree(key_nulls);
	pfree(val_datums);
	pfree(val_nulls);

	rval = cstring_to_text_with_len(result.data, result.len);
	pfree(result.data);

	PG_RETURN_TEXT_P(rval);

}


/*
 * Produce a JSON string literal, properly escaping characters in the text.
 */
void
escape_json(StringInfo buf, const char *str)
{
	const char *p;

	appendStringInfoCharMacro(buf, '\"');
	for (p = str; *p; p++)
	{
		switch (*p)
		{
			case '\b':
				appendStringInfoString(buf, "\\b");
				break;
			case '\f':
				appendStringInfoString(buf, "\\f");
				break;
			case '\n':
				appendStringInfoString(buf, "\\n");
				break;
			case '\r':
				appendStringInfoString(buf, "\\r");
				break;
			case '\t':
				appendStringInfoString(buf, "\\t");
				break;
			case '"':
				appendStringInfoString(buf, "\\\"");
				break;
			case '\\':
				/*
				 * Unicode escapes are passed through as is. There is no
				 * requirement that they denote a valid character in the
				 * server encoding - indeed that is a big part of their
				 * usefulness.
				 *
				 * All we require is that they consist of \uXXXX where
				 * the Xs are hexadecimal digits. It is the responsibility
				 * of the caller of, say, to_json() to make sure that the
				 * unicode escape is valid.
				 *
				 * In the case of a jsonb string value being escaped, the
				 * only unicode escape that should be present is \u0000,
				 * all the other unicode escapes will have been resolved.
				 */
				if (p[1] == 'u' &&
					isxdigit((unsigned char) p[2]) &&
					isxdigit((unsigned char) p[3]) &&
					isxdigit((unsigned char) p[4]) &&
					isxdigit((unsigned char) p[5]))
					appendStringInfoCharMacro(buf, *p);
				else
					appendStringInfoString(buf, "\\\\");
				break;
			default:
				if ((unsigned char) *p < ' ')
					appendStringInfo(buf, "\\u%04x", (int) *p);
				else
					appendStringInfoCharMacro(buf, *p);
				break;
		}
	}
	appendStringInfoCharMacro(buf, '\"');
}

/*
 * SQL function json_typeof(json) -> text
 *
 * Returns the type of the outermost JSON value as TEXT.  Possible types are
 * "object", "array", "string", "number", "boolean", and "null".
 *
 * Performs a single call to json_lex() to get the first token of the supplied
 * value.  This initial token uniquely determines the value's type.  As our
 * input must already have been validated by json_in() or json_recv(), the
 * initial token should never be JSON_TOKEN_OBJECT_END, JSON_TOKEN_ARRAY_END,
 * JSON_TOKEN_COLON, JSON_TOKEN_COMMA, or JSON_TOKEN_END.
 */
Datum
json_typeof(PG_FUNCTION_ARGS)
{
	text	   *json;

	JsonLexContext *lex;
	JsonTokenType tok;
	char	   *type;

	json = PG_GETARG_TEXT_P(0);
	lex = makeJsonLexContext(json, false);

	/* Lex exactly one token from the input and check its type. */
	json_lex(lex);
	tok = lex_peek(lex);
	switch (tok)
	{
		case JSON_TOKEN_OBJECT_START:
			type = "object";
			break;
		case JSON_TOKEN_ARRAY_START:
			type = "array";
			break;
		case JSON_TOKEN_STRING:
			type = "string";
			break;
		case JSON_TOKEN_NUMBER:
			type = "number";
			break;
		case JSON_TOKEN_TRUE:
		case JSON_TOKEN_FALSE:
			type = "boolean";
			break;
		case JSON_TOKEN_NULL:
			type = "null";
			break;
		default:
			elog(ERROR, "unexpected json token: %d", tok);
	}

	PG_RETURN_TEXT_P(cstring_to_text(type));
}
