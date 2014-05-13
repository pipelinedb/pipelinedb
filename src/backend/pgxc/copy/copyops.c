/*-------------------------------------------------------------------------
 *
 * copyops.c
 *	  Functions related to remote COPY data manipulation and materialization
 *	  of data redistribution
 *
 * Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/pgxc/copy/copyops.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "pgxc/copyops.h"
#include "utils/lsyscache.h"

/* NULL print marker */
#define COPYOPS_NULL_PRINT	"\\N"

/* Some octal operations */
#define ISOCTAL(c) (((c) >= '0') && ((c) <= '7'))
#define OCTVALUE(c) ((c) - '0')
/* Send text representation of one attribute, with conversion and escaping */
#define DUMPSOFAR() \
	do { \
		if (ptr > start) \
			appendBinaryStringInfo(buf, (char *) start, ptr - start); \
	} while (0)


static int get_decimal_from_hex(char hex);
static void attribute_out_text(StringInfo buf, char *string);

/*
 * Return decimal value for a hexadecimal digit
 */
static int
get_decimal_from_hex(char hex)
{
	if (isdigit((unsigned char) hex))
		return hex - '0';
	else
		return tolower((unsigned char) hex) - 'a' + 10;
}


/*
 * Output an attribute to text
 * This takes portions of the code of CopyAttributeOutText
 */
static void
attribute_out_text(StringInfo buf, char *string)
{
	char	   *ptr;
	char		c;
	char	   *start;
	char		delimc = COPYOPS_DELIMITER;
	bool		need_transcoding, encoding_embeds_ascii;
	int			file_encoding = pg_get_client_encoding();

	need_transcoding = (file_encoding != GetDatabaseEncoding() ||
						pg_database_encoding_max_length() > 1);
	encoding_embeds_ascii = PG_ENCODING_IS_CLIENT_ONLY(file_encoding);

	if (need_transcoding)
		ptr = pg_server_to_any(string, strlen(string), file_encoding);
	else
		ptr = string;

	/*
	 * We have to grovel through the string searching for control characters
	 * and instances of the delimiter character.  In most cases, though, these
	 * are infrequent.	To avoid overhead from calling CopySendData once per
	 * character, we dump out all characters between escaped characters in a
	 * single call.  The loop invariant is that the data from "start" to "ptr"
	 * can be sent literally, but hasn't yet been.
	 *
	 * We can skip pg_encoding_mblen() overhead when encoding is safe, because
	 * in valid backend encodings, extra bytes of a multibyte character never
	 * look like ASCII.  This loop is sufficiently performance-critical that
	 * it's worth making two copies of it to get the IS_HIGHBIT_SET() test out
	 * of the normal safe-encoding path.
	 */
	if (encoding_embeds_ascii)
	{
		start = ptr;
		while ((c = *ptr) != '\0')
		{
			if ((unsigned char) c < (unsigned char) 0x20)
			{
				/*
				 * \r and \n must be escaped, the others are traditional. We
				 * prefer to dump these using the C-like notation, rather than
				 * a backslash and the literal character, because it makes the
				 * dump file a bit more proof against Microsoftish data
				 * mangling.
				 */
				switch (c)
				{
					case '\b':
						c = 'b';
						break;
					case '\f':
						c = 'f';
						break;
					case '\n':
						c = 'n';
						break;
					case '\r':
						c = 'r';
						break;
					case '\t':
						c = 't';
						break;
					case '\v':
						c = 'v';
						break;
					default:
						/* If it's the delimiter, must backslash it */
						if (c == delimc)
							break;
						/* All ASCII control chars are length 1 */
						ptr++;
						continue;		/* fall to end of loop */
				}

				/* if we get here, we need to convert the control char */
				DUMPSOFAR();
				appendStringInfoCharMacro(buf, '\\');
				appendStringInfoCharMacro(buf, c);
				start = ++ptr;
			}
			else if (c == '\\' || c == delimc)
			{
				DUMPSOFAR();
				appendStringInfoCharMacro(buf, '\\');
				start = ++ptr;
			}
			else if (IS_HIGHBIT_SET(c))
				ptr += pg_encoding_mblen(file_encoding, ptr);
			else
				ptr++;
		}
	}
	else
	{
		start = ptr;
		while ((c = *ptr) != '\0')
		{
			if ((unsigned char) c < (unsigned char) 0x20)
			{
				/*
				 * \r and \n must be escaped, the others are traditional. We
				 * prefer to dump these using the C-like notation, rather than
				 * a backslash and the literal character, because it makes the
				 * dump file a bit more proof against Microsoftish data
				 * mangling.
				 */
				switch (c)
				{
					case '\b':
						c = 'b';
						break;
					case '\f':
						c = 'f';
						break;
					case '\n':
						c = 'n';
						break;
					case '\r':
						c = 'r';
						break;
					case '\t':
						c = 't';
						break;
					case '\v':
						c = 'v';
						break;
					default:
						/* If it's the delimiter, must backslash it */
						if (c == delimc)
							break;
						/* All ASCII control chars are length 1 */
						ptr++;
						continue;		/* fall to end of loop */
				}
				/* if we get here, we need to convert the control char */
				DUMPSOFAR();
				appendStringInfoCharMacro(buf, '\\');
				appendStringInfoCharMacro(buf, c);
				start = ++ptr;
			}
			else if (c == '\\' || c == delimc)
			{
				DUMPSOFAR();
				appendStringInfoCharMacro(buf, '\\');
				start = ++ptr;
			}
			else
				ptr++;
		}
	}

	DUMPSOFAR();
}


/*
 * CopyOps_RawDataToArrayField
 * Convert the raw output of COPY TO to an array of fields.
 * This is a simplified version of CopyReadAttributesText used for data
 * redistribution and storage of tuple data into a tuple store.
 */
char **
CopyOps_RawDataToArrayField(TupleDesc tupdesc, char *message, int len)
{
	char		delimc = COPYOPS_DELIMITER;
	int			fieldno;
	int			null_print_len = strlen(COPYOPS_NULL_PRINT);
	char	   *origin_ptr;
	char	   *output_ptr;
	char	   *cur_ptr;
	char	   *line_end_ptr;
	int			fields = tupdesc->natts;
	char	  **raw_fields;
	Form_pg_attribute *attr = tupdesc->attrs;

	/* Adjust number of fields depending on dropped attributes */
	for (fieldno = 0; fieldno < tupdesc->natts; fieldno++)
	{
		if (attr[fieldno]->attisdropped)
			fields--;
	}

	/* Then alloc necessary space */
	raw_fields = (char **) palloc(fields * sizeof(char *));

	/* Take a copy of message to manipulate */
	origin_ptr = (char *) palloc0(sizeof(char) * (len + 1));
	memcpy(origin_ptr, message, len + 1);

	/* Add clean separator '\0' at the end of message */
	origin_ptr[len] = '\0';

	/* Keep track of original pointer */
	output_ptr = origin_ptr;

	/* set pointer variables for loop */
	cur_ptr = message;
	line_end_ptr = message + len;

	/* Outer loop iterates over fields */
	fieldno = 0;
	for (;;)
	{
		char	   *start_ptr;
        char	   *end_ptr;
        int			input_len;
		bool		found_delim = false;
		bool		saw_non_ascii = false;

		/* Make sure there is enough space for the next value */
		if (fieldno >= fields)
		{
			fields *= 2;
			raw_fields = repalloc(raw_fields, fields * sizeof(char *));
		}

		/* Remember start of field on output side */
		start_ptr = cur_ptr;
		raw_fields[fieldno] = output_ptr;

		/* Scan data for field */
		for (;;)
		{
			char		c;

			end_ptr = cur_ptr;
			if (cur_ptr >= line_end_ptr)
				break;
			c = *cur_ptr++;
			if (c == delimc)
			{
				found_delim = true;
				break;
			}
			if (c == '\\')
			{
				if (cur_ptr >= line_end_ptr)
					break;
				c = *cur_ptr++;
				switch (c)
				{
					case '0':
					case '1':
					case '2':
					case '3':
					case '4':
					case '5':
					case '6':
					case '7':
						{
							/* handle \013 */
							int			val;

							val = OCTVALUE(c);
							if (cur_ptr < line_end_ptr)
							{
								c = *cur_ptr;
								if (ISOCTAL(c))
								{
									cur_ptr++;
									val = (val << 3) + OCTVALUE(c);
									if (cur_ptr < line_end_ptr)
									{
										c = *cur_ptr;
										if (ISOCTAL(c))
										{
											cur_ptr++;
											val = (val << 3) + OCTVALUE(c);
										}
									}
								}
							}
							c = val & 0377;
							if (c == '\0' || IS_HIGHBIT_SET(c))
								saw_non_ascii = true;
						}
						break;
					case 'x':
						/* Handle \x3F */
						if (cur_ptr < line_end_ptr)
						{
							char		hexchar = *cur_ptr;

							if (isxdigit((unsigned char) hexchar))
							{
								int			val = get_decimal_from_hex(hexchar);

								cur_ptr++;
								if (cur_ptr < line_end_ptr)
								{
									hexchar = *cur_ptr;
									if (isxdigit((unsigned char) hexchar))
									{
										cur_ptr++;
										val = (val << 4) + get_decimal_from_hex(hexchar);
									}
								}
								c = val & 0xff;
								if (c == '\0' || IS_HIGHBIT_SET(c))
									saw_non_ascii = true;
							}
						}
						break;
					case 'b':
						c = '\b';
						break;
					case 'f':
						c = '\f';
						break;
					case 'n':
						c = '\n';
						break;
					case 'r':
						c = '\r';
						break;
					case 't':
						c = '\t';
						break;
					case 'v':
						c = '\v';
						break;

					/*
					 * in all other cases, take the char after '\'
					 * literally
					 */
				}
			}

			/* Add c to output string */
			*output_ptr++ = c;
		}

		/* Terminate attribute value in output area */
		*output_ptr++ = '\0';

		/*
		 * If we de-escaped a non-7-bit-ASCII char, make sure we still have
		 * valid data for the db encoding. Avoid calling strlen here for the
		 * sake of efficiency.
		 */
		if (saw_non_ascii)
		{
			char	   *fld = raw_fields[fieldno];

			pg_verifymbstr(fld, output_ptr - (fld + 1), false);
		}

		/* Check whether raw input matched null marker */
		input_len = end_ptr - start_ptr;
		if (input_len == null_print_len &&
			strncmp(start_ptr, COPYOPS_NULL_PRINT, input_len) == 0)
			raw_fields[fieldno] = NULL;

		fieldno++;
		/* Done if we hit EOL instead of a delim */
		if (!found_delim)
			break;
	}

	/* Clean up state of attribute_buf */
	output_ptr--;
	Assert(*output_ptr == '\0');

	return raw_fields;
}

/*
 * CopyOps_BuildOneRowTo
 * Build one row message to be sent to remote nodes through COPY protocol
 */
char *
CopyOps_BuildOneRowTo(TupleDesc tupdesc, Datum *values, bool *nulls, int *len)
{
	bool		need_delim = false;
	char	   *res;
	int			i;
	FmgrInfo   *out_functions;
	Form_pg_attribute *attr = tupdesc->attrs;
	StringInfo	buf;

	/* Get info about the columns we need to process. */
	out_functions = (FmgrInfo *) palloc(tupdesc->natts * sizeof(FmgrInfo));
	for (i = 0; i < tupdesc->natts; i++)
	{
		Oid			out_func_oid;
		bool		isvarlena;

		/* Do not need any information for dropped attributes */
		if (attr[i]->attisdropped)
			continue;

		getTypeOutputInfo(attr[i]->atttypid,
						  &out_func_oid,
						  &isvarlena);
		fmgr_info(out_func_oid, &out_functions[i]);
	}

	/* Initialize output buffer */
	buf = makeStringInfo();

	for (i = 0; i < tupdesc->natts; i++)
	{
		Datum		value = values[i];
		bool		isnull = nulls[i];

		/* Do not need any information for dropped attributes */
		if (attr[i]->attisdropped)
			continue;

		if (need_delim)
			appendStringInfoCharMacro(buf, COPYOPS_DELIMITER);
		need_delim = true;

		if (isnull)
		{
			/* Null print value to client */
			appendBinaryStringInfo(buf, "\\N", strlen("\\N"));
		}
		else
		{
			char *string;
			string = OutputFunctionCall(&out_functions[i],
										value);
			attribute_out_text(buf, string);
			pfree(string);
		}
	}

	/* Record length of message */
	*len = buf->len;
	res = pstrdup(buf->data);
	pfree(out_functions);
	pfree(buf->data);
	pfree(buf);
	return res;
}
