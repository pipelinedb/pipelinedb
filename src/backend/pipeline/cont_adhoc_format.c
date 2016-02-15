/*-------------------------------------------------------------------------
 *
 * cont_adhoc_format.c
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "pipeline/cont_adhoc_format.h"
  
void
adhoc_write_string(StringInfo msgbuf, const char *str)
{
	appendBinaryStringInfo(msgbuf, str, strlen(str));
}

void
adhoc_write_data(StringInfo msgbuf, const void *databuf, int datasize)
{
	appendBinaryStringInfo(msgbuf, databuf, datasize);
}

void
adhoc_write_char(StringInfo msgbuf, char c)
{
	appendStringInfoCharMacro(msgbuf, c);
}

void
adhoc_write_end_of_row(StringInfo msgbuf)
{
	adhoc_write_char(msgbuf, '\n');
}

void
adhoc_write_msg_type(StringInfo buf, char type)
{
	adhoc_write_char(buf, type);
	adhoc_write_char(buf, '\t');
}

/*
 * write to msgbuf the text representation of one attribute,
 * with conversion and escaping
 */
#define DUMPSOFAR() \
	do { \
		if (ptr > start) \
			adhoc_write_data(msgbuf, start, ptr - start); \
	} while (0)

void
adhoc_write_attribute_out_text(StringInfo msgbuf, char delim, char *string)
{
	char	   *ptr;
	char	   *start;
	char		c;
	char		delimc = delim;

	ptr = string;
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
			adhoc_write_char(msgbuf, '\\');
			adhoc_write_char(msgbuf, c);
			start = ++ptr;	/* do not include char in next run */
		}
		else if (c == '\\' || c == delimc)
		{
			DUMPSOFAR();
			adhoc_write_char(msgbuf, '\\');
			start = ptr++;	/* we include char in next run */
		}
		else
			ptr++;
	}

	DUMPSOFAR();
}
