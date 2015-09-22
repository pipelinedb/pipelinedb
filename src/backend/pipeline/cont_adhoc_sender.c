#include "postgres.h"
#include "pipeline/cont_adhoc_sender.h"
#include "libpq/pqformat.h"
#include "libpq/libpq.h"
#include "fmgr.h"
#include "utils/lsyscache.h"
#include "nodes/print.h"
#include <signal.h>

// enum

typedef enum
{
	Append,
	Single,
	Aggregate
} KeyType;

struct AdhocSender
{
	List *attnumlist;
	FmgrInfo *out_funcs;
	StringInfo msg_buf;
	KeyType key_type;
	unsigned row_count;
};

static List * get_attnums(TupleDesc tupDesc);

static void write_one_row(StringInfo msgbuf,
						 FmgrInfo *out_funcs,
						 List *attnumlist,
						 Datum *values,
						 bool *nulls);


static List *
get_attnums(TupleDesc tupDesc);

static void
write_string(StringInfo msgbuf, const char *str);

static void
write_data(StringInfo msgbuf, const void *databuf, int datasize);

static void
write_char(StringInfo msgbuf, char c);

static void
write_end_of_row(StringInfo msgbuf);

static void
write_attribute_out_text(StringInfo msgbuf, char delim, char *string);

static void
sender_header(struct AdhocSender *sender, TupleDesc tup_desc);

static void
sender_keys(struct AdhocSender *sender, TupleDesc tup_desc,
			AttrNumber *keyColIdx, int	numCols);

static void
sender_send(struct AdhocSender *sender);

struct AdhocSender *
sender_create()
{
	struct AdhocSender *sender = palloc0(sizeof(struct AdhocSender));
	return sender;
}

// CreateDestReceiver

void
sender_startup(struct AdhocSender *sender, TupleDesc tup_desc,
		bool is_agg,
		AttrNumber *keyColIdx,
		int	numCols)
{
	// take a tup desc so we know how to output text data

	StringInfoData buf;
	Form_pg_attribute *attr;
	ListCell   *cur;

	sender->attnumlist = get_attnums(tup_desc);
	sender->out_funcs = 
		(FmgrInfo *) palloc(tup_desc->natts * sizeof(FmgrInfo));

	attr = tup_desc->attrs;

	foreach(cur, sender->attnumlist)
	{
		int			attnum = lfirst_int(cur);
		Oid			out_func_oid;
		bool		isvarlena;

		getTypeOutputInfo(attr[attnum - 1]->atttypid,
					  &out_func_oid,
					  &isvarlena);
		fmgr_info(out_func_oid, &sender->out_funcs[attnum - 1]);
	}

	if (!is_agg)
	{
		sender->key_type = Append;
	}
	else
	{
		sender->key_type = numCols ? Aggregate : Single;
	}
	
	sender->msg_buf = makeStringInfo();

	// copy start message
	
	pq_beginmessage(&buf, 'H');
	pq_sendbyte(&buf, 0);		/* overall format */
	pq_sendint(&buf, 0, 2);		/* natts=0 */
	pq_endmessage(&buf);

	sender_header(sender, tup_desc);
	sender_keys(sender, tup_desc, keyColIdx, numCols);

	pq_flush();
}

static void write_msg_type(StringInfo buf, char type)
{
	write_char(buf, type);
	write_char(buf, '\t');
}

void
sender_keys(struct AdhocSender *sender, TupleDesc tup_desc,
		AttrNumber *keyColIdx,
		int	numCols)
{
	bool hdr_delim = false;
//	Form_pg_attribute *attr = tup_desc->attrs;
	int i = 0;
	char num_buf[32];

	resetStringInfo(sender->msg_buf);
	write_msg_type(sender->msg_buf, 'k');

	switch(sender->key_type)
	{
		case Append:
		{
			sprintf(num_buf, "%d", 1);
			write_string(sender->msg_buf, num_buf);
			break;
		}
		case Single:
		{
			sprintf(num_buf, "%d", 1);
			write_string(sender->msg_buf, num_buf);
			break;
		}
		case Aggregate:
		{
			for (i = 0; i < numCols; ++i)
			{
				int attnum = keyColIdx[i];
//				char	   *colname;

				if (hdr_delim)
					write_char(sender->msg_buf, '\t');

				sprintf(num_buf, "%d", attnum);

				hdr_delim = true;
				write_string(sender->msg_buf, num_buf);
//				colname = NameStr(attr[attnum - 1]->attname);
//				write_attribute_out_text(sender->msg_buf, ' ', colname);
			}

			break;
		}
	}

	write_end_of_row(sender->msg_buf);
	sender_send(sender);
}

void
sender_header(struct AdhocSender *sender, TupleDesc tup_desc)
{
	ListCell   *cur;
	bool hdr_delim = false;
	Form_pg_attribute *attr = tup_desc->attrs;

	resetStringInfo(sender->msg_buf);
	write_msg_type(sender->msg_buf, 'h');

	switch(sender->key_type)
	{
		case Single:
		{
			write_string(sender->msg_buf, "result");
			write_char(sender->msg_buf, '\t');
			break;
		}
		case Append:
		{
			write_string(sender->msg_buf, "row_id");
			write_char(sender->msg_buf, '\t');
			break;
		}
		case Aggregate:
		{
			break;
		}
	}

	foreach(cur, sender->attnumlist)
	{
		int			attnum = lfirst_int(cur);
		char	   *colname;

		if (hdr_delim)
			write_char(sender->msg_buf, '\t');

		hdr_delim = true;
		colname = NameStr(attr[attnum - 1]->attname);
		write_attribute_out_text(sender->msg_buf, '\t', colname);
	}

	write_end_of_row(sender->msg_buf);
	sender_send(sender);
}

void
sender_shutdown(struct AdhocSender *sender)
{
	pq_putemptymessage('c'); /* CopyDone */
	pq_flush();
}

static void
do_sender_update(struct AdhocSender *sender, TupleTableSlot *slot, char type)
{
	resetStringInfo(sender->msg_buf);
	write_msg_type(sender->msg_buf, type);

	slot_getallattrs(slot);

	switch(sender->key_type)
	{
		case Single:
		{
			write_string(sender->msg_buf, "result");
			write_char(sender->msg_buf, '\t');
			break;
		}
		case Append:
		{
			char num_buf[32];
			sprintf(num_buf, "%010u\t", sender->row_count);
			write_string(sender->msg_buf, num_buf);
			break;
		}
		case Aggregate:
		{
			break;
		}
	}

	write_one_row(sender->msg_buf,
				 sender->out_funcs,
				 sender->attnumlist,
				 slot->tts_values,
				 slot->tts_isnull);

	sender->row_count++;

	sender_send(sender);
}

void
sender_update(struct AdhocSender *sender, TupleTableSlot *slot)
{
	do_sender_update(sender, slot, 'u');
}

void sender_insert(struct AdhocSender *sender, TupleTableSlot *slot)
{
	do_sender_update(sender, slot, 'i');
}

void
sender_send(struct AdhocSender *sender)
{
	pq_putmessage('d', sender->msg_buf->data, sender->msg_buf->len);
	pq_flush();

}

static void write_one_row(StringInfo msgbuf,
						 FmgrInfo *out_funcs,
						 List *attnumlist,
						 Datum *values,
						 bool *nulls)
{
	ListCell   *cur;
	char	   *string;

	bool need_delim = false;

	foreach(cur, attnumlist)
	{
		int			attnum = lfirst_int(cur);
		Datum		value = values[attnum - 1];
		bool		isnull = nulls[attnum - 1];

		if (need_delim)
			write_char(msgbuf, '\t');

		need_delim = true;

		if (isnull)
		{
			write_string(msgbuf, "\\N");
		}
		else
		{
			string = OutputFunctionCall(&out_funcs[attnum - 1],
										value);

			write_attribute_out_text(msgbuf,'\t',string);
		}
	}

	write_end_of_row(msgbuf);
}
	
static List *
get_attnums(TupleDesc tupDesc)
{
	List *attnums = NIL;

	Form_pg_attribute *attr = tupDesc->attrs;
	int			attr_count = tupDesc->natts;
	int			i;

	for (i = 0; i < attr_count; i++)
	{
		if (attr[i]->attisdropped)
			continue;
		attnums = lappend_int(attnums, i + 1);
	}

	return attnums;
}

static void
write_string(StringInfo msgbuf, const char *str)
{
	appendBinaryStringInfo(msgbuf, str, strlen(str));
}

static void
write_data(StringInfo msgbuf, const void *databuf, int datasize)
{
	appendBinaryStringInfo(msgbuf, databuf, datasize);
}

static void
write_char(StringInfo msgbuf, char c)
{
	appendStringInfoCharMacro(msgbuf, c);
}

static void
write_end_of_row(StringInfo msgbuf)
{
	write_char(msgbuf, '\n');
}

#define DUMPSOFAR() \
	do { \
		if (ptr > start) \
			write_data(msgbuf, start, ptr - start); \
	} while (0)

static void
write_attribute_out_text(StringInfo msgbuf, char delim, char *string)
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
			write_char(msgbuf, '\\');
			write_char(msgbuf, c);
			start = ++ptr;	/* do not include char in next run */
		}
		else if (c == '\\' || c == delimc)
		{
			DUMPSOFAR();
			write_char(msgbuf, '\\');
			start = ptr++;	/* we include char in next run */
		}
		else
			ptr++;
	}

	DUMPSOFAR();
}

