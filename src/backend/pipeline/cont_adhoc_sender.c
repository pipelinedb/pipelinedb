#include "postgres.h"
#include "pipeline/cont_adhoc_sender.h"
#include "libpq/pqformat.h"
#include "libpq/libpq.h"
#include "fmgr.h"
#include "utils/lsyscache.h"
#include "nodes/print.h"

struct AdhocSender
{
	List *attnumlist;
	FmgrInfo *out_funcs;
	StringInfo msg_buf;
};

static List * copy_get_attnums(TupleDesc tupDesc);

static void copy_one_row(StringInfo msgbuf,
						 FmgrInfo *out_funcs,
						 List *attnumlist,
						 Datum *values,
						 bool *nulls);


static List *
copy_get_attnums(TupleDesc tupDesc);

static void
copy_send_string(StringInfo msgbuf, const char *str);

static void
copy_send_data(StringInfo msgbuf, const void *databuf, int datasize);

static void
copy_send_char(StringInfo msgbuf, char c);

static void
copy_send_end_of_row(StringInfo msgbuf);

static void
copy_attribute_out_text(StringInfo msgbuf, char delim, char *string);

static void
sender_header(struct AdhocSender *sender, TupleDesc tup_desc);

static void
sender_keys(struct AdhocSender *sender, TupleDesc tup_desc, AttrNumber *keyColIdx,
		int	numCols);

static void
sender_send(struct AdhocSender *sender);

struct AdhocSender *
sender_create()
{
	struct AdhocSender *sender = palloc0(sizeof(struct AdhocSender));
	return sender;
}

void
sender_startup(struct AdhocSender *sender, TupleDesc tup_desc,
		AttrNumber *keyColIdx,
		int	numCols)
{
	// take a tup desc so we know how to output text data

	StringInfoData buf;
	Form_pg_attribute *attr;
	ListCell   *cur;

	sender->attnumlist = copy_get_attnums(tup_desc);
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

static void copy_msg_type(StringInfo buf, char type)
{
	copy_send_char(buf, type);
	copy_send_char(buf, ' ');
}

void
sender_keys(struct AdhocSender *sender, TupleDesc tup_desc, AttrNumber *keyColIdx,
		int	numCols)
{
	bool hdr_delim = false;
	Form_pg_attribute *attr = tup_desc->attrs;
	int i = 0;

	resetStringInfo(sender->msg_buf);
	copy_msg_type(sender->msg_buf, 'k');

	for (i = 0; i < numCols; ++i)
	{
		int attnum = keyColIdx[i];
		char	   *colname;

		if (hdr_delim)
			copy_send_char(sender->msg_buf, ' ');

		hdr_delim = true;
		colname = NameStr(attr[attnum - 1]->attname);
		copy_attribute_out_text(sender->msg_buf, ' ', colname);
	}

	copy_send_end_of_row(sender->msg_buf);
	sender_send(sender);
}

void
sender_header(struct AdhocSender *sender, TupleDesc tup_desc)
{
	ListCell   *cur;
	bool hdr_delim = false;
	Form_pg_attribute *attr = tup_desc->attrs;

	resetStringInfo(sender->msg_buf);
	copy_msg_type(sender->msg_buf, 'h');

	foreach(cur, sender->attnumlist)
	{
		int			attnum = lfirst_int(cur);
		char	   *colname;

		if (hdr_delim)
			copy_send_char(sender->msg_buf, ' ');

		hdr_delim = true;
		colname = NameStr(attr[attnum - 1]->attname);
		copy_attribute_out_text(sender->msg_buf, ' ', colname);
	}

	copy_send_end_of_row(sender->msg_buf);
	sender_send(sender);
}

void
sender_shutdown(struct AdhocSender *sender)
{
	pq_putemptymessage('c');		/* CopyDone */
	pq_flush();
}

void
sender_insert(struct AdhocSender *sender, TupleTableSlot *slot)
{
	resetStringInfo(sender->msg_buf);
	copy_msg_type(sender->msg_buf, 'i');

	slot_getallattrs(slot);

	copy_one_row(sender->msg_buf,
				 sender->out_funcs,
				 sender->attnumlist,
				 slot->tts_values,
				 slot->tts_isnull);

	sender_send(sender);
}

void
sender_update(struct AdhocSender *sender, TupleTableSlot *slot)
{
	resetStringInfo(sender->msg_buf);
	copy_msg_type(sender->msg_buf, 'u');

	slot_getallattrs(slot);

	copy_one_row(sender->msg_buf,
				 sender->out_funcs,
				 sender->attnumlist,
				 slot->tts_values,
				 slot->tts_isnull);

	sender_send(sender);
}

void
sender_send(struct AdhocSender *sender)
{
	pq_putmessage('d', sender->msg_buf->data, sender->msg_buf->len);
	pq_flush();
}

//	StringInfoData buf;

//	state->frontend = frontend;
//	state->attnumlist = copy_get_attnums(tup_desc);
//	state->out_funcs = 
//		(FmgrInfo *) palloc(tup_desc->natts * sizeof(FmgrInfo));
//
//	attr = tup_desc->attrs;
//	// out_functions
//
//	foreach(cur, state->attnumlist)
//	{
//		int			attnum = lfirst_int(cur);
//		Oid			out_func_oid;
//		bool		isvarlena;
//
//		getTypeOutputInfo(attr[attnum - 1]->atttypid,
//					  &out_func_oid,
//					  &isvarlena);
//		fmgr_info(out_func_oid, &state->out_funcs[attnum - 1]);
//	}
//
//	state->msgbuf = makeStringInfo();
//
//	// psql probably needs this.
//
//		pq_beginmessage(&buf, 'H');
//		pq_sendbyte(&buf, 0);		/* overall format */
//		pq_sendint(&buf, 0, 2);		/* natts */
//		pq_endmessage(&buf);
//
//	}
//}

//	char buf[32];
//	strcpy(buf, "insert");
//	slot_getallattrs(state->slot);
//
//	copy_one_row(state->msgbuf,
//				 state->out_funcs,
//				 state->attnumlist,
//				 state->slot->tts_values,
//				 state->slot->tts_isnull);
//
//	pq_putmessage('d', buf, strlen(buf));
//	pq_flush();
//}

//void sender_update()
//{
//	char buf[32];
//	strcpy(buf, "update");

//	pq_putmessage('d', buf, strlen(buf));
//	pq_flush();
//}

static void copy_one_row(StringInfo msgbuf,
						 FmgrInfo *out_funcs,
						 List *attnumlist,
						 Datum *values,
						 bool *nulls)
{
	ListCell   *cur;
	char	   *string;

	bool need_delim = false;

	// CopyOneRowTo

	foreach(cur, attnumlist)
	{
		int			attnum = lfirst_int(cur);
		Datum		value = values[attnum - 1];
		bool		isnull = nulls[attnum - 1];

		if (need_delim)
			copy_send_char(msgbuf, ' ');

		need_delim = true;

		if (isnull)
		{
			
			// CopySendString

			copy_send_string(msgbuf, "\\N");
		}
		else
		{
			string = OutputFunctionCall(&out_funcs[attnum - 1],
										value);

			// CopyAttributeOutText
			copy_attribute_out_text(msgbuf,' ',string);
		}
	}

	// CopySendEndOfRow
	copy_send_end_of_row(msgbuf);
}
	
static List *
copy_get_attnums(TupleDesc tupDesc)
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
copy_send_string(StringInfo msgbuf, const char *str)
{
	appendBinaryStringInfo(msgbuf, str, strlen(str));
}

static void
copy_send_data(StringInfo msgbuf, const void *databuf, int datasize)
{
	appendBinaryStringInfo(msgbuf, databuf, datasize);
}

static void
copy_send_char(StringInfo msgbuf, char c)
{
	appendStringInfoCharMacro(msgbuf, c);
}

static void
copy_send_end_of_row(StringInfo msgbuf)
{
	copy_send_char(msgbuf, '\n');
}

// CopySendData
#define DUMPSOFAR() \
	do { \
		if (ptr > start) \
			copy_send_data(msgbuf, start, ptr - start); \
	} while (0)

static void
copy_attribute_out_text(StringInfo msgbuf, char delim, char *string)
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
			copy_send_char(msgbuf, '\\');
			copy_send_char(msgbuf, c);
			start = ++ptr;	/* do not include char in next run */
		}
		else if (c == '\\' || c == delimc)
		{
			DUMPSOFAR();
			copy_send_char(msgbuf, '\\');
			start = ptr++;	/* we include char in next run */
		}
		else
			ptr++;
	}

	DUMPSOFAR();
}

