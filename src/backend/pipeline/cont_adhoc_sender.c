#include "postgres.h"
#include "pipeline/cont_adhoc_sender.h"
#include "libpq/pqformat.h"
#include "libpq/libpq.h"
#include "fmgr.h"
#include "utils/lsyscache.h"

struct AdhocSender
{
	List *attnumlist;
	FmgrInfo *out_funcs;
};

static List * copy_get_attnums(TupleDesc tupDesc);

struct AdhocSender *
sender_create()
{
	struct AdhocSender *sender = palloc0(sizeof(struct AdhocSender));
	return sender;
}

void
sender_startup(struct AdhocSender *sender, TupleDesc tup_desc)
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
	
	pq_beginmessage(&buf, 'H');
	pq_sendbyte(&buf, 0);		/* overall format */
	pq_sendint(&buf, 0, 2);		/* natts=0 */
	pq_endmessage(&buf);

	pq_flush();
}

void
sender_shutdown(struct AdhocSender *sender)
{
	pq_putemptymessage('c');		/* CopyDone */
	pq_flush();
}

void
sender_insert(struct AdhocSender *sender)
{
}

void
sender_update(struct AdhocSender *sender)
{
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

//static void copy_one_row(StringInfo msgbuf,
//						 FmgrInfo *out_funcs,
//						 List *attnumlist,
//						 Datum *values,
//						 bool *nulls)
//{
//	ListCell   *cur;
//	char	   *string;
//
//	bool need_delim = false;
//
//	// CopyOneRowTo
//
//	foreach(cur, attnumlist)
//	{
//		int			attnum = lfirst_int(cur);
//		Datum		value = values[attnum - 1];
//		bool		isnull = nulls[attnum - 1];
//
//		if (need_delim)
//			copy_send_char(msgbuf, ' ');
//
//		need_delim = true;
//
//		if (isnull)
//		{
//			
//			// CopySendString
//
//			copy_send_string(msgbuf, "\\N");
//		}
//		else
//		{
//			string = OutputFunctionCall(&out_funcs[attnum - 1],
//										value);
//
//			// CopyAttributeOutText
//			copy_attribute_out_text(msgbuf,' ',string);
//		}
//	}
//
//	// CopySendEndOfRow
//	copy_send_end_of_row(msgbuf);
//}
//
//
//// UTIL CODE
//
//// util code to format output	
//	
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
//
//static void
//copy_send_string(StringInfo msgbuf, const char *str)
//{
//	appendBinaryStringInfo(msgbuf, str, strlen(str));
//}
//
//// CopySendData
//static void
//copy_send_data(StringInfo msgbuf, const void *databuf, int datasize)
//{
//	appendBinaryStringInfo(msgbuf, databuf, datasize);
//}
//
//static void
//copy_send_char(StringInfo msgbuf, char c)
//{
//	appendStringInfoCharMacro(msgbuf, c);
//}
//
//static void
//copy_send_end_of_row(StringInfo msgbuf)
//{
//	copy_send_char(msgbuf, '\n');
//	pq_putmessage('d', msgbuf->data, msgbuf->len);
//	
//	resetStringInfo(msgbuf);
//}
//
//static void
//copy_attribute_out_text(StringInfo msgbuf, char delim, char *string)
//{
//	char	   *ptr;
//	char	   *start;
//	char		c;
//	char		delimc = delim;
//
//	ptr = string;
//	start = ptr;
//
//	while ((c = *ptr) != '\0')
//	{
//		if ((unsigned char) c < (unsigned char) 0x20)
//		{
//			/*
//			 * \r and \n must be escaped, the others are traditional. We
//			 * prefer to dump these using the C-like notation, rather than
//			 * a backslash and the literal character, because it makes the
//			 * dump file a bit more proof against Microsoftish data
//			 * mangling.
//			 */
//			switch (c)
//			{
//				case '\b':
//					c = 'b';
//					break;
//				case '\f':
//					c = 'f';
//					break;
//				case '\n':
//					c = 'n';
//					break;
//				case '\r':
//					c = 'r';
//					break;
//				case '\t':
//					c = 't';
//					break;
//				case '\v':
//					c = 'v';
//					break;
//				default:
//					/* If it's the delimiter, must backslash it */
//					if (c == delimc)
//						break;
//					/* All ASCII control chars are length 1 */
//					ptr++;
//					continue;		/* fall to end of loop */
//			}
//			/* if we get here, we need to convert the control char */
//			DUMPSOFAR();
//			copy_send_char(msgbuf, '\\');
//			copy_send_char(msgbuf, c);
//			start = ++ptr;	/* do not include char in next run */
//		}
//		else if (c == '\\' || c == delimc)
//		{
//			DUMPSOFAR();
//			copy_send_char(msgbuf, '\\');
//			start = ptr++;	/* we include char in next run */
//		}
//		else
//			ptr++;
//	}
//
//	DUMPSOFAR();
//}
//
