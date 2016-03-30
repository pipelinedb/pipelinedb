#include "postgres.h"
#include "lib/stringinfo.h"
#include "utils/lsyscache.h"
#include "pipeline/trigger/tuple_formatter.h"
#include "lib/ilist.h"
#include "utils/rel.h"

/*
 * This is similar to the copy format code in adhoc
 * proof of concept for the time being
 * we will probably replace this with a binary format
 */

void
tf_write_string(StringInfo msgbuf, const char *str)
{
	appendBinaryStringInfo(msgbuf, str, strlen(str));
}

void
tf_write_data(StringInfo msgbuf, const void *databuf, int datasize)
{
	appendBinaryStringInfo(msgbuf, databuf, datasize);
}

void
tf_write_char(StringInfo msgbuf, char c)
{
	appendStringInfoCharMacro(msgbuf, c);
}

/*
 * write to msgbuf the text representation of one attribute,
 * with conversion and escaping
 */
#define DUMPSOFAR() \
	do { \
		if (ptr > start) \
			tf_write_data(msgbuf, start, ptr - start); \
	} while (0)

void
tf_write_attribute_out_text(StringInfo msgbuf, char delim, char *string)
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
			tf_write_char(msgbuf, '\\');
			tf_write_char(msgbuf, c);
			start = ++ptr;	/* do not include char in next run */
		}
		else if (c == '\\' || c == delimc)
		{
			DUMPSOFAR();
			tf_write_char(msgbuf, '\\');
			start = ptr++;	/* we include char in next run */
		}
		else
			ptr++;
	}

	DUMPSOFAR();
}

void
tf_write_one_row(StringInfo msgbuf,
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
			tf_write_char(msgbuf, '\t');

		need_delim = true;

		if (isnull)
		{
			tf_write_string(msgbuf, "\\N");
		}
		else
		{
			string = OutputFunctionCall(&out_funcs[attnum - 1],
										value);

			tf_write_attribute_out_text(msgbuf,'\t',string);
		}
	}
}

void
tf_write_header(TupleFormatter *pdata,
				  TupleDesc tup_desc,
				  StringInfo msg_buf)
{
	ListCell *cur;
	bool hdr_delim = false;
	Form_pg_attribute *attr = tup_desc->attrs;

	foreach(cur, pdata->attnumlist)
	{
		int	attnum = lfirst_int(cur);
		char *colname;

		if (hdr_delim)
			tf_write_char(msg_buf, '\t');

		hdr_delim = true;
		colname = NameStr(attr[attnum - 1]->attname);
		tf_write_attribute_out_text(msg_buf, '\t', colname);
	}
}

List *
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

void
setup_formatter(TupleFormatter *pform,
				TupleDesc tup_desc)
{
	Form_pg_attribute *attr;
	ListCell   *cur;

	pform->attnumlist = get_attnums(tup_desc);
	pform->out_funcs =
		(FmgrInfo *) palloc(tup_desc->natts * sizeof(FmgrInfo));

	attr = tup_desc->attrs;

	foreach(cur, pform->attnumlist)
	{
		int			attnum = lfirst_int(cur);
		Oid			out_func_oid;
		bool		isvarlena;

		getTypeOutputInfo(attr[attnum - 1]->atttypid,
					  &out_func_oid,
					  &isvarlena);
		fmgr_info(out_func_oid, &pform->out_funcs[attnum - 1]);
	}

	pform->buf = makeStringInfo();
	pform->slot = MakeSingleTupleTableSlot(tup_desc);
}

void
tf_write_tuple_slot(TupleFormatter *pform,
				 TupleTableSlot *slot,
				 StringInfo msgbuf,
				 const char *label)
{
	tf_write_string(msgbuf, label);
	tf_write_char(msgbuf, '\t');
	slot_getallattrs(slot);

	tf_write_one_row(msgbuf, pform->out_funcs, pform->attnumlist,
				  slot->tts_values,
				  slot->tts_isnull);
}

void
tf_write_tuple(TupleFormatter *pform,
			   TupleTableSlot *slot,
			   StringInfo msgbuf,
			   HeapTuple tup)
{
	ExecStoreTuple(tup, slot, InvalidBuffer, false);
	slot_getallattrs(slot);

	tf_write_one_row(msgbuf, pform->out_funcs, pform->attnumlist,
				  slot->tts_values,
				  slot->tts_isnull);
}

/* keep a cache of formatters per rel oid */

static TupleFormatter *
find_formatter(dlist_head *l, Oid id)
{
	dlist_iter iter;

	dlist_foreach(iter, l)
	{
		TupleFormatter *f =
			dlist_container(TupleFormatter, list_node, iter.cur);

		if (f->id == id)
			return f;
	}

	return NULL;
}

dlist_head fmap;

TupleFormatter *get_formatter(Oid id)
{
	return find_formatter(&fmap, id);
}

void
register_formatter(Oid id, TupleDesc desc)
{
	TupleFormatter *f = find_formatter(&fmap, id);

	if (f)
		return;

	f = palloc0(sizeof(TupleFormatter));
	f->id = id;

	setup_formatter(f, desc);

	dlist_push_tail(&fmap, &f->list_node);
}
