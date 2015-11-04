/*-------------------------------------------------------------------------
 *
 * adhocReceiver.c
 *	  An implementation of DestReceiver that that allows us to send updates
 *	  to clients for adhoc continuous queries.
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/adhocReceiver.c
 *
 */

#include "postgres.h"
#include "pipeline/adhocReceiver.h"
#include "pipeline/cont_adhoc_format.h"
#include "libpq/pqformat.h"
#include "libpq/libpq.h"
#include "fmgr.h"
#include "utils/lsyscache.h"
#include "tcop/dest.h"

/*
 * Different key types to handle different types of queries.
 *
 * Append - select x::int from stream
 * Single - select count(*) from stream
 * Aggregate - select x::int, count(*) from stream group by x
 */

typedef enum
{
	Append,
	Single,
	Aggregate
} KeyType;

struct AdhocDestReceiver
{
	DestReceiver pub;

	List *attnumlist;
	FmgrInfo *out_funcs;
	StringInfo msg_buf;
	KeyType key_type;
	unsigned row_count;

	bool is_agg;
	AttrNumber *keyColIdx;
	int numCols;
};

/* send the buffered message in copy format, and flush.
 * caller is responsible for clearing the buffer */

static void
adhoc_dest_send(struct AdhocDestReceiver *adhoc_dest)
{
	pq_putmessage('d', adhoc_dest->msg_buf->data, adhoc_dest->msg_buf->len);
	pq_flush();
}

/* output the header message, detailing the column names */
static void
adhoc_dest_output_header(struct AdhocDestReceiver *adhoc_dest, TupleDesc tup_desc)
{
	ListCell   *cur;
	bool hdr_delim = false;
	Form_pg_attribute *attr = tup_desc->attrs;

	resetStringInfo(adhoc_dest->msg_buf);
	adhoc_write_msg_type(adhoc_dest->msg_buf, 'h');


	switch(adhoc_dest->key_type)
	{
		case Single:
		{
			/* add a result column for queries like select count(*) */
			 
			adhoc_write_string(adhoc_dest->msg_buf, "result");
			adhoc_write_char(adhoc_dest->msg_buf, '\t');
			break;
		}
		case Append:
		{
			/* add a row_id column name for append type queries */

			adhoc_write_string(adhoc_dest->msg_buf, "row_id");
			adhoc_write_char(adhoc_dest->msg_buf, '\t');
			break;
		}
		case Aggregate:
		{
			break;
		}
	}

	/* output any extra column names */
	foreach(cur, adhoc_dest->attnumlist)
	{
		int			attnum = lfirst_int(cur);
		char	   *colname;

		if (hdr_delim)
			adhoc_write_char(adhoc_dest->msg_buf, '\t');

		hdr_delim = true;
		colname = NameStr(attr[attnum - 1]->attname);
		adhoc_write_attribute_out_text(adhoc_dest->msg_buf, '\t', colname);
	}

	adhoc_write_end_of_row(adhoc_dest->msg_buf);
	adhoc_dest_send(adhoc_dest);
}

/* output the key message, detailing the column numbers involved in the key */
static void
adhoc_dest_output_keys(struct AdhocDestReceiver *adhoc_dest, TupleDesc tup_desc,
		AttrNumber *keyColIdx,
		int	numCols)
{
	bool hdr_delim = false;
	int i = 0;
	char num_buf[32];

	resetStringInfo(adhoc_dest->msg_buf);
	adhoc_write_msg_type(adhoc_dest->msg_buf, 'k');

	switch(adhoc_dest->key_type)
	{
		case Append:
		{
			/* append type queries use an incrementing row_id in the first col */
			sprintf(num_buf, "%d", 1);
			adhoc_write_string(adhoc_dest->msg_buf, num_buf);
			break;
		}
		case Single:
		{
			/* single type queries use the key 'result' in the first col */
			sprintf(num_buf, "%d", 1);
			adhoc_write_string(adhoc_dest->msg_buf, num_buf);
			break;
		}
		case Aggregate:
		{
			/* otherwise, use the col nums we are given */

			for (i = 0; i < numCols; ++i)
			{
				int attnum = keyColIdx[i];

				if (hdr_delim)
					adhoc_write_char(adhoc_dest->msg_buf, '\t');

				sprintf(num_buf, "%d", attnum);

				hdr_delim = true;
				adhoc_write_string(adhoc_dest->msg_buf, num_buf);
			}

			break;
		}
	}

	adhoc_write_end_of_row(adhoc_dest->msg_buf);
	adhoc_dest_send(adhoc_dest);
}

/* write a tuple in adhoc format to the msg buf 
 * this is pretty much the same as the copy text format */
static void 
adhoc_write_one_row(StringInfo msgbuf,
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
			adhoc_write_char(msgbuf, '\t');

		need_delim = true;

		if (isnull)
		{
			adhoc_write_string(msgbuf, "\\N");
		}
		else
		{
			string = OutputFunctionCall(&out_funcs[attnum - 1],
										value);

			adhoc_write_attribute_out_text(msgbuf,'\t',string);
		}
	}

	adhoc_write_end_of_row(msgbuf);
}

/* util func to help with getting column formatters */
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

/* startup the receiver, and output headers based on the type of query */
static void
adhoc_dest_startup(struct AdhocDestReceiver *adhoc_dest, TupleDesc tup_desc,
		bool is_agg,
		AttrNumber *keyColIdx,
		int	numCols)
{
	StringInfoData buf;
	Form_pg_attribute *attr;
	ListCell   *cur;

	adhoc_dest->attnumlist = get_attnums(tup_desc);
	adhoc_dest->out_funcs = 
		(FmgrInfo *) palloc(tup_desc->natts * sizeof(FmgrInfo));

	attr = tup_desc->attrs;

	foreach(cur, adhoc_dest->attnumlist)
	{
		int			attnum = lfirst_int(cur);
		Oid			out_func_oid;
		bool		isvarlena;

		getTypeOutputInfo(attr[attnum - 1]->atttypid,
					  &out_func_oid,
					  &isvarlena);
		fmgr_info(out_func_oid, &adhoc_dest->out_funcs[attnum - 1]);
	}

	if (!is_agg)
		adhoc_dest->key_type = Append;
	else
		adhoc_dest->key_type = numCols ? Aggregate : Single;
	
	adhoc_dest->msg_buf = makeStringInfo();

	/* output copy start message */
	
	pq_beginmessage(&buf, 'H');
	pq_sendbyte(&buf, 0);		/* overall format */
	pq_sendint(&buf, 0, 2);		/* natts=0 */
	pq_endmessage(&buf);

	adhoc_dest_output_header(adhoc_dest, tup_desc);
	adhoc_dest_output_keys(adhoc_dest, tup_desc, keyColIdx, numCols);

	pq_flush();
}

/* output a new row to the client, adjusting if necessary */
static void
do_adhoc_dest_update(struct AdhocDestReceiver *adhoc_dest,
					 TupleTableSlot *slot, char type)
{
	resetStringInfo(adhoc_dest->msg_buf);
	adhoc_write_msg_type(adhoc_dest->msg_buf, type);

	slot_getallattrs(slot);

	switch(adhoc_dest->key_type)
	{
		case Single:
		{
			/* add a result field */
			adhoc_write_string(adhoc_dest->msg_buf, "result");
			adhoc_write_char(adhoc_dest->msg_buf, '\t');
			break;
		}
		case Append:
		{
			/* add the row_id as the key */
			char num_buf[32];
			sprintf(num_buf, "%010u\t", adhoc_dest->row_count);
			adhoc_write_string(adhoc_dest->msg_buf, num_buf);
			break;
		}
		case Aggregate:
		{
			break;
		}
	}

	adhoc_write_one_row(adhoc_dest->msg_buf,
				 adhoc_dest->out_funcs,
				 adhoc_dest->attnumlist,
				 slot->tts_values,
				 slot->tts_isnull);

	adhoc_dest->row_count++;
	adhoc_dest_send(adhoc_dest);
}

static void adhoc_dest_insert(struct AdhocDestReceiver *adhoc_dest,
							  TupleTableSlot *slot)
{
	do_adhoc_dest_update(adhoc_dest, slot, 'i');
}

/* send an empty copy message, which the client will ignore, but is enough for us
 * to detect a write failure when the client goes away */
static void adhoc_dest_heartbeat(struct AdhocDestReceiver *adhoc_dest)
{
	pq_putemptymessage('d');
	pq_flush();
}

static void adhoc_receive_slot(TupleTableSlot *slot, DestReceiver *self)
{
	adhoc_dest_insert((AdhocDestReceiver *) (self), slot);
}

static void adhoc_startup(DestReceiver *self, int operation,
						  TupleDesc typeinfo)
{
	struct AdhocDestReceiver *r = (AdhocDestReceiver *)(self);
	adhoc_dest_startup(r, typeinfo, r->is_agg, r->keyColIdx, r->numCols);
}

static void adhoc_shutdown(DestReceiver *self)
{
	pq_putemptymessage('c'); /* CopyDone */
	pq_flush();
}

static void adhoc_destroy(DestReceiver *self)
{
}

DestReceiver *
CreateAdhocDestReceiver(void)
{
	AdhocDestReceiver *self = (AdhocDestReceiver *) palloc0(sizeof(AdhocDestReceiver));

	self->pub.receiveSlot = adhoc_receive_slot;
	self->pub.rStartup = adhoc_startup;
	self->pub.rShutdown = adhoc_shutdown;
	self->pub.rDestroy = adhoc_destroy;

	return (DestReceiver *) self;
}

void
SetAdhocDestReceiverParams(DestReceiver *self, bool is_agg, AttrNumber *keyColIdx, int num_cols)
{
	AdhocDestReceiver *a = (AdhocDestReceiver *) self;
	a->is_agg = is_agg;
	a->keyColIdx = keyColIdx;
	a->numCols = num_cols;
}

void AdhocDestReceiverHeartbeat(AdhocDestReceiver *receiver)
{
	adhoc_dest_heartbeat(receiver);
}
