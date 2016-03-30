#ifndef TUPLE_FORMATTER_H
#define TUPLE_FORMATTER_H

#include "fmgr.h"
#include "nodes/pg_list.h"
#include "lib/stringinfo.h"
#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "utils/relcache.h"
#include "lib/ilist.h"

/*
 * Routines to format new/old row data to send to client
 * simple copy based text protocol
 */

typedef struct
{
	Oid id;
	List *attnumlist;
	FmgrInfo *out_funcs;
	StringInfo buf;
	TupleTableSlot *slot;
	dlist_node list_node;

} TupleFormatter;

List * get_attnums(TupleDesc tupDesc);
void setup_formatter(TupleFormatter *pform, TupleDesc desc);

void tf_write_string(StringInfo msgbuf, const char *str);
void tf_write_data(StringInfo msgbuf, const void *databuf, int datasize);
void tf_write_char(StringInfo msgbuf, char c);
void tf_write_attribute_out_text(StringInfo msgbuf, char delim, char *string);
void tf_write_one_row(StringInfo msgbuf,
			  FmgrInfo *out_funcs,
			  List *attnumlist,
			  Datum *values,
			  bool *nulls);

void tf_write_header(TupleFormatter *pdata,
					   TupleDesc tup_desc,
				  StringInfo msg_buf);

void
tf_write_tuple_slot(TupleFormatter *pform,
				 TupleTableSlot *slot,
				 StringInfo msgbuf,
				 const char *label);

void
tf_write_tuple(TupleFormatter *pform,
			    TupleTableSlot *slot,
				 StringInfo msgbuf,
				 HeapTuple tup);

void register_formatter(Oid id, TupleDesc desc);
TupleFormatter * get_formatter(Oid id);


#endif
