/*-------------------------------------------------------------------------
 *
 * bufprint.c
 *	  Routines to print out tuples to a raw char buffer. Most of this is
 *	  ripped straight from printtup.c, except it works on arbitrary
 *	  buffers rather than only the current client connection's buffer.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/common/printbuffer.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/bufprint.h"
#include "libpq/libpq.h"
#include "utils/lsyscache.h"
#include "libpq/pqformat.h"
#include "pgxc/pgxc.h"


/*
 * CreateBufferPrinter
 *
 * Create a new BufferPrinterState to use for printing tuples to a buffer
 */
BufferPrinterState *
CreateBufferPrinter(TupleDesc attrinfo, List *targetlist, int16 *formats)
{
	BufferPrinterState *self = (BufferPrinterState *) palloc0(sizeof(BufferPrinterState));
	int			i;
	int numAttrs;

	self->attrinfo = attrinfo;
	self->typeinfo = NULL;
	self->targetlist = targetlist;
	self->formats = formats;

	/* get rid of any old data */
	if (self->typeinfo)
		pfree(self->typeinfo);
	self->typeinfo = NULL;

	numAttrs = self->attrinfo->natts;
	if (numAttrs <= 0)
		return NULL;

	self->typeinfo = (BufferPrinterAttrInfo *)
		palloc0(numAttrs * sizeof(BufferPrinterAttrInfo));

	for (i = 0; i < numAttrs; i++)
	{
		BufferPrinterAttrInfo *thisState = self->typeinfo + i;
		int16		format = (formats ? formats[i] : 0);

		thisState->format = format;
		if (format == 0)
		{
			getTypeOutputInfo(attrinfo->attrs[i]->atttypid,
							  &thisState->typoutput,
							  &thisState->typisvarlena);
			fmgr_info(thisState->typoutput, &thisState->finfo);
		}
		else if (format == 1)
		{
			getTypeBinaryOutputInfo(attrinfo->attrs[i]->atttypid,
									&thisState->typsend,
									&thisState->typisvarlena);
			fmgr_info(thisState->typsend, &thisState->finfo);
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unsupported format code: %d", format)));
	}

	return self;
}

/*
 * PrintTuple
 *
 * Serialize a tuple to a buffer
 */
void
PrintTuple(BufferPrinterState *self, TupleTableSlot *slot, StringInfo buf)
{

}

/*
 * SendTupDesc
 *
 * Serialize a tuple description message to a buffer
 */
void
SendTupDesc(BufferPrinterState *self, StringInfo buf)
{
	Form_pg_attribute *attrs = self->attrinfo->attrs;
	int			natts = self->attrinfo->natts;
	int			i;
	char	   *typename;
	ListCell   *tlist_item = list_head(self->targetlist);

	pq_beginmessage(buf, 'T'); /* tuple descriptor message type */
	pq_sendint(buf, natts, 2); /* # of attrs in tuples */

	for (i = 0; i < natts; ++i)
	{
		Oid			atttypid = attrs[i]->atttypid;
		int32		atttypmod = attrs[i]->atttypmod;

		pq_sendstring(buf, NameStr(attrs[i]->attname));

		/*
		 * Send the type name from a Postgres-XC backend node.
		 * This preserves from OID inconsistencies as architecture is shared nothing.
		 */
		typename = get_typename(atttypid);
		pq_sendstring(buf, typename);

		/* Do we have a non-resjunk tlist item? */
		while (tlist_item &&
				 ((TargetEntry *) lfirst(tlist_item))->resjunk)
			tlist_item = lnext(tlist_item);
		if (tlist_item)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(tlist_item);
			pq_sendint(buf, tle->resorigtbl, 4);
			pq_sendint(buf, tle->resorigcol, 2);
			tlist_item = lnext(tlist_item);
		}
		else
		{
			/* No info available, so send zeroes */
			pq_sendint(buf, 0, 4);
			pq_sendint(buf, 0, 2);
		}
		/* If column is a domain, send the base type and typmod instead */
		atttypid = getBaseTypeAndTypmod(atttypid, &atttypmod);
		pq_sendint(buf, (int) atttypid, sizeof(atttypid));
		pq_sendint(buf, attrs[i]->attlen, sizeof(attrs[i]->attlen));

		if (self->formats)
			pq_sendint(buf, self->formats[i], 4);
		else
			pq_sendint(buf, 0, 4);

		/* text/binary flag */
		pq_sendint(buf, 1, 2);
	}
	self->tupdescSent = true;
}
