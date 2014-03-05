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

static void bufprint_startup(BufferPrinterState *self, int operation, TupleDesc typeinfo);
static void bufprint(BufferPrinterState *self, TupleTableSlot *slot, char *buf);
static void bufprint_shutdown(BufferPrinterState *self);
static void bufprint_destroy(BufferPrinterState *self);

extern BufferPrinterState *CreateBufferPrinter(void)
{
	BufferPrinterState *self = (BufferPrinterState *) palloc0(sizeof(BufferPrinterState));
	self->attrinfo = NULL;
	self->nattrs = 0;
	self->myinfo = NULL;

	// need target list and formats

	return self;
}


static void
SendTupDesc(TupleDesc typeinfo, List *targetlist, int16 *formats)
{
	Form_pg_attribute *attrs = typeinfo->attrs;
	int			natts = typeinfo->natts;
	int			i;
	StringInfoData buf;
	ListCell   *tlist_item = list_head(targetlist);

	pq_beginmessage(&buf, 'T'); /* tuple descriptor message type */
	pq_sendint(&buf, natts, 2); /* # of attrs in tuples */

	for (i = 0; i < natts; ++i)
	{
		Oid			atttypid = attrs[i]->atttypid;
		int32		atttypmod = attrs[i]->atttypmod;

		pq_sendstring(&buf, NameStr(attrs[i]->attname));

		/*
		 * Send the type name from a Postgres-XC backend node.
		 * This preserves from OID inconsistencies as architecture is shared nothing.
		 */
		if (IsConnFromCoord())
		{
			char	   *typename;
			typename = get_typename(atttypid);
			pq_sendstring(&buf, typename);
		}

		/* Do we have a non-resjunk tlist item? */
		while (tlist_item &&
				 ((TargetEntry *) lfirst(tlist_item))->resjunk)
			tlist_item = lnext(tlist_item);
		if (tlist_item)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(tlist_item);
			pq_sendint(&buf, tle->resorigtbl, 4);
			pq_sendint(&buf, tle->resorigcol, 2);
			tlist_item = lnext(tlist_item);
		}
		else
		{
			/* No info available, so send zeroes */
			pq_sendint(&buf, 0, 4);
			pq_sendint(&buf, 0, 2);
		}
		/* If column is a domain, send the base type and typmod instead */
		atttypid = getBaseTypeAndTypmod(atttypid, &atttypmod);
		pq_sendint(&buf, (int) atttypid, sizeof(atttypid));
		pq_sendint(&buf, attrs[i]->attlen, sizeof(attrs[i]->attlen));

		if (formats)
			pq_sendint(&buf, formats[i], 2);
		else
			pq_sendint(&buf, 0, 2);
	}
	pq_endmessage(&buf);
}

static void
bufprint_startup(BufferPrinterState *self, int operation, TupleDesc typeinfo)
{
//	SendRowDescriptionMessage(typeinfo,
//							  FetchPortalTargetList(portal),
//							  portal->formats);
	SendTupDesc(typeinfo, NULL, NULL);
}

static void
bufprint_prepare_info(BufferPrinterState *myState, TupleDesc typeinfo, int numAttrs)
{

}

static void
bufprint(BufferPrinterState *self, TupleTableSlot *slot, char *buf)
{

}

static void
bufprint_shutdown(BufferPrinterState *self)
{

}

static void
bufprint_destroy(BufferPrinterState *self)
{

}
