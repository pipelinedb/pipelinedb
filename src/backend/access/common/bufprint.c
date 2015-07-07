/* Copyright (c) 2013-2015 PipelineDB */
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
 * TupleToBytes
 *
 * Serialize a tuple to bytes
 */
StringInfo
TupleToBytes(BufferPrinterState *self, TupleTableSlot *slot)
{
	StringInfo buf = makeStringInfo();
	TupleDesc	typeinfo = slot->tts_tupleDescriptor;
	int			natts = typeinfo->natts;
	int			i;
	int			len = 0;

	/* Make sure the tuple is fully deconstructed */
	slot_getallattrs(slot);

	/*
	 * Prepare a DataRow message
	 */
	pq_beginmessage(buf, 'D');
	len += 1;

	pq_sendint(buf, natts, 2);
	len+= 2;

	/*
	 * send the attributes of this tuple
	 */
	for (i = 0; i < natts; ++i)
	{
		BufferPrinterAttrInfo *thisState = self->typeinfo + i;
		Datum		origattr = slot->tts_values[i],
					attr;

		if (slot->tts_isnull[i])
		{
			pq_sendint(buf, -1, 4);
			len += 4;
			continue;
		}

		/*
		 * If we have a toasted datum, forcibly detoast it here to avoid
		 * memory leakage inside the type's output routine.
		 */
		if (thisState->typisvarlena)
			attr = PointerGetDatum(PG_DETOAST_DATUM(origattr));
		else
			attr = origattr;

		if (thisState->format == 0)
		{
			/* Text output */
			char	   *outputstr;

			outputstr = OutputFunctionCall(&thisState->finfo, attr);
			pq_sendcountedtext(buf, outputstr, strlen(outputstr), false);
			len += strlen(outputstr);
			pfree(outputstr);
		}
		else
		{
			/* Binary output */
			bytea	   *outputbytes;
			int size;
			outputbytes = SendFunctionCall(&thisState->finfo, attr);
			pq_sendint(buf, VARSIZE(outputbytes) - VARHDRSZ, 4);
			len += 4;

			size = VARSIZE(outputbytes) - VARHDRSZ;
			pq_sendbytes(buf, VARDATA(outputbytes), size);
			len += size;

			pfree(outputbytes);
		}

		/* Clean up detoasted copy, if any */
		if (DatumGetPointer(attr) != DatumGetPointer(origattr))
			pfree(DatumGetPointer(attr));
	}

	return buf;
}
