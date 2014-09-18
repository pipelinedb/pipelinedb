/*-------------------------------------------------------------------------
 *
 * printbuffer.h
 *
 * Functionality for printing tuples to a raw buffer
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/printbuffer.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PRINTBUFFER_H
#define PRINTBUFFER_H

#include "utils/portal.h"
#include "lib/stringinfo.h"

typedef struct
{								/* Per-attribute information */
	Oid			typoutput;		/* Oid for the type's text output fn */
	Oid			typsend;		/* Oid for the type's binary output fn */
	bool		typisvarlena;	/* is it varlena (ie possibly toastable)? */
	int16		format;			/* format code for this column */
	FmgrInfo	finfo;			/* Precomputed call info for output fn */
} BufferPrinterAttrInfo;

typedef struct BufferPrinterState
{
	BufferPrinterAttrInfo *typeinfo;	/* cached info about each attr */
	TupleDesc	attrinfo;		/* the attr info we are set up for */
	List *targetlist; /* List of TargetEntries */
	int16 *formats; /* array of formats */
	bool		tupdescSent;	/* have we sent the tupdesc yet? */
} BufferPrinterState;

BufferPrinterState *CreateBufferPrinter(TupleDesc attrinfo, List *targetList, int16 *formats);
StringInfo TupleToBytes(BufferPrinterState *self, TupleTableSlot *slot);

#endif   /* PRINTBUFFER_H */
