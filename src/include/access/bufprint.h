/*-------------------------------------------------------------------------
 *
 * printbuffer.h
 *
 * DestReciever for printing tuples to a raw char buffer
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
	bool		sendDescrip;	/* send RowDescription at startup? */
	TupleDesc	attrinfo;		/* The attr info we are set up for */
	int			nattrs;
	TupleTableSlot tupdesc;
	BufferPrinterAttrInfo *myinfo;	/* Cached info about each attr */
} BufferPrinterState;

extern BufferPrinterState *CreateBufferPrinter(void);

#endif   /* PRINTBUFFER_H */
