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

typedef struct
{								/* Per-attribute information */
	Oid			typoutput;		/* Oid for the type's text output fn */
	Oid			typsend;		/* Oid for the type's binary output fn */
	bool		typisvarlena;	/* is it varlena (ie possibly toastable)? */
	int16		format;			/* format code for this column */
	FmgrInfo	finfo;			/* Precomputed call info for output fn */
} BufprintAttrInfo;

typedef struct
{
	DestReceiver pub;			/* publicly-known function pointers */
	Portal		portal;			/* the Portal we are printing from */
	bool		sendDescrip;	/* send RowDescription at startup? */
	TupleDesc	attrinfo;		/* The attr info we are set up for */
	int			nattrs;
	BufprintAttrInfo *myinfo;	/* Cached info about each attr */
} DR_bufprint;

extern DestReceiver *create_printbuffer(CommandDest dest)
{
	return NULL;
}


static void
bufprint_prepare_info(DR_bufprint *myState, TupleDesc typeinfo, int numAttrs)
{

}

static void
bufprint(TupleTableSlot *slot, DestReceiver *self)
{

}

static void
bufprint_shutdown(DestReceiver *self)
{

}

static void
bufprint_destroy(DestReceiver *self)
{

}
