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

extern DestReceiver *create_printbuffer(CommandDest dest);

extern void SendRowDescriptionMessage(TupleDesc typeinfo, List *targetlist,
						  int16 *formats);

#endif   /* PRINTBUFFER_H */
