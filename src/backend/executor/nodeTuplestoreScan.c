/*-------------------------------------------------------------------------
 *
 * nodeTuplestoreScan.c
 *	  Support routines for sequential scans of tuple stores.
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeTuplestoreScan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecTuplestoreScan				sequentially scans a tuplestore.
 *		ExecInitTuplestoreScan			creates and initializes a tuplestore scan node.
 *		ExecEndTuplestoreScan			releases any storage allocated.
 */
#include "postgres.h"

#include "executor/nodeTuplestoreScan.h"

static TupleTableSlot *TupleStoreNext(TuplestoreScanState * node);

static TupleTableSlot *
TupleStoreNext(TuplestoreScanState * node)
{
	return NULL;
}

extern SeqScanState *
ExecInitTuplestoreScan(TuplestoreScan *node, EState *estate, int eflags)
{

	return NULL;
}

extern TupleTableSlot *
ExecTuplestoreScan(TuplestoreScan *node)
{
	return NULL;
}

extern void
ExecEndTuplestoreScan(TuplestoreScan *node)
{

}
