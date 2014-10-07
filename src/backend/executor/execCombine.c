/*-------------------------------------------------------------------------
 *
 * execCombine.c
 *	  executor routines for combining partial continuous query results
 *
 * IDENTIFICATION
 *	  src/backend/executor/execCombine.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/executor.h"

extern void
CombineAggs(void)
{
	// takes two sstates
	// returns one result and a new sstate

	// just start with this for simplicity
	// count(*):
	// input is current count and an aggstate that has row counts
	// set sstate = current + new
	// return current + new
}
