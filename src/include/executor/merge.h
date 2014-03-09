/*-------------------------------------------------------------------------
 *
 * merge.h
 *	  Functionality to support merging partial results in continuous queries
 *
 *	  src/include/executor/merge.h
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

/*
 * create takes tuples from coordinator MERGE request
 * any that don't have a corresponding tuple in the CV are INSERTS,
 * 	all else are updates
 */
