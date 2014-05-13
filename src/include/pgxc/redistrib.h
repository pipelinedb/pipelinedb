/*-------------------------------------------------------------------------
 *
 * redistrib.h
 *	  Routines related to online data redistribution
 *
 * Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  src/include/pgxc/redistrib.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef REDISTRIB_H
#define REDISTRIB_H

#include "nodes/parsenodes.h"
#include "utils/tuplestore.h"

/*
 * Type of data redistribution operations.
 * Online data redistribution is made of one or more of those operations.
 */
typedef enum RedistribOperation {
	DISTRIB_NONE,		/* Default operation */
	DISTRIB_DELETE_HASH,	/* Perform a DELETE with hash value check */
	DISTRIB_DELETE_MODULO,	/* Perform a DELETE with modulo value check */
	DISTRIB_COPY_TO,	/* Perform a COPY TO */
	DISTRIB_COPY_FROM,	/* Perform a COPY FROM */
	DISTRIB_TRUNCATE,	/* Truncate relation */
	DISTRIB_REINDEX		/* Reindex relation */
} RedistribOperation;

/*
 * Determine if operation can be done before or after
 * catalog update on local node.
 */
typedef enum RedistribCatalog {
	CATALOG_UPDATE_NONE,	/* Default state */
	CATALOG_UPDATE_AFTER,	/* After catalog update */
	CATALOG_UPDATE_BEFORE,	/* Before catalog update */
	CATALOG_UPDATE_BOTH		/* Before and after catalog update */
} RedistribCatalog;

/*
 * Redistribution command
 * This contains the tools necessary to perform a redistribution operation.
 */
typedef struct RedistribCommand {
	RedistribOperation type;				/* Operation type */
	ExecNodes	   *execNodes;			/* List of nodes where to perform operation */
	RedistribCatalog	updateState;		/* Flag to determine if operation can be done
										 * before or after catalog update */
} RedistribCommand;

/*
 * Redistribution operation state
 * Maintainer of redistribution state having the list of commands
 * to be performed during redistribution.
 * For the list of commands, we use an array and not a simple list as operations
 * might need to be done in a certain order.
 */
typedef struct RedistribState {
	Oid			relid;			/* Oid of relation redistributed */
	List	   *commands;		/* List of commands */
	Tuplestorestate *store;		/* Tuple store used for temporary data storage */
} RedistribState;

extern void PGXCRedistribTable(RedistribState *distribState, RedistribCatalog type);
extern void PGXCRedistribCreateCommandList(RedistribState *distribState,
										 RelationLocInfo *newLocInfo);
extern RedistribCommand *makeRedistribCommand(RedistribOperation type,
										  RedistribCatalog updateState,
										  ExecNodes *nodes);
extern RedistribState *makeRedistribState(Oid relOid);
extern void FreeRedistribState(RedistribState *state);
extern void FreeRedistribCommand(RedistribCommand *command);

#endif  /* REDISTRIB_H */
