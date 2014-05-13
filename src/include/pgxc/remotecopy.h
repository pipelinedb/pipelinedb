/*-------------------------------------------------------------------------
 *
 * remotecopy.h
 *		Routines for extension of COPY command for cluster management
 *
 *
 * Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *		src/include/pgxc/remotecopy.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef REMOTECOPY_H
#define REMOTECOPY_H

#include "nodes/parsenodes.h"
#include "pgxc/pgxcnode.h"

/*
 * This contains the set of data necessary for remote COPY control.
 */
typedef struct RemoteCopyData {
	/* COPY FROM/TO? */
	bool		is_from;

	/*
	 * On Coordinator we need to rewrite query.
	 * While client may submit a copy command dealing with file, Datanodes
	 * always send/receive data to/from the Coordinator. So we can not use
	 * original statement and should rewrite statement, specifing STDIN/STDOUT
	 * as copy source or destination
	 */
	StringInfoData query_buf;

	/* Execution nodes for COPY */
	ExecNodes   *exec_nodes;

	/* Locator information */
	RelationLocInfo *rel_loc;		/* the locator key */
	int idx_dist_by_col;			/* index of the distributed by column */

	PGXCNodeHandle **connections;	/* Involved Datanode connections */
} RemoteCopyData;

/*
 * List of all the options used for query deparse step
 * As CopyStateData stays private in copy.c and in order not to
 * make Postgres-XC code too much intrusive in PostgreSQL code,
 * this intermediate structure is used primarily to generate remote
 * COPY queries based on deparsed options.
 */
typedef struct RemoteCopyOptions {
	bool		rco_binary;			/* binary format? */
	bool		rco_oids;			/* include OIDs? */
	bool		rco_csv_mode;		/* Comma Separated Value format? */
	char	   *rco_delim;			/* column delimiter (must be 1 byte) */
	char	   *rco_null_print;		/* NULL marker string (server encoding!) */
	char	   *rco_quote;			/* CSV quote char (must be 1 byte) */
	char	   *rco_escape;			/* CSV escape char (must be 1 byte) */
	List	   *rco_force_quote;	/* list of column names */
	List	   *rco_force_notnull;	/* list of column names */
} RemoteCopyOptions;

extern void RemoteCopy_BuildStatement(RemoteCopyData *state,
									  Relation rel,
									  RemoteCopyOptions *options,
									  List *attnamelist,
									  List *attnums);
extern void RemoteCopy_GetRelationLoc(RemoteCopyData *state,
									  Relation rel,
									  List *attnums);
extern RemoteCopyOptions *makeRemoteCopyOptions(void);
extern void FreeRemoteCopyData(RemoteCopyData *state);
extern void FreeRemoteCopyOptions(RemoteCopyOptions *options);
extern void pgxc_node_copybegin(RemoteCopyData *remoteCopyState, char node_type);
#endif
