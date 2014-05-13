/*-------------------------------------------------------------------------
 *
 * pgxc_node.h
 *	  definition of the system "PGXC node" relation (pgxc_node)
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/catalog/pgxc_node.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGXC_NODE_H
#define PGXC_NODE_H

#include "catalog/genbki.h"

#define PgxcNodeRelationId  9015

CATALOG(pgxc_node,9015) BKI_SHARED_RELATION
{
	NameData	node_name;

	/*
	 * Possible node types are defined as follows
	 * Types are defined below PGXC_NODES_XXXX
	 */
	char		node_type;

	/*
	 * Port number of the node to connect to
	 */
	int32 		node_port;

	/*
	 * Host name of IP address of the node to connect to
	 */
	NameData	node_host;

	/*
	 * Is this node primary
	 */
	bool		nodeis_primary;

	/*
	 * Is this node preferred
	 */
	bool		nodeis_preferred;

	/*
	 * Node identifier to be used at places where a fixed length node identification is required
	 */
	int32		node_id;
} FormData_pgxc_node;

typedef FormData_pgxc_node *Form_pgxc_node;

#define Natts_pgxc_node				7

#define Anum_pgxc_node_name			1
#define Anum_pgxc_node_type			2
#define Anum_pgxc_node_port			3
#define Anum_pgxc_node_host			4
#define Anum_pgxc_node_is_primary	5
#define Anum_pgxc_node_is_preferred	6
#define Anum_pgxc_node_id		7

/* Possible types of nodes */
#define PGXC_NODE_COORDINATOR		'C'
#define PGXC_NODE_DATANODE			'D'
#define PGXC_NODE_NONE				'N'

#endif   /* PGXC_NODE_H */
