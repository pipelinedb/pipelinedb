/*-------------------------------------------------------------------------
 *
 * pgxc_group.h
 *	  definition of the system "PGXC group" relation (pgxc_group)
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/catalog/pgxc_group.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGXC_GROUP_H
#define PGXC_GROUP_H

#include "nodes/parsenodes.h"

#define PgxcGroupRelationId  9014

CATALOG(pgxc_group,9014) BKI_SHARED_RELATION
{
	NameData	group_name;			/* Group name */

	/* VARIABLE LENGTH FIELDS: */
	oidvector	group_members;		/* Group members */
} FormData_pgxc_group;

typedef FormData_pgxc_group *Form_pgxc_group;

#define Natts_pgxc_group		2

#define Anum_pgxc_group_name		1
#define Anum_pgxc_group_members		2

#endif   /* PGXC_GROUP_H */
