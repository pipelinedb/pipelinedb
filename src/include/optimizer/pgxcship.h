/*-------------------------------------------------------------------------
 *
 * pgxcship.h
 *		Functionalities for the evaluation of expression shippability
 *		to remote nodes
 *
 *
 * Portions Copyright (c) 1996-2012 PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/optimizer/pgxcship.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PGXCSHIP_H
#define PGXCSHIP_H

#include "nodes/parsenodes.h"
#include "nodes/relation.h"
#include "pgxc/locator.h"


/* Determine if query is shippable */
extern ExecNodes *pgxc_is_query_shippable(Query *query, int query_level);
/* Determine if an expression is shippable */
extern bool pgxc_is_expr_shippable(Expr *node, bool *has_aggs);
/* Determine if given function is shippable */
extern bool pgxc_is_func_shippable(Oid funcid);
/* Check equijoin conditions on given relations */
extern Expr *pgxc_find_dist_equijoin_qual(List *dist_vars1, List *dist_vars2,
											Node *quals);
/* Merge given execution nodes based on join shippability conditions */
extern ExecNodes *pgxc_merge_exec_nodes(ExecNodes *en1, ExecNodes *en2);
/* Check if given Query includes distribution column */
extern bool pgxc_query_has_distcolgrouping(Query *query, ExecNodes *exec_nodes);
/* Check the shippability of an index */
extern bool pgxc_check_index_shippability(RelationLocInfo *relLocInfo,
									   bool is_primary,
									   bool is_unique,
									   bool is_exclusion,
									   List *indexAttrs,
									   List *indexExprs);
/* Check the shippability of a parent-child constraint */
extern bool pgxc_check_fk_shippability(RelationLocInfo *parentLocInfo,
									   RelationLocInfo *childLocInfo,
									   List *parentRefs,
									   List *childRefs);
extern bool pgxc_check_triggers_shippability(Oid relid, int commandType);
extern bool pgxc_find_nonshippable_row_trig(Relation rel, int16 tgtype_event,
									int16 tgtype_timing, bool ignore_timing);
#endif
