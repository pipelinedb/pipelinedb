/*-------------------------------------------------------------------------
 *
 * mutator.c
 *	  Implementation for mutating Queries
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "mutator.h"

/*
 * raw_expression_tree_mutator
 */
Node *
raw_expression_tree_mutator(Node *node, Node *(*mutator) (), void *context)
{
	ListCell *temp;

#define FLATCOPY(newnode, node, nodetype)  \
	( (newnode) = (nodetype *) palloc(sizeof(nodetype)), \
	  memcpy((newnode), (node), sizeof(nodetype)) )

#define CHECKFLATCOPY(newnode, node, nodetype)	\
	( AssertMacro(IsA((node), nodetype)), \
	  (newnode) = (nodetype *) palloc(sizeof(nodetype)), \
	  memcpy((newnode), (node), sizeof(nodetype)) )

#define MUTATE(newfield, oldfield, fieldtype)  \
		( (newfield) = (fieldtype) mutator((Node *) (oldfield), context) )

	/*
	 * The walker has already visited the current node, and so we need only
	 * recurse into any sub-nodes it has.
	 */
	if (node == NULL)
		return NULL;

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	switch (nodeTag(node))
	{
		case T_SetToDefault:
		case T_CurrentOfExpr:
		case T_SQLValueFunction:
		case T_Integer:
		case T_Float:
		case T_String:
		case T_BitString:
		case T_Null:
		case T_ParamRef:
		case T_A_Const:
		case T_A_Star:
		case T_Alias:
		case T_RangeVar:
			/* primitive node types with no subnodes */
			return copyObject(node);
		case T_GroupingFunc:
			{
				GroupingFunc *g = (GroupingFunc *) node;
				GroupingFunc *newnode;

				FLATCOPY(newnode, g, GroupingFunc);
				MUTATE(newnode->args, g->args, List *);
				MUTATE(newnode->refs, g->refs, List *);
				MUTATE(newnode->cols, g->cols, List *);

				return (Node *) newnode;
			}
		case T_SubLink:
			{
				SubLink *sublink = (SubLink *) node;
				SubLink *newnode;

				FLATCOPY(newnode, sublink, SubLink);
				MUTATE(newnode->testexpr, sublink->testexpr, Node *);
				MUTATE(newnode->subselect, sublink->subselect, Node *);

				return (Node *) sublink;
			}
			break;
		case T_CaseExpr:
			{
				CaseExpr *caseexpr = (CaseExpr *) node;
				CaseExpr *newnode;
				List *resultlist = NIL;

				FLATCOPY(newnode, caseexpr, CaseExpr);
				MUTATE(newnode->arg, caseexpr->arg, Expr *);
				MUTATE(newnode->defresult, caseexpr->defresult, Expr *);

				/* we assume walker doesn't care about CaseWhens, either */
				foreach(temp, caseexpr->args)
				{
					CaseWhen *when = lfirst_node(CaseWhen, temp);
					CaseWhen *newwhen;

					FLATCOPY(newwhen, when, CaseWhen);
					MUTATE(newwhen->expr, when->expr, Expr *);
					MUTATE(newwhen->result, when->result, Expr *);

					resultlist = lappend(resultlist, newwhen);
				}

				caseexpr->args = resultlist;
				return (Node *) caseexpr;
			}
			break;
		case T_RowExpr:
			{
				RowExpr *re = (RowExpr *) node;
				RowExpr *newnode;

				/* Assume colnames isn't interesting */
				FLATCOPY(newnode, re, RowExpr);
				MUTATE(newnode->args, re->args, List *);

				return (Node *) newnode;
			}
			break;
		case T_CoalesceExpr:
			{
				CoalesceExpr *ce = (CoalesceExpr *) node;
				CoalesceExpr *newnode;

				FLATCOPY(newnode, ce, CoalesceExpr);
				MUTATE(newnode->args, ce->args, List *);

				return (Node *) newnode;
			}
		case T_MinMaxExpr:
			{
				MinMaxExpr *mm = (MinMaxExpr *) node;
				MinMaxExpr *newnode;

				FLATCOPY(newnode, mm, MinMaxExpr);
				MUTATE(newnode->args, mm->args, List *);

				return (Node *) newnode;
			}
			break;
		case T_XmlExpr:
			{
				XmlExpr    *xexpr = (XmlExpr *) node;
				XmlExpr    *newnode;

				FLATCOPY(newnode, xexpr, XmlExpr);
				MUTATE(newnode->named_args, xexpr->named_args, List *);
				MUTATE(newnode->args, xexpr->args, List *);

				return (Node *) newnode;
			}
			break;
		case T_NullTest:
			{
				NullTest *nt = (NullTest *) node;
				NullTest *newnode;

				FLATCOPY(newnode, nt, NullTest);
				MUTATE(newnode->arg, nt->arg, Expr *);

				return (Node *) newnode;
			}
			break;
		case T_BooleanTest:
			{
				BooleanTest *bt = (BooleanTest *) node;
				BooleanTest *newnode;

				FLATCOPY(newnode, bt, BooleanTest);
				MUTATE(newnode->arg, bt->arg, Expr *);

				return (Node *) newnode;
			}
			break;
		case T_JoinExpr:
			{
				JoinExpr   *join = (JoinExpr *) node;
				JoinExpr   *newnode;

				FLATCOPY(newnode, join, JoinExpr);
				MUTATE(newnode->larg, join->larg, Node *);
				MUTATE(newnode->rarg, join->rarg, Node *);
				MUTATE(newnode->quals, join->quals, Node *);
				MUTATE(newnode->alias, join->alias, Alias *);

				return (Node *) newnode;
			}
			break;
		case T_List:
			{
				List *resultlist = NIL;

				foreach(temp, (List *) node)
				{
					resultlist = lappend(resultlist,
										 mutator((Node *) lfirst(temp),
												 context));
				}
				return (Node *) resultlist;
			}
			break;
		case T_SelectStmt:
			{
				SelectStmt *stmt = (SelectStmt *) node;
				SelectStmt *newnode;

				FLATCOPY(newnode, stmt, SelectStmt);
				MUTATE(newnode->distinctClause, stmt->distinctClause, List *);
				MUTATE(newnode->intoClause, stmt->intoClause, IntoClause *);
				MUTATE(newnode->targetList, stmt->targetList, List *);
				MUTATE(newnode->fromClause, stmt->fromClause, List *);
				MUTATE(newnode->whereClause, stmt->whereClause, Node *);
				MUTATE(newnode->havingClause, stmt->havingClause, Node *);
				MUTATE(newnode->windowClause, stmt->windowClause, List *);
				MUTATE(newnode->valuesLists, stmt->valuesLists, List *);
				MUTATE(newnode->sortClause, stmt->sortClause, List *);
				MUTATE(newnode->limitOffset, stmt->limitOffset, Node *);
				MUTATE(newnode->lockingClause, stmt->lockingClause, List *);
				MUTATE(newnode->withClause, stmt->withClause, WithClause *);
				MUTATE(newnode->larg, stmt->larg, SelectStmt *);
				MUTATE(newnode->rarg, stmt->rarg, SelectStmt *);

				return (Node *) newnode;
			}
			break;
		case T_A_Expr:
			{
				A_Expr *expr = (A_Expr *) node;
				A_Expr *newnode;

				FLATCOPY(newnode, expr, A_Expr);
				MUTATE(newnode->lexpr, expr->lexpr, Node *);
				MUTATE(newnode->rexpr, expr->rexpr, Node *);

				return (Node *) newnode;
			}
			break;
		case T_BoolExpr:
			{
				BoolExpr *be = (BoolExpr *) node;
				BoolExpr *newnode;

				FLATCOPY(newnode, be, BoolExpr);
				MUTATE(newnode->args, be->args, List *);

				return (Node *) newnode;
			}
		case T_ColumnRef:
			return mutator(node, context);
		case T_FuncCall:
			{
				FuncCall *fcall = (FuncCall *) node;
				FuncCall *newnode;

				FLATCOPY(newnode, fcall, FuncCall);
				MUTATE(newnode->args, fcall->args, List *);
				MUTATE(newnode->agg_order, fcall->agg_order, List *);
				MUTATE(newnode->agg_filter, fcall->agg_filter, Node *);
				MUTATE(newnode->over, fcall->over, WindowDef *);

				return (Node *) newnode;
			}
			break;
		case T_NamedArgExpr:
			{
				NamedArgExpr *na = (NamedArgExpr *) node;
				NamedArgExpr *newnode;

				FLATCOPY(newnode, na, NamedArgExpr);
				MUTATE(newnode->arg, na->arg, Expr *);

				return (Node *) newnode;
			}
			break;
		case T_A_Indices:
			{
				A_Indices *indices = (A_Indices *) node;
				A_Indices *newnode;

				FLATCOPY(newnode, indices, A_Indices);

				MUTATE(newnode->lidx, indices->lidx, Node *);
				MUTATE(newnode->uidx, indices->uidx, Node *);

				return (Node *) newnode;
			}
			break;
		case T_A_Indirection:
			{
				A_Indirection *indir = (A_Indirection *) node;
				A_Indirection *newnode;

				FLATCOPY(newnode, indir, A_Indirection);
				MUTATE(newnode->arg, indir->arg, Node *);
				MUTATE(newnode->indirection, indir->indirection, List *);

				return (Node *) newnode;
			}
			break;
		case T_A_ArrayExpr:
			{
				A_ArrayExpr *arr = (A_ArrayExpr *) node;
				A_ArrayExpr *newnode;

				FLATCOPY(newnode, arr, A_ArrayExpr);
				MUTATE(newnode->elements, arr->elements, List *);

				return (Node *) newnode;
			}
			break;
		case T_ResTarget:
			{
				ResTarget  *rt = (ResTarget *) node;
				ResTarget  *newnode;

				FLATCOPY(newnode, rt, ResTarget);
				MUTATE(newnode->val, rt->val, Node *);
				return (Node *) newnode;
			}
			break;
		case T_TypeCast:
			{
				TypeCast *tc = (TypeCast *) node;
				TypeCast *newnode;

				FLATCOPY(newnode, tc, TypeCast);
				MUTATE(newnode->arg, tc->arg, Node *);
				MUTATE(newnode->typeName, tc->typeName, TypeName *);

				return (Node *) newnode;
			}
			break;
		case T_CollateClause:
			{
				CollateClause *cc = (CollateClause *) node;
				CollateClause *newnode;

				FLATCOPY(newnode, cc, CollateClause);
				MUTATE(newnode->arg, cc->arg, Node *);

				return (Node *) newnode;
			}
			break;
		case T_SortBy:
			{
				SortBy *sb = (SortBy *) node;
				SortBy *newnode;

				FLATCOPY(newnode, sb, SortBy);
				MUTATE(newnode->node, sb->node, Node *);

				return (Node *) newnode;
			}
			break;
		case T_RangeSubselect:
			{
				RangeSubselect *rs = (RangeSubselect *) node;
				RangeSubselect *newnode;

				FLATCOPY(newnode, rs, RangeSubselect);
				MUTATE(newnode->subquery, rs->subquery, Node *);
				MUTATE(newnode->alias, rs->alias, Alias *);

				return (Node *) newnode;
			}
			break;
		case T_RangeFunction:
			{
				RangeFunction *rf = (RangeFunction *) node;
				RangeFunction *newnode;

				FLATCOPY(newnode, rf, RangeFunction);
				MUTATE(newnode->functions, rf->functions, List *);
				MUTATE(newnode->alias, rf->alias, Alias *);
				MUTATE(newnode->coldeflist, rf->coldeflist, List *);

				return (Node *) newnode;
			}
			break;
		case T_RangeTableSample:
			{
				RangeTableSample *rts = (RangeTableSample *) node;
				RangeTableSample *newnode;

				FLATCOPY(newnode, rts, RangeTableSample);
				MUTATE(newnode->relation, rts->relation, Node *);
				MUTATE(newnode->args, rts->args, List *);
				MUTATE(newnode->repeatable, rts->repeatable, Node *);

				return (Node *) newnode;
			}
			break;
		case T_RangeTableFunc:
			{
				RangeTableFunc *rtf = (RangeTableFunc *) node;
				RangeTableFunc *newnode;

				FLATCOPY(newnode, rtf, RangeTableFunc);
				MUTATE(newnode->docexpr, rtf->docexpr, Node *);
				MUTATE(newnode->rowexpr, rtf->rowexpr, Node *);
				MUTATE(newnode->namespaces, rtf->namespaces, List *);
				MUTATE(newnode->columns, rtf->columns, List *);
				MUTATE(newnode->alias, rtf->alias, Alias *);

				return (Node *) newnode;
			}
			break;
		case T_RangeTableFuncCol:
			{
				RangeTableFuncCol *rtfc = (RangeTableFuncCol *) node;
				RangeTableFuncCol *newnode;

				FLATCOPY(newnode, rtfc, RangeTableFuncCol);
				MUTATE(newnode->colexpr, rtfc->colexpr, Node *);
				MUTATE(newnode->coldefexpr, rtfc->coldefexpr, Node *);

				return (Node *) newnode;
			}
			break;
		case T_TypeName:
			{
				TypeName *tn = (TypeName *) node;
				TypeName *newnode;

				FLATCOPY(newnode, tn, TypeName);
				MUTATE(newnode->typmods, tn->typmods, List *);
				MUTATE(newnode->arrayBounds, tn->arrayBounds, List *);

				return (Node *) newnode;
			}
			break;
		case T_ColumnDef:
			{
				ColumnDef *coldef = (ColumnDef *) node;
				ColumnDef *newnode;

				FLATCOPY(newnode, coldef, ColumnDef);
				MUTATE(newnode->typeName, coldef->typeName, TypeName *);
				MUTATE(newnode->raw_default, coldef->raw_default, Node *);
				MUTATE(newnode->collClause, coldef->collClause, CollateClause *);

				return (Node *) newnode;
			}
			break;
		case T_GroupingSet:
			{
				GroupingSet *gs = (GroupingSet *) node;
				GroupingSet *newnode;

				FLATCOPY(newnode, gs, GroupingSet);
				MUTATE(newnode->content, gs->content, List *);

				return (Node *) newnode;
			}
			break;
		case T_XmlSerialize:
			{
				XmlSerialize *xs = (XmlSerialize *) node;
				XmlSerialize *newnode;

				FLATCOPY(newnode, xs, XmlSerialize);
				MUTATE(newnode->expr, xs->expr, Node *);
				MUTATE(newnode->typeName, xs->typeName, TypeName *);

				return (Node *) newnode;
			}
			break;
		case T_WithClause:
			{
				WithClause *wc = (WithClause *) node;
				WithClause *newnode;

				FLATCOPY(newnode, wc, WithClause);
				MUTATE(newnode->ctes, wc->ctes, List *);

				return (Node *) newnode;
			}
			break;
		case T_CommonTableExpr:
			{
				CommonTableExpr *cte = (CommonTableExpr *) node;
				CommonTableExpr *newnode;

				FLATCOPY(newnode, cte, CommonTableExpr);
				MUTATE(newnode->ctequery, cte->ctequery, Node *);

				return (Node *) newnode;
			}
			break;
		default:
			elog(ERROR, "unrecognized node type when mutating parse tree: %d",
				 (int) nodeTag(node));
			break;
	}

	return node;
}

