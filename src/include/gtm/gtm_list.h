/*-------------------------------------------------------------------------
 *
 * gtm_list.h
 *	  interface for PostgreSQL generic linked list package
 *
 * This package implements singly-linked homogeneous lists.
 *
 * It is important to have constant-time length, append, and prepend
 * operations. To achieve this, we deal with two distinct data
 * structures:
 *
 *		1. A set of "list cells": each cell contains a data field and
 *		   a link to the next cell in the list or NULL.
 *		2. A single structure containing metadata about the list: the
 *		   type of the list, pointers to the head and tail cells, and
 *		   the length of the list.
 *
 * We support three types of lists:
 *
 *	T_List: lists of pointers
 *		(in practice usually pointers to Nodes, but not always;
 *		declared as "void *" to minimize casting annoyances)
 *	T_IntList: lists of integers
 *	T_OidList: lists of Oids
 *
 * (At the moment, ints and Oids are the same size, but they may not
 * always be so; try to be careful to maintain the distinction.)
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL: pgsql/src/include/nodes/pg_list.h,v 1.59 2008/08/14 18:48:00 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef GTM_LIST_H
#define GTM_LIST_H


typedef struct gtm_ListCell gtm_ListCell;

typedef struct gtm_List
{
	int			length;
	gtm_ListCell   *head;
	gtm_ListCell   *tail;
} gtm_List;

struct gtm_ListCell
{
	union
	{
		void	   *ptr_value;
		int			int_value;
	}			data;
	gtm_ListCell   *next;
};

/*
 * The *only* valid representation of an empty list is gtm_NIL; in other
 * words, a non-gtm_NIL list is guaranteed to have length >= 1 and
 * head/tail != NULL
 */
#define gtm_NIL						((gtm_List *) NULL)

/*
 * These routines are used frequently. However, we can't implement
 * them as macros, since we want to avoid double-evaluation of macro
 * arguments. Therefore, we implement them using GCC inline functions,
 * and as regular functions with non-GCC compilers.
 */
#ifdef __GNUC__

static __inline__ gtm_ListCell *
gtm_list_head(gtm_List *l)
{
	return l ? l->head : NULL;
}

static __inline__ gtm_ListCell *
gtm_list_tail(gtm_List *l)
{
	return l ? l->tail : NULL;
}

static __inline__ int
gtm_list_length(gtm_List *l)
{
	return l ? l->length : 0;
}
#else

extern gtm_ListCell *gtm_list_head(gtm_List *l);
extern gtm_ListCell *gtm_list_tail(gtm_List *l);
extern int	gtm_list_length(gtm_List *l);
#endif   /* __GNUC__ */

/*
 * NB: There is an unfortunate legacy from a previous incarnation of
 * the gtm_List API: the macro gtm_lfirst() was used to mean "the data in this
 * cons cell". To avoid changing every usage of gtm_lfirst(), that meaning
 * has been kept. As a result, gtm_lfirst() takes a gtm_ListCell and returns
 * the data it contains; to get the data in the first cell of a
 * gtm_List, use gtm_linitial(). Worse, gtm_lsecond() is more closely related to
 * gtm_linitial() than gtm_lfirst(): given a gtm_List, gtm_lsecond() returns the data
 * in the second cons cell.
 */

#define gtm_lnext(lc)				((lc)->next)
#define gtm_lfirst(lc)				((lc)->data.ptr_value)
#define gtm_lfirst_int(lc)			((lc)->data.int_value)

#define gtm_linitial(l)				gtm_lfirst(gtm_list_head(l))
#define gtm_linitial_int(l)			gtm_lfirst_int(gtm_list_head(l))

#define gtm_lsecond(l)				gtm_lfirst(gtm_lnext(gtm_list_head(l)))
#define gtm_lsecond_int(l)			gtm_lfirst_int(gtm_lnext(gtm_list_head(l)))

#define gtm_lthird(l)				gtm_lfirst(gtm_lnext(gtm_lnext(gtm_list_head(l))))
#define gtm_lthird_int(l)			gtm_lfirst_int(gtm_lnext(gtm_lnext(gtm_list_head(l))))

#define gtm_lfourth(l)				gtm_lfirst(gtm_lnext(gtm_lnext(gtm_lnext(gtm_list_head(l)))))
#define gtm_lfourth_int(l)			gtm_lfirst_int(gtm_lnext(gtm_lnext(gtm_lnext(gtm_list_head(l)))))

#define gtm_llast(l)				gtm_lfirst(gtm_list_tail(l))
#define gtm_llast_int(l)			gtm_lfirst_int(gtm_list_tail(l))

/*
 * Convenience macros for building fixed-length lists
 */
#define gtm_list_make1(x1)				gtm_lcons(x1, gtm_NIL)
#define gtm_list_make2(x1,x2)			gtm_lcons(x1, gtm_list_make1(x2))
#define gtm_list_make3(x1,x2,x3)		gtm_lcons(x1, gtm_list_make2(x2, x3))
#define gtm_list_make4(x1,x2,x3,x4)		gtm_lcons(x1, gtm_list_make3(x2, x3, x4))

#define gtm_list_make1_int(x1)			gtm_lcons_int(x1, gtm_NIL)
#define gtm_list_make2_int(x1,x2)		gtm_lcons_int(x1, gtm_list_make1_int(x2))
#define gtm_list_make3_int(x1,x2,x3)	gtm_lcons_int(x1, gtm_list_make2_int(x2, x3))
#define gtm_list_make4_int(x1,x2,x3,x4) gtm_lcons_int(x1, gtm_list_make3_int(x2, x3, x4))

/*
 * gtm_foreach -
 *	  a convenience macro which loops through the list
 */
#define gtm_foreach(cell, l)	\
	for ((cell) = gtm_list_head(l); (cell) != NULL; (cell) = gtm_lnext(cell))

/*
 * gtm_for_each_cell -
 *	  a convenience macro which loops through a list starting from a
 *	  specified cell
 */
#define gtm_for_each_cell(cell, initcell)	\
	for ((cell) = (initcell); (cell) != NULL; (cell) = gtm_lnext(cell))

/*
 * gtm_forboth -
 *	  a convenience macro for advancing through two linked lists
 *	  simultaneously. This macro loops through both lists at the same
 *	  time, stopping when either list runs out of elements. Depending
 *	  on the requirements of the call site, it may also be wise to
 *	  assert that the lengths of the two lists are gtm_equal.
 */
#define gtm_forboth(cell1, list1, cell2, list2)							\
	for ((cell1) = gtm_list_head(list1), (cell2) = gtm_list_head(list2);	\
		 (cell1) != NULL && (cell2) != NULL;						\
		 (cell1) = gtm_lnext(cell1), (cell2) = gtm_lnext(cell2))

extern gtm_List *gtm_lappend(gtm_List *list, void *datum);
extern gtm_List *gtm_lappend_int(gtm_List *list, int datum);

extern gtm_ListCell *gtm_lappend_cell(gtm_List *list, gtm_ListCell *prev, void *datum);
extern gtm_ListCell *gtm_lappend_cell_int(gtm_List *list, gtm_ListCell *prev, int datum);

extern gtm_List *gtm_lcons(void *datum, gtm_List *list);
extern gtm_List *gtm_lcons_int(int datum, gtm_List *list);

extern gtm_List *gtm_list_concat(gtm_List *list1, gtm_List *list2);
extern gtm_List *gtm_list_truncate(gtm_List *list, int new_size);

extern void *gtm_list_nth(gtm_List *list, int n);
extern int	gtm_list_nth_int(gtm_List *list, int n);

extern bool gtm_list_member(gtm_List *list, void *datum);
extern bool gtm_list_member_ptr(gtm_List *list, void *datum);
extern bool gtm_list_member_int(gtm_List *list, int datum);

extern gtm_List *gtm_list_delete(gtm_List *list, void *datum);
extern gtm_List *gtm_list_delete_ptr(gtm_List *list, void *datum);
extern gtm_List *gtm_list_delete_int(gtm_List *list, int datum);
extern gtm_List *gtm_list_delete_first(gtm_List *list);
extern gtm_List *gtm_list_delete_cell(gtm_List *list, gtm_ListCell *cell, gtm_ListCell *prev);

extern gtm_List *gtm_list_union(gtm_List *list1, gtm_List *list2);
extern gtm_List *gtm_list_union_ptr(gtm_List *list1, gtm_List *list2);
extern gtm_List *gtm_list_union_int(gtm_List *list1, gtm_List *list2);

extern gtm_List *gtm_list_intersection(gtm_List *list1, gtm_List *list2);
/* currently, there's no need for list_intersection_int etc */

extern gtm_List *gtm_list_difference(gtm_List *list1, gtm_List *list2);
extern gtm_List *gtm_list_difference_ptr(gtm_List *list1, gtm_List *list2);
extern gtm_List *gtm_list_difference_int(gtm_List *list1, gtm_List *list2);

extern gtm_List *gtm_list_append_unique(gtm_List *list, void *datum);
extern gtm_List *gtm_list_append_unique_ptr(gtm_List *list, void *datum);
extern gtm_List *gtm_list_append_unique_int(gtm_List *list, int datum);

extern gtm_List *gtm_list_concat_unique(gtm_List *list1, gtm_List *list2);
extern gtm_List *gtm_list_concat_unique_ptr(gtm_List *list1, gtm_List *list2);
extern gtm_List *gtm_list_concat_unique_int(gtm_List *list1, gtm_List *list2);

extern void gtm_list_free(gtm_List *list);
extern void gtm_list_free_deep(gtm_List *list);

extern gtm_List *gtm_list_copy(gtm_List *list);
extern gtm_List *gtm_list_copy_tail(gtm_List *list, int nskip);

/*
 * To ease migration to the new list API, a set of compatibility
 * macros are provided that reduce the impact of the list API changes
 * as far as possible. Until client code has been rewritten to use the
 * new list API, the ENABLE_LIST_COMPAT symbol can be defined before
 * including pg_list.h
 */
#ifdef ENABLE_LIST_COMPAT

#define gtm_lfirsti(lc)					gtm_lfirst_int(lc)

#define gtm_makeList1(x1)				gtm_list_make1(x1)
#define gtm_makeList2(x1, x2)			gtm_list_make2(x1, x2)
#define gtm_makeList3(x1, x2, x3)		gtm_list_make3(x1, x2, x3)
#define gtm_makeList4(x1, x2, x3, x4)	gtm_list_make4(x1, x2, x3, x4)

#define gtm_makeListi1(x1)				gtm_list_make1_int(x1)
#define gtm_makeListi2(x1, x2)			gtm_list_make2_int(x1, x2)

#define gtm_lconsi(datum, list)			gtm_lcons_int(datum, list)

#define gtm_lappendi(list, datum)		gtm_lappend_int(list, datum)

#define gtm_nconc(l1, l2)				gtm_list_concat(l1, l2)

#define gtm_nth(n, list)				gtm_list_nth(list, n)

#define gtm_member(datum, list)			gtm_list_member(list, datum)
#define gtm_ptrMember(datum, list)		gtm_list_member_ptr(list, datum)
#define gtm_intMember(datum, list)		gtm_list_member_int(list, datum)

/*
 * Note that the old gtm_lremove() determined equality via pointer
 * comparison, whereas the new gtm_list_delete() uses gtm_equal(); in order to
 * keep the same behavior, we therefore need to map gtm_lremove() calls to
 * gtm_list_delete_ptr() rather than gtm_list_delete()
 */
#define gtm_lremove(elem, list)			gtm_list_delete_ptr(list, elem)
#define gtm_LispRemove(elem, list)		gtm_list_delete(list, elem)
#define gtm_lremovei(elem, list)		gtm_list_delete_int(list, elem)

#define gtm_ltruncate(n, list)			gtm_list_truncate(list, n)

#define gtm_set_union(l1, l2)			gtm_list_union(l1, l2)
#define gtm_set_ptrUnion(l1, l2)		gtm_list_union_ptr(l1, l2)

#define gtm_set_difference(l1, l2)		gtm_list_difference(l1, l2)
#define gtm_set_ptrDifference(l1, l2)	gtm_list_difference_ptr(l1, l2)

#define gtm_equali(l1, l2)				gtm_equal(l1, l2)
#define gtm_equalo(l1, l2)				gtm_equal(l1, l2)

#define gtm_freeList(list)				gtm_list_free(list)

#define gtm_listCopy(list)				gtm_list_copy(list)

extern int	gtm_length(gtm_List *list);
#endif   /* ENABLE_LIST_COMPAT */

#endif   /* GTM_LIST_H */
