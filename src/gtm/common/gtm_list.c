/*-------------------------------------------------------------------------
 *
 * gtm_list.c
 *	  implementation for PostgreSQL generic linked list package
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/nodes/list.c,v 1.70 2008/08/14 18:47:58 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "gtm/gtm_c.h"
#include "gtm/gtm.h"
#include "gtm/gtm_list.h"
#include "gtm/memutils.h"
#include "gtm/assert.h"

#define gtm_equal(a, b)		((a) == (b))


#ifdef USE_ASSERT_CHECKING
/*
 * Check that the specified gtm_List is valid (so far as we can tell).
 */
static void
check_list_invariants(gtm_List *list)
{
	if (list == gtm_NIL)
		return;

	Assert(list->length > 0);
	Assert(list->head != NULL);
	Assert(list->tail != NULL);

	if (list->length == 1)
		Assert(list->head == list->tail);
	if (list->length == 2)
		Assert(list->head->next == list->tail);
	Assert(list->tail->next == NULL);
}
#else
#define check_list_invariants(l)
#endif   /* USE_ASSERT_CHECKING */

/*
 * Return a freshly allocated gtm_List. Since empty non-gtm_NIL lists are
 * invalid, new_list() also allocates the head cell of the new list:
 * the caller should be sure to fill in that cell's data.
 */
static gtm_List *
new_list()
{
	gtm_List	   *new_list;
	gtm_ListCell   *new_head;

	new_head = (gtm_ListCell *) palloc(sizeof(*new_head));
	new_head->next = NULL;
	/* new_head->data is left undefined! */

	new_list = (gtm_List *) palloc(sizeof(*new_list));
	new_list->length = 1;
	new_list->head = new_head;
	new_list->tail = new_head;

	return new_list;
}

/*
 * Allocate a new cell and make it the head of the specified
 * list. Assumes the list it is passed is non-gtm_NIL.
 *
 * The data in the new head cell is undefined; the caller should be
 * sure to fill it in
 */
static void
new_head_cell(gtm_List *list)
{
	gtm_ListCell   *new_head;

	new_head = (gtm_ListCell *) palloc(sizeof(*new_head));
	new_head->next = list->head;

	list->head = new_head;
	list->length++;
}

/*
 * Allocate a new cell and make it the tail of the specified
 * list. Assumes the list it is passed is non-gtm_NIL.
 *
 * The data in the new tail cell is undefined; the caller should be
 * sure to fill it in
 */
static void
new_tail_cell(gtm_List *list)
{
	gtm_ListCell   *new_tail;

	new_tail = (gtm_ListCell *) palloc(sizeof(*new_tail));
	new_tail->next = NULL;

	list->tail->next = new_tail;
	list->tail = new_tail;
	list->length++;
}

/*
 * Append a pointer to the list. A pointer to the modified list is
 * returned. Note that this function may or may not destructively
 * modify the list; callers should always use this function's return
 * value, rather than continuing to use the pointer passed as the
 * first argument.
 */
gtm_List *
gtm_lappend(gtm_List *list, void *datum)
{
	if (list == gtm_NIL)
		list = new_list();
	else
		new_tail_cell(list);

	gtm_lfirst(list->tail) = datum;
	check_list_invariants(list);
	return list;
}

/*
 * Add a new cell to the list, in the position after 'prev_cell'. The
 * data in the cell is left undefined, and must be filled in by the
 * caller. 'list' is assumed to be non-gtm_NIL, and 'prev_cell' is assumed
 * to be non-NULL and a gtm_member of 'list'.
 */
static gtm_ListCell *
add_new_cell(gtm_List *list, gtm_ListCell *prev_cell)
{
	gtm_ListCell   *new_cell;

	new_cell = (gtm_ListCell *) palloc(sizeof(*new_cell));
	/* new_cell->data is left undefined! */
	new_cell->next = prev_cell->next;
	prev_cell->next = new_cell;

	if (list->tail == prev_cell)
		list->tail = new_cell;

	list->length++;

	return new_cell;
}

/*
 * Add a new cell to the specified list (which must be non-gtm_NIL);
 * it will be placed after the list cell 'prev' (which must be
 * non-NULL and a gtm_member of 'list'). The data placed in the new cell
 * is 'datum'. The newly-constructed cell is returned.
 */
gtm_ListCell *
gtm_lappend_cell(gtm_List *list, gtm_ListCell *prev, void *datum)
{
	gtm_ListCell   *new_cell;

	new_cell = add_new_cell(list, prev);
	gtm_lfirst(new_cell) = datum;
	check_list_invariants(list);
	return new_cell;
}

/*
 * Prepend a new element to the list. A pointer to the modified list
 * is returned. Note that this function may or may not destructively
 * modify the list; callers should always use this function's return
 * value, rather than continuing to use the pointer passed as the
 * second argument.
 */
gtm_List *
gtm_lcons(void *datum, gtm_List *list)
{
	if (list == gtm_NIL)
		list = new_list();
	else
		new_head_cell(list);

	gtm_lfirst(list->head) = datum;
	check_list_invariants(list);
	return list;
}

/*
 * Concatenate list2 to the end of list1, and return list1. list1 is
 * destructively changed. Callers should be sure to use the return
 * value as the new pointer to the concatenated list: the 'list1'
 * input pointer may or may not be the same as the returned pointer.
 *
 * The nodes in list2 are merely appended to the end of list1 in-place
 * (i.e. they aren't copied; the two lists will share some of the same
 * storage). Therefore, invoking gtm_list_free() on list2 will also
 * invalidate a portion of list1.
 */
gtm_List *
gtm_list_concat(gtm_List *list1, gtm_List *list2)
{
	if (list1 == gtm_NIL)
		return list2;
	if (list2 == gtm_NIL)
		return list1;
	if (list1 == list2)
		elog(ERROR, "cannot gtm_list_concat() a list to itself");


	list1->length += list2->length;
	list1->tail->next = list2->head;
	list1->tail = list2->tail;

	check_list_invariants(list1);
	return list1;
}

/*
 * Truncate 'list' to contain no more than 'new_size' elements. This
 * modifies the list in-place! Despite this, callers should use the
 * pointer returned by this function to refer to the newly truncated
 * list -- it may or may not be the same as the pointer that was
 * passed.
 *
 * Note that any cells removed by gtm_list_truncate() are NOT pfree'd.
 */
gtm_List *
gtm_list_truncate(gtm_List *list, int new_size)
{
	gtm_ListCell   *cell;
	int			n;

	if (new_size <= 0)
		return gtm_NIL;				/* truncate to zero length */

	/* If asked to effectively extend the list, do nothing */
	if (new_size >= gtm_list_length(list))
		return list;

	n = 1;
	gtm_foreach(cell, list)
	{
		if (n == new_size)
		{
			cell->next = NULL;
			list->tail = cell;
			list->length = new_size;
			check_list_invariants(list);
			return list;
		}
		n++;
	}

	/* keep the compiler quiet; never reached */
	Assert(false);
	return list;
}

/*
 * Locate the n'th cell (counting from 0) of the list.  It is an assertion
 * failure if there is no such cell.
 */
static gtm_ListCell *
list_nth_cell(gtm_List *list, int n)
{
	gtm_ListCell   *match;

	Assert(list != gtm_NIL);
	Assert(n >= 0);
	Assert(n < list->length);
	check_list_invariants(list);

	/* Does the caller actually mean to fetch the tail? */
	if (n == list->length - 1)
		return list->tail;

	for (match = list->head; n-- > 0; match = match->next)
		;

	return match;
}

/*
 * Return the data value contained in the n'th element of the
 * specified list. (gtm_List elements begin at 0.)
 */
void *
gtm_list_nth(gtm_List *list, int n)
{
	return gtm_lfirst(list_nth_cell(list, n));
}

/*
 * Return true if 'datum' is a gtm_member of the list. Equality is
 * determined via gtm_equal(), so callers should ensure that they pass a
 * Node as 'datum'.
 */
bool
gtm_list_member(gtm_List *list, void *datum)
{
	gtm_ListCell   *cell;

	check_list_invariants(list);

	gtm_foreach(cell, list)
	{
		if (gtm_equal(gtm_lfirst(cell), datum))
			return true;
	}

	return false;
}

/*
 * Return true if 'datum' is a gtm_member of the list. Equality is
 * determined by using simple pointer comparison.
 */
bool
gtm_list_member_ptr(gtm_List *list, void *datum)
{
	gtm_ListCell   *cell;

	check_list_invariants(list);

	gtm_foreach(cell, list)
	{
		if (gtm_lfirst(cell) == datum)
			return true;
	}

	return false;
}

/*
 * Delete 'cell' from 'list'; 'prev' is the previous element to 'cell'
 * in 'list', if any (i.e. prev == NULL iff list->head == cell)
 *
 * The cell is pfree'd, as is the gtm_List header if this was the last gtm_member.
 */
gtm_List *
gtm_list_delete_cell(gtm_List *list, gtm_ListCell *cell, gtm_ListCell *prev)
{
	check_list_invariants(list);
	Assert(prev != NULL ? gtm_lnext(prev) == cell : gtm_list_head(list) == cell);

	/*
	 * If we're about to delete the last node from the list, free the whole
	 * list instead and return gtm_NIL, which is the only valid representation of
	 * a zero-length list.
	 */
	if (list->length == 1)
	{
		gtm_list_free(list);
		return gtm_NIL;
	}

	/*
	 * Otherwise, adjust the necessary list links, deallocate the particular
	 * node we have just removed, and return the list we were given.
	 */
	list->length--;

	if (prev)
		prev->next = cell->next;
	else
		list->head = cell->next;

	if (list->tail == cell)
		list->tail = prev;

	pfree(cell);
	return list;
}

/*
 * Delete the first cell in list that matches datum, if any.
 * Equality is determined via gtm_equal().
 */
gtm_List *
gtm_list_delete(gtm_List *list, void *datum)
{
	gtm_ListCell   *cell;
	gtm_ListCell   *prev;

	check_list_invariants(list);

	prev = NULL;
	gtm_foreach(cell, list)
	{
		if (gtm_equal(gtm_lfirst(cell), datum))
			return gtm_list_delete_cell(list, cell, prev);

		prev = cell;
	}

	/* Didn't find a match: return the list unmodified */
	return list;
}

/* As above, but use simple pointer equality */
gtm_List *
gtm_list_delete_ptr(gtm_List *list, void *datum)
{
	gtm_ListCell   *cell;
	gtm_ListCell   *prev;

	check_list_invariants(list);

	prev = NULL;
	gtm_foreach(cell, list)
	{
		if (gtm_lfirst(cell) == datum)
			return gtm_list_delete_cell(list, cell, prev);

		prev = cell;
	}

	/* Didn't find a match: return the list unmodified */
	return list;
}


/*
 * Delete the first element of the list.
 *
 * This is useful to replace the Lisp-y code "list = gtm_lnext(list);" in cases
 * where the intent is to alter the list rather than just traverse it.
 * Beware that the removed cell is freed, whereas the gtm_lnext() coding leaves
 * the original list head intact if there's another pointer to it.
 */
gtm_List *
gtm_list_delete_first(gtm_List *list)
{
	check_list_invariants(list);

	if (list == gtm_NIL)
		return gtm_NIL;				/* would an error be better? */

	return gtm_list_delete_cell(list, gtm_list_head(list), NULL);
}

/*
 * Generate the union of two lists. This is calculated by copying
 * list1 via gtm_list_copy(), then adding to it all the members of list2
 * that aren't already in list1.
 *
 * Whether an element is already a member of the list is determined
 * via gtm_equal().
 *
 * The returned list is newly-allocated, although the content of the
 * cells is the same (i.e. any pointed-to objects are not copied).
 *
 * NB: this function will NOT remove any duplicates that are present
 * in list1 (so it only performs a "union" if list1 is known unique to
 * start with).  Also, if you are about to write "x = gtm_list_union(x, y)"
 * you probably want to use gtm_list_concat_unique() instead to avoid wasting
 * the list cells of the old x list.
 *
 * This function could probably be implemented a lot faster if it is a
 * performance bottleneck.
 */
gtm_List *
gtm_list_union(gtm_List *list1, gtm_List *list2)
{
	gtm_List	   *result;
	gtm_ListCell   *cell;

	result = gtm_list_copy(list1);
	gtm_foreach(cell, list2)
	{
		if (!gtm_list_member(result, gtm_lfirst(cell)))
			result = gtm_lappend(result, gtm_lfirst(cell));
	}

	check_list_invariants(result);
	return result;
}

/*
 * This variant of gtm_list_union() determines duplicates via simple
 * pointer comparison.
 */
gtm_List *
gtm_list_union_ptr(gtm_List *list1, gtm_List *list2)
{
	gtm_List	   *result;
	gtm_ListCell   *cell;


	result = gtm_list_copy(list1);
	gtm_foreach(cell, list2)
	{
		if (!gtm_list_member_ptr(result, gtm_lfirst(cell)))
			result = gtm_lappend(result, gtm_lfirst(cell));
	}

	check_list_invariants(result);
	return result;
}

/*
 * Return a list that contains all the cells that are in both list1 and
 * list2.  The returned list is freshly allocated via palloc(), but the
 * cells themselves point to the same objects as the cells of the
 * input lists.
 *
 * Duplicate entries in list1 will not be suppressed, so it's only a true
 * "intersection" if list1 is known unique beforehand.
 *
 * This variant works on lists of pointers, and determines list
 * membership via gtm_equal().  Note that the list1 gtm_member will be pointed
 * to in the result.
 */
gtm_List *
gtm_list_intersection(gtm_List *list1, gtm_List *list2)
{
	gtm_List	   *result;
	gtm_ListCell   *cell;

	if (list1 == gtm_NIL || list2 == gtm_NIL)
		return gtm_NIL;

	result = gtm_NIL;
	gtm_foreach(cell, list1)
	{
		if (gtm_list_member(list2, gtm_lfirst(cell)))
			result = gtm_lappend(result, gtm_lfirst(cell));
	}

	check_list_invariants(result);
	return result;
}

/*
 * Return a list that contains all the cells in list1 that are not in
 * list2. The returned list is freshly allocated via palloc(), but the
 * cells themselves point to the same objects as the cells of the
 * input lists.
 *
 * This variant works on lists of pointers, and determines list
 * membership via gtm_equal()
 */
gtm_List *
gtm_list_difference(gtm_List *list1, gtm_List *list2)
{
	gtm_ListCell   *cell;
	gtm_List	   *result = gtm_NIL;

	if (list2 == gtm_NIL)
		return gtm_list_copy(list1);

	gtm_foreach(cell, list1)
	{
		if (!gtm_list_member(list2, gtm_lfirst(cell)))
			result = gtm_lappend(result, gtm_lfirst(cell));
	}

	check_list_invariants(result);
	return result;
}

/*
 * This variant of gtm_list_difference() determines list membership via
 * simple pointer equality.
 */
gtm_List *
gtm_list_difference_ptr(gtm_List *list1, gtm_List *list2)
{
	gtm_ListCell   *cell;
	gtm_List	   *result = gtm_NIL;

	if (list2 == gtm_NIL)
		return gtm_list_copy(list1);

	gtm_foreach(cell, list1)
	{
		if (!gtm_list_member_ptr(list2, gtm_lfirst(cell)))
			result = gtm_lappend(result, gtm_lfirst(cell));
	}

	check_list_invariants(result);
	return result;
}

/*
 * Append datum to list, but only if it isn't already in the list.
 *
 * Whether an element is already a member of the list is determined
 * via gtm_equal().
 */
gtm_List *
gtm_list_append_unique(gtm_List *list, void *datum)
{
	if (gtm_list_member(list, datum))
		return list;
	else
		return gtm_lappend(list, datum);
}

/*
 * This variant of gtm_list_append_unique() determines list membership via
 * simple pointer equality.
 */
gtm_List *
gtm_list_append_unique_ptr(gtm_List *list, void *datum)
{
	if (gtm_list_member_ptr(list, datum))
		return list;
	else
		return gtm_lappend(list, datum);
}

/*
 * Append to list1 each member of list2 that isn't already in list1.
 *
 * Whether an element is already a member of the list is determined
 * via gtm_equal().
 *
 * This is almost the same functionality as gtm_list_union(), but list1 is
 * modified in-place rather than being copied.	Note also that list2's cells
 * are not inserted in list1, so the analogy to gtm_list_concat() isn't perfect.
 */
gtm_List *
gtm_list_concat_unique(gtm_List *list1, gtm_List *list2)
{
	gtm_ListCell   *cell;

	gtm_foreach(cell, list2)
	{
		if (!gtm_list_member(list1, gtm_lfirst(cell)))
			list1 = gtm_lappend(list1, gtm_lfirst(cell));
	}

	check_list_invariants(list1);
	return list1;
}

/*
 * This variant of gtm_list_concat_unique() determines list membership via
 * simple pointer equality.
 */
gtm_List *
gtm_list_concat_unique_ptr(gtm_List *list1, gtm_List *list2)
{
	gtm_ListCell   *cell;

	gtm_foreach(cell, list2)
	{
		if (!gtm_list_member_ptr(list1, gtm_lfirst(cell)))
			list1 = gtm_lappend(list1, gtm_lfirst(cell));
	}

	check_list_invariants(list1);
	return list1;
}

/*
 * Free all storage in a list, and optionally the pointed-to elements
 */
static void
list_free_private(gtm_List *list, bool deep)
{
	gtm_ListCell   *cell;

	check_list_invariants(list);

	cell = gtm_list_head(list);
	while (cell != NULL)
	{
		gtm_ListCell   *tmp = cell;

		cell = gtm_lnext(cell);
		if (deep)
			pfree(gtm_lfirst(tmp));
		pfree(tmp);
	}

	if (list)
		pfree(list);
}

/*
 * Free all the cells of the list, as well as the list itself. Any
 * objects that are pointed-to by the cells of the list are NOT
 * free'd.
 *
 * On return, the argument to this function has been freed, so the
 * caller would be wise to set it to gtm_NIL for safety's sake.
 */
void
gtm_list_free(gtm_List *list)
{
	list_free_private(list, false);
}

/*
 * Free all the cells of the list, the list itself, and all the
 * objects pointed-to by the cells of the list (each element in the
 * list must contain a pointer to a palloc()'d region of memory!)
 *
 * On return, the argument to this function has been freed, so the
 * caller would be wise to set it to gtm_NIL for safety's sake.
 */
void
gtm_list_free_deep(gtm_List *list)
{
	/*
	 * A "deep" free operation only makes sense on a list of pointers.
	 */
	list_free_private(list, true);
}

/*
 * Return a shallow copy of the specified list.
 */
gtm_List *
gtm_list_copy(gtm_List *oldlist)
{
	gtm_List	   *newlist;
	gtm_ListCell   *newlist_prev;
	gtm_ListCell   *oldlist_cur;

	if (oldlist == gtm_NIL)
		return gtm_NIL;

	newlist = new_list();
	newlist->length = oldlist->length;

	/*
	 * Copy over the data in the first cell; new_list() has already allocated
	 * the head cell itself
	 */
	newlist->head->data = oldlist->head->data;

	newlist_prev = newlist->head;
	oldlist_cur = oldlist->head->next;
	while (oldlist_cur)
	{
		gtm_ListCell   *newlist_cur;

		newlist_cur = (gtm_ListCell *) palloc(sizeof(*newlist_cur));
		newlist_cur->data = oldlist_cur->data;
		newlist_prev->next = newlist_cur;

		newlist_prev = newlist_cur;
		oldlist_cur = oldlist_cur->next;
	}

	newlist_prev->next = NULL;
	newlist->tail = newlist_prev;

	check_list_invariants(newlist);
	return newlist;
}

/*
 * Return a shallow copy of the specified list, without the first N elements.
 */
gtm_List *
gtm_list_copy_tail(gtm_List *oldlist, int nskip)
{
	gtm_List	   *newlist;
	gtm_ListCell   *newlist_prev;
	gtm_ListCell   *oldlist_cur;

	if (nskip < 0)
		nskip = 0;				/* would it be better to elog? */

	if (oldlist == gtm_NIL || nskip >= oldlist->length)
		return gtm_NIL;

	newlist = new_list();
	newlist->length = oldlist->length - nskip;

	/*
	 * Skip over the unwanted elements.
	 */
	oldlist_cur = oldlist->head;
	while (nskip-- > 0)
		oldlist_cur = oldlist_cur->next;

	/*
	 * Copy over the data in the first remaining cell; new_list() has already
	 * allocated the head cell itself
	 */
	newlist->head->data = oldlist_cur->data;

	newlist_prev = newlist->head;
	oldlist_cur = oldlist_cur->next;
	while (oldlist_cur)
	{
		gtm_ListCell   *newlist_cur;

		newlist_cur = (gtm_ListCell *) palloc(sizeof(*newlist_cur));
		newlist_cur->data = oldlist_cur->data;
		newlist_prev->next = newlist_cur;

		newlist_prev = newlist_cur;
		oldlist_cur = oldlist_cur->next;
	}

	newlist_prev->next = NULL;
	newlist->tail = newlist_prev;

	check_list_invariants(newlist);
	return newlist;
}

/*
 * When using non-GCC compilers, we can't define these as inline
 * functions in pg_list.h, so they are defined here.
 *
 * TODO: investigate supporting inlining for some non-GCC compilers.
 */
#ifndef __GNUC__

gtm_ListCell *
gtm_list_head(gtm_List *l)
{
	return l ? l->head : NULL;
}

gtm_ListCell *
gtm_list_tail(gtm_List *l)
{
	return l ? l->tail : NULL;
}

int
gtm_list_length(gtm_List *l)
{
	return l ? l->length : 0;
}
#endif   /* ! __GNUC__ */

/*
 * Temporary compatibility functions
 *
 * In order to avoid warnings for these function definitions, we need
 * to include a prototype here as well as in pg_list.h. That's because
 * we don't enable list API compatibility in list.c, so we
 * don't see the prototypes for these functions.
 */

/*
 * Given a list, return its length. This is merely defined for the
 * sake of backward compatibility: we can't afford to define a macro
 * called "length", so it must be a function. New code should use the
 * gtm_list_length() macro in order to avoid the overhead of a function
 * call.
 */
int			gtm_length(gtm_List *list);

int
gtm_length(gtm_List *list)
{
	return gtm_list_length(list);
}
