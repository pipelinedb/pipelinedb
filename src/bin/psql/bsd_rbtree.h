#ifndef BSD_RBTREE_H
#define BSD_RBTREE_H

/*-
 * Copyright (c) 2001 The NetBSD Foundation, Inc.
 * All rights reserved.
 *
 * This code is derived from software contributed to The NetBSD Foundation
 * by Matt Thomas <matt@3am-software.com>.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE NETBSD FOUNDATION, INC. AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE FOUNDATION OR CONTRIBUTORS
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * rbtree provides red-black trees.  A red-black tree is a binary search
 * tree with the node color as an extra attribute.  It fulfills a set of
 * conditions:
 * 
 *       1.   Every search path from the root to a leaf consists of the same
 *            number of black nodes.
 * 
 *       2.   Each red node (except for the root) has a black parent.
 * 
 *       3.   Each leaf node is black.
 * 
 * Every operation on a red-black tree is bounded as O(lg n).  The maximum
 * height of a red-black tree is 2lg (n+1).
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>

#define	BSD_RB_DIR_LEFT	0
#define	BSD_RB_DIR_RIGHT 1

/*
 * The node-comparison operator.  Defines an ordering on nodes.
 * Returns a negative value if the first node node1 precedes the
 * second node node2.  Returns a positive value if the first node
 * node1 follows the second node node2.  Returns 0 if the first
 * node node1 and the second node node2 are identical according to
 * the ordering.
 */
typedef int (*bsd_rbto_compare_nodes_fn)(void *ctx,
										 const void *node1,
										 const void *node2);
/*
 * The node-key comparison operator.  Defines the order of nodes
 * and keys.  Returns a negative value if the node node precedes
 * the key key.  Returns a positive value if the node node follows
 * the key key.  Returns 0 if the node node is identical to the key
 * key according to the ordering.
 */
typedef int (*bsd_rbto_compare_key_fn)(void *ctx,
									   const void *node,
									   const void *key);

/*
 * Defines the operator for comparing two nodes in the same tree,
 * the operator for comparing a node in the tree with a key, the
 * offset of member rb_node_t within a node, and the opaque context.
 */
typedef struct {
	bsd_rbto_compare_nodes_fn bsd_rbto_compare_nodes;
	bsd_rbto_compare_key_fn bsd_rbto_compare_key;
	size_t bsd_rbto_node_offset;
	void *bsd_rbto_context;
} bsd_rb_tree_ops_t;

/*
 * Defined this way to be opaque, but avoid dynamic memory alloc.
 * Sizes must match definition inside bsd_rbtree.c
 */
typedef struct bsd_rb_node { void *opaque[3]; } bsd_rb_node_t;
typedef struct bsd_rb_tree { void *opaque[5]; } bsd_rb_tree_t;

/*
 * Initialize the red-black tree rbt.  Let the comparison operators
 * given by ops define the order of nodes in the tree for the
 * purposes of insertion, search, and iteration.  rb_tree_init()
 * always succeeds.
 */
void bsd_rb_tree_init(bsd_rb_tree_t *rbt, const bsd_rb_tree_ops_t *ops);

/*
 * Insert the node rb into the tree rbt.  Return inserted node on
 * success, already existing node on failure.
 */
void* bsd_rb_tree_insert_node(bsd_rb_tree_t *rbt, void *rb);

/*
 * Search the tree rbt for a node that exactly matches key and
 * return it.  If no such node is present, return the first node
 * following key or, if no such node is in the tree, return NULL.
 */
void* bsd_rb_tree_find_node(bsd_rb_tree_t *rbt, const void *key);

/*
 * Search the tree rbt for a node that exactly matches key and
 * return it.  If no such node is present, return the first node
 * following key or, if no such node is in the tree, return NULL.
 */
void* bsd_rb_tree_find_node_geq(bsd_rb_tree_t *rbt, const void *key);

/*
 * Search the tree rbt for a node that exactly matches key and
 * return it.  If no such node is present, return the first node
 * preceding key or, if no such node is in the tree, return NULL.
 */
void* bsd_rb_tree_find_node_leq(bsd_rb_tree_t *rbt, const void *key);

/*
 * Remove the node rb from the tree rbt.
 */
void bsd_rb_tree_remove_node(bsd_rb_tree_t *rbt, void *object);

/*
 * If direction is BSD_RB_DIR_LEFT, return the node in the tree rbt
 * immediately preceding the node rb or, if rb is NULL, return the
 * first node in rbt or, if the tree is empty, return NULL.
 * 
 * If direction is BSD_RB_DIR_RIGHT, return the node in the tree rbt
 * immediately following the node rb or, if rb is NULL, return the
 * last node in rbt or, if the tree is empty, return NULL.
 */
void* bsd_rb_tree_iterate(bsd_rb_tree_t *rbt, void *rb, const unsigned int direction);

/*
 * Return the size of the tree
 */
size_t bsd_rb_tree_count(bsd_rb_tree_t *rbt);

#ifdef __cplusplus
}
#endif

#endif
