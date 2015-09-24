#ifndef BSD_RBTREE_H
#define BSD_RBTREE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>

typedef int (*bsd_rbto_compare_nodes_fn)(void *ctx, const void *node1, const void *node2);
typedef int (*bsd_rbto_compare_key_fn)(void *ctx, const void *node, const void *key);

typedef struct {
 bsd_rbto_compare_nodes_fn bsd_rbto_compare_nodes;
 bsd_rbto_compare_key_fn bsd_rbto_compare_key;
 size_t bsd_rbto_node_offset;
 void *bsd_rbto_context;
} bsd_rb_tree_ops_t;

typedef struct bsd_rb_node { void *opaque[3]; } bsd_rb_node_t;
typedef struct bsd_rb_tree { void *opaque[5]; } bsd_rb_tree_t;

void bsd_rb_tree_init(bsd_rb_tree_t *rbt, const bsd_rb_tree_ops_t *ops);
void* bsd_rb_tree_insert_node(bsd_rb_tree_t *rbt, void *rb);
void* bsd_rb_tree_find_node(bsd_rb_tree_t *rbt, const void *key);
void* bsd_rb_tree_find_node_geq(bsd_rb_tree_t *rbt, const void *key);
void* bsd_rb_tree_find_node_leq(bsd_rb_tree_t *rbt, const void *key);
void bsd_rb_tree_remove_node(bsd_rb_tree_t *rbt, void *object);
void* bsd_rb_tree_iterate(bsd_rb_tree_t *rbt, void *rb, const unsigned int direction);
size_t bsd_rb_tree_count(bsd_rb_tree_t *rbt);

#ifdef __cplusplus
}
#endif

#endif
