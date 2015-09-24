#include <sys/types.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>
#include <assert.h>
#include <stdio.h>

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)
#define __predict_false(x) __builtin_expect((x),0)

#define RBSTATS 1

typedef struct bsd_rb_node {
	struct bsd_rb_node *bsd_rb_nodes[2];

#define	BSD_RB_DIR_LEFT		0
#define	BSD_RB_DIR_RIGHT		1
#define	BSD_RB_DIR_OTHER		1
#define	bsd_rb_left			bsd_rb_nodes[BSD_RB_DIR_LEFT]
#define	bsd_rb_right		bsd_rb_nodes[BSD_RB_DIR_RIGHT]

	/*
	 * bsd_rb_info contains the two flags and the parent back pointer.
	 * We put the two flags in the low two bits since we know that
	 * bsd_rb_node will have an alignment of 4 or 8 bytes.
	 */
	uintptr_t bsd_rb_info;
#define	BSD_RB_FLAG_POSITION	0x2
#define	BSD_RB_FLAG_RED		0x1
#define	BSD_RB_FLAG_MASK		(BSD_RB_FLAG_POSITION|BSD_RB_FLAG_RED)
#define	BSD_RB_FATHER(rb) \
    ((struct bsd_rb_node *)((rb)->bsd_rb_info & ~BSD_RB_FLAG_MASK))
#define	BSD_RB_SET_FATHER(rb, father) \
    ((void)((rb)->bsd_rb_info = (uintptr_t)(father)|((rb)->bsd_rb_info & BSD_RB_FLAG_MASK)))

#define	BSD_RB_SENTINEL_P(rb)	((rb) == NULL)
#define	BSD_RB_LEFT_SENTINEL_P(rb)	BSD_RB_SENTINEL_P((rb)->bsd_rb_left)
#define	BSD_RB_RIGHT_SENTINEL_P(rb)	BSD_RB_SENTINEL_P((rb)->bsd_rb_right)
#define	BSD_RB_FATHER_SENTINEL_P(rb) BSD_RB_SENTINEL_P(BSD_RB_FATHER((rb)))
#define	BSD_RB_CHILDLESS_P(rb) \
    (BSD_RB_SENTINEL_P(rb) || (BSD_RB_LEFT_SENTINEL_P(rb) && BSD_RB_RIGHT_SENTINEL_P(rb)))
#define	BSD_RB_TWOCHILDREN_P(rb) \
    (!BSD_RB_SENTINEL_P(rb) && !BSD_RB_LEFT_SENTINEL_P(rb) && !BSD_RB_RIGHT_SENTINEL_P(rb))

#define	BSD_RB_POSITION(rb)	\
    (((rb)->bsd_rb_info & BSD_RB_FLAG_POSITION) ? BSD_RB_DIR_RIGHT : BSD_RB_DIR_LEFT)
#define	BSD_RB_RIGHT_P(rb)		(BSD_RB_POSITION(rb) == BSD_RB_DIR_RIGHT)
#define	BSD_RB_LEFT_P(rb)		(BSD_RB_POSITION(rb) == BSD_RB_DIR_LEFT)
#define	BSD_RB_RED_P(rb) 		(!BSD_RB_SENTINEL_P(rb) && ((rb)->bsd_rb_info & BSD_RB_FLAG_RED) != 0)
#define	BSD_RB_BLACK_P(rb) 		(BSD_RB_SENTINEL_P(rb) || ((rb)->bsd_rb_info & BSD_RB_FLAG_RED) == 0)
#define	BSD_RB_MARK_RED(rb) 	((void)((rb)->bsd_rb_info |= BSD_RB_FLAG_RED))
#define	BSD_RB_MARK_BLACK(rb) 	((void)((rb)->bsd_rb_info &= ~BSD_RB_FLAG_RED))
#define	BSD_RB_INVERT_COLOR(rb) 	((void)((rb)->bsd_rb_info ^= BSD_RB_FLAG_RED))
#define	BSD_RB_ROOT_P(rbt, rb)	((rbt)->rbt_root == (rb))
#define	BSD_RB_SET_POSITION(rb, position) \
    ((void)((position) ? ((rb)->bsd_rb_info |= BSD_RB_FLAG_POSITION) : \
    ((rb)->bsd_rb_info &= ~BSD_RB_FLAG_POSITION)))
#define	BSD_RB_ZERO_PROPERTIES(rb)	((void)((rb)->bsd_rb_info &= ~BSD_RB_FLAG_MASK))
#define	BSD_RB_COPY_PROPERTIES(dst, src) \
    ((void)((dst)->bsd_rb_info ^= ((dst)->bsd_rb_info ^ (src)->bsd_rb_info) & BSD_RB_FLAG_MASK))
#define BSD_RB_SWAP_PROPERTIES(a, b) do { \
    uintptr_t xorinfo = ((a)->bsd_rb_info ^ (b)->bsd_rb_info) & BSD_RB_FLAG_MASK; \
    (a)->bsd_rb_info ^= xorinfo; \
    (b)->bsd_rb_info ^= xorinfo; \
  } while (/*CONSTCOND*/ 0)
#ifdef RBDEBUG
	TAILQ_ENTRY(bsd_rb_node) bsd_rb_link;
#endif
} bsd_rb_node_t;

#define BSD_RB_TREE_MIN(T) bsd_rb_tree_iterate((T), NULL, BSD_RB_DIR_LEFT)
#define BSD_RB_TREE_MAX(T) bsd_rb_tree_iterate((T), NULL, BSD_RB_DIR_RIGHT)
#define BSD_RB_TREE_FOREACH(N, T) \
    for ((N) = BSD_RB_TREE_MIN(T); (N); \
	(N) = bsd_rb_tree_iterate((T), (N), BSD_RB_DIR_RIGHT))
#define BSD_RB_TREE_FOREACH_REVERSE(N, T) \
    for ((N) = BSD_RB_TREE_MAX(T); (N); \
	(N) = bsd_rb_tree_iterate((T), (N), BSD_RB_DIR_LEFT))

#ifdef RBDEBUG
TAILQ_HEAD(bsd_rb_node_qh, bsd_rb_node);

#define	BSD_RB_TAILQ_REMOVE(a, b, c)		TAILQ_REMOVE(a, b, c)
#define	BSD_RB_TAILQ_INIT(a)			TAILQ_INIT(a)
#define	BSD_RB_TAILQ_INSERT_HEAD(a, b, c)		TAILQ_INSERT_HEAD(a, b, c)
#define	BSD_RB_TAILQ_INSERT_BEFORE(a, b, c)		TAILQ_INSERT_BEFORE(a, b, c)
#define	BSD_RB_TAILQ_INSERT_AFTER(a, b, c, d)	TAILQ_INSERT_AFTER(a, b, c, d)
#else
#define	BSD_RB_TAILQ_REMOVE(a, b, c)		do { } while (/*CONSTCOND*/0)
#define	BSD_RB_TAILQ_INIT(a)			do { } while (/*CONSTCOND*/0)
#define	BSD_RB_TAILQ_INSERT_HEAD(a, b, c)		do { } while (/*CONSTCOND*/0)
#define	BSD_RB_TAILQ_INSERT_BEFORE(a, b, c)		do { } while (/*CONSTCOND*/0)
#define	BSD_RB_TAILQ_INSERT_AFTER(a, b, c, d)	do { } while (/*CONSTCOND*/0)
#endif /* RBDEBUG */

/*
 * bsd_rbto_compare_nodes_fn:
 *	return a positive value if the first node > the second node.
 *	return a negative value if the first node < the second node.
 *	return 0 if they are considered same.
 *
 * bsd_rbto_compare_key_fn:
 *	return a positive value if the node > the key.
 *	return a negative value if the node < the key.
 *	return 0 if they are considered same.
 */

typedef signed int (*const bsd_rbto_compare_nodes_fn)(void *,
    const void *, const void *);
typedef signed int (*const bsd_rbto_compare_key_fn)(void *,
    const void *, const void *);

typedef struct {
	bsd_rbto_compare_nodes_fn bsd_rbto_compare_nodes;
	bsd_rbto_compare_key_fn bsd_rbto_compare_key;
	size_t bsd_rbto_node_offset;
	void *bsd_rbto_context;
} bsd_rb_tree_ops_t;

typedef struct bsd_rb_tree {
	struct bsd_rb_node *rbt_root;
	const bsd_rb_tree_ops_t *rbt_ops;
	struct bsd_rb_node *rbt_minmax[2];

#ifdef RBDEBUG
	struct bsd_rb_node_qh rbt_nodes;
#endif

#ifdef RBSTATS
	size_t rbt_count;
#endif
} bsd_rb_tree_t;

#ifdef RBSTATS
#define	RBSTAT_INC(v)	((void)((v)++))
#define	RBSTAT_DEC(v)	((void)((v)--))
#else
#define	RBSTAT_INC(v)	do { } while (/*CONSTCOND*/0)
#define	RBSTAT_DEC(v)	do { } while (/*CONSTCOND*/0)
#endif

void	bsd_rb_tree_init(bsd_rb_tree_t *, const bsd_rb_tree_ops_t *);
void *	bsd_rb_tree_insert_node(bsd_rb_tree_t *, void *);
void *	bsd_rb_tree_find_node(bsd_rb_tree_t *, const void *);
void *	bsd_rb_tree_find_node_geq(bsd_rb_tree_t *, const void *);
void *	bsd_rb_tree_find_node_leq(bsd_rb_tree_t *, const void *);
void	bsd_rb_tree_remove_node(bsd_rb_tree_t *, void *);
void *	bsd_rb_tree_iterate(bsd_rb_tree_t *, void *, const unsigned int);
#ifdef RBDEBUG
void	bsd_rb_tree_check(const bsd_rb_tree_t *, bool);
#endif
#ifdef RBSTATS
size_t bsd_rb_tree_count(bsd_rb_tree_t *tree);
void	bsd_rb_tree_depths(const bsd_rb_tree_t *, size_t *);
#endif


#define	KASSERT(s)	assert(s)

static void bsd_rb_tree_insert_rebalance(struct bsd_rb_tree *, struct bsd_rb_node *);
static void bsd_rb_tree_removal_rebalance(struct bsd_rb_tree *, struct bsd_rb_node *,
	unsigned int);
#ifdef RBDEBUG
static const struct bsd_rb_node *bsd_rb_tree_iterate_const(const struct bsd_rb_tree *,
	const struct bsd_rb_node *, const unsigned int);
static bool bsd_rb_tree_check_node(const struct bsd_rb_tree *, const struct bsd_rb_node *,
	const struct bsd_rb_node *, bool);
#else
#define	bsd_rb_tree_check_node(a, b, c, d)	true
#endif

#define	BSD_RB_NODETOITEM(rbto, rbn)	\
    ((void *)((uintptr_t)(rbn) - (rbto)->bsd_rbto_node_offset))
#define	BSD_RB_ITEMTONODE(rbto, rbn)	\
    ((bsd_rb_node_t *)((uintptr_t)(rbn) + (rbto)->bsd_rbto_node_offset))

#define	BSD_RB_SENTINEL_NODE	NULL

void
bsd_rb_tree_init(struct bsd_rb_tree *rbt, const bsd_rb_tree_ops_t *ops)
{

	rbt->rbt_ops = ops;
	rbt->rbt_root = BSD_RB_SENTINEL_NODE;
	BSD_RB_TAILQ_INIT(&rbt->rbt_nodes);
#ifndef RBSMALL
	rbt->rbt_minmax[BSD_RB_DIR_LEFT] = rbt->rbt_root;	/* minimum node */
	rbt->rbt_minmax[BSD_RB_DIR_RIGHT] = rbt->rbt_root;	/* maximum node */
#endif
#ifdef RBSTATS
	rbt->rbt_count = 0;
#endif
}

void *
bsd_rb_tree_find_node(struct bsd_rb_tree *rbt, const void *key)
{
	const bsd_rb_tree_ops_t *rbto = rbt->rbt_ops;
	bsd_rbto_compare_key_fn compare_key = rbto->bsd_rbto_compare_key;
	struct bsd_rb_node *parent = rbt->rbt_root;

	while (!BSD_RB_SENTINEL_P(parent)) {
		void *pobj = BSD_RB_NODETOITEM(rbto, parent);
		const signed int diff = (*compare_key)(rbto->bsd_rbto_context,
		    pobj, key);
		if (diff == 0)
			return pobj;
		parent = parent->bsd_rb_nodes[diff < 0];
	}

	return NULL;
}

void *
bsd_rb_tree_find_node_geq(struct bsd_rb_tree *rbt, const void *key)
{
	const bsd_rb_tree_ops_t *rbto = rbt->rbt_ops;
	bsd_rbto_compare_key_fn compare_key = rbto->bsd_rbto_compare_key;
	struct bsd_rb_node *parent = rbt->rbt_root, *last = NULL;

	while (!BSD_RB_SENTINEL_P(parent)) {
		void *pobj = BSD_RB_NODETOITEM(rbto, parent);
		const signed int diff = (*compare_key)(rbto->bsd_rbto_context,
		    pobj, key);
		if (diff == 0)
			return pobj;
		if (diff > 0)
			last = parent;
		parent = parent->bsd_rb_nodes[diff < 0];
	}

	return last == NULL ? NULL : BSD_RB_NODETOITEM(rbto, last);
}

void *
bsd_rb_tree_find_node_leq(struct bsd_rb_tree *rbt, const void *key)
{
	const bsd_rb_tree_ops_t *rbto = rbt->rbt_ops;
	bsd_rbto_compare_key_fn compare_key = rbto->bsd_rbto_compare_key;
	struct bsd_rb_node *parent = rbt->rbt_root, *last = NULL;

	while (!BSD_RB_SENTINEL_P(parent)) {
		void *pobj = BSD_RB_NODETOITEM(rbto, parent);
		const signed int diff = (*compare_key)(rbto->bsd_rbto_context,
		    pobj, key);
		if (diff == 0)
			return pobj;
		if (diff < 0)
			last = parent;
		parent = parent->bsd_rb_nodes[diff < 0];
	}

	return last == NULL ? NULL : BSD_RB_NODETOITEM(rbto, last);
}

void *
bsd_rb_tree_insert_node(struct bsd_rb_tree *rbt, void *object)
{
	const bsd_rb_tree_ops_t *rbto = rbt->rbt_ops;
	bsd_rbto_compare_nodes_fn compare_nodes = rbto->bsd_rbto_compare_nodes;
	struct bsd_rb_node *parent, *tmp, *self = BSD_RB_ITEMTONODE(rbto, object);
	unsigned int position;
	bool rebalance;

	tmp = rbt->rbt_root;
	/*
	 * This is a hack.  Because rbt->rbt_root is just a struct bsd_rb_node *,
	 * just like bsd_rb_node->bsd_rb_nodes[BSD_RB_DIR_LEFT], we can use this fact to
	 * avoid a lot of tests for root and know that even at root,
	 * updating BSD_RB_FATHER(bsd_rb_node)->bsd_rb_nodes[BSD_RB_POSITION(bsd_rb_node)] will
	 * update rbt->rbt_root.
	 */
	parent = (struct bsd_rb_node *)(void *)&rbt->rbt_root;
	position = BSD_RB_DIR_LEFT;

	/*
	 * Find out where to place this new leaf.
	 */
	while (!BSD_RB_SENTINEL_P(tmp)) {
		void *tobj = BSD_RB_NODETOITEM(rbto, tmp);
		const signed int diff = (*compare_nodes)(rbto->bsd_rbto_context,
		    tobj, object);
		if (__predict_false(diff == 0)) {
			/*
			 * Node already exists; return it.
			 */
			return tobj;
		}
		parent = tmp;
		position = (diff < 0);
		tmp = parent->bsd_rb_nodes[position];
	}

#ifdef RBDEBUG
	{
		struct bsd_rb_node *prev = NULL, *next = NULL;

		if (position == BSD_RB_DIR_RIGHT)
			prev = parent;
		else if (tmp != rbt->rbt_root)
			next = parent;

		/*
		 * Verify our sequential position
		 */
		KASSERT(prev == NULL || !BSD_RB_SENTINEL_P(prev));
		KASSERT(next == NULL || !BSD_RB_SENTINEL_P(next));
		if (prev != NULL && next == NULL)
			next = TAILQ_NEXT(prev, bsd_rb_link);
		if (prev == NULL && next != NULL)
			prev = TAILQ_PREV(next, bsd_rb_node_qh, bsd_rb_link);
		KASSERT(prev == NULL || !BSD_RB_SENTINEL_P(prev));
		KASSERT(next == NULL || !BSD_RB_SENTINEL_P(next));
		KASSERT(prev == NULL || (*compare_nodes)(rbto->bsd_rbto_context,
		    BSD_RB_NODETOITEM(rbto, prev), BSD_RB_NODETOITEM(rbto, self)) < 0);
		KASSERT(next == NULL || (*compare_nodes)(rbto->bsd_rbto_context,
		    BSD_RB_NODETOITEM(rbto, self), BSD_RB_NODETOITEM(rbto, next)) < 0);
	}
#endif

	/*
	 * Initialize the node and insert as a leaf into the tree.
	 */
	BSD_RB_SET_FATHER(self, parent);
	BSD_RB_SET_POSITION(self, position);
	if (__predict_false(parent == (struct bsd_rb_node *)(void *)&rbt->rbt_root)) {
		BSD_RB_MARK_BLACK(self);		/* root is always black */
#ifndef RBSMALL
		rbt->rbt_minmax[BSD_RB_DIR_LEFT] = self;
		rbt->rbt_minmax[BSD_RB_DIR_RIGHT] = self;
#endif
		rebalance = false;
	} else {
		KASSERT(position == BSD_RB_DIR_LEFT || position == BSD_RB_DIR_RIGHT);
#ifndef RBSMALL
		/*
		 * Keep track of the minimum and maximum nodes.  If our
		 * parent is a minmax node and we on their min/max side,
		 * we must be the new min/max node.
		 */
		if (parent == rbt->rbt_minmax[position])
			rbt->rbt_minmax[position] = self;
#endif /* !RBSMALL */
		/*
		 * All new nodes are colored red.  We only need to rebalance
		 * if our parent is also red.
		 */
		BSD_RB_MARK_RED(self);
		rebalance = BSD_RB_RED_P(parent);
	}
	KASSERT(BSD_RB_SENTINEL_P(parent->bsd_rb_nodes[position]));
	self->bsd_rb_left = parent->bsd_rb_nodes[position];
	self->bsd_rb_right = parent->bsd_rb_nodes[position];
	parent->bsd_rb_nodes[position] = self;
	KASSERT(BSD_RB_CHILDLESS_P(self));

	/*
	 * Insert the new node into a sorted list for easy sequential access
	 */
	RBSTAT_INC(rbt->rbt_count);
#ifdef RBDEBUG
	if (BSD_RB_ROOT_P(rbt, self)) {
		BSD_RB_TAILQ_INSERT_HEAD(&rbt->rbt_nodes, self, bsd_rb_link);
	} else if (position == BSD_RB_DIR_LEFT) {
		KASSERT((*compare_nodes)(rbto->bsd_rbto_context,
		    BSD_RB_NODETOITEM(rbto, self),
		    BSD_RB_NODETOITEM(rbto, BSD_RB_FATHER(self))) < 0);
		BSD_RB_TAILQ_INSERT_BEFORE(BSD_RB_FATHER(self), self, bsd_rb_link);
	} else {
		KASSERT((*compare_nodes)(rbto->bsd_rbto_context,
		    BSD_RB_NODETOITEM(rbto, BSD_RB_FATHER(self)),
		    BSD_RB_NODETOITEM(rbto, self)) < 0);
		BSD_RB_TAILQ_INSERT_AFTER(&rbt->rbt_nodes, BSD_RB_FATHER(self),
		    self, bsd_rb_link);
	}
#endif
	KASSERT(bsd_rb_tree_check_node(rbt, self, NULL, !rebalance));

	/*
	 * Rebalance tree after insertion
	 */
	if (rebalance) {
		bsd_rb_tree_insert_rebalance(rbt, self);
		KASSERT(bsd_rb_tree_check_node(rbt, self, NULL, true));
	}

	/* Succesfully inserted, return our node pointer. */
	return object;
}

/*
 * Swap the location and colors of 'self' and its child @ which.  The child
 * can not be a sentinel node.  This is our rotation function.  However,
 * since it preserves coloring, it great simplifies both insertion and
 * removal since rotation almost always involves the exchanging of colors
 * as a separate step.
 */
/*ARGSUSED*/
static void
bsd_rb_tree_reparent_nodes(struct bsd_rb_tree *rbt, struct bsd_rb_node *old_father,
	const unsigned int which)
{
	const unsigned int other = which ^ BSD_RB_DIR_OTHER;
	struct bsd_rb_node * const grandpa = BSD_RB_FATHER(old_father);
	struct bsd_rb_node * const old_child = old_father->bsd_rb_nodes[which];
	struct bsd_rb_node * const new_father = old_child;
	struct bsd_rb_node * const new_child = old_father;

	KASSERT(which == BSD_RB_DIR_LEFT || which == BSD_RB_DIR_RIGHT);

	KASSERT(!BSD_RB_SENTINEL_P(old_child));
	KASSERT(BSD_RB_FATHER(old_child) == old_father);

	KASSERT(bsd_rb_tree_check_node(rbt, old_father, NULL, false));
	KASSERT(bsd_rb_tree_check_node(rbt, old_child, NULL, false));
	KASSERT(BSD_RB_ROOT_P(rbt, old_father) ||
	    bsd_rb_tree_check_node(rbt, grandpa, NULL, false));

	/*
	 * Exchange descendant linkages.
	 */
	grandpa->bsd_rb_nodes[BSD_RB_POSITION(old_father)] = new_father;
	new_child->bsd_rb_nodes[which] = old_child->bsd_rb_nodes[other];
	new_father->bsd_rb_nodes[other] = new_child;

	/*
	 * Update ancestor linkages
	 */
	BSD_RB_SET_FATHER(new_father, grandpa);
	BSD_RB_SET_FATHER(new_child, new_father);

	/*
	 * Exchange properties between new_father and new_child.  The only
	 * change is that new_child's position is now on the other side.
	 */
#if 0
	{
		struct bsd_rb_node tmp;
		tmp.bsd_rb_info = 0;
		BSD_RB_COPY_PROPERTIES(&tmp, old_child);
		BSD_RB_COPY_PROPERTIES(new_father, old_father);
		BSD_RB_COPY_PROPERTIES(new_child, &tmp);
	}
#else
	BSD_RB_SWAP_PROPERTIES(new_father, new_child);
#endif
	BSD_RB_SET_POSITION(new_child, other);

	/*
	 * Make sure to reparent the new child to ourself.
	 */
	if (!BSD_RB_SENTINEL_P(new_child->bsd_rb_nodes[which])) {
		BSD_RB_SET_FATHER(new_child->bsd_rb_nodes[which], new_child);
		BSD_RB_SET_POSITION(new_child->bsd_rb_nodes[which], which);
	}

	KASSERT(bsd_rb_tree_check_node(rbt, new_father, NULL, false));
	KASSERT(bsd_rb_tree_check_node(rbt, new_child, NULL, false));
	KASSERT(BSD_RB_ROOT_P(rbt, new_father) ||
	    bsd_rb_tree_check_node(rbt, grandpa, NULL, false));
}

static void
bsd_rb_tree_insert_rebalance(struct bsd_rb_tree *rbt, struct bsd_rb_node *self)
{
	struct bsd_rb_node * father = BSD_RB_FATHER(self);
	struct bsd_rb_node * grandpa = BSD_RB_FATHER(father);
	struct bsd_rb_node * uncle;
	unsigned int which;
	unsigned int other;

	KASSERT(!BSD_RB_ROOT_P(rbt, self));
	KASSERT(BSD_RB_RED_P(self));
	KASSERT(BSD_RB_RED_P(father));
	for (;;) {
		KASSERT(!BSD_RB_SENTINEL_P(self));

		KASSERT(BSD_RB_RED_P(self));
		KASSERT(BSD_RB_RED_P(father));
		/*
		 * We are red and our parent is red, therefore we must have a
		 * grandfather and he must be black.
		 */
		grandpa = BSD_RB_FATHER(father);
		KASSERT(BSD_RB_BLACK_P(grandpa));
		KASSERT(BSD_RB_DIR_RIGHT == 1 && BSD_RB_DIR_LEFT == 0);
		which = (father == grandpa->bsd_rb_right);
		other = which ^ BSD_RB_DIR_OTHER;
		uncle = grandpa->bsd_rb_nodes[other];

		if (BSD_RB_BLACK_P(uncle))
			break;

		/*
		 * Case 1: our uncle is red
		 *   Simply invert the colors of our parent and
		 *   uncle and make our grandparent red.  And
		 *   then solve the problem up at his level.
		 */
		BSD_RB_MARK_BLACK(uncle);
		BSD_RB_MARK_BLACK(father);
		if (__predict_false(BSD_RB_ROOT_P(rbt, grandpa))) {
			/*
			 * If our grandpa is root, don't bother
			 * setting him to red, just return.
			 */
			KASSERT(BSD_RB_BLACK_P(grandpa));
			return;
		}
		BSD_RB_MARK_RED(grandpa);
		self = grandpa;
		father = BSD_RB_FATHER(self);
		KASSERT(BSD_RB_RED_P(self));
		if (BSD_RB_BLACK_P(father)) {
			/*
			 * If our greatgrandpa is black, we're done.
			 */
			KASSERT(BSD_RB_BLACK_P(rbt->rbt_root));
			return;
		}
	}

	KASSERT(!BSD_RB_ROOT_P(rbt, self));
	KASSERT(BSD_RB_RED_P(self));
	KASSERT(BSD_RB_RED_P(father));
	KASSERT(BSD_RB_BLACK_P(uncle));
	KASSERT(BSD_RB_BLACK_P(grandpa));
	/*
	 * Case 2&3: our uncle is black.
	 */
	if (self == father->bsd_rb_nodes[other]) {
		/*
		 * Case 2: we are on the same side as our uncle
		 *   Swap ourselves with our parent so this case
		 *   becomes case 3.  Basically our parent becomes our
		 *   child.
		 */
		bsd_rb_tree_reparent_nodes(rbt, father, other);
		KASSERT(BSD_RB_FATHER(father) == self);
		KASSERT(self->bsd_rb_nodes[which] == father);
		KASSERT(BSD_RB_FATHER(self) == grandpa);
		self = father;
		father = BSD_RB_FATHER(self);
	}
	KASSERT(BSD_RB_RED_P(self) && BSD_RB_RED_P(father));
	KASSERT(grandpa->bsd_rb_nodes[which] == father);
	/*
	 * Case 3: we are opposite a child of a black uncle.
	 *   Swap our parent and grandparent.  Since our grandfather
	 *   is black, our father will become black and our new sibling
	 *   (former grandparent) will become red.
	 */
	bsd_rb_tree_reparent_nodes(rbt, grandpa, which);
	KASSERT(BSD_RB_FATHER(self) == father);
	KASSERT(BSD_RB_FATHER(self)->bsd_rb_nodes[BSD_RB_POSITION(self) ^ BSD_RB_DIR_OTHER] == grandpa);
	KASSERT(BSD_RB_RED_P(self));
	KASSERT(BSD_RB_BLACK_P(father));
	KASSERT(BSD_RB_RED_P(grandpa));

	/*
	 * Final step: Set the root to black.
	 */
	BSD_RB_MARK_BLACK(rbt->rbt_root);
}

static void
bsd_rb_tree_prune_node(struct bsd_rb_tree *rbt, struct bsd_rb_node *self, bool rebalance)
{
	const unsigned int which = BSD_RB_POSITION(self);
	struct bsd_rb_node *father = BSD_RB_FATHER(self);
#ifndef RBSMALL
	const bool was_root = BSD_RB_ROOT_P(rbt, self);
#endif

	KASSERT(rebalance || (BSD_RB_ROOT_P(rbt, self) || BSD_RB_RED_P(self)));
	KASSERT(!rebalance || BSD_RB_BLACK_P(self));
	KASSERT(BSD_RB_CHILDLESS_P(self));
	KASSERT(bsd_rb_tree_check_node(rbt, self, NULL, false));

	/*
	 * Since we are childless, we know that self->bsd_rb_left is pointing
	 * to the sentinel node.
	 */
	father->bsd_rb_nodes[which] = self->bsd_rb_left;

	/*
	 * Remove ourselves from the node list, decrement the count,
	 * and update min/max.
	 */
	BSD_RB_TAILQ_REMOVE(&rbt->rbt_nodes, self, bsd_rb_link);
	RBSTAT_DEC(rbt->rbt_count);
#ifndef RBSMALL
	if (__predict_false(rbt->rbt_minmax[BSD_RB_POSITION(self)] == self)) {
		rbt->rbt_minmax[BSD_RB_POSITION(self)] = father;
		/*
		 * When removing the root, rbt->rbt_minmax[BSD_RB_DIR_LEFT] is
		 * updated automatically, but we also need to update 
		 * rbt->rbt_minmax[BSD_RB_DIR_RIGHT];
		 */
		if (__predict_false(was_root)) {
			rbt->rbt_minmax[BSD_RB_DIR_RIGHT] = father;
		}
	}
	BSD_RB_SET_FATHER(self, NULL);
#endif

	/*
	 * Rebalance if requested.
	 */
	if (rebalance)
		bsd_rb_tree_removal_rebalance(rbt, father, which);
	KASSERT(was_root || bsd_rb_tree_check_node(rbt, father, NULL, true));
}

/*
 * When deleting an interior node
 */
static void
bsd_rb_tree_swap_prune_and_rebalance(struct bsd_rb_tree *rbt, struct bsd_rb_node *self,
	struct bsd_rb_node *standin)
{
	const unsigned int standin_which = BSD_RB_POSITION(standin);
	unsigned int standin_other = standin_which ^ BSD_RB_DIR_OTHER;
	struct bsd_rb_node *standin_son;
	struct bsd_rb_node *standin_father = BSD_RB_FATHER(standin);
	bool rebalance = BSD_RB_BLACK_P(standin);

	if (standin_father == self) {
		/*
		 * As a child of self, any childen would be opposite of
		 * our parent.
		 */
		KASSERT(BSD_RB_SENTINEL_P(standin->bsd_rb_nodes[standin_other]));
		standin_son = standin->bsd_rb_nodes[standin_which];
	} else {
		/*
		 * Since we aren't a child of self, any childen would be
		 * on the same side as our parent.
		 */
		KASSERT(BSD_RB_SENTINEL_P(standin->bsd_rb_nodes[standin_which]));
		standin_son = standin->bsd_rb_nodes[standin_other];
	}

	/*
	 * the node we are removing must have two children.
	 */
	KASSERT(BSD_RB_TWOCHILDREN_P(self));
	/*
	 * If standin has a child, it must be red.
	 */
	KASSERT(BSD_RB_SENTINEL_P(standin_son) || BSD_RB_RED_P(standin_son));

	/*
	 * Verify things are sane.
	 */
	KASSERT(bsd_rb_tree_check_node(rbt, self, NULL, false));
	KASSERT(bsd_rb_tree_check_node(rbt, standin, NULL, false));

	if (__predict_false(BSD_RB_RED_P(standin_son))) {
		/*
		 * We know we have a red child so if we flip it to black
		 * we don't have to rebalance.
		 */
		KASSERT(bsd_rb_tree_check_node(rbt, standin_son, NULL, true));
		BSD_RB_MARK_BLACK(standin_son);
		rebalance = false;

		if (standin_father == self) {
			KASSERT(BSD_RB_POSITION(standin_son) == standin_which);
		} else {
			KASSERT(BSD_RB_POSITION(standin_son) == standin_other);
			/*
			 * Change the son's parentage to point to his grandpa.
			 */
			BSD_RB_SET_FATHER(standin_son, standin_father);
			BSD_RB_SET_POSITION(standin_son, standin_which);
		}
	}

	if (standin_father == self) {
		/*
		 * If we are about to delete the standin's father, then when
		 * we call rebalance, we need to use ourselves as our father.
		 * Otherwise remember our original father.  Also, sincef we are
		 * our standin's father we only need to reparent the standin's
		 * brother.
		 *
		 * |    R      -->     S    |
		 * |  Q   S    -->   Q   T  |
		 * |        t  -->          |
		 */
		KASSERT(BSD_RB_SENTINEL_P(standin->bsd_rb_nodes[standin_other]));
		KASSERT(!BSD_RB_SENTINEL_P(self->bsd_rb_nodes[standin_other]));
		KASSERT(self->bsd_rb_nodes[standin_which] == standin);
		/*
		 * Have our son/standin adopt his brother as his new son.
		 */
		standin_father = standin;
	} else {
		/*
		 * |    R          -->    S       .  |
		 * |   / \  |   T  -->   / \  |  /   |
		 * |  ..... | S    -->  ..... | T    |
		 *
		 * Sever standin's connection to his father.
		 */
		standin_father->bsd_rb_nodes[standin_which] = standin_son;
		/*
		 * Adopt the far son.
		 */
		standin->bsd_rb_nodes[standin_other] = self->bsd_rb_nodes[standin_other];
		BSD_RB_SET_FATHER(standin->bsd_rb_nodes[standin_other], standin);
		KASSERT(BSD_RB_POSITION(self->bsd_rb_nodes[standin_other]) == standin_other);
		/*
		 * Use standin_other because we need to preserve standin_which
		 * for the removal_rebalance.
		 */
		standin_other = standin_which;
	}

	/*
	 * Move the only remaining son to our standin.  If our standin is our
	 * son, this will be the only son needed to be moved.
	 */
	KASSERT(standin->bsd_rb_nodes[standin_other] != self->bsd_rb_nodes[standin_other]);
	standin->bsd_rb_nodes[standin_other] = self->bsd_rb_nodes[standin_other];
	BSD_RB_SET_FATHER(standin->bsd_rb_nodes[standin_other], standin);

	/*
	 * Now copy the result of self to standin and then replace
	 * self with standin in the tree.
	 */
	BSD_RB_COPY_PROPERTIES(standin, self);
	BSD_RB_SET_FATHER(standin, BSD_RB_FATHER(self));
	BSD_RB_FATHER(standin)->bsd_rb_nodes[BSD_RB_POSITION(standin)] = standin;

	/*
	 * Remove ourselves from the node list, decrement the count,
	 * and update min/max.
	 */
	BSD_RB_TAILQ_REMOVE(&rbt->rbt_nodes, self, bsd_rb_link);
	RBSTAT_DEC(rbt->rbt_count);
#ifndef RBSMALL
	if (__predict_false(rbt->rbt_minmax[BSD_RB_POSITION(self)] == self))
		rbt->rbt_minmax[BSD_RB_POSITION(self)] = BSD_RB_FATHER(self);
	BSD_RB_SET_FATHER(self, NULL);
#endif

	KASSERT(bsd_rb_tree_check_node(rbt, standin, NULL, false));
	KASSERT(BSD_RB_FATHER_SENTINEL_P(standin)
		|| bsd_rb_tree_check_node(rbt, standin_father, NULL, false));
	KASSERT(BSD_RB_LEFT_SENTINEL_P(standin)
		|| bsd_rb_tree_check_node(rbt, standin->bsd_rb_left, NULL, false));
	KASSERT(BSD_RB_RIGHT_SENTINEL_P(standin)
		|| bsd_rb_tree_check_node(rbt, standin->bsd_rb_right, NULL, false));

	if (!rebalance)
		return;

	bsd_rb_tree_removal_rebalance(rbt, standin_father, standin_which);
	KASSERT(bsd_rb_tree_check_node(rbt, standin, NULL, true));
}

/*
 * We could do this by doing
 *	bsd_rb_tree_node_swap(rbt, self, which);
 *	bsd_rb_tree_prune_node(rbt, self, false);
 *
 * But it's more efficient to just evalate and recolor the child.
 */
static void
bsd_rb_tree_prune_blackred_branch(struct bsd_rb_tree *rbt, struct bsd_rb_node *self,
	unsigned int which)
{
	struct bsd_rb_node *father = BSD_RB_FATHER(self);
	struct bsd_rb_node *son = self->bsd_rb_nodes[which];
#ifndef RBSMALL
	const bool was_root = BSD_RB_ROOT_P(rbt, self);
#endif

	KASSERT(which == BSD_RB_DIR_LEFT || which == BSD_RB_DIR_RIGHT);
	KASSERT(BSD_RB_BLACK_P(self) && BSD_RB_RED_P(son));
	KASSERT(!BSD_RB_TWOCHILDREN_P(son));
	KASSERT(BSD_RB_CHILDLESS_P(son));
	KASSERT(bsd_rb_tree_check_node(rbt, self, NULL, false));
	KASSERT(bsd_rb_tree_check_node(rbt, son, NULL, false));

	/*
	 * Remove ourselves from the tree and give our former child our
	 * properties (position, color, root).
	 */
	BSD_RB_COPY_PROPERTIES(son, self);
	father->bsd_rb_nodes[BSD_RB_POSITION(son)] = son;
	BSD_RB_SET_FATHER(son, father);

	/*
	 * Remove ourselves from the node list, decrement the count,
	 * and update minmax.
	 */
	BSD_RB_TAILQ_REMOVE(&rbt->rbt_nodes, self, bsd_rb_link);
	RBSTAT_DEC(rbt->rbt_count);
#ifndef RBSMALL
	if (__predict_false(was_root)) {
		KASSERT(rbt->rbt_minmax[which] == son);
		rbt->rbt_minmax[which ^ BSD_RB_DIR_OTHER] = son;
	} else if (rbt->rbt_minmax[BSD_RB_POSITION(self)] == self) {
		rbt->rbt_minmax[BSD_RB_POSITION(self)] = son;
	}
	BSD_RB_SET_FATHER(self, NULL);
#endif

	KASSERT(was_root || bsd_rb_tree_check_node(rbt, father, NULL, true));
	KASSERT(bsd_rb_tree_check_node(rbt, son, NULL, true));
}

void
bsd_rb_tree_remove_node(struct bsd_rb_tree *rbt, void *object)
{
	const bsd_rb_tree_ops_t *rbto = rbt->rbt_ops;
	struct bsd_rb_node *standin, *self = BSD_RB_ITEMTONODE(rbto, object);
	unsigned int which;

	KASSERT(!BSD_RB_SENTINEL_P(self));

	/*
	 * In the following diagrams, we (the node to be removed) are S.  Red
	 * nodes are lowercase.  T could be either red or black.
	 *
	 * Remember the major axiom of the red-black tree: the number of
	 * black nodes from the root to each leaf is constant across all
	 * leaves, only the number of red nodes varies.
	 *
	 * Thus removing a red leaf doesn't require any other changes to a
	 * red-black tree.  So if we must remove a node, attempt to rearrange
	 * the tree so we can remove a red node.
	 *
	 * The simpliest case is a childless red node or a childless root node:
	 *
	 * |    T  -->    T  |    or    |  R  -->  *  |
	 * |  s    -->  *    |
	 */
	if (BSD_RB_CHILDLESS_P(self)) {
		const bool rebalance = BSD_RB_BLACK_P(self) && !BSD_RB_ROOT_P(rbt, self);
		bsd_rb_tree_prune_node(rbt, self, rebalance);
		return;
	}
	KASSERT(!BSD_RB_CHILDLESS_P(self));
	if (!BSD_RB_TWOCHILDREN_P(self)) {
		/*
		 * The next simpliest case is the node we are deleting is
		 * black and has one red child.
		 *
		 * |      T  -->      T  -->      T  |
		 * |    S    -->  R      -->  R      |
		 * |  r      -->    s    -->    *    |
		 */
		which = BSD_RB_LEFT_SENTINEL_P(self) ? BSD_RB_DIR_RIGHT : BSD_RB_DIR_LEFT;
		KASSERT(BSD_RB_BLACK_P(self));
		KASSERT(BSD_RB_RED_P(self->bsd_rb_nodes[which]));
		KASSERT(BSD_RB_CHILDLESS_P(self->bsd_rb_nodes[which]));
		bsd_rb_tree_prune_blackred_branch(rbt, self, which);
		return;
	}
	KASSERT(BSD_RB_TWOCHILDREN_P(self));

	/*
	 * We invert these because we prefer to remove from the inside of
	 * the tree.
	 */
	which = BSD_RB_POSITION(self) ^ BSD_RB_DIR_OTHER;

	/*
	 * Let's find the node closes to us opposite of our parent
	 * Now swap it with ourself, "prune" it, and rebalance, if needed.
	 */
	standin = BSD_RB_ITEMTONODE(rbto, bsd_rb_tree_iterate(rbt, object, which));
	bsd_rb_tree_swap_prune_and_rebalance(rbt, self, standin);
}

static void
bsd_rb_tree_removal_rebalance(struct bsd_rb_tree *rbt, struct bsd_rb_node *parent,
	unsigned int which)
{
	KASSERT(!BSD_RB_SENTINEL_P(parent));
	KASSERT(BSD_RB_SENTINEL_P(parent->bsd_rb_nodes[which]));
	KASSERT(which == BSD_RB_DIR_LEFT || which == BSD_RB_DIR_RIGHT);

	while (BSD_RB_BLACK_P(parent->bsd_rb_nodes[which])) {
		unsigned int other = which ^ BSD_RB_DIR_OTHER;
		struct bsd_rb_node *brother = parent->bsd_rb_nodes[other];

		KASSERT(!BSD_RB_SENTINEL_P(brother));
		/*
		 * For cases 1, 2a, and 2b, our brother's children must
		 * be black and our father must be black
		 */
		if (BSD_RB_BLACK_P(parent)
		    && BSD_RB_BLACK_P(brother->bsd_rb_left)
		    && BSD_RB_BLACK_P(brother->bsd_rb_right)) {
			if (BSD_RB_RED_P(brother)) {
				/*
				 * Case 1: Our brother is red, swap its
				 * position (and colors) with our parent. 
				 * This should now be case 2b (unless C or E
				 * has a red child which is case 3; thus no
				 * explicit branch to case 2b).
				 *
				 *    B         ->        D
				 *  A     d     ->    b     E
				 *      C   E   ->  A   C
				 */
				KASSERT(BSD_RB_BLACK_P(parent));
				bsd_rb_tree_reparent_nodes(rbt, parent, other);
				brother = parent->bsd_rb_nodes[other];
				KASSERT(!BSD_RB_SENTINEL_P(brother));
				KASSERT(BSD_RB_RED_P(parent));
				KASSERT(BSD_RB_BLACK_P(brother));
				KASSERT(bsd_rb_tree_check_node(rbt, brother, NULL, false));
				KASSERT(bsd_rb_tree_check_node(rbt, parent, NULL, false));
			} else {
				/*
				 * Both our parent and brother are black.
				 * Change our brother to red, advance up rank
				 * and go through the loop again.
				 *
				 *    B         ->   *B
				 * *A     D     ->  A     d
				 *      C   E   ->      C   E
				 */
				BSD_RB_MARK_RED(brother);
				KASSERT(BSD_RB_BLACK_P(brother->bsd_rb_left));
				KASSERT(BSD_RB_BLACK_P(brother->bsd_rb_right));
				if (BSD_RB_ROOT_P(rbt, parent))
					return;	/* root == parent == black */
				KASSERT(bsd_rb_tree_check_node(rbt, brother, NULL, false));
				KASSERT(bsd_rb_tree_check_node(rbt, parent, NULL, false));
				which = BSD_RB_POSITION(parent);
				parent = BSD_RB_FATHER(parent);
				continue;
			}
		}
		/*
		 * Avoid an else here so that case 2a above can hit either
		 * case 2b, 3, or 4.
		 */
		if (BSD_RB_RED_P(parent)
		    && BSD_RB_BLACK_P(brother)
		    && BSD_RB_BLACK_P(brother->bsd_rb_left)
		    && BSD_RB_BLACK_P(brother->bsd_rb_right)) {
			KASSERT(BSD_RB_RED_P(parent));
			KASSERT(BSD_RB_BLACK_P(brother));
			KASSERT(BSD_RB_BLACK_P(brother->bsd_rb_left));
			KASSERT(BSD_RB_BLACK_P(brother->bsd_rb_right));
			/*
			 * We are black, our father is red, our brother and
			 * both nephews are black.  Simply invert/exchange the
			 * colors of our father and brother (to black and red
			 * respectively).
			 *
			 *	|    f        -->    F        |
			 *	|  *     B    -->  *     b    |
			 *	|      N   N  -->      N   N  |
			 */
			BSD_RB_MARK_BLACK(parent);
			BSD_RB_MARK_RED(brother);
			KASSERT(bsd_rb_tree_check_node(rbt, brother, NULL, true));
			break;		/* We're done! */
		} else {
			/*
			 * Our brother must be black and have at least one
			 * red child (it may have two).
			 */
			KASSERT(BSD_RB_BLACK_P(brother));
			KASSERT(BSD_RB_RED_P(brother->bsd_rb_nodes[which]) ||
				BSD_RB_RED_P(brother->bsd_rb_nodes[other]));
			if (BSD_RB_BLACK_P(brother->bsd_rb_nodes[other])) {
				/*
				 * Case 3: our brother is black, our near
				 * nephew is red, and our far nephew is black.
				 * Swap our brother with our near nephew.  
				 * This result in a tree that matches case 4.
				 * (Our father could be red or black).
				 *
				 *	|    F      -->    F      |
				 *	|  x     B  -->  x   B    |
				 *	|      n    -->        n  |
				 */
				KASSERT(BSD_RB_RED_P(brother->bsd_rb_nodes[which]));
				bsd_rb_tree_reparent_nodes(rbt, brother, which);
				KASSERT(BSD_RB_FATHER(brother) == parent->bsd_rb_nodes[other]);
				brother = parent->bsd_rb_nodes[other];
				KASSERT(BSD_RB_RED_P(brother->bsd_rb_nodes[other]));
			}
			/*
			 * Case 4: our brother is black and our far nephew
			 * is red.  Swap our father and brother locations and
			 * change our far nephew to black.  (these can be
			 * done in either order so we change the color first).
			 * The result is a valid red-black tree and is a
			 * terminal case.  (again we don't care about the
			 * father's color)
			 *
			 * If the father is red, we will get a red-black-black
			 * tree:
			 *	|  f      ->  f      -->    b    |
			 *	|    B    ->    B    -->  F   N  |
			 *	|      n  ->      N  -->         |
			 *
			 * If the father is black, we will get an all black
			 * tree:
			 *	|  F      ->  F      -->    B    |
			 *	|    B    ->    B    -->  F   N  |
			 *	|      n  ->      N  -->         |
			 *
			 * If we had two red nephews, then after the swap,
			 * our former father would have a red grandson. 
			 */
			KASSERT(BSD_RB_BLACK_P(brother));
			KASSERT(BSD_RB_RED_P(brother->bsd_rb_nodes[other]));
			BSD_RB_MARK_BLACK(brother->bsd_rb_nodes[other]);
			bsd_rb_tree_reparent_nodes(rbt, parent, other);
			break;		/* We're done! */
		}
	}
	KASSERT(bsd_rb_tree_check_node(rbt, parent, NULL, true));
}

void *
bsd_rb_tree_iterate(struct bsd_rb_tree *rbt, void *object, const unsigned int direction)
{
	const bsd_rb_tree_ops_t *rbto = rbt->rbt_ops;
	const unsigned int other = direction ^ BSD_RB_DIR_OTHER;
	struct bsd_rb_node *self;

	KASSERT(direction == BSD_RB_DIR_LEFT || direction == BSD_RB_DIR_RIGHT);

	if (object == NULL) {
#ifndef RBSMALL
		if (BSD_RB_SENTINEL_P(rbt->rbt_root))
			return NULL;
		return BSD_RB_NODETOITEM(rbto, rbt->rbt_minmax[direction]);
#else
		self = rbt->rbt_root;
		if (BSD_RB_SENTINEL_P(self))
			return NULL;
		while (!BSD_RB_SENTINEL_P(self->bsd_rb_nodes[direction]))
			self = self->bsd_rb_nodes[direction];
		return BSD_RB_NODETOITEM(rbto, self);
#endif /* !RBSMALL */
	}
	self = BSD_RB_ITEMTONODE(rbto, object);
	KASSERT(!BSD_RB_SENTINEL_P(self));
	/*
	 * We can't go any further in this direction.  We proceed up in the
	 * opposite direction until our parent is in direction we want to go.
	 */
	if (BSD_RB_SENTINEL_P(self->bsd_rb_nodes[direction])) {
		while (!BSD_RB_ROOT_P(rbt, self)) {
			if (other == BSD_RB_POSITION(self))
				return BSD_RB_NODETOITEM(rbto, BSD_RB_FATHER(self));
			self = BSD_RB_FATHER(self);
		}
		return NULL;
	}

	/*
	 * Advance down one in current direction and go down as far as possible
	 * in the opposite direction.
	 */
	self = self->bsd_rb_nodes[direction];
	KASSERT(!BSD_RB_SENTINEL_P(self));
	while (!BSD_RB_SENTINEL_P(self->bsd_rb_nodes[other]))
		self = self->bsd_rb_nodes[other];
	return BSD_RB_NODETOITEM(rbto, self);
}

#ifdef RBDEBUG
static const struct bsd_rb_node *
bsd_rb_tree_iterate_const(const struct bsd_rb_tree *rbt, const struct bsd_rb_node *self,
	const unsigned int direction)
{
	const unsigned int other = direction ^ BSD_RB_DIR_OTHER;
	KASSERT(direction == BSD_RB_DIR_LEFT || direction == BSD_RB_DIR_RIGHT);

	if (self == NULL) {
#ifndef RBSMALL
		if (BSD_RB_SENTINEL_P(rbt->rbt_root))
			return NULL;
		return rbt->rbt_minmax[direction];
#else
		self = rbt->rbt_root;
		if (BSD_RB_SENTINEL_P(self))
			return NULL;
		while (!BSD_RB_SENTINEL_P(self->bsd_rb_nodes[direction]))
			self = self->bsd_rb_nodes[direction];
		return self;
#endif /* !RBSMALL */
	}
	KASSERT(!BSD_RB_SENTINEL_P(self));
	/*
	 * We can't go any further in this direction.  We proceed up in the
	 * opposite direction until our parent is in direction we want to go.
	 */
	if (BSD_RB_SENTINEL_P(self->bsd_rb_nodes[direction])) {
		while (!BSD_RB_ROOT_P(rbt, self)) {
			if (other == BSD_RB_POSITION(self))
				return BSD_RB_FATHER(self);
			self = BSD_RB_FATHER(self);
		}
		return NULL;
	}

	/*
	 * Advance down one in current direction and go down as far as possible
	 * in the opposite direction.
	 */
	self = self->bsd_rb_nodes[direction];
	KASSERT(!BSD_RB_SENTINEL_P(self));
	while (!BSD_RB_SENTINEL_P(self->bsd_rb_nodes[other]))
		self = self->bsd_rb_nodes[other];
	return self;
}

static unsigned int
bsd_rb_tree_count_black(const struct bsd_rb_node *self)
{
	unsigned int left, right;

	if (BSD_RB_SENTINEL_P(self))
		return 0;

	left = bsd_rb_tree_count_black(self->bsd_rb_left);
	right = bsd_rb_tree_count_black(self->bsd_rb_right);

	KASSERT(left == right);

	return left + BSD_RB_BLACK_P(self);
}

static bool
bsd_rb_tree_check_node(const struct bsd_rb_tree *rbt, const struct bsd_rb_node *self,
	const struct bsd_rb_node *prev, bool red_check)
{
	const bsd_rb_tree_ops_t *rbto = rbt->rbt_ops;
	bsd_rbto_compare_nodes_fn compare_nodes = rbto->bsd_rbto_compare_nodes;

	KASSERT(!BSD_RB_SENTINEL_P(self));
	KASSERT(prev == NULL || (*compare_nodes)(rbto->bsd_rbto_context,
	    BSD_RB_NODETOITEM(rbto, prev), BSD_RB_NODETOITEM(rbto, self)) < 0);

	/*
	 * Verify our relationship to our parent.
	 */
	if (BSD_RB_ROOT_P(rbt, self)) {
		KASSERT(self == rbt->rbt_root);
		KASSERT(BSD_RB_POSITION(self) == BSD_RB_DIR_LEFT);
		KASSERT(BSD_RB_FATHER(self)->bsd_rb_nodes[BSD_RB_DIR_LEFT] == self);
		KASSERT(BSD_RB_FATHER(self) == (const struct bsd_rb_node *) &rbt->rbt_root);
	} else {
		int diff = (*compare_nodes)(rbto->bsd_rbto_context,
		    BSD_RB_NODETOITEM(rbto, self),
		    BSD_RB_NODETOITEM(rbto, BSD_RB_FATHER(self)));

		KASSERT(self != rbt->rbt_root);
		KASSERT(!BSD_RB_FATHER_SENTINEL_P(self));
		if (BSD_RB_POSITION(self) == BSD_RB_DIR_LEFT) {
			KASSERT(diff < 0);
			KASSERT(BSD_RB_FATHER(self)->bsd_rb_nodes[BSD_RB_DIR_LEFT] == self);
		} else {
			KASSERT(diff > 0);
			KASSERT(BSD_RB_FATHER(self)->bsd_rb_nodes[BSD_RB_DIR_RIGHT] == self);
		}
	}

	/*
	 * Verify our position in the linked list against the tree itself.
	 */
	{
		const struct bsd_rb_node *prev0 = bsd_rb_tree_iterate_const(rbt, self, BSD_RB_DIR_LEFT);
		const struct bsd_rb_node *next0 = bsd_rb_tree_iterate_const(rbt, self, BSD_RB_DIR_RIGHT);
		KASSERT(prev0 == TAILQ_PREV(self, bsd_rb_node_qh, bsd_rb_link));
		KASSERT(next0 == TAILQ_NEXT(self, bsd_rb_link));
#ifndef RBSMALL
		KASSERT(prev0 != NULL || self == rbt->rbt_minmax[BSD_RB_DIR_LEFT]);
		KASSERT(next0 != NULL || self == rbt->rbt_minmax[BSD_RB_DIR_RIGHT]);
#endif
	}

	/*
	 * The root must be black.
	 * There can never be two adjacent red nodes. 
	 */
	if (red_check) {
		KASSERT(!BSD_RB_ROOT_P(rbt, self) || BSD_RB_BLACK_P(self));
		(void) bsd_rb_tree_count_black(self);
		if (BSD_RB_RED_P(self)) {
			const struct bsd_rb_node *brother;
			KASSERT(!BSD_RB_ROOT_P(rbt, self));
			brother = BSD_RB_FATHER(self)->bsd_rb_nodes[BSD_RB_POSITION(self) ^ BSD_RB_DIR_OTHER];
			KASSERT(BSD_RB_BLACK_P(BSD_RB_FATHER(self)));
			/* 
			 * I'm red and have no children, then I must either
			 * have no brother or my brother also be red and
			 * also have no children.  (black count == 0)
			 */
			KASSERT(!BSD_RB_CHILDLESS_P(self)
				|| BSD_RB_SENTINEL_P(brother)
				|| BSD_RB_RED_P(brother)
				|| BSD_RB_CHILDLESS_P(brother));
			/*
			 * If I'm not childless, I must have two children
			 * and they must be both be black.
			 */
			KASSERT(BSD_RB_CHILDLESS_P(self)
				|| (BSD_RB_TWOCHILDREN_P(self)
				    && BSD_RB_BLACK_P(self->bsd_rb_left)
				    && BSD_RB_BLACK_P(self->bsd_rb_right)));
			/*
			 * If I'm not childless, thus I have black children,
			 * then my brother must either be black or have two
			 * black children.
			 */
			KASSERT(BSD_RB_CHILDLESS_P(self)
				|| BSD_RB_BLACK_P(brother)
				|| (BSD_RB_TWOCHILDREN_P(brother)
				    && BSD_RB_BLACK_P(brother->bsd_rb_left)
				    && BSD_RB_BLACK_P(brother->bsd_rb_right)));
		} else {
			/*
			 * If I'm black and have one child, that child must
			 * be red and childless.
			 */
			KASSERT(BSD_RB_CHILDLESS_P(self)
				|| BSD_RB_TWOCHILDREN_P(self)
				|| (!BSD_RB_LEFT_SENTINEL_P(self)
				    && BSD_RB_RIGHT_SENTINEL_P(self)
				    && BSD_RB_RED_P(self->bsd_rb_left)
				    && BSD_RB_CHILDLESS_P(self->bsd_rb_left))
				|| (!BSD_RB_RIGHT_SENTINEL_P(self)
				    && BSD_RB_LEFT_SENTINEL_P(self)
				    && BSD_RB_RED_P(self->bsd_rb_right)
				    && BSD_RB_CHILDLESS_P(self->bsd_rb_right)));

			/*
			 * If I'm a childless black node and my parent is
			 * black, my 2nd closet relative away from my parent
			 * is either red or has a red parent or red children.
			 */
			if (!BSD_RB_ROOT_P(rbt, self)
			    && BSD_RB_CHILDLESS_P(self)
			    && BSD_RB_BLACK_P(BSD_RB_FATHER(self))) {
				const unsigned int which = BSD_RB_POSITION(self);
				const unsigned int other = which ^ BSD_RB_DIR_OTHER;
				const struct bsd_rb_node *relative0, *relative;

				relative0 = bsd_rb_tree_iterate_const(rbt,
				    self, other);
				KASSERT(relative0 != NULL);
				relative = bsd_rb_tree_iterate_const(rbt,
				    relative0, other);
				KASSERT(relative != NULL);
				KASSERT(BSD_RB_SENTINEL_P(relative->bsd_rb_nodes[which]));
#if 0
				KASSERT(BSD_RB_RED_P(relative)
					|| BSD_RB_RED_P(relative->bsd_rb_left)
					|| BSD_RB_RED_P(relative->bsd_rb_right)
					|| BSD_RB_RED_P(BSD_RB_FATHER(relative)));
#endif
			}
		}
		/*
		 * A grandparent's children must be real nodes and not
		 * sentinels.  First check out grandparent.
		 */
		KASSERT(BSD_RB_ROOT_P(rbt, self)
			|| BSD_RB_ROOT_P(rbt, BSD_RB_FATHER(self))
			|| BSD_RB_TWOCHILDREN_P(BSD_RB_FATHER(BSD_RB_FATHER(self))));
		/*
		 * If we are have grandchildren on our left, then
		 * we must have a child on our right.
		 */
		KASSERT(BSD_RB_LEFT_SENTINEL_P(self)
			|| BSD_RB_CHILDLESS_P(self->bsd_rb_left)
			|| !BSD_RB_RIGHT_SENTINEL_P(self));
		/*
		 * If we are have grandchildren on our right, then
		 * we must have a child on our left.
		 */
		KASSERT(BSD_RB_RIGHT_SENTINEL_P(self)
			|| BSD_RB_CHILDLESS_P(self->bsd_rb_right)
			|| !BSD_RB_LEFT_SENTINEL_P(self));

		/*
		 * If we have a child on the left and it doesn't have two
		 * children make sure we don't have great-great-grandchildren on
		 * the right.
		 */
		KASSERT(BSD_RB_TWOCHILDREN_P(self->bsd_rb_left)
			|| BSD_RB_CHILDLESS_P(self->bsd_rb_right)
			|| BSD_RB_CHILDLESS_P(self->bsd_rb_right->bsd_rb_left)
			|| BSD_RB_CHILDLESS_P(self->bsd_rb_right->bsd_rb_left->bsd_rb_left)
			|| BSD_RB_CHILDLESS_P(self->bsd_rb_right->bsd_rb_left->bsd_rb_right)
			|| BSD_RB_CHILDLESS_P(self->bsd_rb_right->bsd_rb_right)
			|| BSD_RB_CHILDLESS_P(self->bsd_rb_right->bsd_rb_right->bsd_rb_left)
			|| BSD_RB_CHILDLESS_P(self->bsd_rb_right->bsd_rb_right->bsd_rb_right));

		/*
		 * If we have a child on the right and it doesn't have two
		 * children make sure we don't have great-great-grandchildren on
		 * the left.
		 */
		KASSERT(BSD_RB_TWOCHILDREN_P(self->bsd_rb_right)
			|| BSD_RB_CHILDLESS_P(self->bsd_rb_left)
			|| BSD_RB_CHILDLESS_P(self->bsd_rb_left->bsd_rb_left)
			|| BSD_RB_CHILDLESS_P(self->bsd_rb_left->bsd_rb_left->bsd_rb_left)
			|| BSD_RB_CHILDLESS_P(self->bsd_rb_left->bsd_rb_left->bsd_rb_right)
			|| BSD_RB_CHILDLESS_P(self->bsd_rb_left->bsd_rb_right)
			|| BSD_RB_CHILDLESS_P(self->bsd_rb_left->bsd_rb_right->bsd_rb_left)
			|| BSD_RB_CHILDLESS_P(self->bsd_rb_left->bsd_rb_right->bsd_rb_right));

		/*
		 * If we are fully interior node, then our predecessors and
		 * successors must have no children in our direction.
		 */
		if (BSD_RB_TWOCHILDREN_P(self)) {
			const struct bsd_rb_node *prev0;
			const struct bsd_rb_node *next0;

			prev0 = bsd_rb_tree_iterate_const(rbt, self, BSD_RB_DIR_LEFT);
			KASSERT(prev0 != NULL);
			KASSERT(BSD_RB_RIGHT_SENTINEL_P(prev0));

			next0 = bsd_rb_tree_iterate_const(rbt, self, BSD_RB_DIR_RIGHT);
			KASSERT(next0 != NULL);
			KASSERT(BSD_RB_LEFT_SENTINEL_P(next0));
		}
	}

	return true;
}

void
bsd_rb_tree_check(const struct bsd_rb_tree *rbt, bool red_check)
{
	const struct bsd_rb_node *self;
	const struct bsd_rb_node *prev;
#ifdef RBSTATS
	unsigned int count = 0;
#endif

	KASSERT(rbt->rbt_root != NULL);
	KASSERT(BSD_RB_LEFT_P(rbt->rbt_root));

#if defined(RBSTATS) && !defined(RBSMALL)
	KASSERT(rbt->rbt_count > 1
	    || rbt->rbt_minmax[BSD_RB_DIR_LEFT] == rbt->rbt_minmax[BSD_RB_DIR_RIGHT]);
#endif

	prev = NULL;
	TAILQ_FOREACH(self, &rbt->rbt_nodes, bsd_rb_link) {
		bsd_rb_tree_check_node(rbt, self, prev, false);
#ifdef RBSTATS
		count++;
#endif
	}
#ifdef RBSTATS
	KASSERT(rbt->rbt_count == count);
#endif
	if (red_check) {
		KASSERT(BSD_RB_BLACK_P(rbt->rbt_root));
		KASSERT(BSD_RB_SENTINEL_P(rbt->rbt_root)
			|| bsd_rb_tree_count_black(rbt->rbt_root));

		/*
		 * The root must be black.
		 * There can never be two adjacent red nodes. 
		 */
		TAILQ_FOREACH(self, &rbt->rbt_nodes, bsd_rb_link) {
			bsd_rb_tree_check_node(rbt, self, NULL, true);
		}
	}
}
#endif /* RBDEBUG */


size_t bsd_rb_tree_count(bsd_rb_tree_t *tree)
{
    return tree->rbt_count;
}

#ifdef RBSTATS
static void
bsd_rb_tree_mark_depth(const struct bsd_rb_tree *rbt, const struct bsd_rb_node *self,
	size_t *depths, size_t depth)
{
	if (BSD_RB_SENTINEL_P(self))
		return;

	if (BSD_RB_TWOCHILDREN_P(self)) {
		bsd_rb_tree_mark_depth(rbt, self->bsd_rb_left, depths, depth + 1);
		bsd_rb_tree_mark_depth(rbt, self->bsd_rb_right, depths, depth + 1);
		return;
	}
	depths[depth]++;
	if (!BSD_RB_LEFT_SENTINEL_P(self)) {
		bsd_rb_tree_mark_depth(rbt, self->bsd_rb_left, depths, depth + 1);
	}
	if (!BSD_RB_RIGHT_SENTINEL_P(self)) {
		bsd_rb_tree_mark_depth(rbt, self->bsd_rb_right, depths, depth + 1);
	}
}

void
bsd_rb_tree_depths(const struct bsd_rb_tree *rbt, size_t *depths)
{
	bsd_rb_tree_mark_depth(rbt, rbt->rbt_root, depths, 1);
}
#endif /* RBSTATS */
