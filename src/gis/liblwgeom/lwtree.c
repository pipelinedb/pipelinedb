#include "liblwgeom_internal.h"
#include "lwgeom_log.h"
#include "lwtree.h"


/**
* Internal nodes have their point references set to NULL.
*/
static int rect_node_is_leaf(const RECT_NODE *node)
{
	return (node->p1 != NULL);
}

/**
* Recurse from top of node tree and free all children.
* does not free underlying point array.
*/
void rect_tree_free(RECT_NODE *node)
{
	if ( node->left_node )
	{
		rect_tree_free(node->left_node);
		node->left_node = 0;
	}
	if ( node->right_node )
	{
		rect_tree_free(node->right_node);
		node->right_node = 0;
	}
	lwfree(node);
}

/* 0 => no containment */
int rect_tree_contains_point(const RECT_NODE *node, const POINT2D *pt, int *on_boundary)
{
	if ( FP_CONTAINS_INCL(node->ymin, pt->y, node->ymax) )
	{
		if ( rect_node_is_leaf(node) )
		{
			double side = lw_segment_side(node->p1, node->p2, pt);
			if ( side == 0 )
				*on_boundary = LW_TRUE;
			return (side < 0 ? -1 : 1 );
		}
		else
		{
			return rect_tree_contains_point(node->left_node, pt, on_boundary) +
			       rect_tree_contains_point(node->right_node, pt, on_boundary);
		}
	}
	/* printf("NOT in measure range\n"); */
	return 0;
}

int rect_tree_intersects_tree(const RECT_NODE *n1, const RECT_NODE *n2)
{
	LWDEBUGF(4,"n1 (%.9g %.9g,%.9g %.9g) vs n2 (%.9g %.9g,%.9g %.9g)",n1->xmin,n1->ymin,n1->xmax,n1->ymax,n2->xmin,n2->ymin,n2->xmax,n2->ymax);
	/* There can only be an edge intersection if the rectangles overlap */
	if ( ! ( FP_GT(n1->xmin, n2->xmax) || FP_GT(n2->xmin, n1->xmax) || FP_GT(n1->ymin, n2->ymax) || FP_GT(n2->ymin, n1->ymax) ) )
	{
		LWDEBUG(4," interaction found");
		/* We can only test for a true intersection if the nodes are both leaf nodes */
		if ( rect_node_is_leaf(n1) && rect_node_is_leaf(n2) )
		{
			LWDEBUG(4,"  leaf node test");
			/* Check for true intersection */
			if ( lw_segment_intersects(n1->p1, n1->p2, n2->p1, n2->p2) )
				return LW_TRUE;
			else
				return LW_FALSE;
		}
		else
		{
			LWDEBUG(4,"  internal node found, recursing");
			/* Recurse to children */
			if ( rect_node_is_leaf(n1) )
			{
				if ( rect_tree_intersects_tree(n2->left_node, n1) || rect_tree_intersects_tree(n2->right_node, n1) )
					return LW_TRUE;
				else
					return LW_FALSE;
			}
			else
			{
				if ( rect_tree_intersects_tree(n1->left_node, n2) || rect_tree_intersects_tree(n1->right_node, n2) )
					return LW_TRUE;
				else
					return LW_FALSE;
			}
		}
	}
	else
	{
		LWDEBUG(4," no interaction found");
		return LW_FALSE;
	}
}


/**
* Create a new leaf node, calculating a measure value for each point on the
* edge and storing pointers back to the end points for later.
*/
RECT_NODE* rect_node_leaf_new(const POINTARRAY *pa, int i)
{
	POINT2D *p1, *p2;
	RECT_NODE *node;

	p1 = (POINT2D*)getPoint_internal(pa, i);
	p2 = (POINT2D*)getPoint_internal(pa, i+1);

	/* Zero length edge, doesn't get a node */
	if ( FP_EQUALS(p1->x, p2->x) && FP_EQUALS(p1->y, p2->y) )
		return NULL;

	node = lwalloc(sizeof(RECT_NODE));
	node->p1 = p1;
	node->p2 = p2;
	node->xmin = FP_MIN(p1->x,p2->x);
	node->xmax = FP_MAX(p1->x,p2->x);
	node->ymin = FP_MIN(p1->y,p2->y);
	node->ymax = FP_MAX(p1->y,p2->y);
	node->left_node = NULL;
	node->right_node = NULL;
	return node;
}

/**
* Create a new internal node, calculating the new measure range for the node,
* and storing pointers to the child nodes.
*/
RECT_NODE* rect_node_internal_new(RECT_NODE *left_node, RECT_NODE *right_node)
{
	RECT_NODE *node = lwalloc(sizeof(RECT_NODE));
	node->p1 = NULL;
	node->p2 = NULL;
	node->xmin = FP_MIN(left_node->xmin, right_node->xmin);
	node->xmax = FP_MAX(left_node->xmax, right_node->xmax);
	node->ymin = FP_MIN(left_node->ymin, right_node->ymin);
	node->ymax = FP_MAX(left_node->ymax, right_node->ymax);
	node->left_node = left_node;
	node->right_node = right_node;
	return node;
}

/**
* Build a tree of nodes from a point array, one node per edge, and each
* with an associated measure range along a one-dimensional space. We
* can then search that space as a range tree.
*/
RECT_NODE* rect_tree_new(const POINTARRAY *pa)
{
	int num_edges, num_children, num_parents;
	int i, j;
	RECT_NODE **nodes;
	RECT_NODE *node;
	RECT_NODE *tree;

	if ( pa->npoints < 2 )
	{
		return NULL;
	}

	/*
	** First create a flat list of nodes, one per edge.
	** For each vertex, transform into our one-dimensional measure.
	** Hopefully, when projected, the points turn into a fairly
	** uniformly distributed collection of measures.
	*/
	num_edges = pa->npoints - 1;
	nodes = lwalloc(sizeof(RECT_NODE*) * pa->npoints);
	j = 0;
	for ( i = 0; i < num_edges; i++ )
	{
		node = rect_node_leaf_new(pa, i);
		if ( node ) /* Not zero length? */
		{
			nodes[j] = node;
			j++;
		}
	}

	/*
	** If we sort the nodelist first, we'll get a more balanced tree
	** in the end, but at the cost of sorting. For now, we just
	** build the tree knowing that point arrays tend to have a
	** reasonable amount of sorting already.
	*/

	num_children = j;
	num_parents = num_children / 2;
	while ( num_parents > 0 )
	{
		j = 0;
		while ( j < num_parents )
		{
			/*
			** Each new parent includes pointers to the children, so even though
			** we are over-writing their place in the list, we still have references
			** to them via the tree.
			*/
			nodes[j] = rect_node_internal_new(nodes[2*j], nodes[(2*j)+1]);
			j++;
		}
		/* Odd number of children, just copy the last node up a level */
		if ( num_children % 2 )
		{
			nodes[j] = nodes[num_children - 1];
			num_parents++;
		}
		num_children = num_parents;
		num_parents = num_children / 2;
	}

	/* Take a reference to the head of the tree*/
	tree = nodes[0];

	/* Free the old list structure, leaving the tree in place */
	lwfree(nodes);

	return tree;

}

