/**
* Note that p1 and p2 are pointers into an independent POINTARRAY, do not free them.
*/
typedef struct rect_node
{
	double xmin;
	double xmax;
	double ymin;
	double ymax;
	struct rect_node *left_node;
	struct rect_node *right_node;
	POINT2D *p1;
	POINT2D *p2;
} RECT_NODE;	

int rect_tree_contains_point(const RECT_NODE *tree, const POINT2D *pt, int *on_boundary);
int rect_tree_intersects_tree(const RECT_NODE *tree1, const RECT_NODE *tree2);
void rect_tree_free(RECT_NODE *node);
RECT_NODE* rect_node_leaf_new(const POINTARRAY *pa, int i);
RECT_NODE* rect_node_internal_new(RECT_NODE *left_node, RECT_NODE *right_node);
RECT_NODE* rect_tree_new(const POINTARRAY *pa);
