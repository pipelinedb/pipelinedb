
#ifndef _LWGEODETIC_TREE_H
#define _LWGEODETIC_TREE_H 1

#include "lwgeodetic.h"

#define CIRC_NODE_SIZE 8

/**
* Note that p1 and p2 are pointers into an independent POINTARRAY, do not free them.
*/
typedef struct circ_node
{
	GEOGRAPHIC_POINT center;
	double radius;
	int num_nodes;
	struct circ_node** nodes;
	int edge_num;
    int geom_type;
    POINT2D pt_outside;
	POINT2D* p1;
	POINT2D* p2;
} CIRC_NODE;

void circ_tree_print(const CIRC_NODE* node, int depth);
CIRC_NODE* circ_tree_new(const POINTARRAY* pa);
void circ_tree_free(CIRC_NODE* node);
int circ_tree_contains_point(const CIRC_NODE* node, const POINT2D* pt, const POINT2D* pt_outside, int* on_boundary);
double circ_tree_distance_tree(const CIRC_NODE* n1, const CIRC_NODE* n2, const SPHEROID *spheroid, double threshold);
CIRC_NODE* lwgeom_calculate_circ_tree(const LWGEOM* lwgeom);
int circ_tree_get_point(const CIRC_NODE* node, POINT2D* pt);

#endif /* _LWGEODETIC_TREE_H */


