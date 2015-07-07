
/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Copyright 2010 Nicklas Avén
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include "liblwgeom_internal.h"

/* for the measure functions*/
#define DIST_MAX		-1
#define DIST_MIN		1

/** 
* Structure used in distance-calculations
*/
typedef struct
{
	double distance;	/*the distance between p1 and p2*/
	POINT2D p1;
	POINT2D p2;
	int mode;	/*the direction of looking, if thedir = -1 then we look for maxdistance and if it is 1 then we look for mindistance*/
	int twisted; /*To preserve the order of incoming points to match the first and secon point in shortest and longest line*/
	double tolerance; /*the tolerance for dwithin and dfullywithin*/
} DISTPTS;

typedef struct
{
	double themeasure;	/*a value calculated to compare distances*/
	int pnr;	/*pointnumber. the ordernumber of the point*/
} LISTSTRUCT;


/*
* Preprocessing functions
*/
int lw_dist2d_comp(LWGEOM *lw1, LWGEOM *lw2, DISTPTS *dl);
int lw_dist2d_distribute_bruteforce(LWGEOM *lwg1, LWGEOM *lwg2, DISTPTS *dl);
int lw_dist2d_recursive(const LWGEOM *lwg1, const LWGEOM *lwg2, DISTPTS *dl);
int lw_dist2d_check_overlap(LWGEOM *lwg1, LWGEOM *lwg2);
int lw_dist2d_distribute_fast(LWGEOM *lwg1, LWGEOM *lwg2, DISTPTS *dl);

/*
* Brute force functions
*/
int lw_dist2d_pt_ptarray(const POINT2D *p, POINTARRAY *pa, DISTPTS *dl);
int lw_dist2d_pt_ptarrayarc(const POINT2D *p, const POINTARRAY *pa, DISTPTS *dl);
int lw_dist2d_ptarray_ptarray(POINTARRAY *l1, POINTARRAY *l2, DISTPTS *dl);
int lw_dist2d_ptarray_ptarrayarc(const POINTARRAY *pa, const POINTARRAY *pb, DISTPTS *dl);
int lw_dist2d_ptarrayarc_ptarrayarc(const POINTARRAY *pa, const POINTARRAY *pb, DISTPTS *dl);
int lw_dist2d_ptarray_poly(POINTARRAY *pa, LWPOLY *poly, DISTPTS *dl);
int lw_dist2d_point_point(LWPOINT *point1, LWPOINT *point2, DISTPTS *dl);
int lw_dist2d_point_line(LWPOINT *point, LWLINE *line, DISTPTS *dl);
int lw_dist2d_point_circstring(LWPOINT *point, LWCIRCSTRING *circ, DISTPTS *dl);
int lw_dist2d_point_poly(LWPOINT *point, LWPOLY *poly, DISTPTS *dl);
int lw_dist2d_point_curvepoly(LWPOINT *point, LWCURVEPOLY *poly, DISTPTS *dl);
int lw_dist2d_line_line(LWLINE *line1, LWLINE *line2, DISTPTS *dl);
int lw_dist2d_line_circstring(LWLINE *line1, LWCIRCSTRING *line2, DISTPTS *dl);
int lw_dist2d_line_poly(LWLINE *line, LWPOLY *poly, DISTPTS *dl);
int lw_dist2d_line_curvepoly(LWLINE *line, LWCURVEPOLY *poly, DISTPTS *dl);
int lw_dist2d_circstring_circstring(LWCIRCSTRING *line1, LWCIRCSTRING *line2, DISTPTS *dl);
int lw_dist2d_circstring_poly(LWCIRCSTRING *circ, LWPOLY *poly, DISTPTS *dl);
int lw_dist2d_circstring_curvepoly(LWCIRCSTRING *circ, LWCURVEPOLY *poly, DISTPTS *dl);
int lw_dist2d_poly_poly(LWPOLY *poly1, LWPOLY *poly2, DISTPTS *dl);
int lw_dist2d_poly_curvepoly(LWPOLY *poly1, LWCURVEPOLY *curvepoly2, DISTPTS *dl);
int lw_dist2d_curvepoly_curvepoly(LWCURVEPOLY *poly1, LWCURVEPOLY *poly2, DISTPTS *dl);

/*
* New faster distance calculations
*/
int lw_dist2d_pre_seg_seg(POINTARRAY *l1, POINTARRAY *l2,LISTSTRUCT *list1, LISTSTRUCT *list2,double k, DISTPTS *dl);
int lw_dist2d_selected_seg_seg(const POINT2D *A, const POINT2D *B, const POINT2D *C, const POINT2D *D, DISTPTS *dl);
int struct_cmp_by_measure(const void *a, const void *b);
int lw_dist2d_fast_ptarray_ptarray(POINTARRAY *l1,POINTARRAY *l2, DISTPTS *dl,  GBOX *box1, GBOX *box2);

/*
* Distance calculation primitives. 
*/
int lw_dist2d_pt_pt  (const POINT2D *P,  const POINT2D *Q,  DISTPTS *dl);
int lw_dist2d_pt_seg (const POINT2D *P,  const POINT2D *A1, const POINT2D *A2, DISTPTS *dl);
int lw_dist2d_pt_arc (const POINT2D *P,  const POINT2D *A1, const POINT2D *A2, const POINT2D *A3, DISTPTS *dl);
int lw_dist2d_seg_seg(const POINT2D *A1, const POINT2D *A2, const POINT2D *B1, const POINT2D *B2, DISTPTS *dl);
int lw_dist2d_seg_arc(const POINT2D *A1, const POINT2D *A2, const POINT2D *B1, const POINT2D *B2, const POINT2D *B3, DISTPTS *dl);
int lw_dist2d_arc_arc(const POINT2D *A1, const POINT2D *A2, const POINT2D *A3, const POINT2D *B1, const POINT2D *B2, const POINT2D* B3, DISTPTS *dl);
void lw_dist2d_distpts_init(DISTPTS *dl, int mode);

/*
* Length primitives
*/
double lw_arc_length(const POINT2D *A1, const POINT2D *A2, const POINT2D *A3);

/*
* Geometry returning functions
*/
LWGEOM* lw_dist2d_distancepoint(LWGEOM *lw1, LWGEOM *lw2, int srid, int mode);
LWGEOM* lw_dist2d_distanceline(LWGEOM *lw1, LWGEOM *lw2, int srid, int mode);


