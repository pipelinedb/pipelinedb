
/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Copyright 2011 Nicklas Avén
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include "measures.h"

#define DOT(u,v)   (u.x * v.x + u.y * v.y + u.z * v.z)
#define VECTORLENGTH(v)   sqrt((v.x * v.x) + (v.y * v.y) + (v.z * v.z))


/**

Structure used in distance-calculations
*/
typedef struct
{
	double distance;	/*the distance between p1 and p2*/
	POINT3DZ p1;
	POINT3DZ p2;
	int mode;	/*the direction of looking, if thedir = -1 then we look for 3dmaxdistance and if it is 1 then we look for 3dmindistance*/
	int twisted; /*To preserve the order of incoming points to match the first and second point in 3dshortest and 3dlongest line*/
	double tolerance; /*the tolerance for 3ddwithin and 3ddfullywithin*/
} DISTPTS3D;

typedef struct
{
	double	x,y,z;  
}
VECTOR3D; 

typedef struct
{
	POINT3DZ		pop;  /*Point On Plane*/
	VECTOR3D	pv;  /*Perpendicular normal vector*/
}
PLANE3D; 


/*
Geometry returning functions
*/
LWGEOM * lw_dist3d_distancepoint(LWGEOM *lw1, LWGEOM *lw2,int srid,int mode);
LWGEOM * lw_dist3d_distanceline(LWGEOM *lw1, LWGEOM *lw2,int srid,int mode);

/*
Preprocessing functions
*/
int lw_dist3d_distribute_bruteforce(LWGEOM *lwg1, LWGEOM *lwg2, DISTPTS3D *dl);
int lw_dist3d_recursive(const LWGEOM *lwg1,const LWGEOM *lwg2, DISTPTS3D *dl);
int lw_dist3d_distribute_fast(LWGEOM *lwg1, LWGEOM *lwg2, DISTPTS3D *dl);

/*
Brute force functions
*/
int lw_dist3d_pt_ptarray(POINT3DZ *p, POINTARRAY *pa, DISTPTS3D *dl);
int lw_dist3d_point_point(LWPOINT *point1, LWPOINT *point2, DISTPTS3D *dl);
int lw_dist3d_point_line(LWPOINT *point, LWLINE *line, DISTPTS3D *dl);
int lw_dist3d_line_line(LWLINE *line1,LWLINE *line2 , DISTPTS3D *dl);
int lw_dist3d_point_poly(LWPOINT *point, LWPOLY *poly, DISTPTS3D *dl);
int lw_dist3d_line_poly(LWLINE *line, LWPOLY *poly, DISTPTS3D *dl);
int lw_dist3d_poly_poly(LWPOLY *poly1, LWPOLY *poly2, DISTPTS3D *dl);
int lw_dist3d_ptarray_ptarray(POINTARRAY *l1, POINTARRAY *l2,DISTPTS3D *dl);
int lw_dist3d_seg_seg(POINT3DZ *A, POINT3DZ *B, POINT3DZ *C, POINT3DZ *D, DISTPTS3D *dl);
int lw_dist3d_pt_pt(POINT3DZ *p1, POINT3DZ *p2, DISTPTS3D *dl);
int lw_dist3d_pt_seg(POINT3DZ *p, POINT3DZ *A, POINT3DZ *B, DISTPTS3D *dl);
int lw_dist3d_pt_poly(POINT3DZ *p, LWPOLY *poly, PLANE3D *plane,POINT3DZ *projp,  DISTPTS3D *dl);
int lw_dist3d_ptarray_poly(POINTARRAY *pa, LWPOLY *poly, PLANE3D *plane, DISTPTS3D *dl);



double project_point_on_plane(POINT3DZ *p,  PLANE3D *pl, POINT3DZ *p0);
int define_plane(POINTARRAY *pa, PLANE3D *pl);
int pt_in_ring_3d(const POINT3DZ *p, const POINTARRAY *ring,PLANE3D *plane);
/*
Helper functions
*/
int get_3dvector_from_points(POINT3DZ *p1,POINT3DZ *p2, VECTOR3D *v);
int get_3dcross_product(VECTOR3D *v1,VECTOR3D *v2, VECTOR3D *v);


int
get_3dvector_from_points(POINT3DZ *p1,POINT3DZ *p2, VECTOR3D *v)
{
	v->x=p2->x-p1->x;
	v->y=p2->y-p1->y;
	v->z=p2->z-p1->z;
	
	return LW_TRUE;
}

int
get_3dcross_product(VECTOR3D *v1,VECTOR3D *v2, VECTOR3D *v)
{
	v->x=(v1->y*v2->z)-(v1->z*v2->y);
	v->y=(v1->z*v2->x)-(v1->x*v2->z);
	v->z=(v1->x*v2->y)-(v1->y*v2->x);

	return LW_TRUE;
}
