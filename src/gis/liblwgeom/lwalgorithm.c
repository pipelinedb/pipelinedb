/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Copyright 2008 Paul Ramsey
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include "liblwgeom_internal.h"
#include "lwgeom_log.h"
#include <ctype.h> /* for tolower */


/**
* Returns -1 if n < 0.0 and 1 if n > 0.0
*/
int signum(double n)
{
	if( n < 0 ) return -1;
	if( n > 0 ) return 1;
	return 0;
}

int
p4d_same(const POINT4D *p1, const POINT4D *p2)
{
	if( FP_EQUALS(p1->x,p2->x) && FP_EQUALS(p1->y,p2->y) && FP_EQUALS(p1->z,p2->z) && FP_EQUALS(p1->m,p2->m) )
		return LW_TRUE;
	else
		return LW_FALSE;
}

int
p3d_same(const POINT3D *p1, const POINT3D *p2)
{
	if( FP_EQUALS(p1->x,p2->x) && FP_EQUALS(p1->y,p2->y) && FP_EQUALS(p1->z,p2->z) )
		return LW_TRUE;
	else
		return LW_FALSE;
}

int
p2d_same(const POINT2D *p1, const POINT2D *p2)
{
	if( FP_EQUALS(p1->x,p2->x) && FP_EQUALS(p1->y,p2->y) )
		return LW_TRUE;
	else
		return LW_FALSE;
}

/**
* lw_segment_side()
*
* Return -1  if point Q is left of segment P
* Return  1  if point Q is right of segment P
* Return  0  if point Q in on segment P
*/
int lw_segment_side(const POINT2D *p1, const POINT2D *p2, const POINT2D *q)
{
	double side = ( (q->x - p1->x) * (p2->y - p1->y) - (p2->x - p1->x) * (q->y - p1->y) );
	if ( side == 0.0 )
		return 0;
	else
		return signum(side);
}

/**
* Returns the length of a linear segment
*/
double
lw_seg_length(const POINT2D *A1, const POINT2D *A2)
{
	return sqrt((A1->x-A2->x)*(A1->x-A2->x)+(A1->y-A2->y)*(A1->y-A2->y));
}

/**
* Returns true if P is on the same side of the plane partition 
* defined by A1/A3 as A2 is. Only makes sense if P has already been
* determined to be on the circle defined by A1/A2/A3.
*/
int
lw_pt_in_arc(const POINT2D *P, const POINT2D *A1, const POINT2D *A2, const POINT2D *A3)
{
	return lw_segment_side(A1, A3, A2) == lw_segment_side(A1, A3, P);
}

/**
* Returns true if P is between A1/A2. Only makes sense if P has already been
* deterined to be on the line defined by A1/A2.
*/
int
lw_pt_in_seg(const POINT2D *P, const POINT2D *A1, const POINT2D *A2)
{
	return ((A1->x <= P->x && P->x < A2->x) || (A1->x >= P->x && P->x > A2->x)) ||
	       ((A1->y <= P->y && P->y < A2->y) || (A1->y >= P->y && P->y > A2->y));
}

/**
* Returns true if arc A is actually a point (all vertices are the same) .
*/
int
lw_arc_is_pt(const POINT2D *A1, const POINT2D *A2, const POINT2D *A3)
{
	if ( A1->x == A2->x && A2->x == A3->x && 
	     A1->y == A2->y && A2->y == A3->y )
		return LW_TRUE;
	else
		return LW_FALSE;
}

/**
* Returns the length of a circular arc segment
*/
double
lw_arc_length(const POINT2D *A1, const POINT2D *A2, const POINT2D *A3)
{
	POINT2D C;
	double radius_A, circumference_A;
	int a2_side, clockwise;
	double a1, a3;
	double angle;
	
	if ( lw_arc_is_pt(A1, A2, A3) )
		return 0.0;
	
	radius_A = lw_arc_center(A1, A2, A3, &C);

	/* Co-linear! Return linear distance! */
	if ( radius_A < 0 ) 
	{
        double dx = A1->x - A3->x;
        double dy = A1->y - A3->y;
		return sqrt(dx*dx + dy*dy);
	}
	
	/* Closed circle! Return the circumference! */
	circumference_A = M_PI * 2 * radius_A;
	if ( p2d_same(A1, A3) )
		return circumference_A;
	
	/* Determine the orientation of the arc */
	a2_side = lw_segment_side(A1, A3, A2);

	/* The side of the A1/A3 line that A2 falls on dictates the sweep  
	   direction from A1 to A3. */
	if ( a2_side == -1 ) 
		clockwise = LW_TRUE;
	else 
		clockwise = LW_FALSE;
		
	/* Angles of each point that defines the arc section */
	a1 = atan2(A1->y - C.y, A1->x - C.x);
	a3 = atan2(A3->y - C.y, A3->x - C.x);

	/* What's the sweep from A1 to A3? */
	if ( clockwise )
	{
		if ( a1 > a3 )
			angle = a1 - a3;
		else
			angle = 2*M_PI + a1 - a3; 
	}
	else
	{
		if ( a3 > a1 )
			angle = a3 - a1;
		else
			angle = 2*M_PI + a3 - a1; 			
	}

	/* Length as proportion of circumference */
	return circumference_A * (angle / (2*M_PI));
}

int lw_arc_side(const POINT2D *A1, const POINT2D *A2, const POINT2D *A3, const POINT2D *Q)
{
	POINT2D C;
	double radius_A;
	double side_Q, side_A2;
	double d;
	
	side_Q = lw_segment_side(A1, A3, Q);
	radius_A = lw_arc_center(A1, A2, A3, &C);
	side_A2 = lw_segment_side(A1, A3, A2);
	
	/* Linear case */
	if ( radius_A < 0 )
		return side_Q;
		
	d = distance2d_pt_pt(Q, &C);
	
	/* Q is on the arc boundary */
	if ( d == radius_A && side_Q == side_A2 )
	{	
		return 0;
	}
	
	/* Q on A1-A3 line, so its on opposite side to A2 */
	if ( side_Q == 0 )
	{
		return -1 * side_A2;
	}
	
	/* 
	* Q is inside the arc boundary, so it's not on the side we 
	* might think from examining only the end points
	*/
	if ( d < radius_A && side_Q == side_A2 )
	{
		side_Q *= -1;
	}
	
	return side_Q;
}

/**
* Determines the center of the circle defined by the three given points.
* In the event the circle is complete, the midpoint of the segment defined
* by the first and second points is returned.  If the points are colinear,
* as determined by equal slopes, then NULL is returned.  If the interior
* point is coincident with either end point, they are taken as colinear.
*/
double
lw_arc_center(const POINT2D *p1, const POINT2D *p2, const POINT2D *p3, POINT2D *result)
{
	POINT2D c;
	double cx, cy, cr;
	double temp, bc, cd, det;

    c.x = c.y = 0.0;

	LWDEBUGF(2, "lw_arc_center called (%.16f,%.16f), (%.16f,%.16f), (%.16f,%.16f).", p1->x, p1->y, p2->x, p2->y, p3->x, p3->y);

	/* Closed circle */
	if (fabs(p1->x - p3->x) < EPSILON_SQLMM &&
	    fabs(p1->y - p3->y) < EPSILON_SQLMM)
	{
		cx = p1->x + (p2->x - p1->x) / 2.0;
		cy = p1->y + (p2->y - p1->y) / 2.0;
		c.x = cx;
		c.y = cy;
		*result = c;
		cr = sqrt(pow(cx - p1->x, 2.0) + pow(cy - p1->y, 2.0));
		return cr;
	}

	temp = p2->x*p2->x + p2->y*p2->y;
	bc = (p1->x*p1->x + p1->y*p1->y - temp) / 2.0;
	cd = (temp - p3->x*p3->x - p3->y*p3->y) / 2.0;
	det = (p1->x - p2->x)*(p2->y - p3->y)-(p2->x - p3->x)*(p1->y - p2->y);

	/* Check colinearity */
	if (fabs(det) < EPSILON_SQLMM)
		return -1.0;


	det = 1.0 / det;
	cx = (bc*(p2->y - p3->y)-cd*(p1->y - p2->y))*det;
	cy = ((p1->x - p2->x)*cd-(p2->x - p3->x)*bc)*det;
	c.x = cx;
	c.y = cy;
	*result = c;
	cr = sqrt((cx-p1->x)*(cx-p1->x)+(cy-p1->y)*(cy-p1->y));
	
	LWDEBUGF(2, "lw_arc_center center is (%.16f,%.16f)", result->x, result->y);
	
	return cr;
}


int
pt_in_ring_2d(const POINT2D *p, const POINTARRAY *ring)
{
	int cn = 0;    /* the crossing number counter */
	int i;
	const POINT2D *v1, *v2;
	const POINT2D *first, *last;

	first = getPoint2d_cp(ring, 0);
	last = getPoint2d_cp(ring, ring->npoints-1);
	if ( memcmp(first, last, sizeof(POINT2D)) )
	{
		lwerror("pt_in_ring_2d: V[n] != V[0] (%g %g != %g %g)",
		        first->x, first->y, last->x, last->y);
		return LW_FALSE;

	}

	LWDEBUGF(2, "pt_in_ring_2d called with point: %g %g", p->x, p->y);
	/* printPA(ring); */

	/* loop through all edges of the polygon */
	v1 = getPoint2d_cp(ring, 0);
	for (i=0; i<ring->npoints-1; i++)
	{
		double vt;
		v2 = getPoint2d_cp(ring, i+1);

		/* edge from vertex i to vertex i+1 */
		if
		(
		    /* an upward crossing */
		    ((v1->y <= p->y) && (v2->y > p->y))
		    /* a downward crossing */
		    || ((v1->y > p->y) && (v2->y <= p->y))
		)
		{

			vt = (double)(p->y - v1->y) / (v2->y - v1->y);

			/* P->x <intersect */
			if (p->x < v1->x + vt * (v2->x - v1->x))
			{
				/* a valid crossing of y=p->y right of p->x */
				++cn;
			}
		}
		v1 = v2;
	}

	LWDEBUGF(3, "pt_in_ring_2d returning %d", cn&1);

	return (cn&1);    /* 0 if even (out), and 1 if odd (in) */
}


static int 
lw_seg_interact(const POINT2D *p1, const POINT2D *p2, const POINT2D *q1, const POINT2D *q2)
{
	double minq=FP_MIN(q1->x,q2->x);
	double maxq=FP_MAX(q1->x,q2->x);
	double minp=FP_MIN(p1->x,p2->x);
	double maxp=FP_MAX(p1->x,p2->x);

	if (FP_GT(minp,maxq) || FP_LT(maxp,minq))
		return LW_FALSE;

	minq=FP_MIN(q1->y,q2->y);
	maxq=FP_MAX(q1->y,q2->y);
	minp=FP_MIN(p1->y,p2->y);
	maxp=FP_MAX(p1->y,p2->y);

	if (FP_GT(minp,maxq) || FP_LT(maxp,minq))
		return LW_FALSE;

	return LW_TRUE;
}

/**
** @brief returns the kind of #CG_SEGMENT_INTERSECTION_TYPE  behavior of lineseg 1 (constructed from p1 and p2) and lineseg 2 (constructed from q1 and q2)
**	@param p1 start point of first straight linesegment
**	@param p2 end point of first straight linesegment
**	@param q1 start point of second line segment
**	@param q2 end point of second line segment
**	@return a #CG_SEGMENT_INTERSECTION_TYPE
** 	Returns one of
**		SEG_ERROR = -1,
**		SEG_NO_INTERSECTION = 0,
**		SEG_COLINEAR = 1,
**		SEG_CROSS_LEFT = 2,
**		SEG_CROSS_RIGHT = 3,
*/
int lw_segment_intersects(const POINT2D *p1, const POINT2D *p2, const POINT2D *q1, const POINT2D *q2)
{

	int pq1, pq2, qp1, qp2;

	/* No envelope interaction => we are done. */
	if (!lw_seg_interact(p1, p2, q1, p2))
	{
		return SEG_NO_INTERSECTION;
	}

	/* Are the start and end points of q on the same side of p? */
	pq1=lw_segment_side(p1,p2,q1);
	pq2=lw_segment_side(p1,p2,q2);
	if ((pq1>0 && pq2>0) || (pq1<0 && pq2<0))
	{
		return SEG_NO_INTERSECTION;
	}

	/* Are the start and end points of p on the same side of q? */
	qp1=lw_segment_side(q1,q2,p1);
	qp2=lw_segment_side(q1,q2,p2);
	if ( (qp1 > 0.0 && qp2 > 0.0) || (qp1 < 0.0 && qp2 < 0.0) )
	{
		return SEG_NO_INTERSECTION;
	}

	/* Nobody is on one side or another? Must be colinear. */
	if ( pq1 == 0.0 && pq2 == 0.0 && qp1 == 0.0 && qp2 == 0.0 )
	{
		return SEG_COLINEAR;
	}

	/*
	** When one end-point touches, the sidedness is determined by the
	** location of the other end-point. Only touches by the first point
	** will be considered "real" to avoid double counting.
	*/
	LWDEBUGF(4, "pq1=%.15g pq2=%.15g", pq1, pq2);
	LWDEBUGF(4, "qp1=%.15g qp2=%.15g", qp1, qp2);

	/* Second point of p or q touches, it's not a crossing. */
	if ( pq2 == 0 || qp2 == 0 )
	{
		return SEG_NO_INTERSECTION;
	}

	/* First point of p touches, it's a "crossing". */
	if ( pq1 == 0 )
	{
		if ( pq2 > 0 )
			return SEG_CROSS_RIGHT;
		else
			return SEG_CROSS_LEFT;
	}

	/* First point of q touches, it's a crossing. */
	if ( qp1 == 0 )
	{
		if ( pq1 < pq2 )
			return SEG_CROSS_RIGHT;
		else
			return SEG_CROSS_LEFT;
	}

	/* The segments cross, what direction is the crossing? */
	if ( pq1 < pq2 )
		return SEG_CROSS_RIGHT;
	else
		return SEG_CROSS_LEFT;

	/* This should never happen! */
	return SEG_ERROR;
}

/**
** @brief lwline_crossing_direction: returns the kind of #CG_LINE_CROSS_TYPE behavior  of 2 linestrings
** @param l1 first line string
** @param l2 second line string
** @return a #CG_LINE_CROSS_TYPE
**   LINE_NO_CROSS = 0
**   LINE_CROSS_LEFT = -1
**   LINE_CROSS_RIGHT = 1
**   LINE_MULTICROSS_END_LEFT = -2
**   LINE_MULTICROSS_END_RIGHT = 2
**   LINE_MULTICROSS_END_SAME_FIRST_LEFT = -3
**   LINE_MULTICROSS_END_SAME_FIRST_RIGHT = 3
**
*/
int lwline_crossing_direction(const LWLINE *l1, const LWLINE *l2)
{
	int i = 0, j = 0;
	const POINT2D *p1, *p2, *q1, *q2;
	POINTARRAY *pa1 = NULL, *pa2 = NULL;
	int cross_left = 0;
	int cross_right = 0;
	int first_cross = 0;
	int this_cross = 0;

	pa1 = (POINTARRAY*)l1->points;
	pa2 = (POINTARRAY*)l2->points;

	/* One-point lines can't intersect (and shouldn't exist). */
	if ( pa1->npoints < 2 || pa2->npoints < 2 )
		return LINE_NO_CROSS;

	LWDEBUGF(4, "l1 = %s", lwgeom_to_ewkt((LWGEOM*)l1));
	LWDEBUGF(4, "l2 = %s", lwgeom_to_ewkt((LWGEOM*)l2));

	/* Initialize first point of q */
	q1 = getPoint2d_cp(pa2, 0);

	for ( i = 1; i < pa2->npoints; i++ )
	{

		/* Update second point of q to next value */
		q2 = getPoint2d_cp(pa2, i);

		/* Initialize first point of p */
		p1 = getPoint2d_cp(pa1, 0);

		for ( j = 1; j < pa1->npoints; j++ )
		{

			/* Update second point of p to next value */
			p2 = getPoint2d_cp(pa1, j);

			this_cross = lw_segment_intersects(p1, p2, q1, q2);

			LWDEBUGF(4, "i=%d, j=%d (%.8g %.8g, %.8g %.8g)", this_cross, i, j, p1->x, p1->y, p2->x, p2->y);

			if ( this_cross == SEG_CROSS_LEFT )
			{
				LWDEBUG(4,"this_cross == SEG_CROSS_LEFT");
				cross_left++;
				if ( ! first_cross )
					first_cross = SEG_CROSS_LEFT;
			}

			if ( this_cross == SEG_CROSS_RIGHT )
			{
				LWDEBUG(4,"this_cross == SEG_CROSS_RIGHT");
				cross_right++;
				if ( ! first_cross )
					first_cross = SEG_CROSS_LEFT;
			}

			/*
			** Crossing at a co-linearity can be turned handled by extending
			** segment to next vertext and seeing if the end points straddle
			** the co-linear segment.
			*/
			if ( this_cross == SEG_COLINEAR )
			{
				LWDEBUG(4,"this_cross == SEG_COLINEAR");
				/* TODO: Add logic here and in segment_intersects()
				continue;
				*/
			}

			LWDEBUG(4,"this_cross == SEG_NO_INTERSECTION");

			/* Turn second point of p into first point */
			p1 = p2;

		}

		/* Turn second point of q into first point */
		q1 = q2;

	}

	LWDEBUGF(4, "first_cross=%d, cross_left=%d, cross_right=%d", first_cross, cross_left, cross_right);

	if ( !cross_left && !cross_right )
		return LINE_NO_CROSS;

	if ( !cross_left && cross_right == 1 )
		return LINE_CROSS_RIGHT;

	if ( !cross_right && cross_left == 1 )
		return LINE_CROSS_LEFT;

	if ( cross_left - cross_right == 1 )
		return LINE_MULTICROSS_END_LEFT;

	if ( cross_left - cross_right == -1 )
		return LINE_MULTICROSS_END_RIGHT;

	if ( cross_left - cross_right == 0 && first_cross == SEG_CROSS_LEFT )
		return LINE_MULTICROSS_END_SAME_FIRST_LEFT;

	if ( cross_left - cross_right == 0 && first_cross == SEG_CROSS_RIGHT )
		return LINE_MULTICROSS_END_SAME_FIRST_RIGHT;

	return LINE_NO_CROSS;

}





static char *base32 = "0123456789bcdefghjkmnpqrstuvwxyz";

/*
** Calculate the geohash, iterating downwards and gaining precision.
** From geohash-native.c, (c) 2008 David Troy <dave@roundhousetech.com>
** Released under the MIT License.
*/
char *geohash_point(double longitude, double latitude, int precision)
{
	int is_even=1, i=0;
	double lat[2], lon[2], mid;
	char bits[] = {16,8,4,2,1};
	int bit=0, ch=0;
	char *geohash = NULL;

	geohash = lwalloc(precision + 1);

	lat[0] = -90.0;
	lat[1] = 90.0;
	lon[0] = -180.0;
	lon[1] = 180.0;

	while (i < precision)
	{
		if (is_even)
		{
			mid = (lon[0] + lon[1]) / 2;
			if (longitude >= mid)
			{
				ch |= bits[bit];
				lon[0] = mid;
			}
			else
			{
				lon[1] = mid;
			}
		}
		else
		{
			mid = (lat[0] + lat[1]) / 2;
			if (latitude >= mid)
			{
				ch |= bits[bit];
				lat[0] = mid;
			}
			else
			{
				lat[1] = mid;
			}
		}

		is_even = !is_even;
		if (bit < 4)
		{
			bit++;
		}
		else
		{
			geohash[i++] = base32[ch];
			bit = 0;
			ch = 0;
		}
	}
	geohash[i] = 0;
	return geohash;
}


/*
** Calculate the geohash, iterating downwards and gaining precision.
** From geohash-native.c, (c) 2008 David Troy <dave@roundhousetech.com>
** Released under the MIT License.
*/
unsigned int geohash_point_as_int(POINT2D *pt)
{
	int is_even=1;
	double lat[2], lon[2], mid;
	int bit=32;
	unsigned int ch = 0;

	double longitude = pt->x;
	double latitude = pt->y;

	lat[0] = -90.0;
	lat[1] = 90.0;
	lon[0] = -180.0;
	lon[1] = 180.0;

	while (--bit >= 0)
	{
		if (is_even)
		{
			mid = (lon[0] + lon[1]) / 2;
			if (longitude > mid)
			{
				ch |= 0x0001 << bit;
				lon[0] = mid;
			}
			else
			{
				lon[1] = mid;
			}
		}
		else
		{
			mid = (lat[0] + lat[1]) / 2;
			if (latitude > mid)
			{
				ch |= 0x0001 << bit;
				lat[0] = mid;
			}
			else
			{
				lat[1] = mid;
			}
		}

		is_even = !is_even;
	}
	return ch;
}

/*
** Decode a GeoHash into a bounding box. The lat and lon arguments should
** both be passed as double arrays of length 2 at a minimum where the values
** set in them will be the southwest and northeast coordinates of the bounding
** box accordingly. A precision less than 0 indicates that the entire length
** of the GeoHash should be used.
*/
void decode_geohash_bbox(char *geohash, double *lat, double *lon, int precision)
{
	int i, j, hashlen;
	char c, cd, mask, is_even = 1;
	static char bits[] = {16, 8, 4, 2, 1};

	lat[0] = -90.0;
	lat[1] = 90.0;
	lon[0] = -180.0;
	lon[1] = 180.0;

	hashlen = strlen(geohash);

	if (precision < 0 || precision > hashlen)
	{
		precision = hashlen;
	}

	for (i = 0; i < precision; i++)
	{
		c = tolower(geohash[i]);
		cd = strchr(base32, c) - base32;

		for (j = 0; j < 5; j++)
		{
			mask = bits[j];
			if (is_even)
			{
				lon[!(cd & mask)] = (lon[0] + lon[1]) / 2;
			}
			else
			{
				lat[!(cd & mask)] = (lat[0] + lat[1]) / 2;
			}
			is_even = !is_even;
		}
	}
}

int lwgeom_geohash_precision(GBOX bbox, GBOX *bounds)
{
	double minx, miny, maxx, maxy;
	double latmax, latmin, lonmax, lonmin;
	double lonwidth, latwidth;
	double latmaxadjust, lonmaxadjust, latminadjust, lonminadjust;
	int precision = 0;

	/* Get the bounding box, return error if things don't work out. */
	minx = bbox.xmin;
	miny = bbox.ymin;
	maxx = bbox.xmax;
	maxy = bbox.ymax;

	if ( minx == maxx && miny == maxy )
	{
		/* It's a point. Doubles have 51 bits of precision.
		** 2 * 51 / 5 == 20 */
		return 20;
	}

	lonmin = -180.0;
	latmin = -90.0;
	lonmax = 180.0;
	latmax = 90.0;

	/* Shrink a world bounding box until one of the edges interferes with the
	** bounds of our rectangle. */
	while ( 1 )
	{
		lonwidth = lonmax - lonmin;
		latwidth = latmax - latmin;
		latmaxadjust = lonmaxadjust = latminadjust = lonminadjust = 0.0;

		if ( minx > lonmin + lonwidth / 2.0 )
		{
			lonminadjust = lonwidth / 2.0;
		}
		else if ( maxx < lonmax - lonwidth / 2.0 )
		{
			lonmaxadjust = -1 * lonwidth / 2.0;
		}
		if ( miny > latmin + latwidth / 2.0 )
		{
			latminadjust = latwidth / 2.0;
		}
		else if (maxy < latmax - latwidth / 2.0 )
		{
			latmaxadjust = -1 * latwidth / 2.0;
		}
		/* Only adjust if adjustments are legal (we haven't crossed any edges). */
		if ( (lonminadjust || lonmaxadjust) && (latminadjust || latmaxadjust ) )
		{
			latmin += latminadjust;
			lonmin += lonminadjust;
			latmax += latmaxadjust;
			lonmax += lonmaxadjust;
			/* Each adjustment cycle corresponds to 2 bits of storage in the
			** geohash.	*/
			precision += 2;
		}
		else
		{
			break;
		}
	}

	/* Save the edges of our bounds, in case someone cares later. */
	bounds->xmin = lonmin;
	bounds->xmax = lonmax;
	bounds->ymin = latmin;
	bounds->ymax = latmax;

	/* Each geohash character (base32) can contain 5 bits of information.
	** We are returning the precision in characters, so here we divide. */
	return precision / 5;
}


/*
** Return a geohash string for the geometry. <http://geohash.org>
** Where the precision is non-positive, calculate a precision based on the
** bounds of the feature. Big features have loose precision.
** Small features have tight precision.
*/
char *lwgeom_geohash(const LWGEOM *lwgeom, int precision)
{
	GBOX gbox;
	GBOX gbox_bounds;
	double lat, lon;
	int result;

	gbox_init(&gbox);
	gbox_init(&gbox_bounds);

	result = lwgeom_calculate_gbox_cartesian(lwgeom, &gbox);	
	if ( result == LW_FAILURE ) return NULL;

	/* Return error if we are being fed something outside our working bounds */
	if ( gbox.xmin < -180 || gbox.ymin < -90 || gbox.xmax > 180 || gbox.ymax > 90 )
	{
		lwerror("Geohash requires inputs in decimal degrees.");
		return NULL;
	}

	/* What is the center of our geometry bounds? We'll use that to
	** approximate location. */
	lon = gbox.xmin + (gbox.xmax - gbox.xmin) / 2;
	lat = gbox.ymin + (gbox.ymax - gbox.ymin) / 2;

	if ( precision <= 0 )
	{
		precision = lwgeom_geohash_precision(gbox, &gbox_bounds);
	}

	/*
	** Return the geohash of the center, with a precision determined by the
	** extent of the bounds.
	** Possible change: return the point at the center of the precision bounds?
	*/
	return geohash_point(lon, lat, precision);
}























