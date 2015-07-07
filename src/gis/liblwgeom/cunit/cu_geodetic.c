/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Copyright 2009 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 * Note: Geodesic measurements have been independently verified using
 * GeographicLib v 1.37 with MPFR C++ using utilities GeodSolve and
 * Planimeter, with -E "exact" flag with the following env vars:
 *     export GEOGRAPHICLIB_DIGITS=1000
 *     export WGS84_ELIPSOID="6378137 298.257223563"
 *     export WGS84_SPHERE="6371008.7714150598325213222 0"
 **********************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "CUnit/Basic.h"

#include "liblwgeom_internal.h"
#include "lwgeodetic.h"
#include "cu_tester.h"

#define RANDOM_TEST 0

/**
* Convert an edge from degrees to radians.
*/
static void edge_deg2rad(GEOGRAPHIC_EDGE *e)
{
	(e->start).lat = deg2rad((e->start).lat);
	(e->end).lat = deg2rad((e->end).lat);
	(e->start).lon = deg2rad((e->start).lon);
	(e->end).lon = deg2rad((e->end).lon);
}

/**
* Convert an edge from radians to degrees.
static void edge_rad2deg(GEOGRAPHIC_EDGE *e)
{
	(e->start).lat = rad2deg((e->start).lat);
	(e->end).lat = rad2deg((e->end).lat);
	(e->start).lon = rad2deg((e->start).lon);
	(e->end).lon = rad2deg((e->end).lon);
}
*/

/**
* Convert a point from degrees to radians.
*/
static void point_deg2rad(GEOGRAPHIC_POINT *p)
{
	p->lat = latitude_radians_normalize(deg2rad(p->lat));
	p->lon = longitude_radians_normalize(deg2rad(p->lon));
}

/**
* Convert a point from radians to degrees.
*/
static void point_rad2deg(GEOGRAPHIC_POINT *p)
{
	p->lat = rad2deg(p->lat);
	p->lon = rad2deg(p->lon);
}

static void test_signum(void)
{
	CU_ASSERT_EQUAL(signum(-5.0),-1);
	CU_ASSERT_EQUAL(signum(5.0),1);
}


static void test_sphere_direction(void)
{
	GEOGRAPHIC_POINT s, e;
	double dir, dist;

	geographic_point_init(0, 0, &s);
	geographic_point_init(1, 0, &e);
	dist = sphere_distance(&s, &e);
	dir = sphere_direction(&s, &e, dist);
	/* GeodSolve -i -E -p 16 -e 1 0 --input-string "0 0 0 1" */
	CU_ASSERT_DOUBLE_EQUAL(dir, M_PI_2, 1e-14);
	CU_ASSERT_DOUBLE_EQUAL(dist, 0.0174532925199433, 1e-14);

	geographic_point_init(0, 0, &s);
	geographic_point_init(0, 1, &e);
	dist = sphere_distance(&s, &e);
	dir = sphere_direction(&s, &e, dist);
	/* GeodSolve -i -E -p 16 -e 1 0 --input-string "0 0 1 0" */
	CU_ASSERT_DOUBLE_EQUAL(dir, 0.0, 1e-14);
	CU_ASSERT_DOUBLE_EQUAL(dist, 0.0174532925199433, 1e-14);
	
}

static void test_sphere_project(void)
{
	GEOGRAPHIC_POINT s, e;
	double dir1, dist1, dir2, dist2;

	dir1 = M_PI_2;
	dist1 = 0.1;
	
	geographic_point_init(0, 0, &s);
	sphere_project(&s, dist1, dir1, &e);

	CU_ASSERT_DOUBLE_EQUAL(e.lon, 0.1, 1e-14);
	CU_ASSERT_DOUBLE_EQUAL(e.lat, 0.0, 1e-14);
	
	/* Direct and inverse solutions agree */
	dist2 = sphere_distance(&s, &e);
	dir2 = sphere_direction(&s, &e, dist1);

	CU_ASSERT_DOUBLE_EQUAL(dist1, dist2, 1e-14);
	CU_ASSERT_DOUBLE_EQUAL(dir1, dir2, 1e-14);
	
	dist1 = sphere_distance(&e, &s);
	dir1 = sphere_direction(&e, &s, dist1);
	sphere_project(&e, dist1, dir1, &s);

	CU_ASSERT_DOUBLE_EQUAL(s.lon, 0.0, 1e-14);
	CU_ASSERT_DOUBLE_EQUAL(s.lat, 0.0, 1e-14);

	geographic_point_init(0, 0.2, &e);  
	geographic_point_init(0, 0.4, &s);  
	dist1 = sphere_distance(&s, &e);
	dir1 = sphere_direction(&e, &s, dist1);
	/* GeodSolve -i -E -p 16 -e 1 0 --input-string "0.2 0 0.4 0" */
	CU_ASSERT_DOUBLE_EQUAL(dir1, 0.0, 1e-14);
	CU_ASSERT_DOUBLE_EQUAL(dist1, 0.0034906585039887, 1e-14);

	geographic_point_init(0, 1, &s); /* same start point for remainder of tests */
	geographic_point_init(0, 2, &e);  
	dist2 = sphere_distance(&s, &e);
	dir2 = sphere_direction(&s, &e, dist2);
	/* GeodSolve -i -E -p 16 -e 1 0 --input-string "1 0 2 0" */
	CU_ASSERT_DOUBLE_EQUAL(dir2, 0.0, 1e-14);
	CU_ASSERT_DOUBLE_EQUAL(dist2, 0.0174532925199433, 1e-14);
	
	geographic_point_init(1, 1, &e);  
	dist2 = sphere_distance(&s, &e);
	dir2 = sphere_direction(&s, &e, dist2);
	/* GeodSolve -i -E -p 16 -e 1 0 --input-string "1 0 1 1" */
	CU_ASSERT_DOUBLE_EQUAL(dir2, 89.991273575329292895136 * M_PI / 180.0, 1e-14);
	CU_ASSERT_DOUBLE_EQUAL(dist2, 0.0174506342314906, 1e-14);

	geographic_point_init(0, 0, &e);  
	dist2 = sphere_distance(&s, &e);
	dir2 = sphere_direction(&s, &e, dist2);
	/* GeodSolve -i -E -p 16 -e 1 0 --input-string "1 0 0 0" */
	CU_ASSERT_DOUBLE_EQUAL(dir2, M_PI, 1e-14);
	CU_ASSERT_DOUBLE_EQUAL(dist2, 0.0174532925199433, 1e-14);

	geographic_point_init(-1, 1, &e);  
	dist2 = sphere_distance(&s, &e);
	dir2 = sphere_direction(&s, &e, dist2);
	/* GeodSolve -i -E -p 16 -e 1 0 --input-string "1 0 1 -1" */
	CU_ASSERT_DOUBLE_EQUAL(dir2, -89.991273575329292895136 * M_PI / 180.0, 1e-14);
	CU_ASSERT_DOUBLE_EQUAL(dist2, 0.0174506342314906, 1e-14);

	geographic_point_init(1, 2, &e);  
	dist2 = sphere_distance(&s, &e);
	dir2 = sphere_direction(&s, &e, dist2);
	/* GeodSolve -i -E -p 16 -e 1 0 --input-string "1 0 2 1" */
	CU_ASSERT_DOUBLE_EQUAL(dir2, 44.978182941465044354783 * M_PI / 180.0, 1e-14);
	CU_ASSERT_DOUBLE_EQUAL(dist2, 0.0246782972905467, 1e-14);

	geographic_point_init(-1, 0, &e);  
	dist2 = sphere_distance(&s, &e);
	dir2 = sphere_direction(&s, &e, dist2);
	/* GeodSolve -i -E -p 16 -e 1 0 --input-string "1 0 0 -1" */
	CU_ASSERT_DOUBLE_EQUAL(dir2, -134.995636455344851488216 * M_PI / 180.0, 1e-14);
	CU_ASSERT_DOUBLE_EQUAL(dist2, 0.0246820563917664, 1e-14);
}

#if 0
/**
* Tests the relative numerical stability of the "robust" and
* naive cross product calculation methods.
*/
static void cross_product_stability(void)
{
	POINT2D p1, p2;
	int i;
	GEOGRAPHIC_POINT g1, g2;
	POINT3D A1, A2;
	POINT3D Nr, Nc;
	POINT3D Or, Oc;

	p1.x = 10.0;
	p1.y = 45.0;
	p2.x = 10.0;
	p2.y = 50.0;
	
	geographic_point_init(p1.x, p1.y, &g1);
	ll2cart(&p1, &A1);

	for ( i = 0; i < 40; i++ )
	{
		geographic_point_init(p2.x, p2.y, &g2);
		ll2cart(&p2, &A2);
		
		/* Skea */
		robust_cross_product(&g1, &g2, &Nr);
		normalize(&Nr);
		
		/* Ramsey */
		unit_normal(&A1, &A2, &Nc);

		if ( i > 0 ) 
		{
			printf("\n- %d -------------------- %.24g ------------------------\n", i, p2.y);
			printf("Skea:         %.24g,%.24g,%.24g\n", Nr.x, Nr.y, Nr.z);
			printf("Skea Diff:    %.24g,%.24g,%.24g\n", Or.x-Nr.x, Or.y-Nr.y, Or.z-Nr.z);
			printf("Ramsey:       %.24g,%.24g,%.24g\n", Nc.x, Nc.y, Nc.z);
			printf("Ramsey Diff:  %.24g,%.24g,%.24g\n", Oc.x-Nc.x, Oc.y-Nc.y, Oc.z-Nc.z);
			printf("Diff:         %.24g,%.24g,%.24g\n", Nr.x-Nc.x, Nr.y-Nc.y, Nr.z-Nc.z);
		}
		
		Or = Nr;
		Oc = Nc;
		
		p2.y += (p1.y - p2.y)/2.0;
	}
}
#endif

static void test_gbox_from_spherical_coordinates(void)
{
#if RANDOM_TEST
	const double gtolerance = 0.000001;
	const int loops = RANDOM_TEST;
	int i;
	double ll[64];
	GBOX gbox;
	GBOX gbox_slow;
	int rndlat;
	int rndlon;

	POINTARRAY *pa;
	LWGEOM *lwline;

	ll[0] = -3.083333333333333333333333333333333;
	ll[1] = 9.83333333333333333333333333333333;
	ll[2] = 15.5;
	ll[3] = -5.25;

	pa = ptarray_construct_reference_data(0, 0, 2, (uint8_t*)ll);
	
	lwline = lwline_as_lwgeom(lwline_construct(SRID_UNKNOWN, 0, pa));
	FLAGS_SET_GEODETIC(lwline->flags, 1);

	srandomdev();

	for ( i = 0; i < loops; i++ )
	{
		rndlat = (int)(90.0 - 180.0 * (double)random() / pow(2.0, 31.0));
		rndlon = (int)(180.0 - 360.0 * (double)random() / pow(2.0, 31.0));
		ll[0] = (double)rndlon;
		ll[1] = (double)rndlat;

		rndlat = (int)(90.0 - 180.0 * (double)random() / pow(2.0, 31.0));
		rndlon = (int)(180.0 - 360.0 * (double)random() / pow(2.0, 31.0));
		ll[2] = (double)rndlon;
		ll[3] = (double)rndlat;

		gbox_geocentric_slow = LW_FALSE;
		lwgeom_calculate_gbox_geodetic(lwline, &gbox);
		gbox_geocentric_slow = LW_TRUE;
		lwgeom_calculate_gbox_geodetic(lwline, &gbox_slow);
		gbox_geocentric_slow = LW_FALSE;

		if (
			( fabs( gbox.xmin - gbox_slow.xmin ) > gtolerance ) ||
			( fabs( gbox.xmax - gbox_slow.xmax ) > gtolerance ) ||
			( fabs( gbox.ymin - gbox_slow.ymin ) > gtolerance ) ||
			( fabs( gbox.ymax - gbox_slow.ymax ) > gtolerance ) ||
			( fabs( gbox.zmin - gbox_slow.zmin ) > gtolerance ) ||
			( fabs( gbox.zmax - gbox_slow.zmax ) > gtolerance ) )
		{
			printf("\n-------\n");
			printf("If you are seeing this, cut and paste it, it is a randomly generated test case!\n");
			printf("LOOP: %d\n", i);
			printf("SEGMENT (Lon Lat): (%.9g %.9g) (%.9g %.9g)\n", ll[0], ll[1], ll[2], ll[3]);
			printf("CALC: %s\n", gbox_to_string(&gbox));
			printf("SLOW: %s\n", gbox_to_string(&gbox_slow));
			printf("-------\n\n");
			CU_FAIL_FATAL(Slow (GOOD) and fast (CALC) box calculations returned different values!!);
		}

	}

	lwgeom_free(lwline);
#endif /* RANDOM_TEST */
}

#include "cu_geodetic_data.h"

static void test_gserialized_get_gbox_geocentric(void)
{
	LWGEOM *lwg;
	GBOX gbox, gbox_slow;
	int i;

	for ( i = 0; i < gbox_data_length; i++ )
	{
#if 0
//		if ( i != 0 ) continue; /* skip our bad case */
		printf("\n\n------------\n");
		printf("%s\n", gbox_data[i]);
#endif
		lwg = lwgeom_from_wkt(gbox_data[i], LW_PARSER_CHECK_NONE);
		FLAGS_SET_GEODETIC(lwg->flags, 1);
		gbox_geocentric_slow = LW_FALSE;
		lwgeom_calculate_gbox(lwg, &gbox);
		gbox_geocentric_slow = LW_TRUE;
		lwgeom_calculate_gbox(lwg, &gbox_slow);
		gbox_geocentric_slow = LW_FALSE;
		lwgeom_free(lwg);
#if 0
		printf("\nCALC: %s\n", gbox_to_string(&gbox));
		printf("GOOD: %s\n", gbox_to_string(&gbox_slow));
		printf("line %d: diff %.9g\n", i, fabs(gbox.xmin - gbox_slow.xmin)+fabs(gbox.ymin - gbox_slow.ymin)+fabs(gbox.zmin - gbox_slow.zmin));
		printf("------------\n");
#endif
		CU_ASSERT_DOUBLE_EQUAL(gbox.xmin, gbox_slow.xmin, 0.00000001);
		CU_ASSERT_DOUBLE_EQUAL(gbox.ymin, gbox_slow.ymin, 0.00000001);
		CU_ASSERT_DOUBLE_EQUAL(gbox.zmin, gbox_slow.zmin, 0.00000001);
		CU_ASSERT_DOUBLE_EQUAL(gbox.xmax, gbox_slow.xmax, 0.00000001);
		CU_ASSERT_DOUBLE_EQUAL(gbox.ymax, gbox_slow.ymax, 0.00000001);
		CU_ASSERT_DOUBLE_EQUAL(gbox.zmax, gbox_slow.zmax, 0.00000001);
	}

}

/*
* Build LWGEOM on top of *aligned* structure so we can use the read-only
* point access methods on them.
static LWGEOM* lwgeom_over_gserialized(char *wkt)
{
	LWGEOM *lwg;
	GSERIALIZED *g;

	lwg = lwgeom_from_wkt(wkt, LW_PARSER_CHECK_NONE);
	g = gserialized_from_lwgeom(lwg, 1, 0);
	lwgeom_free(lwg);
	return lwgeom_from_gserialized(g);
}
*/

static void edge_set(double lon1, double lat1, double lon2, double lat2, GEOGRAPHIC_EDGE *e)
{
	e->start.lon = lon1;
	e->start.lat = lat1;
	e->end.lon = lon2;
	e->end.lat = lat2;
	edge_deg2rad(e);
}

static void point_set(double lon, double lat, GEOGRAPHIC_POINT *p)
{
	p->lon = lon;
	p->lat = lat;
	point_deg2rad(p);
}

static void test_clairaut(void)
{

	GEOGRAPHIC_POINT gs, ge;
	POINT3D vs, ve;
	GEOGRAPHIC_POINT g_out_top, g_out_bottom, v_out_top, v_out_bottom;

	point_set(-45.0, 60.0, &gs);
	point_set(135.0, 60.0, &ge);

	geog2cart(&gs, &vs);
	geog2cart(&ge, &ve);

	clairaut_cartesian(&vs, &ve, &v_out_top, &v_out_bottom);
	clairaut_geographic(&gs, &ge, &g_out_top, &g_out_bottom);

	CU_ASSERT_DOUBLE_EQUAL(v_out_top.lat, g_out_top.lat, 0.000001);
	CU_ASSERT_DOUBLE_EQUAL(v_out_top.lon, g_out_top.lon, 0.000001);
	CU_ASSERT_DOUBLE_EQUAL(v_out_bottom.lat, g_out_bottom.lat, 0.000001);
	CU_ASSERT_DOUBLE_EQUAL(v_out_bottom.lon, g_out_bottom.lon, 0.000001);

	gs.lat = 1.3021240033804449;
	ge.lat = 1.3021240033804449;
	gs.lon = -1.3387392931438733;
	ge.lon = 1.80285336044592;

	geog2cart(&gs, &vs);
	geog2cart(&ge, &ve);

	clairaut_cartesian(&vs, &ve, &v_out_top, &v_out_bottom);
	clairaut_geographic(&gs, &ge, &g_out_top, &g_out_bottom);

	CU_ASSERT_DOUBLE_EQUAL(v_out_top.lat, g_out_top.lat, 0.000001);
	CU_ASSERT_DOUBLE_EQUAL(v_out_top.lon, g_out_top.lon, 0.000001);
	CU_ASSERT_DOUBLE_EQUAL(v_out_bottom.lat, g_out_bottom.lat, 0.000001);
	CU_ASSERT_DOUBLE_EQUAL(v_out_bottom.lon, g_out_bottom.lon, 0.000001);
}

static void test_edge_intersection(void)
{
	GEOGRAPHIC_EDGE e1, e2;
	GEOGRAPHIC_POINT g;
	int rv;

	/* Covers case, end-to-end intersection */
	edge_set(50, -10.999999999999998224, -10.0, 50.0, &e1);
	edge_set(-10.0, 50.0, -10.272779983831613393, -16.937003313332997578, &e2);
	rv = edge_intersection(&e1, &e2, &g);
	CU_ASSERT_EQUAL(rv, LW_TRUE);

	/* Medford case, very short segment vs very long one */
	e1.start.lat = 0.74123572595649878103;
	e1.start.lon = -2.1496353191142714145;
	e1.end.lat = 0.74123631950116664058;
	e1.end.lon = -2.1496353248304860273;
	e2.start.lat = 0.73856343764436815924;
	e2.start.lon = -2.1461493501950630325;
	e2.end.lat = 0.70971354024834598651;
	e2.end.lon = 2.1082194552519770703;
	rv = edge_intersection(&e1, &e2, &g);
	CU_ASSERT_EQUAL(rv, LW_FALSE);

	/* Again, this time with a less exact input edge. */
	edge_set(-123.165031277506, 42.4696787216231, -123.165031605021, 42.4697127292275, &e1);
	rv = edge_intersection(&e1, &e2, &g);
	CU_ASSERT_EQUAL(rv, LW_FALSE);

	/* Second Medford case, very short segment vs very long one
	e1.start.lat = 0.73826546728290887156;
	e1.start.lon = -2.14426380171833042;
	e1.end.lat = 0.73826545883786642843;
	e1.end.lon = -2.1442638997530165668;
	e2.start.lat = 0.73775469118192538165;
	e2.start.lon = -2.1436035534281718817;
	e2.end.lat = 0.71021099548296817705;
	e2.end.lon = 2.1065275171200439353;
	rv = edge_intersection(e1, e2, &g);
	CU_ASSERT_EQUAL(rv, LW_FALSE);
	*/

	/* Intersection at (0 0) */
	edge_set(-1.0, 0.0, 1.0, 0.0, &e1);
	edge_set(0.0, -1.0, 0.0, 1.0, &e2);
	rv = edge_intersection(&e1, &e2, &g);
	point_rad2deg(&g);
	CU_ASSERT_DOUBLE_EQUAL(g.lat, 0.0, 0.00001);
	CU_ASSERT_DOUBLE_EQUAL(g.lon, 0.0, 0.00001);
	CU_ASSERT_EQUAL(rv, LW_TRUE);

	/*  No intersection at (0 0) */
	edge_set(-1.0, 0.0, 1.0, 0.0, &e1);
	edge_set(0.0, -1.0, 0.0, -2.0, &e2);
	rv = edge_intersection(&e1, &e2, &g);
	CU_ASSERT_EQUAL(rv, LW_FALSE);

	/*  End touches middle of segment at (0 0) */
	edge_set(-1.0, 0.0, 1.0, 0.0, &e1);
	edge_set(0.0, -1.0, 0.0, 0.0, &e2);
	rv = edge_intersection(&e1, &e2, &g);
	point_rad2deg(&g);
#if 0
	printf("\n");
	printf("LINESTRING(%.15g %.15g, %.15g %.15g)\n", e1.start.lon,  e1.start.lat, e1.end.lon,  e1.end.lat);
	printf("LINESTRING(%.15g %.15g, %.15g %.15g)\n", e2.start.lon,  e2.start.lat, e2.end.lon,  e2.end.lat);
	printf("g = (%.15g %.15g)\n", g.lon, g.lat);
	printf("rv = %d\n", rv);
#endif
	CU_ASSERT_DOUBLE_EQUAL(g.lon, 0.0, 0.00001);
	CU_ASSERT_EQUAL(rv, LW_TRUE);

	/*  End touches end of segment at (0 0) */
	edge_set(0.0, 0.0, 1.0, 0.0, &e1);
	edge_set(0.0, -1.0, 0.0, 0.0, &e2);
	rv = edge_intersection(&e1, &e2, &g);
	point_rad2deg(&g);
#if 0
	printf("\n");
	printf("LINESTRING(%.15g %.15g, %.15g %.15g)\n", e1.start.lon,  e1.start.lat, e1.end.lon,  e1.end.lat);
	printf("LINESTRING(%.15g %.15g, %.15g %.15g)\n", e2.start.lon,  e2.start.lat, e2.end.lon,  e2.end.lat);
	printf("g = (%.15g %.15g)\n", g.lon, g.lat);
	printf("rv = %d\n", rv);
#endif
	CU_ASSERT_DOUBLE_EQUAL(g.lat, 0.0, 0.00001);
	CU_ASSERT_DOUBLE_EQUAL(g.lon, 0.0, 0.00001);
	CU_ASSERT_EQUAL(rv, LW_TRUE);

	/* Intersection at (180 0) */
	edge_set(-179.0, -1.0, 179.0, 1.0, &e1);
	edge_set(-179.0, 1.0, 179.0, -1.0, &e2);
	rv = edge_intersection(&e1, &e2, &g);
	point_rad2deg(&g);
	CU_ASSERT_DOUBLE_EQUAL(g.lat, 0.0, 0.00001);
	CU_ASSERT_DOUBLE_EQUAL(fabs(g.lon), 180.0, 0.00001);
	CU_ASSERT_EQUAL(rv, LW_TRUE);

	/* Intersection at (180 0) */
	edge_set(-170.0, 0.0, 170.0, 0.0, &e1);
	edge_set(180.0, -10.0, 180.0, 10.0, &e2);
	rv = edge_intersection(&e1, &e2, &g);
	point_rad2deg(&g);
	CU_ASSERT_DOUBLE_EQUAL(g.lat, 0.0, 0.00001);
	CU_ASSERT_DOUBLE_EQUAL(fabs(g.lon), 180.0, 0.00001);
	CU_ASSERT_EQUAL(rv, LW_TRUE);

	/* Intersection at north pole */
	edge_set(-180.0, 80.0, 0.0, 80.0, &e1);
	edge_set(90.0, 80.0, -90.0, 80.0, &e2);
	rv = edge_intersection(&e1, &e2, &g);
	point_rad2deg(&g);
	CU_ASSERT_DOUBLE_EQUAL(g.lat, 90.0, 0.00001);
	CU_ASSERT_EQUAL(rv, LW_TRUE);

	/* Equal edges return true */
	edge_set(45.0, 10.0, 50.0, 20.0, &e1);
	edge_set(45.0, 10.0, 50.0, 20.0, &e2);
	rv = edge_intersection(&e1, &e2, &g);
	point_rad2deg(&g);
	CU_ASSERT_EQUAL(rv, LW_TRUE);

	/* Parallel edges (same great circle, different end points) return true  */
	edge_set(40.0, 0.0, 70.0, 0.0, &e1);
	edge_set(60.0, 0.0, 50.0, 0.0, &e2);
	rv = edge_intersection(&e1, &e2, &g);
	point_rad2deg(&g);
	CU_ASSERT_EQUAL(rv, 2); /* Hack, returning 2 as the 'co-linear' value */

	/* End touches arc at north pole */
	edge_set(-180.0, 80.0, 0.0, 80.0, &e1);
	edge_set(90.0, 80.0, -90.0, 90.0, &e2);
	rv = edge_intersection(&e1, &e2, &g);
	point_rad2deg(&g);
#if 0
	printf("\n");
	printf("LINESTRING(%.15g %.15g, %.15g %.15g)\n", e1.start.lon,  e1.start.lat, e1.end.lon,  e1.end.lat);
	printf("LINESTRING(%.15g %.15g, %.15g %.15g)\n", e2.start.lon,  e2.start.lat, e2.end.lon,  e2.end.lat);
	printf("g = (%.15g %.15g)\n", g.lon, g.lat);
	printf("rv = %d\n", rv);
#endif
	CU_ASSERT_DOUBLE_EQUAL(g.lat, 90.0, 0.00001);
	CU_ASSERT_EQUAL(rv, LW_TRUE);

	/* End touches end at north pole */
	edge_set(-180.0, 80.0, 0.0, 90.0, &e1);
	edge_set(90.0, 80.0, -90.0, 90.0, &e2);
	rv = edge_intersection(&e1, &e2, &g);
	point_rad2deg(&g);
#if 0
	printf("\n");
	printf("LINESTRING(%.15g %.15g, %.15g %.15g)\n", e1.start.lon,  e1.start.lat, e1.end.lon,  e1.end.lat);
	printf("LINESTRING(%.15g %.15g, %.15g %.15g)\n", e2.start.lon,  e2.start.lat, e2.end.lon,  e2.end.lat);
	printf("g = (%.15g %.15g)\n", g.lon, g.lat);
	printf("rv = %d\n", rv);
#endif
	CU_ASSERT_DOUBLE_EQUAL(g.lat, 90.0, 0.00001);
	CU_ASSERT_EQUAL(rv, LW_TRUE);

}

static void line2pts(const char *wkt, POINT3D *A1, POINT3D *A2)
{
	LWLINE *l = (LWLINE*)lwgeom_from_wkt(wkt, LW_PARSER_CHECK_NONE);
	POINTARRAY *pa;
	POINT2D p1, p2;
	GEOGRAPHIC_POINT g1, g2;
	if ( ! l ) 
	{
		printf("BAD WKT FOUND in test_edge_intersects:\n  %s\n\n", wkt);
		exit(0);
	}
	pa = l->points;
	getPoint2d_p(pa, 0, &p1);
	getPoint2d_p(pa, 1, &p2);
	geographic_point_init(p1.x, p1.y, &g1);
	geographic_point_init(p2.x, p2.y, &g2);
	geog2cart(&g1, A1);
	geog2cart(&g2, A2);
	lwline_free(l);
	return;
}

static void test_edge_intersects(void)
{
	POINT3D A1, A2, B1, B2;
	GEOGRAPHIC_POINT g;
	int rv;

	/* Covers case, end-to-end intersection */
	line2pts("LINESTRING(50 -10.999999999999998224, -10.0 50.0)", &A1, &A2);
	line2pts("LINESTRING(-10.0 50.0, -10.272779983831613393 -16.937003313332997578)", &B1, &B2);
	rv = edge_intersects(&A1, &A2, &B1, &B2);
	CU_ASSERT(rv & PIR_INTERSECTS);

	/* Medford case, very short segment vs very long one */
	g.lat = 0.74123572595649878103;
	g.lon = -2.1496353191142714145;
	geog2cart(&g, &A1);
	g.lat = 0.74123631950116664058;
	g.lon = -2.1496353248304860273;
	geog2cart(&g, &A2);
	g.lat = 0.73856343764436815924;
	g.lon = -2.1461493501950630325;
	geog2cart(&g, &B1);
	g.lat = 0.70971354024834598651;
	g.lon = 2.1082194552519770703;
	geog2cart(&g, &B2);
	rv = edge_intersects(&A1, &A2, &B1, &B2);
	CU_ASSERT(rv == 0);

	/* Second Medford case, very short segment vs very long one */
	g.lat = 0.73826546728290887156;
	g.lon = -2.14426380171833042;
	geog2cart(&g, &A1);
	g.lat = 0.73826545883786642843;
	g.lon = -2.1442638997530165668;
	geog2cart(&g, &A2);
	g.lat = 0.73775469118192538165;
	g.lon = -2.1436035534281718817;
	geog2cart(&g, &B1);
	g.lat = 0.71021099548296817705;
	g.lon = 2.1065275171200439353;
	geog2cart(&g, &B2);
	rv = edge_intersects(&A1, &A2, &B1, &B2);
	CU_ASSERT(rv == PIR_INTERSECTS);

	/* Again, this time with a less exact input edge. */
	line2pts("LINESTRING(-123.165031277506 42.4696787216231, -123.165031605021 42.4697127292275)", &A1, &A2);
	rv = edge_intersects(&A1, &A2, &B1, &B2);
	CU_ASSERT(rv == 0);

	/* Intersection at (0 0) */
	line2pts("LINESTRING(-1.0 0.0, 1.0 0.0)", &A1, &A2);
	line2pts("LINESTRING(0.0 -1.0, 0.0 1.0)", &B1, &B2);
	rv = edge_intersects(&A1, &A2, &B1, &B2);
	CU_ASSERT(rv == PIR_INTERSECTS);

	/*  No intersection at (0 0) */
	line2pts("LINESTRING(-1.0 0.0, 1.0 0.0)", &A1, &A2);
	line2pts("LINESTRING(0.0 -1.0, 0.0 -2.0)", &B1, &B2);
	rv = edge_intersects(&A1, &A2, &B1, &B2);
	CU_ASSERT(rv == 0);

	/*  End touches middle of segment at (0 0) */
	line2pts("LINESTRING(-1.0 0.0, 1.0 0.0)", &A1, &A2);
	line2pts("LINESTRING(0.0 -1.0, 0.0 0.0)", &B1, &B2);
	rv = edge_intersects(&A1, &A2, &B1, &B2);
	CU_ASSERT(rv == (PIR_INTERSECTS|PIR_B_TOUCH_RIGHT) );

	/*  End touches end of segment at (0 0) */
	line2pts("LINESTRING(0.0 0.0, 1.0 0.0)", &A1, &A2);
	line2pts("LINESTRING(0.0 -1.0, 0.0 0.0)", &B1, &B2);
	rv = edge_intersects(&A1, &A2, &B1, &B2);
	CU_ASSERT(rv == (PIR_INTERSECTS|PIR_B_TOUCH_RIGHT|PIR_A_TOUCH_RIGHT) );

	/* Intersection at (180 0) */
	line2pts("LINESTRING(-179.0 -1.0, 179.0 1.0)", &A1, &A2);
	line2pts("LINESTRING(-179.0 1.0, 179.0 -1.0)", &B1, &B2);
	rv = edge_intersects(&A1, &A2, &B1, &B2);
	CU_ASSERT(rv == PIR_INTERSECTS);

	/* Intersection at (180 0) */
	line2pts("LINESTRING(-170.0 0.0, 170.0 0.0)", &A1, &A2);
	line2pts("LINESTRING(180.0 -10.0, 180.0 10.0)", &B1, &B2);
	rv = edge_intersects(&A1, &A2, &B1, &B2);
	CU_ASSERT(rv == PIR_INTERSECTS);

	/* Intersection at north pole */
	line2pts("LINESTRING(-180.0 80.0, 0.0 80.0)", &A1, &A2);
	line2pts("LINESTRING(90.0 80.0, -90.0 80.0)", &B1, &B2);
	rv = edge_intersects(&A1, &A2, &B1, &B2);
	CU_ASSERT(rv == PIR_INTERSECTS);

	/* Equal edges return true */
	line2pts("LINESTRING(45.0 10.0, 50.0 20.0)", &A1, &A2);
	line2pts("LINESTRING(45.0 10.0, 50.0 20.0)", &B1, &B2);
	rv = edge_intersects(&A1, &A2, &B1, &B2);
	CU_ASSERT(rv & PIR_INTERSECTS);	
	
	/* Parallel edges (same great circle, different end points) return true  */
	line2pts("LINESTRING(40.0 0.0, 70.0 0.0)", &A1, &A2);
	line2pts("LINESTRING(60.0 0.0, 50.0 0.0)", &B1, &B2);
	rv = edge_intersects(&A1, &A2, &B1, &B2);
	CU_ASSERT(rv == (PIR_INTERSECTS|PIR_COLINEAR) );

	/* End touches arc at north pole */
	line2pts("LINESTRING(-180.0 80.0, 0.0 80.0)", &A1, &A2);
	line2pts("LINESTRING(90.0 80.0, -90.0 90.0)", &B1, &B2);
	rv = edge_intersects(&A1, &A2, &B1, &B2);
	CU_ASSERT(rv == (PIR_INTERSECTS|PIR_B_TOUCH_LEFT) );
	
	/* End touches end at north pole */
	line2pts("LINESTRING(-180.0 80.0, 0.0 90.0)", &A1, &A2);
	line2pts("LINESTRING(90.0 80.0, -90.0 90.0)", &B1, &B2);
	rv = edge_intersects(&A1, &A2, &B1, &B2);
	CU_ASSERT(rv == (PIR_INTERSECTS|PIR_B_TOUCH_LEFT|PIR_A_TOUCH_RIGHT) );

	/* Antipodal straddles. Great circles cross but at opposite */
	/* sides of the globe */
	/* #2534 */
	/* http://www.gcmap.com/mapui?P=60N+90E-20S+90E%0D%0A0N+0E-90.04868865037885W+57.44011727050777S%0D%0A&MS=wls&DU=mi */
	line2pts("LINESTRING(90.0 60.0, 90.0 -20.0)", &A1, &A2);
	line2pts("LINESTRING(0.0 0.0, -90.04868865037885 -57.44011727050777)", &B1, &B2);
	rv = edge_intersects(&A1, &A2, &B1, &B2);
	CU_ASSERT(rv == 0);

	line2pts("LINESTRING(-5 0, 5 0)", &A1, &A2);
	line2pts("LINESTRING(179 -5, 179 5)", &B1, &B2);
	rv = edge_intersects(&A1, &A2, &B1, &B2);
	CU_ASSERT(rv == 0);

	line2pts("LINESTRING(175 -85, 175 85)", &A1, &A2);
	line2pts("LINESTRING(65 0, -105 0)", &B1, &B2);
	rv = edge_intersects(&A1, &A2, &B1, &B2);
	CU_ASSERT(rv == 0);
	
	line2pts("LINESTRING(175 -85, 175 85)", &A1, &A2);
	line2pts("LINESTRING(45 0, -125 0)", &B1, &B2);
	rv = edge_intersects(&A1, &A2, &B1, &B2);
	CU_ASSERT(rv == 0);
	
}

static void test_edge_distance_to_point(void)
{
	GEOGRAPHIC_EDGE e;
	GEOGRAPHIC_POINT g;
	GEOGRAPHIC_POINT closest;
	double d;

	/* closest point at origin, one degree away */
	edge_set(-50.0, 0.0, 50.0, 0.0, &e);
	point_set(0.0, 1.0, &g);
	d = edge_distance_to_point(&e, &g, 0);
	CU_ASSERT_DOUBLE_EQUAL(d, M_PI / 180.0, 0.00001);

	/* closest point at origin, one degree away */
	edge_set(-50.0, 0.0, 50.0, 0.0, &e);
	point_set(0.0, 2.0, &g);
	d = edge_distance_to_point(&e, &g, &closest);
#if 0
	printf("LINESTRING(%.8g %.8g, %.8g %.8g)\n", e.start.lon,  e.start.lat, e.end.lon,  e.end.lat);
	printf("POINT(%.9g %.9g)\n", g.lon, g.lat);
	printf("\nDISTANCE == %.8g\n", d);
#endif
	CU_ASSERT_DOUBLE_EQUAL(d, M_PI / 90.0, 0.00001);
	CU_ASSERT_DOUBLE_EQUAL(closest.lat, 0.0, 0.00001);
	CU_ASSERT_DOUBLE_EQUAL(closest.lon, 0.0, 0.00001);

	/* Ticket #2351 */
	edge_set(149.386990599235, -26.3567415843982, 149.386990599247, -26.3567415843965, &e);
	point_set(149.386990599235, -26.3567415843982, &g);
	d = edge_distance_to_point(&e, &g, &closest);
	CU_ASSERT_DOUBLE_EQUAL(d, 0.0, 0.00001);
	// printf("CLOSE POINT(%g %g)\n", closest.lon,  closest.lat);
	// printf(" ORIG POINT(%g %g)\n", g.lon, g.lat);
	CU_ASSERT_DOUBLE_EQUAL(g.lat, closest.lat, 0.00001);
	CU_ASSERT_DOUBLE_EQUAL(g.lon, closest.lon, 0.00001);		 
}

static void test_edge_distance_to_edge(void)
{
	GEOGRAPHIC_EDGE e1, e2;
	GEOGRAPHIC_POINT c1, c2;
	double d;

	/* closest point at origin, one degree away */
	edge_set(-50.0, 0.0, 50.0, 0.0, &e1);
	edge_set(-5.0, 20.0, 0.0, 1.0, &e2);
	d = edge_distance_to_edge(&e1, &e2, &c1, &c2);
#if 0
	printf("LINESTRING(%.8g %.8g, %.8g %.8g)\n", e1.start.lon,  e1.start.lat, e1.end.lon,  e1.end.lat);
	printf("LINESTRING(%.8g %.8g, %.8g %.8g)\n", e2.start.lon,  e2.start.lat, e2.end.lon,  e2.end.lat);
	printf("\nDISTANCE == %.8g\n", d);
#endif
	CU_ASSERT_DOUBLE_EQUAL(d, M_PI / 180.0, 0.00001);
	CU_ASSERT_DOUBLE_EQUAL(c1.lat, 0.0, 0.00001);
	CU_ASSERT_DOUBLE_EQUAL(c2.lat, M_PI / 180.0, 0.00001);
	CU_ASSERT_DOUBLE_EQUAL(c1.lon, 0.0, 0.00001);
	CU_ASSERT_DOUBLE_EQUAL(c2.lon, 0.0, 0.00001);
}



/*
* Build LWGEOM on top of *aligned* structure so we can use the read-only
* point access methods on them.
*/
static LWGEOM* lwgeom_over_gserialized(char *wkt, GSERIALIZED **g)
{
	LWGEOM *lwg;

	lwg = lwgeom_from_wkt(wkt, LW_PARSER_CHECK_NONE);
	FLAGS_SET_GEODETIC(lwg->flags, 1);
	*g = gserialized_from_lwgeom(lwg, 1, 0);
	lwgeom_free(lwg);
	return lwgeom_from_gserialized(*g);
}

static void test_lwgeom_check_geodetic(void)
{
	LWGEOM *geom;
	int i = 0;

	char ewkt[][512] =
	{
		"POINT(0 0.2)",
		"LINESTRING(-1 -1,-1 2.5,2 2,2 -1)",
		"SRID=1;MULTILINESTRING((-1 -1,-1 2.5,2 2,2 -1),(-1 -1,-1 2.5,2 2,2 -1),(-1 -1,-1 2.5,2 2,2 -1),(-1 -1,-1 2.5,2 2,2 -1))",
		"POLYGON((-1 -1,-1 2.5,2 2,2 -1,-1 -1),(0 0,0 1,1 1,1 0,0 0))",
		"SRID=4326;MULTIPOLYGON(((-1 -1,-1 2.5,2 2,2 -1,-1 -1),(0 0,0 1,1 1,1 0,0 0),(-0.5 -0.5,-0.5 -0.4,-0.4 -0.4,-0.4 -0.5,-0.5 -0.5)),((-1 -1,-1 2.5,2 2,2 -1,-1 -1),(0 0,0 1,1 1,1 0,0 0),(-0.5 -0.5,-0.5 -0.4,-0.4 -0.4,-0.4 -0.5,-0.5 -0.5)))",
		"SRID=4326;GEOMETRYCOLLECTION(POINT(0 1),POLYGON((-1 -1,-1 2.5,2 2,2 -1,-1 -1),(0 0,0 1,1 1,1 0,0 0)),MULTIPOLYGON(((-1 -1,-1 2.5,2 2,2 -1,-1 -1),(0 0,0 1,1 1,1 0,0 0),(-0.5 -0.5,-0.5 -0.4,-0.4 -0.4,-0.4 -0.5,-0.5 -0.5))))",
		"POINT(0 220.2)",
		"LINESTRING(-1 -1,-1231 2.5,2 2,2 -1)",
		"SRID=1;MULTILINESTRING((-1 -131,-1 2.5,2 2,2 -1),(-1 -1,-1 2.5,2 2,2 -1),(-1 -1,-1 2.5,2 2,2 -1),(-1 -1,-1 2.5,2 2,2 -1))",
		"POLYGON((-1 -1,-1 2.5,2 2,2 -133,-1 -1),(0 0,0 1,1 1,1 0,0 0))",
		"SRID=4326;MULTIPOLYGON(((-1 -1,-1 2.5,211 2,2 -1,-1 -1),(0 0,0 1,1 1,1 0,0 0),(-0.5 -0.5,-0.5 -0.4,-0.4 -0.4,-0.4 -0.5,-0.5 -0.5)),((-1 -1,-1 2.5,2 2,2 -1,-1 -1),(0 0,0 1,1 1,1 0,0 0),(-0.5 -0.5,-0.5 -0.4,-0.4 -0.4,-0.4 -0.5,-0.5 -0.5)))",
		"SRID=4326;GEOMETRYCOLLECTION(POINT(0 1),POLYGON((-1 -1,-1111 2.5,2 2,2 -1,-1 -1),(0 0,0 1,1 1,1 0,0 0)),MULTIPOLYGON(((-1 -1,-1 2.5,2 2,2 -1,-1 -1),(0 0,0 1,1 1,1 0,0 0),(-0.5 -0.5,-0.5 -0.4,-0.4 -0.4,-0.4 -0.5,-0.5 -0.5))))",
	};

	for ( i = 0; i < 6; i++ )
	{
		GSERIALIZED *g;
		geom = lwgeom_over_gserialized(ewkt[i], &g);
		CU_ASSERT_EQUAL(lwgeom_check_geodetic(geom), LW_TRUE);
		lwgeom_free(geom);
		lwfree(g);
	}

	for ( i = 6; i < 12; i++ )
	{
		GSERIALIZED *g;
		//char *out_ewkt;
		geom = lwgeom_over_gserialized(ewkt[i], &g);
		CU_ASSERT_EQUAL(lwgeom_check_geodetic(geom), LW_FALSE);
		//out_ewkt = lwgeom_to_ewkt(geom);
		//printf("%s\n", out_ewkt);
		lwgeom_free(geom);
		lwfree(g);
	}

}

/*
static void test_gbox_calculation(void)
{

	LWGEOM *geom;
	int i = 0;
	GBOX *gbox = gbox_new(gflags(0,0,0));
	BOX3D *box3d;

	char ewkt[][512] =
	{
		"POINT(0 0.2)",
		"LINESTRING(-1 -1,-1 2.5,2 2,2 -1)",
		"SRID=1;MULTILINESTRING((-1 -1,-1 2.5,2 2,2 -1),(-1 -1,-1 2.5,2 2,2 -1),(-1 -1,-1 2.5,2 2,2 -1),(-1 -1,-1 2.5,2 2,2 -1))",
		"POLYGON((-1 -1,-1 2.5,2 2,2 -1,-1 -1),(0 0,0 1,1 1,1 0,0 0))",
		"SRID=4326;MULTIPOLYGON(((-1 -1,-1 2.5,2 2,2 -1,-1 -1),(0 0,0 1,1 1,1 0,0 0),(-0.5 -0.5,-0.5 -0.4,-0.4 -0.4,-0.4 -0.5,-0.5 -0.5)),((-1 -1,-1 2.5,2 2,2 -1,-1 -1),(0 0,0 1,1 1,1 0,0 0),(-0.5 -0.5,-0.5 -0.4,-0.4 -0.4,-0.4 -0.5,-0.5 -0.5)))",
		"SRID=4326;GEOMETRYCOLLECTION(POINT(0 1),POLYGON((-1 -1,-1 2.5,2 2,2 -1,-1 -1),(0 0,0 1,1 1,1 0,0 0)),MULTIPOLYGON(((-1 -1,-1 2.5,2 2,2 -1,-1 -1),(0 0,0 1,1 1,1 0,0 0),(-0.5 -0.5,-0.5 -0.4,-0.4 -0.4,-0.4 -0.5,-0.5 -0.5))))",
		"POINT(0 220.2)",
		"LINESTRING(-1 -1,-1231 2.5,2 2,2 -1)",
		"SRID=1;MULTILINESTRING((-1 -131,-1 2.5,2 2,2 -1),(-1 -1,-1 2.5,2 2,2 -1),(-1 -1,-1 2.5,2 2,2 -1),(-1 -1,-1 2.5,2 2,2 -1))",
		"POLYGON((-1 -1,-1 2.5,2 2,2 -133,-1 -1),(0 0,0 1,1 1,1 0,0 0))",
		"SRID=4326;MULTIPOLYGON(((-1 -1,-1 2.5,211 2,2 -1,-1 -1),(0 0,0 1,1 1,1 0,0 0),(-0.5 -0.5,-0.5 -0.4,-0.4 -0.4,-0.4 -0.5,-0.5 -0.5)),((-1 -1,-1 2.5,2 2,2 -1,-1 -1),(0 0,0 1,1 1,1 0,0 0),(-0.5 -0.5,-0.5 -0.4,-0.4 -0.4,-0.4 -0.5,-0.5 -0.5)))",
		"SRID=4326;GEOMETRYCOLLECTION(POINT(0 1),POLYGON((-1 -1,-1111 2.5,2 2,2 -1,-1 -1),(0 0,0 1,1 1,1 0,0 0)),MULTIPOLYGON(((-1 -1,-1 2.5,2 2,2 -1,-1 -1),(0 0,0 1,1 1,1 0,0 0),(-0.5 -0.5,-0.5 -0.4,-0.4 -0.4,-0.4 -0.5,-0.5 -0.5))))",
	};

	for ( i = 0; i < 6; i++ )
	{
		GSERIALIZED *g;
		geom = lwgeom_over_gserialized(ewkt[i], &g);
		lwgeom_calculate_gbox_cartesian(geom, gbox);
		box3d = lwgeom_compute_box3d(geom);
		//printf("%g %g\n", gbox->xmin, box3d->xmin);
		CU_ASSERT_EQUAL(gbox->xmin, box3d->xmin);
		CU_ASSERT_EQUAL(gbox->xmax, box3d->xmax);
		CU_ASSERT_EQUAL(gbox->ymin, box3d->ymin);
		CU_ASSERT_EQUAL(gbox->ymax, box3d->ymax);
		lwgeom_free(geom);
		lwfree(box3d);
		lwfree(g);
	}
	lwfree(gbox);
}
*/

static void test_gserialized_from_lwgeom(void)
{
	LWGEOM *geom;
	GSERIALIZED *g;
	uint32_t type;
	double *inspect; /* To poke right into the blob. */

	geom = lwgeom_from_wkt("POINT(0 0.2)", LW_PARSER_CHECK_NONE);
	FLAGS_SET_GEODETIC(geom->flags, 1);
	g = gserialized_from_lwgeom(geom, 1, 0);
	type = gserialized_get_type(g);
	CU_ASSERT_EQUAL( type, POINTTYPE );
	inspect = (double*)g;
	CU_ASSERT_EQUAL(inspect[3], 0.2);
	lwgeom_free(geom);
	lwfree(g);

	geom = lwgeom_from_wkt("POLYGON((-1 -1, -1 2.5, 2 2, 2 -1, -1 -1), (0 0, 0 1, 1 1, 1 0, 0 0))", LW_PARSER_CHECK_NONE);
	FLAGS_SET_GEODETIC(geom->flags, 1);
	g = gserialized_from_lwgeom(geom, 1, 0);
	type = gserialized_get_type(g);
	CU_ASSERT_EQUAL( type, POLYGONTYPE );
	inspect = (double*)g;
	CU_ASSERT_EQUAL(inspect[9], 2.5);
	lwgeom_free(geom);
	lwfree(g);

	geom = lwgeom_from_wkt("MULTILINESTRING((0 0, 1 1),(0 0.1, 1 1))", LW_PARSER_CHECK_NONE);
	FLAGS_SET_GEODETIC(geom->flags, 1);
	g = gserialized_from_lwgeom(geom, 1, 0);
	type = gserialized_get_type(g);
	CU_ASSERT_EQUAL( type, MULTILINETYPE );
	inspect = (double*)g;
	CU_ASSERT_EQUAL(inspect[12], 0.1);
	lwgeom_free(geom);
	lwfree(g);

}

static void test_ptarray_contains_point_sphere(void)
{
	LWGEOM *lwg;
	LWPOLY *poly;
	POINT2D pt_to_test;
	POINT2D pt_outside;
	int result;

	/* Small polygon and huge distance between outside point and close-but-not-quite-inside point. Should return LW_FALSE. Pretty degenerate case. */
	lwg = lwgeom_from_hexwkb("0103000020E61000000100000025000000ACAD6F91DDB65EC03F84A86D57264540CCABC279DDB65EC0FCE6926B57264540B6DEAA62DDB65EC0A79F6B63572645402E0BE84CDDB65EC065677155572645405D0B1D39DDB65EC0316310425726454082B5DB27DDB65EC060A4E12957264540798BB619DDB65EC0C393A10D57264540D4BC160FDDB65EC0BD0320EE56264540D7AC4E08DDB65EC096C862CC56264540AFD29205DDB65EC02A1F68A956264540363AFA06DDB65EC0722E418656264540B63A780CDDB65EC06E9B0064562645409614E215DDB65EC0E09DA84356264540FF71EF22DDB65EC0B48145265626454036033F33DDB65EC081B8A60C5626454066FB4546DDB65EC08A47A6F7552645409061785BDDB65EC0F05AE0E755264540D4B63772DDB65EC05C86CEDD55264540D2E4C689DDB65EC09B6EBFD95526454082E573A1DDB65EC0C90BD5DB552645401ABE85B8DDB65EC06692FCE35526454039844ECEDDB65EC04D8AF6F155264540928319E2DDB65EC0AD8D570556264540D31055F3DDB65EC02D618F1D56264540343B7A01DEB65EC0EB70CF3956264540920A1A0CDEB65EC03B00515956264540911BE212DEB65EC0E43A0E7B56264540E3F69D15DEB65EC017E4089E562645408D903614DEB65EC0F0D42FC1562645402191B80EDEB65EC0586870E35626454012B84E05DEB65EC09166C80357264540215B41F8DDB65EC08F832B21572645408392F7E7DDB65EC01138C13A57264540F999F0D4DDB65EC0E4A9C14F57264540AC3FB8BFDDB65EC0EED6875F57264540D3DCFEA8DDB65EC04F6C996957264540ACAD6F91DDB65EC03F84A86D57264540", LW_PARSER_CHECK_NONE);
	poly = (LWPOLY*)lwg;
	pt_to_test.x = -122.819436560680316;
	pt_to_test.y = 42.2702301207017328;
	pt_outside.x = 120.695136159150778;
	pt_outside.y = 40.6920926049588516;
	result = ptarray_contains_point_sphere(poly->rings[0], &pt_outside, &pt_to_test);
	CU_ASSERT_EQUAL(result, LW_FALSE);
	lwgeom_free(lwg);
	
	/* Point on ring between vertexes case */
	lwg = lwgeom_from_wkt("POLYGON((1.0 1.0, 1.0 1.1, 1.1 1.1, 1.1 1.0, 1.0 1.0))", LW_PARSER_CHECK_NONE);
	poly = (LWPOLY*)lwg;
	pt_to_test.x = 1.1;
	pt_to_test.y = 1.05;
	pt_outside.x = 1.2;
	pt_outside.y = 1.05;
	result = ptarray_contains_point_sphere(poly->rings[0], &pt_outside, &pt_to_test);
	CU_ASSERT_EQUAL(result, LW_TRUE);
	lwgeom_free(lwg);
	
	/* Simple containment case */
	lwg = lwgeom_from_wkt("POLYGON((1.0 1.0, 1.0 1.1, 1.1 1.1, 1.1 1.0, 1.0 1.0))", LW_PARSER_CHECK_NONE);
	poly = (LWPOLY*)lwg;
	pt_to_test.x = 1.05;
	pt_to_test.y = 1.05;
	pt_outside.x = 1.2;
	pt_outside.y = 1.15;
	result = ptarray_contains_point_sphere(poly->rings[0], &pt_outside, &pt_to_test);
	CU_ASSERT_EQUAL(result, LW_TRUE);
	lwgeom_free(lwg);

	/* Less Simple containment case. */
	/* Interior point quite close to boundary and stab line going through bottom edge vertex */
	/* This breaks the "extend-it" trick of handling vertex crossings */
	/* It should also break the "lowest end" trick. */
	lwg = lwgeom_from_wkt("POLYGON((1.0 1.0, 1.0 1.1, 1.1 1.1, 1.1 1.0, 1.05 0.95, 1.0 1.0))", LW_PARSER_CHECK_NONE);
	poly = (LWPOLY*)lwg;
	pt_to_test.x = 1.05;
	pt_to_test.y = 1.00;
	pt_outside.x = 1.05;
	pt_outside.y = 0.5;
	result = ptarray_contains_point_sphere(poly->rings[0], &pt_outside, &pt_to_test);
	CU_ASSERT_EQUAL(result, LW_TRUE);
	lwgeom_free(lwg);

	/* Simple noncontainment case */
	lwg = lwgeom_from_wkt("POLYGON((1.0 1.0, 1.0 1.1, 1.1 1.1, 1.1 1.0, 1.0 1.0))", LW_PARSER_CHECK_NONE);
	poly = (LWPOLY*)lwg;
	pt_to_test.x = 1.05;
	pt_to_test.y = 1.15;
	pt_outside.x = 1.2;
	pt_outside.y = 1.2;
	result = ptarray_contains_point_sphere(poly->rings[0], &pt_outside, &pt_to_test);
	CU_ASSERT_EQUAL(result, LW_FALSE);
	lwgeom_free(lwg);

	/* Harder noncontainment case */
	lwg = lwgeom_from_wkt("POLYGON((1.0 1.0, 1.0 1.1, 1.1 1.1, 1.1 1.0, 1.0 1.0))", LW_PARSER_CHECK_NONE);
	poly = (LWPOLY*)lwg;
	pt_to_test.x = 1.05;
	pt_to_test.y = 0.9;
	pt_outside.x = 1.2;
	pt_outside.y = 1.05;
	result = ptarray_contains_point_sphere(poly->rings[0], &pt_outside, &pt_to_test);
	CU_ASSERT_EQUAL(result, LW_FALSE);
	lwgeom_free(lwg);

	/* Harder containment case */
	lwg = lwgeom_from_wkt("POLYGON((0 0, 0 2, 1 2, 0 3, 2 3, 0 4, 3 5, 0 6, 6 10, 6 1, 0 0))", LW_PARSER_CHECK_NONE);
	poly = (LWPOLY*)lwg;
	pt_to_test.x = 1.0;
	pt_to_test.y = 1.0;
	pt_outside.x = 1.0;
	pt_outside.y = 10.0;
	result = ptarray_contains_point_sphere(poly->rings[0], &pt_outside, &pt_to_test);
	CU_ASSERT_EQUAL(result, LW_TRUE);
	lwgeom_free(lwg);

	/* Point on ring at first vertex case */
	lwg = lwgeom_from_wkt("POLYGON((1.0 1.0, 1.0 1.1, 1.1 1.1, 1.1 1.0, 1.0 1.0))", LW_PARSER_CHECK_NONE);
	poly = (LWPOLY*)lwg;
	pt_to_test.x = 1.0;
	pt_to_test.y = 1.0;
	pt_outside.x = 1.2;
	pt_outside.y = 1.05;
	result = ptarray_contains_point_sphere(poly->rings[0], &pt_outside, &pt_to_test);
	CU_ASSERT_EQUAL(result, LW_TRUE);
	lwgeom_free(lwg);

	/* Point on ring at vertex case */
	lwg = lwgeom_from_wkt("POLYGON((1.0 1.0, 1.0 1.1, 1.1 1.1, 1.1 1.0, 1.0 1.0))", LW_PARSER_CHECK_NONE);
	poly = (LWPOLY*)lwg;
	pt_to_test.x = 1.0;
	pt_to_test.y = 1.1;
	pt_outside.x = 1.2;
	pt_outside.y = 1.05;
	result = ptarray_contains_point_sphere(poly->rings[0], &pt_outside, &pt_to_test);
	CU_ASSERT_EQUAL(result, LW_TRUE);
	lwgeom_free(lwg);

	/* Co-linear crossing case for point-in-polygon test, should return LW_TRUE */
	lwg = lwgeom_from_wkt("POLYGON((1.0 1.0, 1.0 1.1, 1.1 1.1, 1.1 1.2, 1.2 1.2, 1.2 1.0, 1.0 1.0))", LW_PARSER_CHECK_NONE);
	poly = (LWPOLY*)lwg;
	pt_to_test.x = 1.1;
	pt_to_test.y = 1.05;
	pt_outside.x = 1.1;
	pt_outside.y = 1.3;
	result = ptarray_contains_point_sphere(poly->rings[0], &pt_outside, &pt_to_test);
	CU_ASSERT_EQUAL(result, LW_TRUE);
	lwgeom_free(lwg);

	/* Co-linear grazing case for point-in-polygon test, should return LW_FALSE */
	lwg = lwgeom_from_wkt("POLYGON((1.0 1.0, 1.0 1.1, 1.1 1.1, 1.1 1.2, 1.2 1.2, 1.2 1.0, 1.0 1.0))", LW_PARSER_CHECK_NONE);
	poly = (LWPOLY*)lwg;
	pt_to_test.x = 1.0;
	pt_to_test.y = 0.0;
	pt_outside.x = 1.0;
	pt_outside.y = 2.0;
	result = ptarray_contains_point_sphere(poly->rings[0], &pt_outside, &pt_to_test);
	CU_ASSERT_EQUAL(result, LW_FALSE);
	lwgeom_free(lwg);

	/* Grazing case for point-in-polygon test, should return LW_FALSE */
	lwg = lwgeom_from_wkt("POLYGON((1.0 1.0, 1.0 2.0, 1.5 1.5, 1.0 1.0))", LW_PARSER_CHECK_NONE);
	poly = (LWPOLY*)lwg;
	pt_to_test.x = 1.5;
	pt_to_test.y = 1.0;
	pt_outside.x = 1.5;
	pt_outside.y = 2.0;
	result = ptarray_contains_point_sphere(poly->rings[0], &pt_outside, &pt_to_test);
	CU_ASSERT_EQUAL(result, LW_FALSE);
	lwgeom_free(lwg);

	/* Grazing case at first point for point-in-polygon test, should return LW_FALSE */
	lwg = lwgeom_from_wkt("POLYGON((1.0 1.0, 2.0 3.0, 2.0 0.0, 1.0 1.0))", LW_PARSER_CHECK_NONE);
	poly = (LWPOLY*)lwg;
	pt_to_test.x = 1.0;
	pt_to_test.y = 0.0;
	pt_outside.x = 1.0;
	pt_outside.y = 2.0;
	result = ptarray_contains_point_sphere(poly->rings[0], &pt_outside, &pt_to_test);
	CU_ASSERT_EQUAL(result, LW_FALSE);
	lwgeom_free(lwg);

	/* Outside multi-crossing case for point-in-polygon test, should return LW_FALSE */
	lwg = lwgeom_from_wkt("POLYGON((1.0 1.0, 1.0 1.1, 1.1 1.1, 1.1 1.2, 1.2 1.2, 1.2 1.0, 1.0 1.0))", LW_PARSER_CHECK_NONE);
	poly = (LWPOLY*)lwg;
	pt_to_test.x = 0.99;
	pt_to_test.y = 0.99;
	pt_outside.x = 1.21;
	pt_outside.y = 1.21;
	result = ptarray_contains_point_sphere(poly->rings[0], &pt_outside, &pt_to_test);
	CU_ASSERT_EQUAL(result, LW_FALSE);
	lwgeom_free(lwg);

	/* Inside multi-crossing case for point-in-polygon test, should return LW_TRUE */
	lwg = lwgeom_from_wkt("POLYGON((1.0 1.0, 1.0 1.1, 1.1 1.1, 1.1 1.2, 1.2 1.2, 1.2 1.0, 1.0 1.0))", LW_PARSER_CHECK_NONE);
	poly = (LWPOLY*)lwg;
	pt_to_test.x = 1.11;
	pt_to_test.y = 1.11;
	pt_outside.x = 1.21;
	pt_outside.y = 1.21;
	result = ptarray_contains_point_sphere(poly->rings[0], &pt_outside, &pt_to_test);
	CU_ASSERT_EQUAL(result, LW_TRUE);
	lwgeom_free(lwg);

	/* Point on vertex of ring */
	lwg = lwgeom_from_wkt("POLYGON((-9 50,51 -11,-10 50,-9 50))", LW_PARSER_CHECK_NONE);
	poly = (LWPOLY*)lwg;
	pt_to_test.x = -10.0;
	pt_to_test.y = 50.0;
	pt_outside.x = -10.2727799838316134;
	pt_outside.y = -16.9370033133329976;
	result = ptarray_contains_point_sphere(poly->rings[0], &pt_outside, &pt_to_test);
	CU_ASSERT_EQUAL(result, LW_TRUE);
	lwgeom_free(lwg);

}

static void test_lwpoly_covers_point2d(void)
{
	LWPOLY *poly;
	LWGEOM *lwg;
	POINT2D pt_to_test;
	int result;

	lwg = lwgeom_from_wkt("POLYGON((-9 50,51 -11,-10 50,-9 50))", LW_PARSER_CHECK_NONE);
	poly = (LWPOLY*)lwg;
	pt_to_test.x = -10.0;
	pt_to_test.y = 50.0;
	result = lwpoly_covers_point2d(poly, &pt_to_test);
	CU_ASSERT_EQUAL(result, LW_TRUE);
	lwgeom_free(lwg);

	/* Great big ring */
	lwg = lwgeom_from_wkt("POLYGON((-40.0 52.0, 102.0 -6.0, -67.0 -29.0, -40.0 52.0))", LW_PARSER_CHECK_NONE);
	poly = (LWPOLY*)lwg;
	pt_to_test.x = 4.0;
	pt_to_test.y = 11.0;
	result = lwpoly_covers_point2d(poly, &pt_to_test);
	CU_ASSERT_EQUAL(result, LW_TRUE);
	lwgeom_free(lwg);

	/* Triangle over the antimeridian */
	lwg = lwgeom_from_wkt("POLYGON((140 52, 152.0 -6.0, -120.0 -29.0, 140 52))", LW_PARSER_CHECK_NONE);
	poly = (LWPOLY*)lwg;
	pt_to_test.x = -172.0;
	pt_to_test.y = -13.0;
	result = lwpoly_covers_point2d(poly, &pt_to_test);
	CU_ASSERT_EQUAL(result, LW_TRUE);
	lwgeom_free(lwg);
	
}

static void test_ptarray_contains_point_sphere_iowa(void)
{
	LWGEOM *lwg = lwgeom_from_wkt(iowa_data, LW_PARSER_CHECK_NONE);
	LWPOLY *poly = (LWPOLY*)lwg;
	POINTARRAY *pa = poly->rings[0];
	POINT2D pt_outside, pt_to_test;
	int rv;

	pt_to_test.x = -95.900000000000006;
	pt_to_test.y = 42.899999999999999;
	pt_outside.x = -96.381873780830645;
	pt_outside.y = 40.185394449416371;
	
	rv = ptarray_contains_point_sphere(pa, &pt_outside, &pt_to_test);
	CU_ASSERT_EQUAL(rv, LW_TRUE);

	lwgeom_free(lwg);
}


static void test_lwgeom_distance_sphere(void)
{
	LWGEOM *lwg1, *lwg2;
	double d;
	SPHEROID s;

	/* Init and force spherical */
	spheroid_init(&s, 6378137.0, 6356752.314245179498);
	s.a = s.b = s.radius;

	/* Line/line distance, 1 degree apart */
	lwg1 = lwgeom_from_wkt("LINESTRING(-30 10, -20 5, -10 3, 0 1)", LW_PARSER_CHECK_NONE);
	lwg2 = lwgeom_from_wkt("LINESTRING(-10 -5, -5 0, 5 0, 10 -5)", LW_PARSER_CHECK_NONE);
	d = lwgeom_distance_spheroid(lwg1, lwg2, &s, 0.0);
	CU_ASSERT_DOUBLE_EQUAL(d, s.radius * M_PI / 180.0, 0.00001);
	lwgeom_free(lwg1);
	lwgeom_free(lwg2);

	/* Line/line distance, crossing, 0.0 apart */
	lwg1 = lwgeom_from_wkt("LINESTRING(-30 10, -20 5, -10 3, 0 1)", LW_PARSER_CHECK_NONE);
	lwg2 = lwgeom_from_wkt("LINESTRING(-10 -5, -5 20, 5 0, 10 -5)", LW_PARSER_CHECK_NONE);
	d = lwgeom_distance_spheroid(lwg1, lwg2, &s, 0.0);
	CU_ASSERT_DOUBLE_EQUAL(d, 0.0, 0.00001);
	lwgeom_free(lwg1);
	lwgeom_free(lwg2);

	/* Line/point distance, 1 degree apart */
	lwg1 = lwgeom_from_wkt("POINT(-4 1)", LW_PARSER_CHECK_NONE);
	lwg2 = lwgeom_from_wkt("LINESTRING(-10 -5, -5 0, 5 0, 10 -5)", LW_PARSER_CHECK_NONE);
	d = lwgeom_distance_spheroid(lwg1, lwg2, &s, 0.0);
	CU_ASSERT_DOUBLE_EQUAL(d, s.radius * M_PI / 180.0, 0.00001);
	lwgeom_free(lwg1);
	lwgeom_free(lwg2);

	lwg1 = lwgeom_from_wkt("POINT(-4 1)", LW_PARSER_CHECK_NONE);
	lwg2 = lwgeom_from_wkt("POINT(-4 -1)", LW_PARSER_CHECK_NONE);
	d = lwgeom_distance_spheroid(lwg1, lwg2, &s, 0.0);
	CU_ASSERT_DOUBLE_EQUAL(d, s.radius * M_PI / 90.0, 0.00001);
	lwgeom_free(lwg1);
	lwgeom_free(lwg2);

	/* Poly/point distance, point inside polygon, 0.0 apart */
	lwg1 = lwgeom_from_wkt("POLYGON((-4 1, -3 5, 1 2, 1.5 -5, -4 1))", LW_PARSER_CHECK_NONE);
	lwg2 = lwgeom_from_wkt("POINT(-1 -1)", LW_PARSER_CHECK_NONE);
	d = lwgeom_distance_spheroid(lwg1, lwg2, &s, 0.0);
	CU_ASSERT_DOUBLE_EQUAL(d, 0.0, 0.00001);
	lwgeom_free(lwg1);
	lwgeom_free(lwg2);

	/* Poly/point distance, point inside polygon hole, 1 degree apart */
	lwg1 = lwgeom_from_wkt("POLYGON((-4 -4, -4 4, 4 4, 4 -4, -4 -4), (-2 -2, -2 2, 2 2, 2 -2, -2 -2))", LW_PARSER_CHECK_NONE);
	lwg2 = lwgeom_from_wkt("POINT(-1 -1)", LW_PARSER_CHECK_NONE);
	d = lwgeom_distance_spheroid(lwg1, lwg2, &s, 0.0);
	CU_ASSERT_DOUBLE_EQUAL(d, 111178.142466, 0.1);
	lwgeom_free(lwg1);
	lwgeom_free(lwg2);

	/* Poly/point distance, point on hole boundary, 0.0 apart */
	lwg1 = lwgeom_from_wkt("POLYGON((-4 -4, -4 4, 4 4, 4 -4, -4 -4), (-2 -2, -2 2, 2 2, 2 -2, -2 -2))", LW_PARSER_CHECK_NONE);
	lwg2 = lwgeom_from_wkt("POINT(2 2)", LW_PARSER_CHECK_NONE);
	d = lwgeom_distance_spheroid(lwg1, lwg2, &s, 0.0);
	CU_ASSERT_DOUBLE_EQUAL(d, 0.0, 0.00001);
	lwgeom_free(lwg1);
	lwgeom_free(lwg2);

	/* Medford test case #1 */
	lwg1 = lwgeom_from_hexwkb("0105000020E610000001000000010200000002000000EF7B8779C7BD5EC0FD20D94B852845400E539C62B9BD5EC0F0A5BE767C284540", LW_PARSER_CHECK_NONE);
	lwg2 = lwgeom_from_hexwkb("0106000020E61000000100000001030000000100000007000000280EC3FB8CCA5EC0A5CDC747233C45402787C8F58CCA5EC0659EA2761E3C45400CED58DF8FCA5EC0C37FAE6E1E3C4540AE97B8E08FCA5EC00346F58B1F3C4540250359FD8ECA5EC05460628E1F3C45403738F4018FCA5EC05DC84042233C4540280EC3FB8CCA5EC0A5CDC747233C4540", LW_PARSER_CHECK_NONE);
	d = lwgeom_distance_spheroid(lwg1, lwg2, &s, 0.0);
	CU_ASSERT_DOUBLE_EQUAL(d, 23630.8003, 0.1);
	lwgeom_free(lwg1);
	lwgeom_free(lwg2);

	/* Ticket #2351 */
	lwg1 = lwgeom_from_wkt("LINESTRING(149.386990599235 -26.3567415843982,149.386990599247 -26.3567415843965)", LW_PARSER_CHECK_NONE);
	lwg2 = lwgeom_from_wkt("POINT(149.386990599235 -26.3567415843982)", LW_PARSER_CHECK_NONE);
	d = lwgeom_distance_spheroid(lwg1, lwg2, &s, 0.0);
	CU_ASSERT_DOUBLE_EQUAL(d, 0.0, 0.00001);
	lwgeom_free(lwg1);
	lwgeom_free(lwg2);

	/* Ticket #2638, no "M" */
	lwg1 = lwgeom_from_wkt("LINESTRING (-41.0821 50.3036,50 -41)", LW_PARSER_CHECK_NONE);
	lwg2 = lwgeom_from_wkt("POLYGON((0 0,10 0,10 10,0 10,0 0),(5 5,7 5,7 7,5 7,5 5))", LW_PARSER_CHECK_NONE);
	d = lwgeom_distance_spheroid(lwg1, lwg2, &s, 0.0);
	CU_ASSERT_DOUBLE_EQUAL(d, 0.0, 0.00001);
	lwgeom_free(lwg1);
	lwgeom_free(lwg2);

	/* Ticket #2638, with "M" */
	lwg1 = lwgeom_from_wkt("LINESTRING M (-41.0821 50.3036 1,50 -41 1)", LW_PARSER_CHECK_NONE);
	lwg2 = lwgeom_from_wkt("POLYGON M ((0 0 2,10 0 1,10 10 -2,0 10 -5,0 0 -5),(5 5 6,7 5 6,7 7 6,5 7 10,5 5 -2))", LW_PARSER_CHECK_NONE);
	d = lwgeom_distance_spheroid(lwg1, lwg2, &s, 0.0);
	CU_ASSERT_DOUBLE_EQUAL(d, 0.0, 0.00001);
	lwgeom_free(lwg1);
	lwgeom_free(lwg2);
}

static void test_spheroid_distance(void)
{
	GEOGRAPHIC_POINT g1, g2;
	double d;
#ifdef USE_PRE22GEODESIC
	double epsilon; /* irregular */
#else
	const double epsilon = 1e-8; /* at least 10 nm precision */
#endif
	SPHEROID s;

	/* Init to WGS84 */
	spheroid_init(&s, 6378137.0, 6356752.314245179498);

	/* One vertical degree
	$ GeodSolve -E -i -p 16 --input-string "0 0 1 0" */
	point_set(0.0, 0.0, &g1);
	point_set(0.0, 1.0, &g2);
	d = spheroid_distance(&g1, &g2, &s);
#ifdef USE_PRE22GEODESIC
	epsilon = 1e-6;
#endif
	CU_ASSERT_DOUBLE_EQUAL(d, 110574.3885577987957342, epsilon);

	/* Ten horizontal degrees
	$ GeodSolve -E -i -p 16 --input-string "0 -10 0 0" */
	point_set(-10.0, 0.0, &g1);
	point_set(0.0, 0.0, &g2);
	d = spheroid_distance(&g1, &g2, &s);
#ifdef USE_PRE22GEODESIC
	epsilon = 1e-3;
#endif
	CU_ASSERT_DOUBLE_EQUAL(d, 1113194.9079327357264771, epsilon);

	/* One horizonal degree
	$ GeodSolve -E -i -p 16 --input-string "0 -1 0 0" */
	point_set(-1.0, 0.0, &g1);
	point_set(0.0, 0.0, &g2);
	d = spheroid_distance(&g1, &g2, &s);
#ifdef USE_PRE22GEODESIC
	epsilon = 1e-4;
#endif
	CU_ASSERT_DOUBLE_EQUAL(d, 111319.4907932735726477, epsilon);

	/* Around world w/ slight bend
	$ GeodSolve -E -i -p 16 --input-string "0 -180 1 0" */
	point_set(-180.0, 0.0, &g1);
	point_set(0.0, 1.0, &g2);
	d = spheroid_distance(&g1, &g2, &s);
#ifdef USE_PRE22GEODESIC
	epsilon = 1e-5;
#endif
	CU_ASSERT_DOUBLE_EQUAL(d, 19893357.0700676468277450, epsilon);

	/* Up to pole
	$ GeodSolve -E -i -p 16 --input-string "0 -180 90 0" */
	point_set(-180.0, 0.0, &g1);
	point_set(0.0, 90.0, &g2);
	d = spheroid_distance(&g1, &g2, &s);
#ifdef USE_PRE22GEODESIC
	epsilon = 1e-6;
#endif
	CU_ASSERT_DOUBLE_EQUAL(d, 10001965.7293127228117396, epsilon);

}

static void test_spheroid_area(void)
{
	LWGEOM *lwg;
	GBOX gbox;
	double a1, a2;
	SPHEROID s;

	/* Init to WGS84 */
	spheroid_init(&s, WGS84_MAJOR_AXIS, WGS84_MINOR_AXIS);

	gbox.flags = gflags(0, 0, 1);

	/* Medford lot test polygon */
	lwg = lwgeom_from_wkt("POLYGON((-122.848227067007 42.5007249610493,-122.848309475585 42.5007179884263,-122.848327688675 42.500835880696,-122.848245279942 42.5008428533324,-122.848227067007 42.5007249610493))", LW_PARSER_CHECK_NONE);
	lwgeom_calculate_gbox_geodetic(lwg, &gbox);
	/* sphere: Planimeter -E -p 20 -e $WGS84_SPHERE -r --input-string \
	"42.5007249610493 -122.848227067007;42.5007179884263 -122.848309475585;"\
	"42.500835880696 -122.848327688675;42.5008428533324 -122.848245279942" */
	a1 = lwgeom_area_sphere(lwg, &s);
	CU_ASSERT_DOUBLE_EQUAL(a1, 89.721147136698008, 0.1);
	/* spheroid: Planimeter -E -p 20 -r --input-string \
	"42.5007249610493 -122.848227067007;42.5007179884263 -122.848309475585;"\
	"42.500835880696 -122.848327688675;42.5008428533324 -122.848245279942" */
	a2 = lwgeom_area_spheroid(lwg, &s);
	CU_ASSERT_DOUBLE_EQUAL(a2, 89.868413479309585, 0.1);
	lwgeom_free(lwg);

	/* Big-ass polygon */
	lwg = lwgeom_from_wkt("POLYGON((-2 3, -2 4, -1 4, -1 3, -2 3))", LW_PARSER_CHECK_NONE);
	lwgeom_calculate_gbox_geodetic(lwg, &gbox);
	/* sphere: Planimeter -E -p 20 -e $WGS84_SPHERE -r --input-string "3 -2;4 -2;4 -1;3 -1" */
	a1 = lwgeom_area_sphere(lwg, &s);
	CU_ASSERT_DOUBLE_EQUAL(a1, 12341436880.106982993974659, 0.1);
	/* spheroid: Planimeter -E -p 20 -r --input-string "3 -2;4 -2;4 -1;3 -1" */
#ifndef USE_PRE22GEODESIC
	a2 = lwgeom_area_spheroid(lwg, &s);
	CU_ASSERT_DOUBLE_EQUAL(a2, 12286884908.946891319597874, 0.1);
#endif
	lwgeom_free(lwg);

	/* One-degree square */
	lwg = lwgeom_from_wkt("POLYGON((8.5 2,8.5 1,9.5 1,9.5 2,8.5 2))", LW_PARSER_CHECK_NONE);
	lwgeom_calculate_gbox_geodetic(lwg, &gbox);
	/* sphere: Planimeter -E -p 20 -e $WGS84_SPHERE --input-string "2 8.5;1 8.5;1 9.5;2 9.5" */
	a1 = lwgeom_area_sphere(lwg, &s);
	CU_ASSERT_DOUBLE_EQUAL(a1, 12360265021.368023059138681, 0.1);
	/* spheroid: Planimeter -E -p 20 --input-string "2 8.5;1 8.5;1 9.5;2 9.5" */
#ifndef USE_PRE22GEODESIC
	a2 = lwgeom_area_spheroid(lwg, &s);
	CU_ASSERT_DOUBLE_EQUAL(a2, 12305128751.042900673161556, 0.1);
#endif
	lwgeom_free(lwg);

	/* One-degree square *near* the antimeridian */
	lwg = lwgeom_from_wkt("POLYGON((179.5 2,179.5 1,178.5 1,178.5 2,179.5 2))", LW_PARSER_CHECK_NONE);
	lwgeom_calculate_gbox_geodetic(lwg, &gbox);
	/* sphere: Planimeter -E -p 20 -e $WGS84_SPHERE -r --input-string "2 179.5;1 179.5;1 178.5;2 178.5" */
	a1 = lwgeom_area_sphere(lwg, &s);
	CU_ASSERT_DOUBLE_EQUAL(a1, 12360265021.368023059138681, 0.1);
	/* spheroid: Planimeter -E -p 20 -r --input-string "2 179.5;1 179.5;1 178.5;2 178.5" */
#ifndef USE_PRE22GEODESIC
	a2 = lwgeom_area_spheroid(lwg, &s);
	CU_ASSERT_DOUBLE_EQUAL(a2, 12305128751.042900673161556, 0.1);
#endif
	lwgeom_free(lwg);

	/* One-degree square *across* the antimeridian */
	lwg = lwgeom_from_wkt("POLYGON((179.5 2,179.5 1,-179.5 1,-179.5 2,179.5 2))", LW_PARSER_CHECK_NONE);
	lwgeom_calculate_gbox_geodetic(lwg, &gbox);
	/* sphere: Planimeter -E -p 20 -e $WGS84_SPHERE --input-string "2 179.5;1 179.5;1 -179.5;2 -179.5" */
	a1 = lwgeom_area_sphere(lwg, &s);
	CU_ASSERT_DOUBLE_EQUAL(a1, 12360265021.368023059138681, 0.1);
	/* spheroid: Planimeter -E -p 20 --input-string "2 179.5;1 179.5;1 -179.5;2 -179.5" */
#ifndef USE_PRE22GEODESIC
	a2 = lwgeom_area_spheroid(lwg, &s);
	CU_ASSERT_DOUBLE_EQUAL(a2, 12305128751.042900673161556, 0.1);
#endif
	lwgeom_free(lwg);
}

static void test_gbox_utils(void)
{
	LWGEOM *lwg;
	GBOX gbox;
	double a1, a2;
	SPHEROID s;
	POINT2D pt;

	/* Init to WGS84 */
	spheroid_init(&s, WGS84_MAJOR_AXIS, WGS84_MINOR_AXIS);

	gbox.flags = gflags(0, 0, 1);
	
	/* One-degree square by equator */
	lwg = lwgeom_from_wkt("POLYGON((1 20,1 21,2 21,2 20,1 20))", LW_PARSER_CHECK_NONE);
	lwgeom_calculate_gbox_geodetic(lwg, &gbox);
	a1 = gbox_angular_width(&gbox);
	a2 = gbox_angular_height(&gbox);
	CU_ASSERT_DOUBLE_EQUAL(a1, 0.0177951, 0.0000001);
	CU_ASSERT_DOUBLE_EQUAL(a2, 0.017764, 0.0000001);
	lwgeom_free(lwg);

	/* One-degree square *across* antimeridian */
	lwg = lwgeom_from_wkt("POLYGON((179.5 2,179.5 1,-179.5 1,-179.5 2,179.5 2))", LW_PARSER_CHECK_NONE);
	lwgeom_calculate_gbox_geodetic(lwg, &gbox);
	a1 = gbox_angular_width(&gbox);
	a2 = gbox_angular_height(&gbox);
	//printf("a1=%g a2=%g\n", a1, a2);
	CU_ASSERT_DOUBLE_EQUAL(a1, 0.0174613, 0.0000001);
	CU_ASSERT_DOUBLE_EQUAL(a2, 0.0174553, 0.0000001);
	lwgeom_free(lwg);
	
	/* One-degree square *across* antimeridian */
	lwg = lwgeom_from_wkt("POLYGON((178.5 2,178.5 1,-179.5 1,-179.5 2,178.5 2))", LW_PARSER_CHECK_NONE);
	lwgeom_calculate_gbox_geodetic(lwg, &gbox);
	gbox_centroid(&gbox, &pt);
	//printf("POINT(%g %g)\n", pt.x, pt.y);
	CU_ASSERT_DOUBLE_EQUAL(pt.x, 179.5, 0.0001);
	CU_ASSERT_DOUBLE_EQUAL(pt.y, 1.50024, 0.0001);
	lwgeom_free(lwg);
	
}

static void test_vector_angle(void)
{
	POINT3D p1, p2;
	double angle;
	
	memset(&p1, 0, sizeof(POINT3D));
	memset(&p2, 0, sizeof(POINT3D));
	
	p1.x = 1.0;
	p2.y = 1.0;
	angle = vector_angle(&p1, &p2);
	CU_ASSERT_DOUBLE_EQUAL(angle, M_PI_2, 0.00001);

	p1.x = p2.y = 0.0;
	p1.y = 1.0;
	p2.x = 1.0;
	angle = vector_angle(&p1, &p2);
	CU_ASSERT_DOUBLE_EQUAL(angle, M_PI_2, 0.00001);

	p2.y = p2.x = 1.0;
	normalize(&p2);
	angle = vector_angle(&p1, &p2);
	CU_ASSERT_DOUBLE_EQUAL(angle, M_PI_4, 0.00001);

	p2.x = p2.y = p2.z = 1.0;
	normalize(&p2);
	angle = vector_angle(&p1, &p2);
	CU_ASSERT_DOUBLE_EQUAL(angle, 0.955317, 0.00001);	
	//printf ("angle = %g\n\n", angle);
}

static void test_vector_rotate(void)
{
	POINT3D p1, p2, n;
	double angle;
	
	memset(&p1, 0, sizeof(POINT3D));
	memset(&p2, 0, sizeof(POINT3D));
	memset(&n, 0, sizeof(POINT3D));
	
	p1.x = 1.0;
	p2.y = 1.0;
	angle = M_PI_4;
	vector_rotate(&p1, &p2, angle, &n);
	//printf("%g %g %g\n\n", n.x, n.y, n.z);
	CU_ASSERT_DOUBLE_EQUAL(n.x, 0.707107, 0.00001);	

	angle = 2*M_PI/400000000;
	vector_rotate(&p1, &p2, angle, &n);
	//printf("%.21g %.21g %.21g\n\n", n.x, n.y, n.z);
	CU_ASSERT_DOUBLE_EQUAL(n.x, 0.999999999999999888978, 0.0000000000000001);	
	CU_ASSERT_DOUBLE_EQUAL(n.y, 1.57079632679489654446e-08, 0.0000000000000001);	

	angle = 0;
	vector_rotate(&p1, &p2, angle, &n);
	//printf("%.16g %.16g %.16g\n\n", n.x, n.y, n.z);
	CU_ASSERT_DOUBLE_EQUAL(n.x, 1.0, 0.00000001);	
}

static void test_lwgeom_segmentize_sphere(void)
{
	LWGEOM *lwg1, *lwg2;
	LWLINE *lwl;
	double max = 100000.0 / WGS84_RADIUS;
	//char *wkt;

	/* Simple case */
	lwg1 = lwgeom_from_wkt("LINESTRING(0 20, 5 20)", LW_PARSER_CHECK_NONE);
	lwg2 = lwgeom_segmentize_sphere(lwg1, max);
	lwl = (LWLINE*)lwg2;
	//wkt = lwgeom_to_ewkt(lwg2);
	CU_ASSERT_EQUAL(lwl->points->npoints, 7);
	lwgeom_free(lwg1);
	lwgeom_free(lwg2);
	//lwfree(wkt);
	
	return;
}

static void test_lwgeom_area_sphere(void)
{
	LWGEOM *lwg;
	double area;
	SPHEROID s;

	/* Init to WGS84 */
	spheroid_init(&s, WGS84_MAJOR_AXIS, WGS84_MINOR_AXIS);

	/* Simple case */
	lwg = lwgeom_from_wkt("POLYGON((1 1, 1 2, 2 2, 2 1, 1 1))", LW_PARSER_CHECK_NONE);
	area = lwgeom_area_sphere(lwg, &s);
	
	CU_ASSERT_DOUBLE_EQUAL(area, 12360265021.3561, 1.0);
	lwgeom_free(lwg);	
	return;
}

/*
** Used by test harness to register the tests in this file.
*/
void geodetic_suite_setup(void);
void geodetic_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("geodetic", NULL, NULL);
	PG_ADD_TEST(suite, test_sphere_direction);
	PG_ADD_TEST(suite, test_sphere_project);
	PG_ADD_TEST(suite, test_lwgeom_area_sphere);
	PG_ADD_TEST(suite, test_signum);
	PG_ADD_TEST(suite, test_gbox_from_spherical_coordinates);
	PG_ADD_TEST(suite, test_gserialized_get_gbox_geocentric);
	PG_ADD_TEST(suite, test_clairaut);
	PG_ADD_TEST(suite, test_edge_intersection);
	PG_ADD_TEST(suite, test_edge_intersects);
	PG_ADD_TEST(suite, test_edge_distance_to_point);
	PG_ADD_TEST(suite, test_edge_distance_to_edge);
	PG_ADD_TEST(suite, test_lwgeom_distance_sphere);
	PG_ADD_TEST(suite, test_lwgeom_check_geodetic);
	PG_ADD_TEST(suite, test_gserialized_from_lwgeom);
	PG_ADD_TEST(suite, test_spheroid_distance);
	PG_ADD_TEST(suite, test_spheroid_area);
	PG_ADD_TEST(suite, test_lwpoly_covers_point2d);
	PG_ADD_TEST(suite, test_gbox_utils);
	PG_ADD_TEST(suite, test_vector_angle);
	PG_ADD_TEST(suite, test_vector_rotate);
	PG_ADD_TEST(suite, test_lwgeom_segmentize_sphere);
	PG_ADD_TEST(suite, test_ptarray_contains_point_sphere);
	PG_ADD_TEST(suite, test_ptarray_contains_point_sphere_iowa);
}
