/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * Copyright 2012 Sandro Santilli <strk@keybit.net>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "CUnit/Basic.h"

#include "liblwgeom_internal.h"
#include "effectivearea.h"
#include "cu_tester.h"


static void do_test_lwgeom_effectivearea(POINTARRAY *pa,double *the_areas,int avoid_collaps)
{

	int i;
	EFFECTIVE_AREAS *ea;	
	
	ea=initiate_effectivearea(pa);		
	ptarray_calc_areas(ea,avoid_collaps,1,0);

	for (i=0;i<pa->npoints;i++)
	{
		CU_ASSERT_EQUAL(ea->res_arealist[i],the_areas[i]);
	}

	destroy_effectivearea(ea);
	
	
}

static void do_test_lwgeom_effectivearea_lines(void)
{
	LWLINE *the_geom;
	int avoid_collaps=2;
	/*Line 1*/
	the_geom = (LWLINE*)lwgeom_from_wkt("LINESTRING(1 1, 0 1, 0 2, -1 4, -1 4)", LW_PARSER_CHECK_NONE);
	double the_areas1[]={FLT_MAX,0.5,0.5,0,FLT_MAX};
	do_test_lwgeom_effectivearea(the_geom->points,the_areas1,avoid_collaps);
	lwline_free(the_geom);
	/*Line 2*/
	the_geom = (LWLINE*)lwgeom_from_wkt("LINESTRING(10 10,12 8, 15 7, 18 7, 20 20, 15 21, 18 22, 10 30, 40 100)", LW_PARSER_CHECK_NONE);
	double the_areas2[]={FLT_MAX,5,1.5,55,100,4,4,300,FLT_MAX};
	do_test_lwgeom_effectivearea(the_geom->points,the_areas2,avoid_collaps);
	lwline_free(the_geom);
}



static void do_test_lwgeom_effectivearea_polys(void)
{
	LWPOLY *the_geom;
	int avoid_collaps=4;
	
	/*POLYGON 1*/
	the_geom = (LWPOLY*)lwgeom_from_wkt("POLYGON((10 10,12 8, 15 7, 18 7, 20 20, 15 21, 18 22, 10 30,1 99, 0 100, 10 10))", LW_PARSER_CHECK_NONE);
	double the_areas1[]={FLT_MAX,5,1.5,55,100,4,4,FLT_MAX,30,FLT_MAX,FLT_MAX};
	do_test_lwgeom_effectivearea(the_geom->rings[0],the_areas1,avoid_collaps);
	lwpoly_free(the_geom);
}


void effectivearea_suite_setup(void);
void effectivearea_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("effectivearea",NULL,NULL);
	PG_ADD_TEST(suite, do_test_lwgeom_effectivearea_lines);
	PG_ADD_TEST(suite, do_test_lwgeom_effectivearea_polys);
}
