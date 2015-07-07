/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Copyright 2001-2011 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include "lwgeom_rtree.h"

/*
** Public prototypes for analytic functions.
*/

int point_in_polygon_rtree(RTREE_NODE **root, int ringCount, LWPOINT *point);
int point_in_multipolygon_rtree(RTREE_NODE **root, int polyCount, int *ringCounts, LWPOINT *point);
int point_in_polygon(LWPOLY *polygon, LWPOINT *point);
int point_in_multipolygon(LWMPOLY *mpolygon, LWPOINT *pont);

