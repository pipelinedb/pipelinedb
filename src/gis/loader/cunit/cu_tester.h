/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Copyright 2010 Mark Leslie
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/


#ifndef __cu_tester_h__
#define __cu_tester_h__

CU_pSuite register_list_suite(void);
CU_pSuite register_shp2pgsql_suite(void);
CU_pSuite register_pgsql2shp_suite(void);

#endif /* __cu_tester_h__ */
