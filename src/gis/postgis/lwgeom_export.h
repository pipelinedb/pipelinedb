/**********************************************************************
 *
 * PostGIS - Export functions for PostgreSQL/PostGIS
 * Copyright 2009 Olivier Courtin <olivier.courtin@oslandia.com>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

char * getSRSbySRID(int SRID, bool short_crs);
int getSRIDbySRS(const char* SRS);
