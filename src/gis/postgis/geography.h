/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * Copyright 2009 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/


/**********************************************************************
**  Useful functions for all GSERIALIZED handlers. 
**  TODO: Move to common.h in pgcommon
*/

/* Check that the typmod matches the flags on the lwgeom */
GSERIALIZED* postgis_valid_typmod(GSERIALIZED *gser, int32_t typmod);
/* Check that the type is legal in geography (no curves please!) */
void geography_valid_type(uint8_t type);

/* Expand the embedded bounding box in a #GSERIALIZED */
GSERIALIZED* gserialized_expand(GSERIALIZED *g, double distance);

