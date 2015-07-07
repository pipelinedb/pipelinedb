/**********************************************************************
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * Copyright 2012 (C) Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/


/**********************************************************************
 THEORY OF OPERATION 

The ANALYZE command hooks to a callback (gserialized_analyze_nd) that
calculates (compute_gserialized_stats_mode) two histograms of occurances of 
features, once for the 2D domain (and the && operator) one for the 
ND domain (and the &&& operator).

Queries in PostgreSQL call into the selectivity sub-system to find out
the relative effectiveness of different clauses in sub-setting 
relations. Queries with constant arguments call gserialized_gist_sel,
queries with relations on both sides call gserialized_gist_joinsel.

gserialized_gist_sel sums up the values in the histogram that overlap
the contant search box.

gserialized_gist_joinsel sums up the product of the overlapping 
cells in each relation's histogram.

Depending on the operator and type, the mode of selectivity calculation 
will be 2D or ND.

- geometry && geometry ==> 2D
- geometry &&& geometry ==> ND
- geography && geography ==> ND

The 2D mode is put in effect by retrieving the 2D histogram from the 
statistics cache and then allowing the generic ND calculations to
go to work.

TO DO: More testing and examination of the &&& operator and mixed
dimensionality cases. (2D geometry) &&& (3D column), etc.

**********************************************************************/

#include "postgres.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "commands/vacuum.h"
#include "nodes/relation.h"
#include "parser/parsetree.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"

#include "../postgis_config.h"

#if POSTGIS_PGSQL_VERSION >= 93
	#include "access/htup_details.h"
#endif

#include "stringbuffer.h"
#include "liblwgeom.h"
#include "lwgeom_pg.h"       /* For debugging macros. */
#include "gserialized_gist.h" /* For index common functions */

#include <math.h>
#if HAVE_IEEEFP_H
#include <ieeefp.h>
#endif
#include <float.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <ctype.h>

/* Prototypes */
Datum gserialized_gist_joinsel(PG_FUNCTION_ARGS);
Datum gserialized_gist_joinsel_2d(PG_FUNCTION_ARGS);
Datum gserialized_gist_joinsel_nd(PG_FUNCTION_ARGS);
Datum gserialized_gist_sel(PG_FUNCTION_ARGS);
Datum gserialized_gist_sel_2d(PG_FUNCTION_ARGS);
Datum gserialized_gist_sel_nd(PG_FUNCTION_ARGS);
Datum gserialized_analyze_nd(PG_FUNCTION_ARGS);
Datum gserialized_estimated_extent(PG_FUNCTION_ARGS);
Datum _postgis_gserialized_sel(PG_FUNCTION_ARGS);
Datum _postgis_gserialized_joinsel(PG_FUNCTION_ARGS);
Datum _postgis_gserialized_stats(PG_FUNCTION_ARGS);

/* Old Prototype */
Datum geometry_estimated_extent(PG_FUNCTION_ARGS);

/**
* Assign a number to the n-dimensional statistics kind
*
* tgl suggested:
*
* 1-100:	reserved for assignment by the core Postgres project
* 100-199: reserved for assignment by PostGIS
* 200-9999: reserved for other globally-known stats kinds
* 10000-32767: reserved for private site-local use
*/
#define STATISTIC_KIND_ND 102
#define STATISTIC_KIND_2D 103
#define STATISTIC_SLOT_ND 0
#define STATISTIC_SLOT_2D 1

/*
* The SD factor restricts the side of the statistics histogram
* based on the standard deviation of the extent of the data.
* SDFACTOR is the number of standard deviations from the mean
* the histogram will extend.
*/
#define SDFACTOR 3.25

/**
* The maximum number of dimensions our code can handle.
* We'll use this to statically allocate a bunch of
* arrays below.
*/
#define ND_DIMS 4

/**
* Minimum width of a dimension that we'll bother trying to
* compute statistics on. Bearing in mind we have no control
* over units, but noting that for geographics, 10E-5 is in the
* range of meters, we go lower than that.
*/
#define MIN_DIMENSION_WIDTH 0.000000001

/**
* Default geometry selectivity factor
*/
#define DEFAULT_ND_SEL 0.0001
#define DEFAULT_ND_JOINSEL 0.001

/**
* More modest fallback selectivity factor
*/
#define FALLBACK_ND_SEL 0.2	
#define FALLBACK_ND_JOINSEL 0.3	

/**
* N-dimensional box type for calculations, to avoid doing
* explicit axis conversions from GBOX in all calculations
* at every step.
*/
typedef struct ND_BOX_T
{
	float4 min[ND_DIMS];
	float4 max[ND_DIMS];
} ND_BOX;

/**
* N-dimensional box index type
*/
typedef struct ND_IBOX_T
{
	int min[ND_DIMS];
	int max[ND_DIMS];
} ND_IBOX;


/**
* N-dimensional statistics structure. Well, actually 
* four-dimensional, but set up to handle arbirary dimensions
* if necessary (really, we just want to get the 2,3,4-d cases
* into one shared piece of code).
*/
typedef struct ND_STATS_T
{
	/* Dimensionality of the histogram. */
	float4 ndims;
	
	/* Size of n-d histogram in each dimension. */
	float4 size[ND_DIMS];

	/* Lower-left (min) and upper-right (max) spatial bounds of histogram. */
	ND_BOX extent;

	/* How many rows in the table itself? */
	float4 table_features;
	
	/* How many rows were in the sample that built this histogram? */
	float4 sample_features;
	
	/* How many not-Null/Empty features were in the sample? */
	float4 not_null_features;

	/* How many features actually got sampled in the histogram? */
	float4 histogram_features;

	/* How many cells in histogram? (sizex*sizey*sizez*sizem) */
	float4 histogram_cells;
	
	/* How many cells did those histogram features cover? */
	/* Since we are pro-rating coverage, this number should */
	/* now always equal histogram_features */
	float4 cells_covered;
	
	/* Variable length # of floats for histogram */
	float4 value[1];
} ND_STATS;




/**
* Given that geodetic boxes are X/Y/Z regardless of the 
* underlying geometry dimensionality and other boxes
* are guided by HAS_Z/HAS_M in their dimesionality,
* we have a little utility function to make it easy.
*/
static int
gbox_ndims(const GBOX* gbox)
{
	int dims = 2;
	if ( FLAGS_GET_GEODETIC(gbox->flags) )
		return 3;
	if ( FLAGS_GET_Z(gbox->flags) )
		dims++;
	if ( FLAGS_GET_M(gbox->flags) )
		dims++;
	return dims;
}

/**
* Utility function to see if the first
* letter of the mode argument is 'N'. Used
* by the _postgis_* functions.
*/
static int
text_p_get_mode(const text *txt)
{
	int mode = 2;
	char *modestr = text2cstring(txt);
	if ( modestr[0] == 'N' )
		mode = 0;
	pfree(modestr);
	return mode;
}


/**
* Integer comparison function for qsort 
*/
static int 
cmp_int (const void *a, const void *b)
{
	int ia = *((const int*)a);
	int ib = *((const int*)b);

	if ( ia == ib )
		return 0;
	else if ( ia > ib )
		return 1;
	else 
		return -1;
}

/**
* The difference between the fourth and first quintile values,
* the "inter-quintile range"
*/
static int
range_quintile(int *vals, int nvals)
{
	qsort(vals, nvals, sizeof(int), cmp_int);
	return vals[4*nvals/5] - vals[nvals/5];
}

/**
* Given double array, return sum of values.
*/
static double 
total_double(const double *vals, int nvals)
{
	int i;
	float total = 0;
	/* Calculate total */
	for ( i = 0; i < nvals; i++ )
		total += vals[i];

	return total;
}

#if POSTGIS_DEBUG_LEVEL >= 3

/**
* Given int array, return sum of values.
*/
static int 
total_int(const int *vals, int nvals)
{
	int i;
	int total = 0;
	/* Calculate total */
	for ( i = 0; i < nvals; i++ )
		total += vals[i];

	return total;
}

/**
* The average of an array of integers.
*/
static double
avg(const int *vals, int nvals)
{
	int t = total_int(vals, nvals);
	return (double)t / (double)nvals;
}

/**
* The standard deviation of an array of integers.
*/
static double
stddev(const int *vals, int nvals)
{
	int i;
	double sigma2 = 0;
	double mean = avg(vals, nvals);
	
	/* Calculate sigma2 */
	for ( i = 0; i < nvals; i++ )
	{
		double v = (double)(vals[i]);
		sigma2 += (mean - v) * (mean - v);
	}
	return sqrt(sigma2 / nvals);
}
#endif /* POSTGIS_DEBUG_LEVEL >= 3 */

/**
* Given a position in the n-d histogram (i,j,k) return the
* position in the 1-d values array.
*/
static int 
nd_stats_value_index(const ND_STATS *stats, int *indexes)
{
	int d;
	int accum = 1, vdx = 0;
	
	/* Calculate the index into the 1-d values array that the (i,j,k,l) */
	/* n-d histogram coordinate implies. */
	/* index = x + y * sizex + z * sizex * sizey + m * sizex * sizey * sizez */
	for ( d = 0; d < (int)(stats->ndims); d++ )
	{
		int size = (int)(stats->size[d]);
		if ( indexes[d] < 0 || indexes[d] >= size )
		{
			POSTGIS_DEBUGF(3, " bad index at (%d, %d)", indexes[0], indexes[1]);
			return -1;
		}
		vdx += indexes[d] * accum;
		accum *= size;
	}
	return vdx;
}

/** 
* Convert an #ND_BOX to a JSON string for printing 
*/
static char*
nd_box_to_json(const ND_BOX *nd_box, int ndims)
{
	char *rv;
	int i;
	stringbuffer_t *sb = stringbuffer_create();
	
	stringbuffer_append(sb, "{\"min\":[");
	for ( i = 0; i < ndims; i++ )
	{
		if ( i ) stringbuffer_append(sb, ",");
		stringbuffer_aprintf(sb, "%.6g", nd_box->min[i]);
	}
	stringbuffer_append(sb,  "],\"max\":[");
	for ( i = 0; i < ndims; i++ )
	{
		if ( i ) stringbuffer_append(sb, ",");
		stringbuffer_aprintf(sb, "%.6g", nd_box->max[i]);
	}
	stringbuffer_append(sb,  "]}");

	rv = stringbuffer_getstringcopy(sb);
	stringbuffer_destroy(sb);
	return rv;	
}


/**
* Convert an #ND_STATS to a JSON representation for 
* external use.
*/
static char*
nd_stats_to_json(const ND_STATS *nd_stats)
{
	char *json_extent, *str;
	int d;
	stringbuffer_t *sb = stringbuffer_create();
	int ndims = (int)roundf(nd_stats->ndims);
	
	stringbuffer_append(sb, "{");
	stringbuffer_aprintf(sb, "\"ndims\":%d,", ndims);
	
	/* Size */
	stringbuffer_append(sb, "\"size\":[");
	for ( d = 0; d < ndims; d++ )
	{
		if ( d ) stringbuffer_append(sb, ",");
		stringbuffer_aprintf(sb, "%d", (int)roundf(nd_stats->size[d]));
	}
	stringbuffer_append(sb, "],");

	/* Extent */
	json_extent = nd_box_to_json(&(nd_stats->extent), ndims);
	stringbuffer_aprintf(sb, "\"extent\":%s,", json_extent);
	pfree(json_extent);
	
	stringbuffer_aprintf(sb, "\"table_features\":%d,", (int)roundf(nd_stats->table_features));
	stringbuffer_aprintf(sb, "\"sample_features\":%d,", (int)roundf(nd_stats->sample_features));
	stringbuffer_aprintf(sb, "\"not_null_features\":%d,", (int)roundf(nd_stats->not_null_features));
	stringbuffer_aprintf(sb, "\"histogram_features\":%d,", (int)roundf(nd_stats->histogram_features));
	stringbuffer_aprintf(sb, "\"histogram_cells\":%d,", (int)roundf(nd_stats->histogram_cells));
	stringbuffer_aprintf(sb, "\"cells_covered\":%d", (int)roundf(nd_stats->cells_covered));
	stringbuffer_append(sb, "}");

	str = stringbuffer_getstringcopy(sb);
	stringbuffer_destroy(sb);
	return str;
}	


/**
* Create a printable view of the #ND_STATS histogram.
* Caller is responsible for freeing.
* Currently only prints first two dimensions.
*/
// static char* 
// nd_stats_to_grid(const ND_STATS *stats)
// {
//  char *rv;
//  int j, k;
//  int sizex = (int)roundf(stats->size[0]);
//  int sizey = (int)roundf(stats->size[1]);
//  stringbuffer_t *sb = stringbuffer_create();
// 
//  for ( k = 0; k < sizey; k++ )
//  {
//      for ( j = 0; j < sizex; j++ )
//      {
//          stringbuffer_aprintf(sb, "%3d ", (int)roundf(stats->value[j + k*sizex]));
//      }
//      stringbuffer_append(sb,  "\n");
//  }
//      
//  rv = stringbuffer_getstringcopy(sb);
//  stringbuffer_destroy(sb);
//  return rv;
// }


/** Expand the bounds of target to include source */
static int 
nd_box_merge(const ND_BOX *source, ND_BOX *target)
{
	int d;
	for ( d = 0; d < ND_DIMS; d++ )
	{
		target->min[d] = Min(target->min[d], source->min[d]);
		target->max[d] = Max(target->max[d], source->max[d]);
	}
	return TRUE;
}

/** Zero out an ND_BOX */
static int 
nd_box_init(ND_BOX *a)
{
	memset(a, 0, sizeof(ND_BOX));
	return TRUE;
}

/** 
* Prepare an ND_BOX for bounds calculation: 
* set the maxes to the smallest thing possible and
* the mins to the largest. 
*/
static int 
nd_box_init_bounds(ND_BOX *a)
{
	int d;
	for ( d = 0; d < ND_DIMS; d++ )
	{
		a->min[d] = FLT_MAX;
		a->max[d] = -1 * FLT_MAX;
	}
	return TRUE;
}

/** Set the values of an #ND_BOX from a #GBOX */
static void 
nd_box_from_gbox(const GBOX *gbox, ND_BOX *nd_box)
{
	int d = 0;
	POSTGIS_DEBUGF(3, " %s", gbox_to_string(gbox));
	
	nd_box_init(nd_box);
	nd_box->min[d] = gbox->xmin;
	nd_box->max[d] = gbox->xmax;
	d++;
	nd_box->min[d] = gbox->ymin;
	nd_box->max[d] = gbox->ymax;
	d++;
	if ( FLAGS_GET_GEODETIC(gbox->flags) )
	{
		nd_box->min[d] = gbox->zmin;
		nd_box->max[d] = gbox->zmax;
		return;
	}
	if ( FLAGS_GET_Z(gbox->flags) )
	{
		nd_box->min[d] = gbox->zmin;
		nd_box->max[d] = gbox->zmax;
		d++;
	}
	if ( FLAGS_GET_M(gbox->flags) )
	{
		nd_box->min[d] = gbox->mmin;
		nd_box->max[d] = gbox->mmax;
		d++;
	}
	return;
}

/** 
* Return TRUE if #ND_BOX a overlaps b, false otherwise.
*/
static int
nd_box_intersects(const ND_BOX *a, const ND_BOX *b, int ndims)
{
	int d;
	for ( d = 0; d < ndims; d++ )
	{
		if ( (a->min[d] > b->max[d]) || (a->max[d] < b->min[d]) )
			return FALSE;
	}
	return TRUE;
}

/** 
* Return TRUE if #ND_BOX a contains b, false otherwise.
*/
static int
nd_box_contains(const ND_BOX *a, const ND_BOX *b, int ndims)
{
	int d;
	for ( d = 0; d < ndims; d++ )
	{
		if ( ! ((a->min[d] < b->min[d]) && (a->max[d] > b->max[d])) )
			return FALSE;
	}
	return TRUE;
}

/**
* Expand an #ND_BOX ever so slightly. Expand parameter is the proportion
* of total width to add.
*/
static int 
nd_box_expand(ND_BOX *nd_box, double expansion_factor)
{
	int d;
	double size;
	for ( d = 0; d < ND_DIMS; d++ )
	{
		size = nd_box->max[d] - nd_box->min[d];
		if ( size <= 0 ) continue;
		nd_box->min[d] -= size * expansion_factor / 2;
		nd_box->max[d] += size * expansion_factor / 2;
	}
	return TRUE;
}

/** 
* What stats cells overlap with this ND_BOX? Put the lowest cell
* addresses in ND_IBOX->min and the highest in ND_IBOX->max
*/
static inline int
nd_box_overlap(const ND_STATS *nd_stats, const ND_BOX *nd_box, ND_IBOX *nd_ibox)
{
	int d;
	
	POSTGIS_DEBUGF(4, " nd_box: %s", nd_box_to_json(nd_box, nd_stats->ndims));
	
	/* Initialize ibox */
	memset(nd_ibox, 0, sizeof(ND_IBOX));
	
	/* In each dimension... */
	for ( d = 0; d < nd_stats->ndims; d++ )
	{
		double smin = nd_stats->extent.min[d];
		double smax = nd_stats->extent.max[d];
		double width = smax - smin;
		int size = roundf(nd_stats->size[d]);
		
		/* ... find cells the box overlaps with in this dimension */
		nd_ibox->min[d] = floor(size * (nd_box->min[d] - smin) / width);
		nd_ibox->max[d] = floor(size * (nd_box->max[d] - smin) / width);

		POSTGIS_DEBUGF(5, " stats: dim %d: min %g: max %g: width %g", d, smin, smax, width);
		POSTGIS_DEBUGF(5, " overlap: dim %d: (%d, %d)", d, nd_ibox->min[d], nd_ibox->max[d]);
		
		/* Push any out-of range values into range */
		nd_ibox->min[d] = Max(nd_ibox->min[d], 0);
		nd_ibox->max[d] = Min(nd_ibox->max[d], size-1);
	}
	return TRUE;
}

/**
* Returns the proportion of b2 that is covered by b1.
*/
static inline double
nd_box_ratio(const ND_BOX *b1, const ND_BOX *b2, int ndims)
{
	int d;
	bool covered = TRUE;
	double ivol = 1.0;
	double vol2 = 1.0;
	double vol1 = 1.0;
	
	for ( d = 0 ; d < ndims; d++ )
	{
		if ( b1->max[d] <= b2->min[d] || b1->min[d] >= b2->max[d] ) 
			return 0.0; /* Disjoint */
		
		if ( b1->min[d] > b2->min[d] || b1->max[d] < b2->max[d] )
			covered = FALSE;
	}
	
	if ( covered ) 
		return 1.0;

	for ( d = 0; d < ndims; d++ )
	{
		double width1 = b1->max[d] - b1->min[d];
		double width2 = b2->max[d] - b2->min[d];
		double imin, imax, iwidth;
		
		vol1 *= width1;
		vol2 *= width2;
		
		imin = Max(b1->min[d], b2->min[d]);
		imax = Min(b1->max[d], b2->max[d]);
		iwidth = imax - imin;
		iwidth = Max(0.0, iwidth);
		
		ivol *= iwidth;
	}
	
	if ( vol2 == 0.0 ) 
		return vol2;
	
	return ivol / vol2;
}


/**
* Calculate how much a set of boxes is homogenously distributed
* or contentrated within one dimension, returning the range_quintile of
* of the overlap counts per cell in a uniform
* partition of the extent of the dimension. 
* A uniform distribution of counts will have a small range
* and will require few cells in a selectivity histogram.
* A diverse distribution of counts will have a larger range
* and require more cells in a selectivity histogram (to
* distinguish between areas of feature density and areas
* of feature sparseness. This measurement should help us
* identify cases like X/Y/Z data where there is lots of variability
* in density in X/Y (diversely in a multi-kilometer range) and far 
* less in Z (in a few-hundred meter range).
*/
static int
nd_box_array_distribution(const ND_BOX **nd_boxes, int num_boxes, const ND_BOX *extent, int ndims, double *distribution)
{
	/* How many bins shall we use in figuring out the distribution? */
	static int num_bins = 50; 
	int d, i, k, range;
	int counts[num_bins];
	double smin, smax;   /* Spatial min, spatial max */
	double swidth;       /* Spatial width of dimension */
#if POSTGIS_DEBUG_LEVEL >= 3
	double average, sdev, sdev_ratio;
#endif
	int   bmin, bmax;   /* Bin min, bin max */
	const ND_BOX *ndb; 
	
	/* For each dimension... */	
	for ( d = 0; d < ndims; d++ )
	{
		/* Initialize counts for this dimension */
		memset(counts, 0, sizeof(int)*num_bins);
		
		smin = extent->min[d];
		smax = extent->max[d];
		swidth = smax - smin;
		
		/* Don't try and calculate distribution of overly narrow dimensions */
		if ( swidth < MIN_DIMENSION_WIDTH )
		{
			distribution[d] = 0;
			continue;
		}

		/* Sum up the overlaps of each feature with the dimensional bins */
		for ( i = 0; i < num_boxes; i++ )
		{
			double minoffset, maxoffset;
			
			/* Skip null entries */
			ndb = nd_boxes[i];	
			if ( ! ndb ) continue;
			
			/* Where does box fall relative to the working range */
			minoffset = ndb->min[d] - smin;
			maxoffset = ndb->max[d] - smin;				

			/* Skip boxes that our outside our working range */
			if ( minoffset < 0 || minoffset > swidth ||
			     maxoffset < 0 || maxoffset > swidth )
			{
				continue;
			}
				
			/* What bins does this range correspond to? */
			bmin = num_bins * (minoffset) / swidth;
			bmax = num_bins * (maxoffset) / swidth;
			
			POSTGIS_DEBUGF(4, " dimension %d, feature %d: bin %d to bin %d", d, i, bmin, bmax);
		
			/* Increment the counts in all the bins this feature overlaps */
			for ( k = bmin; k <= bmax; k++ )
			{
				counts[k] += 1;
			}
			
		}

		/* How dispersed is the distribution of features across bins? */
		range = range_quintile(counts, num_bins);

#if POSTGIS_DEBUG_LEVEL >= 3
		average = avg(counts, num_bins);
		sdev = stddev(counts, num_bins);
		sdev_ratio = sdev/average;

		POSTGIS_DEBUGF(3, " dimension %d: range = %d", d, range);
		POSTGIS_DEBUGF(3, " dimension %d: average = %.6g", d, average);
		POSTGIS_DEBUGF(3, " dimension %d: stddev = %.6g", d, sdev);
		POSTGIS_DEBUGF(3, " dimension %d: stddev_ratio = %.6g", d, sdev_ratio);
#endif
		
		distribution[d] = range;
	}
	
	return TRUE;
}

/**
* Given an n-d index array (counter), and a domain to increment it
* in (ibox) increment it by one, unless it's already at the max of 
* the domain, in which case return false.
*/
static inline int
nd_increment(ND_IBOX *ibox, int ndims, int *counter)
{
	int d = 0;

	while ( d < ndims )
	{
		if ( counter[d] < ibox->max[d] )
		{
			counter[d] += 1;
			break;
		}
		counter[d] = ibox->min[d];
		d++;
	}
	/* That's it, cannot increment any more! */
	if ( d == ndims )
		return FALSE;

	/* Increment complete! */
	return TRUE;
}

static ND_STATS*
pg_nd_stats_from_tuple(HeapTuple stats_tuple, int mode)
{
  int stats_kind = STATISTIC_KIND_ND;
  int rv, nvalues;
	float4 *floatptr;
	ND_STATS *nd_stats;

  /* If we're in 2D mode, set the kind appropriately */
  if ( mode == 2 ) stats_kind = STATISTIC_KIND_2D;

  /* Then read the geom status histogram from that */
  rv = get_attstatsslot(stats_tuple, 0, 0, stats_kind, InvalidOid,
                        NULL, NULL, NULL, &floatptr, &nvalues);
  if ( ! rv ) {
    POSTGIS_DEBUGF(2,
            "no slot of kind %d in stats tuple", stats_kind);
    return NULL;
  }
	
  /* Clone the stats here so we can release the attstatsslot immediately */
  nd_stats = palloc(sizeof(float) * nvalues);
  memcpy(nd_stats, floatptr, sizeof(float) * nvalues);

  /* Clean up */
  free_attstatsslot(0, NULL, 0, floatptr, nvalues);

  return nd_stats;
}

/**
* Pull the stats object from the PgSQL system catalogs. Used
* by the selectivity functions and the debugging functions.
*/
static ND_STATS*
pg_get_nd_stats(const Oid table_oid, AttrNumber att_num, int mode)
{
	HeapTuple stats_tuple;
	ND_STATS *nd_stats;

	/* First pull the stats tuple */
	stats_tuple = SearchSysCache2(STATRELATT, table_oid, att_num);
	if ( ! stats_tuple )
	{
		POSTGIS_DEBUGF(2, "stats for \"%s\" do not exist", get_rel_name(table_oid)? get_rel_name(table_oid) : "NULL");
		return NULL;
	}

	nd_stats = pg_nd_stats_from_tuple(stats_tuple, mode);
	ReleaseSysCache(stats_tuple);
	if ( ! nd_stats )
	{
		POSTGIS_DEBUGF(2,
			"histogram for attribute %d of table \"%s\" does not exist?",
			att_num, get_rel_name(table_oid));
	}
	
	return nd_stats;
}

/**
* Pull the stats object from the PgSQL system catalogs. The
* debugging functions are taking human input (table names)
* and columns, so we have to look those up first.
*/
static ND_STATS*
pg_get_nd_stats_by_name(const Oid table_oid, const text *att_text, int mode)
{
	const char *att_name = text2cstring(att_text);
	AttrNumber att_num;

	/* We know the name? Look up the num */
	if ( att_text )
	{
		/* Get the attribute number */
		att_num = get_attnum(table_oid, att_name);
		if  ( ! att_num ) {
			elog(ERROR, "attribute \"%s\" does not exist", att_name);
			return NULL;
		}
	}
	else 
	{
		elog(ERROR, "attribute name is null");
		return NULL;
	}
	
	return pg_get_nd_stats(table_oid, att_num, mode);
}

/**
* Given two statistics histograms, what is the selectivity
* of a join driven by the && or &&& operator?
* 
* Join selectivity is defined as the number of rows returned by the
* join operator divided by the number of rows that an 
* unconstrained join would return (nrows1*nrows2).
* 
* To get the estimate of join rows, we walk through the cells
* of one histogram, and multiply the cell value by the
* proportion of the cells in the other histogram the cell
* overlaps: val += val1 * ( val2 * overlap_ratio )
*/
static float8
estimate_join_selectivity(const ND_STATS *s1, const ND_STATS *s2)
{
	int ncells1, ncells2;
	int ndims1, ndims2, ndims;
	double ntuples_max;
	double ntuples_not_null1, ntuples_not_null2;
	
	ND_BOX extent1, extent2;
	ND_IBOX ibox1, ibox2;
	int at1[ND_DIMS];
	int at2[ND_DIMS];
	double min1[ND_DIMS];
	double width1[ND_DIMS];
	double cellsize1[ND_DIMS];
	int size2[ND_DIMS];
	double min2[ND_DIMS];
	double width2[ND_DIMS];
	double cellsize2[ND_DIMS];
	int size1[ND_DIMS];
	int d;
	double val = 0;
	float8 selectivity;

	/* Drop out on null inputs */
	if ( ! ( s1 && s2 ) )
	{
		elog(NOTICE, " estimate_join_selectivity called with null inputs");
		return FALLBACK_ND_SEL;
	}

	/* We need to know how many cells each side has... */
	ncells1 = (int)roundf(s1->histogram_cells);
	ncells2 = (int)roundf(s2->histogram_cells);

	/* ...so that we can drive the summation loop with the smaller histogram. */
	if ( ncells1 > ncells2 )
	{
		const ND_STATS *stats_tmp = s1;
		s1 = s2;
		s2 = stats_tmp;
	}
	
	POSTGIS_DEBUGF(3, "s1: %s", nd_stats_to_json(s1));
	POSTGIS_DEBUGF(3, "s2: %s", nd_stats_to_json(s2));
	
	/* Re-read that info after the swap */
	ncells1 = (int)roundf(s1->histogram_cells);
	ncells2 = (int)roundf(s2->histogram_cells);

	/* Q: What's the largest possible join size these relations can create? */
	/* A: The product of the # of non-null rows in each relation. */
	ntuples_not_null1 = s1->table_features * (s1->not_null_features / s1->sample_features);
	ntuples_not_null2 = s2->table_features * (s2->not_null_features / s2->sample_features);
	ntuples_max = ntuples_not_null1 * ntuples_not_null2;

	/* Get the ndims as ints */
	ndims1 = (int)roundf(s1->ndims);
	ndims2 = (int)roundf(s2->ndims);
	ndims = Max(ndims1, ndims2);
	
	/* Get the extents */
	extent1 = s1->extent;
	extent2 = s2->extent;

	/* If relation stats do not intersect, join is very very selective. */
	if ( ! nd_box_intersects(&extent1, &extent2, ndims) )
	{
		POSTGIS_DEBUG(3, "relation stats do not intersect, returning 0");
		PG_RETURN_FLOAT8(0.0);
	}
	
	/* 
	 * First find the index range of the part of the smaller 
	 * histogram that overlaps the larger one.
	 */
	if ( ! nd_box_overlap(s1, &extent2, &ibox1) )
	{
		POSTGIS_DEBUG(3, "could not calculate overlap of relations");
		PG_RETURN_FLOAT8(FALLBACK_ND_JOINSEL);		
	}
	
	/* Initialize counters / constants on s1 */
	for ( d = 0; d < ndims1; d++ )
	{
		at1[d] = ibox1.min[d];
		min1[d] = s1->extent.min[d];
		width1[d] = s1->extent.max[d] - s1->extent.min[d];
		size1[d] = (int)roundf(s1->size[d]);
		cellsize1[d] = width1[d] / size1[d];
	}

	/* Initialize counters / constants on s2 */
	for ( d = 0; d < ndims2; d++ )
	{
		min2[d] = s2->extent.min[d];
		width2[d] = s2->extent.max[d] - s2->extent.min[d];
		size2[d] = (int)roundf(s2->size[d]);
		cellsize2[d] = width2[d] / size2[d];
	}

	/* For each affected cell of s1... */
	do
	{
		double val1;
		/* Construct the bounds of this cell */
		ND_BOX nd_cell1;
		nd_box_init(&nd_cell1);
		for ( d = 0; d < ndims1; d++ )
		{
			nd_cell1.min[d] = min1[d] + (at1[d]+0) * cellsize1[d];
			nd_cell1.max[d] = min1[d] + (at1[d]+1) * cellsize1[d];
		}
		
		/* Find the cells of s2 that cell1 overlaps.. */
		nd_box_overlap(s2, &nd_cell1, &ibox2);
		
		/* Initialize counter */
		for ( d = 0; d < ndims2; d++ )
		{
			at2[d] = ibox2.min[d];
		}
		
		POSTGIS_DEBUGF(3, "at1 %d,%d  %s", at1[0], at1[1], nd_box_to_json(&nd_cell1, ndims1));
		
		/* Get the value at this cell */
		val1 = s1->value[nd_stats_value_index(s1, at1)];
		
		/* For each overlapped cell of s2... */
		do
		{
			double ratio2;
			double val2;
			
			/* Construct the bounds of this cell */
			ND_BOX nd_cell2;
			nd_box_init(&nd_cell2);
			for ( d = 0; d < ndims2; d++ )
			{
				nd_cell2.min[d] = min2[d] + (at2[d]+0) * cellsize2[d];
				nd_cell2.max[d] = min2[d] + (at2[d]+1) * cellsize2[d];
			}

			POSTGIS_DEBUGF(3, "  at2 %d,%d  %s", at2[0], at2[1], nd_box_to_json(&nd_cell2, ndims2));
			
			/* Calculate overlap ratio of the cells */
			ratio2 = nd_box_ratio(&nd_cell1, &nd_cell2, Max(ndims1, ndims2));
			
			/* Multiply the cell counts, scaled by overlap ratio */
			val2 = s2->value[nd_stats_value_index(s2, at2)];
			POSTGIS_DEBUGF(3, "  val1 %.6g  val2 %.6g  ratio %.6g", val1, val2, ratio2);
			val += val1 * (val2 * ratio2);
		}
		while ( nd_increment(&ibox2, ndims2, at2) );
		
	}
	while( nd_increment(&ibox1, ndims1, at1) );
	
	POSTGIS_DEBUGF(3, "val of histogram = %g", val);
	
	/*
	 * In order to compare our total cell count "val" to the 
	 * ntuples_max, we need to scale val up to reflect a full
	 * table estimate. So, multiply by ratio of table size to
	 * sample size.
	 */
	val *= (s1->table_features / s1->sample_features);
	val *= (s2->table_features / s2->sample_features);

	POSTGIS_DEBUGF(3, "val scaled to full table size = %g", val);
	
	/*
	 * Because the cell counts are over-determined due to 
	 * double counting of features that overlap multiple cells
	 * (see the compute_gserialized_stats routine)
	 * we also have to scale our cell count "val" *down*
	 * to adjust for the double counting.
	 */
//	val /= (s1->cells_covered / s1->histogram_features);
//	val /= (s2->cells_covered / s2->histogram_features);

	/*
	 * Finally, the selectivity is the estimated number of 
	 * rows to be returned divided by the maximum possible
	 * number of rows that can be returned.
	 */
	selectivity = val / ntuples_max;

    /* Guard against over-estimates :) */
    if ( selectivity > 1.0 ) 
        selectivity = 1.0;
	
	return selectivity;
}

/**
* For (geometry &&& geometry) and (geography && geography)
* we call into the N-D mode.
*/
PG_FUNCTION_INFO_V1(gserialized_gist_joinsel_nd);
Datum gserialized_gist_joinsel_nd(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(DirectFunctionCall5(
	   gserialized_gist_joinsel,
	   PG_GETARG_DATUM(0), PG_GETARG_DATUM(1),
	   PG_GETARG_DATUM(2), PG_GETARG_DATUM(3),
	   Int32GetDatum(0) /* ND mode */
	));
}

/**
* For (geometry && geometry) 
* we call into the 2-D mode.
*/
PG_FUNCTION_INFO_V1(gserialized_gist_joinsel_2d);
Datum gserialized_gist_joinsel_2d(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(DirectFunctionCall5(
	   gserialized_gist_joinsel,
	   PG_GETARG_DATUM(0), PG_GETARG_DATUM(1),
	   PG_GETARG_DATUM(2), PG_GETARG_DATUM(3),
	   Int32GetDatum(2) /* 2D mode */
	));
}

/**
* Join selectivity of the && operator. The selectivity
* is the ratio of the number of rows we think will be 
* returned divided the maximum number of rows the join
* could possibly return (the full combinatoric join).
*
* joinsel = estimated_nrows / (totalrows1 * totalrows2)
*/
PG_FUNCTION_INFO_V1(gserialized_gist_joinsel);
Datum gserialized_gist_joinsel(PG_FUNCTION_ARGS)
{
	PlannerInfo *root = (PlannerInfo *) PG_GETARG_POINTER(0);
	/* Oid operator = PG_GETARG_OID(1); */
	List *args = (List *) PG_GETARG_POINTER(2);
	JoinType jointype = (JoinType) PG_GETARG_INT16(3);
	int mode = PG_GETARG_INT32(4);

	Node *arg1, *arg2;
	Var *var1, *var2;
	Oid relid1, relid2;
	
	ND_STATS *stats1, *stats2;
	float8 selectivity;

	/* Only respond to an inner join/unknown context join */
	if (jointype != JOIN_INNER)
	{
		elog(NOTICE, "gserialized_gist_joinsel: jointype %d not supported", jointype);
		PG_RETURN_FLOAT8(DEFAULT_ND_JOINSEL);
	}

	/* Find Oids of the geometry columns we are working with */
	arg1 = (Node*) linitial(args);
	arg2 = (Node*) lsecond(args);
	var1 = (Var*) arg1;
	var2 = (Var*) arg2;

	/* We only do column joins right now, no functional joins */
	/* TODO: handle g1 && ST_Expand(g2) */
	if (!IsA(arg1, Var) || !IsA(arg2, Var))
	{
		elog(DEBUG1, "gserialized_gist_joinsel called with arguments that are not column references");
		PG_RETURN_FLOAT8(DEFAULT_ND_JOINSEL);
	}

	/* What are the Oids of our tables/relations? */
	relid1 = getrelid(var1->varno, root->parse->rtable);
	relid2 = getrelid(var2->varno, root->parse->rtable);

	POSTGIS_DEBUGF(3, "using relations \"%s\" Oid(%d), \"%s\" Oid(%d)", 
	                 get_rel_name(relid1) ? get_rel_name(relid1) : "NULL", relid1, get_rel_name(relid2) ? get_rel_name(relid2) : "NULL", relid2); 

	/* Pull the stats from the stats system. */
	stats1 = pg_get_nd_stats(relid1, var1->varattno, mode);
	stats2 = pg_get_nd_stats(relid2, var2->varattno, mode);

	/* If we can't get stats, we have to stop here! */
	if ( ! stats1 )
	{
		POSTGIS_DEBUGF(3, "unable to retrieve stats for \"%s\" Oid(%d)", get_rel_name(relid1) ? get_rel_name(relid1) : "NULL" , relid1);
		PG_RETURN_FLOAT8(DEFAULT_ND_JOINSEL);
	}
	else if ( ! stats2 )
	{
		POSTGIS_DEBUGF(3, "unable to retrieve stats for \"%s\" Oid(%d)", get_rel_name(relid2) ? get_rel_name(relid2) : "NULL", relid2);
		PG_RETURN_FLOAT8(DEFAULT_ND_JOINSEL);
	}

	selectivity = estimate_join_selectivity(stats1, stats2);
	POSTGIS_DEBUGF(2, "got selectivity %g", selectivity);
	
	pfree(stats1);
	pfree(stats2);
	PG_RETURN_FLOAT8(selectivity);
}




/**
 * The gserialized_analyze_nd sets this function as a 
 * callback on the stats object when called by the ANALYZE
 * command. ANALYZE then gathers the requisite number of 
 * sample rows and then calls this function.
 * 
 * We could also pass stats->extra_data in from 
 * gserialized_analyze_nd (things like the column type or
 * other stuff from the system catalogs) but so far we 
 * don't use that capability.
 *
 * Our job is to build some statistics on the sample data
 * for use by operator estimators.
 * 
 * We will populate an n-d histogram using the provided 
 * sample rows. The selectivity estimators (sel and j_oinsel)
 * can then use the histogram 
 */
static void
compute_gserialized_stats_mode(VacAttrStats *stats, AnalyzeAttrFetchFunc fetchfunc,
                          int sample_rows, double total_rows, int mode)
{
	MemoryContext old_context;
	int d, i;                          /* Counters */
	int notnull_cnt = 0;               /* # not null rows in the sample */
	int null_cnt = 0;                  /* # null rows in the sample */
	int histogram_features = 0;        /* # rows that actually got counted in the histogram */

	ND_STATS *nd_stats;                /* Our histogram */
	size_t    nd_stats_size;           /* Size to allocate */
	
	double total_width = 0;            /* # of bytes used by sample */
	double total_sample_volume = 0;    /* Area/volume coverage of the sample */
	double total_cell_count = 0;       /* # of cells in histogram affected by sample */

	ND_BOX sum;                        /* Sum of extents of sample boxes */
	ND_BOX avg;                        /* Avg of extents of sample boxes */
	ND_BOX stddev;                     /* StdDev of extents of sample boxes */
	
	const ND_BOX **sample_boxes;       /* ND_BOXes for each of the sample features */
	ND_BOX sample_extent;              /* Extent of the raw sample */
	int    histo_size[ND_DIMS];        /* histogram nrows, ncols, etc */
	ND_BOX histo_extent;               /* Spatial extent of the histogram */
	ND_BOX histo_extent_new;           /* Temporary variable */
	int    histo_cells_target;         /* Number of cells we will shoot for, given the stats target */
	int    histo_cells;                /* Number of cells in the histogram */
	int    histo_cells_new = 1;        /* Temporary variable */
	
	int   ndims = 2;                    /* Dimensionality of the sample */
	int   histo_ndims = 0;              /* Dimensionality of the histogram */
	double sample_distribution[ND_DIMS]; /* How homogeneous is distribution of sample in each axis? */
	double total_distribution;           /* Total of sample_distribution */

	int stats_slot;                     /* What slot is this data going into? (2D vs ND) */
	int stats_kind;                     /* And this is what? (2D vs ND) */

	/* Initialize sums */
	memset(&sum, 0, sizeof(ND_BOX));

	/*
	 * This is where gserialized_analyze_nd
	 * should put its' custom parameters.
	 */
	/* void *mystats = stats->extra_data; */

	POSTGIS_DEBUG(2, "compute_gserialized_stats called");
	POSTGIS_DEBUGF(3, " # sample_rows: %d", sample_rows);
	POSTGIS_DEBUGF(3, " estimate of total_rows: %.6g", total_rows);

	/*
	 * We might need less space, but don't think
	 * its worth saving...
	 */
	sample_boxes = palloc(sizeof(ND_BOX*) * sample_rows);

	/*
	 * First scan:
	 *  o read boxes
	 *  o find dimensionality of the sample
	 *  o find extent of the sample
	 *  o count null-infinite/not-null values
	 *  o compute total_width
	 *  o compute total features's box area (for avgFeatureArea)
	 *  o sum features box coordinates (for standard deviation)
	 */
	for ( i = 0; i < sample_rows; i++ )
	{
		Datum datum;
		GSERIALIZED *geom;
		GBOX gbox;
		ND_BOX *nd_box;
		bool is_null;
		bool is_copy;

		datum = fetchfunc(stats, i, &is_null);

		/* Skip all NULLs. */
		if ( is_null )
		{
			POSTGIS_DEBUGF(4, " skipped null geometry %d", i);
			null_cnt++;
			continue;
		}
			
		/* Read the bounds from the gserialized. */
		geom = (GSERIALIZED *)PG_DETOAST_DATUM(datum);
		is_copy = VARATT_IS_EXTENDED(datum);
		if ( LW_FAILURE == gserialized_get_gbox_p(geom, &gbox) )
		{
			/* Skip empties too. */
			POSTGIS_DEBUGF(3, " skipped empty geometry %d", i);
			continue;
		}
		
		/* If we're in 2D mode, zero out the higher dimensions for "safety" */
		if ( mode == 2 )
			gbox.zmin = gbox.zmax = gbox.mmin = gbox.mmax = 0.0;
		
		/* Check bounds for validity (finite and not NaN) */
		if ( ! gbox_is_valid(&gbox) )
		{
			POSTGIS_DEBUGF(3, " skipped infinite/nan geometry %d", i);
			continue;
		}

		/* 
		 * In N-D mode, set the ndims to the maximum dimensionality found
		 * in the sample. Otherwise, leave at ndims == 2.
		 */
		if ( mode != 2 )
			ndims = Max(gbox_ndims(&gbox), ndims);
		
		/* Convert gbox to n-d box */		
		nd_box = palloc(sizeof(ND_BOX));
		nd_box_from_gbox(&gbox, nd_box);
		
		/* Cache n-d bounding box */
		sample_boxes[notnull_cnt] = nd_box;

		/* Initialize sample extent before merging first entry */
		if ( ! notnull_cnt )
			nd_box_init_bounds(&sample_extent);
		
		/* Add current sample to overall sample extent */
		nd_box_merge(nd_box, &sample_extent);
		
		/* How many bytes does this sample use? */
		total_width += VARSIZE(geom);
		
		/* Add bounds coordinates to sums for stddev calculation */
		for ( d = 0; d < ndims; d++ )
		{
			sum.min[d] += nd_box->min[d];
			sum.max[d] += nd_box->max[d];
		}

		/* Increment our "good feature" count */
		notnull_cnt++;
		
		/* Free up memory if our sample geometry was copied */
		if ( is_copy ) 
			pfree(geom);

		/* Give backend a chance of interrupting us */
		vacuum_delay_point();
	}

	/*
	 * We'll build a histogram having stats->attr->attstattarget cells
	 * on each side,  within reason... we'll use ndims*10000 as the 
	 * maximum number of cells. 
	 * Also, if we're sampling a relatively small table, we'll try to ensure that 
	 * we have an average of 5 features for each cell so the histogram isn't
	 * so sparse.
	 */
	histo_cells_target = (int)pow((double)(stats->attr->attstattarget), (double)ndims);
	histo_cells_target = Min(histo_cells_target, ndims * 10000);
	histo_cells_target = Min(histo_cells_target, (int)(total_rows/5));
	POSTGIS_DEBUGF(3, " stats->attr->attstattarget: %d", stats->attr->attstattarget);
	POSTGIS_DEBUGF(3, " target # of histogram cells: %d", histo_cells_target);

	/* If there's no useful features, we can't work out stats */
	if ( ! notnull_cnt )
	{
		elog(NOTICE, "no non-null/empty features, unable to compute statistics");
		stats->stats_valid = false;
		return;
	}

	POSTGIS_DEBUGF(3, " sample_extent: %s", nd_box_to_json(&sample_extent, ndims));

	/*
	 * Second scan:
	 *  o compute standard deviation
	 */	
	for ( d = 0; d < ndims; d++ )
	{
		/* Calculate average bounds values */
		avg.min[d] = sum.min[d] / notnull_cnt;
		avg.max[d] = sum.max[d] / notnull_cnt;
		
		/* Calculate standard deviation for this dimension bounds */
		for ( i = 0; i < notnull_cnt; i++ )
		{
			const ND_BOX *ndb = sample_boxes[i];
			stddev.min[d] += (ndb->min[d] - avg.min[d]) * (ndb->min[d] - avg.min[d]);
			stddev.max[d] += (ndb->max[d] - avg.max[d]) * (ndb->max[d] - avg.max[d]);
		}
		stddev.min[d] = sqrt(stddev.min[d] / notnull_cnt);
		stddev.max[d] = sqrt(stddev.max[d] / notnull_cnt);
		
		/* Histogram bounds for this dimension bounds is avg +/- SDFACTOR * stdev */
		histo_extent.min[d] = Max(avg.min[d] - SDFACTOR * stddev.min[d], sample_extent.min[d]);
		histo_extent.max[d] = Min(avg.max[d] + SDFACTOR * stddev.max[d], sample_extent.max[d]);
	}

	/*
	 * Third scan:
	 *   o skip hard deviants
	 *   o compute new histogram box
	 */
	nd_box_init_bounds(&histo_extent_new);
	for ( i = 0; i < notnull_cnt; i++ )
	{
		const ND_BOX *ndb = sample_boxes[i];
		/* Skip any hard deviants (boxes entirely outside our histo_extent */
		if ( ! nd_box_intersects(&histo_extent, ndb, ndims) )
		{
			POSTGIS_DEBUGF(4, " feature %d is a hard deviant, skipped", i);
			sample_boxes[i] = NULL;
			continue;
		}	
		/* Expand our new box to fit all the other features. */
		nd_box_merge(ndb, &histo_extent_new);
	}
	/*
	 * Expand the box slightly (1%) to avoid edge effects 
	 * with objects that are on the boundary 
	 */
	nd_box_expand(&histo_extent_new, 0.01);
	histo_extent = histo_extent_new;
	
	/*
	 * How should we allocate our histogram cells to the 
	 * different dimensions? We can't do it by raw dimensional width,
	 * because in x/y/z space, the z can have different units
	 * from the x/y. Similarly for x/y/t space. 
	 * So, we instead calculate how much features overlap
	 * each other in their dimension to figure out which
	 *  dimensions have useful selectivity characteristics (more 
	 * variability in density) and therefor would find
	 * more cells useful (to distinguish between dense places and
	 * homogeneous places).
	 */ 
	nd_box_array_distribution(sample_boxes, notnull_cnt, &histo_extent, ndims,
	                          sample_distribution);

	/* 
	 * The sample_distribution array now tells us how spread out the 
	 * data is in each dimension, so we use that data to allocate
	 * the histogram cells we have available.
	 * At this point, histo_cells_target is the approximate target number
	 * of cells.
	 */
	
	/*
	 * Some dimensions have basically a uniform distribution, we want
	 * to allocate no cells to those dimensions, only to dimensions
	 * that have some interesting differences in data distribution.
	 * Here we count up the number of interesting dimensions 
	 */
	for ( d = 0; d < ndims; d++ )
	{
		if ( sample_distribution[d] > 0 )
			histo_ndims++;
	}
	
	if ( histo_ndims == 0 )
	{
		/* Special case: all our dimensions had low variability! */
		/* We just divide the cells up evenly */
		POSTGIS_DEBUG(3, " special case: no axes have variability");
		histo_cells_new = 1;
		for ( d = 0; d < ndims; d++ )
		{
			histo_size[d] = (int)pow((double)histo_cells_target, 1/(double)ndims);
			POSTGIS_DEBUGF(3, "   histo_size[d]: %d", histo_size[d]);
			histo_cells_new *= histo_size[d];
		}
		POSTGIS_DEBUGF(3, " histo_cells_new: %d", histo_cells_new);
	}
	else
	{
		/* 
		 * We're going to express the amount of variability in each dimension
		 * as a proportion of the total variability and allocate cells in that
		 * dimension relative to that proportion.
		 */
		POSTGIS_DEBUG(3, " allocating histogram axes based on axis variability");
		total_distribution = total_double(sample_distribution, ndims); /* First get the total */
		POSTGIS_DEBUGF(3, " total_distribution: %.8g", total_distribution);
		histo_cells_new = 1; /* For the number of cells in the final histogram */
		for ( d = 0; d < ndims; d++ )
		{
			if ( sample_distribution[d] == 0 ) /* Uninteresting dimensions don't get any room */
			{
				histo_size[d] = 1;
			}
			else /* Interesting dimension */
			{
				/* How does this dims variability compare to the total? */
				float edge_ratio = (float)sample_distribution[d] / (float)total_distribution;
				/* 
				 * Scale the target cells number by the # of dims and ratio,
				 * then take the appropriate root to get the estimated number of cells 
				 * on this axis (eg, pow(0.5) for 2d, pow(0.333) for 3d, pow(0.25) for 4d)
				*/
				histo_size[d] = (int)pow(histo_cells_target * histo_ndims * edge_ratio, 1/(double)histo_ndims);
				/* If something goes awry, just give this dim one slot */
				if ( ! histo_size[d] )
					histo_size[d] = 1;
			}
			histo_cells_new *= histo_size[d];
		} 
		POSTGIS_DEBUGF(3, " histo_cells_new: %d", histo_cells_new);
	}
	
	/* Update histo_cells to the actual number of cells we need to allocate */
	histo_cells = histo_cells_new;
	POSTGIS_DEBUGF(3, " histo_cells: %d", histo_cells);
	
	/*
	 * Create the histogram (ND_STATS) in the stats memory context
	 */
	old_context = MemoryContextSwitchTo(stats->anl_context);
	nd_stats_size = sizeof(ND_STATS) + ((histo_cells - 1) * sizeof(float4));
	nd_stats = palloc(nd_stats_size);
	memset(nd_stats, 0, nd_stats_size); /* Initialize all values to 0 */
	MemoryContextSwitchTo(old_context);

	/* Initialize the #ND_STATS objects */
	nd_stats->ndims = ndims;
	nd_stats->extent = histo_extent;
	nd_stats->sample_features = sample_rows;
	nd_stats->table_features = total_rows;
	nd_stats->not_null_features = notnull_cnt;
	/* Copy in the histogram dimensions */
	for ( d = 0; d < ndims; d++ )
		nd_stats->size[d] = histo_size[d];

	/*
	 * Fourth scan:
	 *  o fill histogram values with the proportion of
	 *    features' bbox overlaps: a feature's bvol
	 *    can fully overlap (1) or partially overlap
	 *    (fraction of 1) an histogram cell.
	 *
	 * Note that we are filling each cell with the "portion of
	 * the feature's box that overlaps the cell". So, if we sum
	 * up the values in the histogram, we could get the
	 * histogram feature count.
	 *
	 */
	for ( i = 0; i < notnull_cnt; i++ )
	{
		const ND_BOX *nd_box;
		ND_IBOX nd_ibox;
		int at[ND_DIMS];
		int d;
		double num_cells = 0;
		double tmp_volume = 1.0;
		double min[ND_DIMS];
		double max[ND_DIMS];
		double cellsize[ND_DIMS];

		nd_box = sample_boxes[i];
		if ( ! nd_box ) continue; /* Skip Null'ed out hard deviants */

		/* Give backend a chance of interrupting us */
		vacuum_delay_point();
		
		/* Find the cells that overlap with this box and put them into the ND_IBOX */
		nd_box_overlap(nd_stats, nd_box, &nd_ibox);
		memset(at, 0, sizeof(int)*ND_DIMS);
		
		POSTGIS_DEBUGF(3, " feature %d: ibox (%d, %d, %d, %d) (%d, %d, %d, %d)", i, 
		  nd_ibox.min[0], nd_ibox.min[1], nd_ibox.min[2], nd_ibox.min[3],
		  nd_ibox.max[0], nd_ibox.max[1], nd_ibox.max[2], nd_ibox.max[3]);

		for ( d = 0; d < nd_stats->ndims; d++ )
		{
			/* Initialize the starting values */
			at[d] = nd_ibox.min[d];
			min[d] = nd_stats->extent.min[d];
			max[d] = nd_stats->extent.max[d];
			cellsize[d] = (max[d] - min[d])/(nd_stats->size[d]);
			
			/* What's the volume (area) of this feature's box? */
			tmp_volume *= (nd_box->max[d] - nd_box->min[d]);
		}

		/* Add feature volume (area) to our total */
		total_sample_volume += tmp_volume;

		/* 
		 * Move through all the overlaped histogram cells values and 
		 * add the box overlap proportion to them. 
		 */
		do 
		{
			ND_BOX nd_cell;
			double ratio;
			/* Create a box for this histogram cell */
			for ( d = 0; d < nd_stats->ndims; d++ )
			{
				nd_cell.min[d] = min[d] + (at[d]+0) * cellsize[d];
				nd_cell.max[d] = min[d] + (at[d]+1) * cellsize[d];
			}

			/* 
			 * If a feature box is completely inside one cell the ratio will be
			 * 1.0. If a feature box is 50% in two cells, each cell will get
			 * 0.5 added on.
			 */
			ratio = nd_box_ratio(&nd_cell, nd_box, nd_stats->ndims);
			nd_stats->value[nd_stats_value_index(nd_stats, at)] += ratio;
			num_cells += ratio;
			POSTGIS_DEBUGF(3, "               ratio (%.8g)  num_cells (%.8g)", ratio, num_cells);
			POSTGIS_DEBUGF(3, "               at (%d, %d, %d, %d)", at[0], at[1], at[2], at[3]);
		} 
		while ( nd_increment(&nd_ibox, nd_stats->ndims, at) );
		
		/* Keep track of overall number of overlaps counted */
		total_cell_count += num_cells;
		/* How many features have we added to this histogram? */
		histogram_features++;
	}

	POSTGIS_DEBUGF(3, " histogram_features: %d", histogram_features);
	POSTGIS_DEBUGF(3, " sample_rows: %d", sample_rows);
	POSTGIS_DEBUGF(3, " table_rows: %.6g", total_rows);

	/* Error out if we got no sample information */
	if ( ! histogram_features )
	{
		POSTGIS_DEBUG(3, " no stats have been gathered");
		elog(NOTICE, " no features lie in the stats histogram, invalid stats");
		stats->stats_valid = false;
		return;
	}
	
	nd_stats->histogram_features = histogram_features;
	nd_stats->histogram_cells = histo_cells;
	nd_stats->cells_covered = total_cell_count;

	/* Put this histogram data into the right slot/kind */
	if ( mode == 2 )
	{
		stats_slot = STATISTIC_SLOT_2D;
		stats_kind = STATISTIC_KIND_2D;
	}
	else
	{
		stats_slot = STATISTIC_SLOT_ND;
		stats_kind = STATISTIC_KIND_ND;
	}
	
	/* Write the statistics data */
	stats->stakind[stats_slot] = stats_kind;
	stats->staop[stats_slot] = InvalidOid;
	stats->stanumbers[stats_slot] = (float4*)nd_stats;
	stats->numnumbers[stats_slot] = nd_stats_size/sizeof(float4);
	stats->stanullfrac = (float4)null_cnt/sample_rows;
	stats->stawidth = total_width/notnull_cnt;
	stats->stadistinct = -1.0;
	stats->stats_valid = true;

	POSTGIS_DEBUGF(3, " out: slot 0: kind %d (STATISTIC_KIND_ND)", stats->stakind[0]);
	POSTGIS_DEBUGF(3, " out: slot 0: op %d (InvalidOid)", stats->staop[0]);
	POSTGIS_DEBUGF(3, " out: slot 0: numnumbers %d", stats->numnumbers[0]);
	POSTGIS_DEBUGF(3, " out: null fraction: %f=%d/%d", stats->stanullfrac, null_cnt, sample_rows);
	POSTGIS_DEBUGF(3, " out: average width: %d bytes", stats->stawidth);
	POSTGIS_DEBUG (3, " out: distinct values: all (no check done)");
	POSTGIS_DEBUGF(3, " out: %s", nd_stats_to_json(nd_stats));
	/*
	POSTGIS_DEBUGF(3, " out histogram:\n%s", nd_stats_to_grid(nd_stats));
	*/

	return;
}


/**
* In order to do useful selectivity calculations in both 2-D and N-D 
* modes, we actually have to generate two stats objects, one for 2-D
* and one for N-D. 
* You would think that an N-D histogram would be sufficient for 2-D 
* calculations of selectivity, but you'd be wrong. For features that
* overlap multiple cells, the N-D histogram over-estimates the number
* of hits, and can't contain the requisite information to correct
* that over-estimate. 
* We use the convenient PgSQL facility of stats slots to store 
* one 2-D and one N-D stats object, and here in the compute function
* we just call the computation twice, once in each mode.
* It would be more efficient to have the computation calculate
* the two histograms simultaneously, but that would also complicate
* the (already complicated) logic in the function, 
* so we'll take the CPU hit and do the computation twice.
*/
static void
compute_gserialized_stats(VacAttrStats *stats, AnalyzeAttrFetchFunc fetchfunc,
                          int sample_rows, double total_rows)
{
	/* 2D Mode */
	compute_gserialized_stats_mode(stats, fetchfunc, sample_rows, total_rows, 2);
	/* ND Mode */
	compute_gserialized_stats_mode(stats, fetchfunc, sample_rows, total_rows, 0);	
}


/**
* This function will be called when the ANALYZE command is run
* on a column of the "geometry" or "geography" type.
*
* It will need to return a stats builder function reference
* and a "minimum" sample rows to feed it.
* If we want analisys to be completely skipped we can return
* FALSE and leave output vals untouched.
*
* What we know from this call is:
*
* 	o The pg_attribute row referring to the specific column.
* 	  Could be used to get reltuples from pg_class (which
* 	  might quite inexact though...) and use them to set an
* 	  appropriate minimum number of sample rows to feed to
* 	  the stats builder. The stats builder will also receive
* 	  a more accurate "estimation" of the number or rows.
*
* 	o The pg_type row for the specific column.
* 	  Could be used to set stat builder / sample rows
* 	  based on domain type (when postgis will be implemented
* 	  that way).
*
* Being this experimental we'll stick to a static stat_builder/sample_rows
* value for now.
*
*/
PG_FUNCTION_INFO_V1(gserialized_analyze_nd);
Datum gserialized_analyze_nd(PG_FUNCTION_ARGS)
{
	VacAttrStats *stats = (VacAttrStats *)PG_GETARG_POINTER(0);
	Form_pg_attribute attr = stats->attr;

	POSTGIS_DEBUG(2, "gserialized_analyze_nd called");

	/* If the attstattarget column is negative, use the default value */
	/* NB: it is okay to scribble on stats->attr since it's a copy */
	if (attr->attstattarget < 0)
		attr->attstattarget = default_statistics_target;

	POSTGIS_DEBUGF(3, " attribute stat target: %d", attr->attstattarget);

	/* Setup the minimum rows and the algorithm function */
	stats->minrows = 300 * stats->attr->attstattarget;
	stats->compute_stats = compute_gserialized_stats;

	POSTGIS_DEBUGF(3, " minrows: %d", stats->minrows);

	/* Indicate we are done successfully */
	PG_RETURN_BOOL(true);
}

/**
* This function returns an estimate of the selectivity
* of a search GBOX by looking at data in the ND_STATS
* structure. The selectivity is a float from 0-1, that estimates
* the proportion of the rows in the table that will be returned
* as a result of the search box.
*
* To get our estimate,
* we need "only" sum up the values * the proportion of each cell
* in the histogram that falls within the search box, then 
* divide by the number of features that generated the histogram.
*/
static float8
estimate_selectivity(const GBOX *box, const ND_STATS *nd_stats, int mode)
{
	int d; /* counter */	
	float8 selectivity;
	ND_BOX nd_box;
	ND_IBOX nd_ibox;
	int at[ND_DIMS];
	double cell_size[ND_DIMS];
	double min[ND_DIMS];
	double max[ND_DIMS];
	double total_count = 0.0;
	int ndims_max = Max(nd_stats->ndims, gbox_ndims(box));	
//	int ndims_min = Min(nd_stats->ndims, gbox_ndims(box));	

	/* Calculate the overlap of the box on the histogram */
	if ( ! nd_stats )
	{
		elog(NOTICE, " estimate_selectivity called with null input");
		return FALLBACK_ND_SEL;
	}

	/* Initialize nd_box. */
	nd_box_from_gbox(box, &nd_box);

	/*
	 * To return 2D stats on an ND sample, we need to make the 
	 * 2D box cover the full range of the other dimensions in the
	 * histogram.
	 */
	POSTGIS_DEBUGF(3, " mode: %d", mode);
	if ( mode == 2 )
	{
		POSTGIS_DEBUG(3, " in 2d mode, stripping the computation down to 2d");
		ndims_max = 2;
	}
	
	POSTGIS_DEBUGF(3, " nd_stats->extent: %s", nd_box_to_json(&(nd_stats->extent), nd_stats->ndims));
	POSTGIS_DEBUGF(3, " nd_box: %s", nd_box_to_json(&(nd_box), gbox_ndims(box)));

	/* 
	 * Search box completely misses histogram extent? 
	 * We have to intersect in all N dimensions or else we have
	 * zero interaction under the &&& operator. It's important
	 * to short circuit in this case, as some of the tests below
	 * will return junk results when run on non-intersecting inputs.
	 */
	if ( ! nd_box_intersects(&nd_box, &(nd_stats->extent), ndims_max) )
	{
		POSTGIS_DEBUG(3, " search box does not overlap histogram, returning 0");
		return 0.0;
	}

	/* Search box completely contains histogram extent! */
	if ( nd_box_contains(&nd_box, &(nd_stats->extent), ndims_max) )
	{
		POSTGIS_DEBUG(3, " search box contains histogram, returning 1");
		return 1.0;
	}

	/* Calculate the overlap of the box on the histogram */
	if ( ! nd_box_overlap(nd_stats, &nd_box, &nd_ibox) )
	{
		POSTGIS_DEBUG(3, " search box overlap with stats histogram failed");
		return FALLBACK_ND_SEL;
	}

	/* Work out some measurements of the histogram */
	for ( d = 0; d < nd_stats->ndims; d++ )
	{
		/* Cell size in each dim */
		min[d] = nd_stats->extent.min[d];
		max[d] = nd_stats->extent.max[d];
		cell_size[d] = (max[d] - min[d]) / nd_stats->size[d];
		POSTGIS_DEBUGF(3, " cell_size[%d] : %.9g", d, cell_size[d]);
		
		/* Initialize the counter */
		at[d] = nd_ibox.min[d];
	}

	/* Move through all the overlap values and sum them */
	do 
	{
		float cell_count, ratio;
		ND_BOX nd_cell;
		
		/* We have to pro-rate partially overlapped cells. */
		for ( d = 0; d < nd_stats->ndims; d++ )
		{
			nd_cell.min[d] = min[d] + (at[d]+0) * cell_size[d];
			nd_cell.max[d] = min[d] + (at[d]+1) * cell_size[d];
		}

		ratio = nd_box_ratio(&nd_box, &nd_cell, nd_stats->ndims);
		cell_count = nd_stats->value[nd_stats_value_index(nd_stats, at)];
		
		/* Add the pro-rated count for this cell to the overall total */
		total_count += cell_count * ratio;	
		POSTGIS_DEBUGF(4, " cell (%d,%d), cell value %.6f, ratio %.6f", at[0], at[1], cell_count, ratio);	
	} 
	while ( nd_increment(&nd_ibox, nd_stats->ndims, at) );

	/* Scale by the number of features in our histogram to get the proportion */
	selectivity = total_count / nd_stats->histogram_features;

	POSTGIS_DEBUGF(3, " nd_stats->histogram_features = %f", nd_stats->histogram_features);
	POSTGIS_DEBUGF(3, " nd_stats->histogram_cells = %f", nd_stats->histogram_cells);
	POSTGIS_DEBUGF(3, " sum(overlapped histogram cells) = %f", total_count);
	POSTGIS_DEBUGF(3, " selectivity = %f", selectivity);

	/* Prevent rounding overflows */
	if (selectivity > 1.0) selectivity = 1.0;
	else if (selectivity < 0.0) selectivity = 0.0;

	return selectivity;
}



/**
* Utility function to print the statistics information for a 
* given table/column in JSON. Used for debugging the selectivity code.
*/
PG_FUNCTION_INFO_V1(_postgis_gserialized_stats);
Datum _postgis_gserialized_stats(PG_FUNCTION_ARGS)
{
	Oid table_oid = PG_GETARG_OID(0);
	text *att_text = PG_GETARG_TEXT_P(1);
	ND_STATS *nd_stats;
	char *str;
	text *json;
	int mode = 2; /* default to 2D mode */
	
	/* Check if we've been asked to not use 2d mode */
	if ( ! PG_ARGISNULL(2) )
		mode = text_p_get_mode(PG_GETARG_TEXT_P(2));

	/* Retrieve the stats object */
	nd_stats = pg_get_nd_stats_by_name(table_oid, att_text, mode);
	if ( ! nd_stats )
		elog(ERROR, "stats for \"%s.%s\" do not exist", get_rel_name(table_oid), text2cstring(att_text));
		
	/* Convert to JSON */
	str = nd_stats_to_json(nd_stats);
	json = cstring2text(str);
	pfree(str);
	pfree(nd_stats);
	PG_RETURN_TEXT_P(json);
}


/**
* Utility function to read the calculated selectivity for a given search
* box and table/column. Used for debugging the selectivity code.
*/
PG_FUNCTION_INFO_V1(_postgis_gserialized_sel);
Datum _postgis_gserialized_sel(PG_FUNCTION_ARGS)
{
	Oid table_oid = PG_GETARG_OID(0);
	text *att_text = PG_GETARG_TEXT_P(1);
	Datum geom_datum = PG_GETARG_DATUM(2);
	GBOX gbox; /* search box read from gserialized datum */
	float8 selectivity = 0;
	ND_STATS *nd_stats;
	int mode = 2; /* 2D mode by default */

	/* Check if we've been asked to not use 2d mode */
	if ( ! PG_ARGISNULL(3) )
		mode = text_p_get_mode(PG_GETARG_TEXT_P(3));

	/* Retrieve the stats object */
	nd_stats = pg_get_nd_stats_by_name(table_oid, att_text, mode);
	
	if ( ! nd_stats )
		elog(ERROR, "stats for \"%s.%s\" do not exist", get_rel_name(table_oid), text2cstring(att_text));

	/* Calculate the gbox */
	if ( ! gserialized_datum_get_gbox_p(geom_datum, &gbox) )
		elog(ERROR, "unable to calculate bounding box from geometry");

	POSTGIS_DEBUGF(3, " %s", gbox_to_string(&gbox));
	
	/* Do the estimation */
	selectivity = estimate_selectivity(&gbox, nd_stats, mode);
	
	pfree(nd_stats);
	PG_RETURN_FLOAT8(selectivity);
}


/**
* Utility function to read the calculated join selectivity for a 
* pair of tables. Used for debugging the selectivity code.
*/
PG_FUNCTION_INFO_V1(_postgis_gserialized_joinsel);
Datum _postgis_gserialized_joinsel(PG_FUNCTION_ARGS)
{
	Oid table_oid1 = PG_GETARG_OID(0);
	text *att_text1 = PG_GETARG_TEXT_P(1);
	Oid table_oid2 = PG_GETARG_OID(2);
	text *att_text2 = PG_GETARG_TEXT_P(3);
	ND_STATS *nd_stats1, *nd_stats2;
	float8 selectivity = 0;
	int mode = 2; /* 2D mode by default */


	/* Retrieve the stats object */
	nd_stats1 = pg_get_nd_stats_by_name(table_oid1, att_text1, mode);
	nd_stats2 = pg_get_nd_stats_by_name(table_oid2, att_text2, mode);

	if ( ! nd_stats1 ) 
		elog(ERROR, "stats for \"%s.%s\" do not exist", get_rel_name(table_oid1), text2cstring(att_text1));

	if ( ! nd_stats2 ) 
		elog(ERROR, "stats for \"%s.%s\" do not exist", get_rel_name(table_oid2), text2cstring(att_text2));

	/* Check if we've been asked to not use 2d mode */
	if ( ! PG_ARGISNULL(4) )
	{
		text *modetxt = PG_GETARG_TEXT_P(4);
		char *modestr = text2cstring(modetxt);
		if ( modestr[0] == 'N' )
			mode = 0;		
	}

	/* Do the estimation */
	selectivity = estimate_join_selectivity(nd_stats1, nd_stats2);
	
	pfree(nd_stats1);
	pfree(nd_stats2);
	PG_RETURN_FLOAT8(selectivity);
}

/**
* For (geometry && geometry) 
* we call into the 2-D mode.
*/
PG_FUNCTION_INFO_V1(gserialized_gist_sel_2d);
Datum gserialized_gist_sel_2d(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(DirectFunctionCall5(
	   gserialized_gist_sel,
	   PG_GETARG_DATUM(0), PG_GETARG_DATUM(1),
	   PG_GETARG_DATUM(2), PG_GETARG_DATUM(3),
	   Int32GetDatum(2) /* 2-D mode */
	));
}

/**
* For (geometry &&& geometry) and (geography && geography)
* we call into the N-D mode.
*/
PG_FUNCTION_INFO_V1(gserialized_gist_sel_nd);
Datum gserialized_gist_sel_nd(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(DirectFunctionCall5(
	   gserialized_gist_sel,
	   PG_GETARG_DATUM(0), PG_GETARG_DATUM(1),
	   PG_GETARG_DATUM(2), PG_GETARG_DATUM(3),
	   Int32GetDatum(0) /* N-D mode */
	));
}

/**
 * This function should return an estimation of the number of
 * rows returned by a query involving an overlap check
 * ( it's the restrict function for the && operator )
 *
 * It can make use (if available) of the statistics collected
 * by the geometry analyzer function.
 *
 * Note that the good work is done by estimate_selectivity() above.
 * This function just tries to find the search_box, loads the statistics
 * and invoke the work-horse.
 *
 */
PG_FUNCTION_INFO_V1(gserialized_gist_sel);
Datum gserialized_gist_sel(PG_FUNCTION_ARGS)
{
	PlannerInfo *root = (PlannerInfo *) PG_GETARG_POINTER(0);
	/* Oid operator_oid = PG_GETARG_OID(1); */
	List *args = (List *) PG_GETARG_POINTER(2);
	/* int varRelid = PG_GETARG_INT32(3); */
	int mode = PG_GETARG_INT32(4);
	
	VariableStatData vardata;
	ND_STATS *nd_stats = NULL;

	Node *other;
	Var *self;
	GBOX search_box;
	float8 selectivity = 0;
	
	POSTGIS_DEBUG(2, "gserialized_gist_sel called");

	/* 
	 * TODO: This is a big one,
	 * All this statistics code *only* tries to generate a valid
	 * selectivity for && and &&&. That leaves all the other 
	 * geometry operators with bad stats! The selectivity
	 * calculation should take account of the incoming operator
	 * type and do the right thing.
	 */
	
	/* Fail if not a binary opclause (probably shouldn't happen) */
	if (list_length(args) != 2)
	{
		POSTGIS_DEBUG(3, "gserialized_gist_sel: not a binary opclause");
		PG_RETURN_FLOAT8(DEFAULT_ND_SEL);
	}

	/* Find the constant part */
	other = (Node *) linitial(args);
	if ( ! IsA(other, Const) )
	{
		self = (Var *)other;
		other = (Node *) lsecond(args);
	}
	else
	{
		self = (Var *) lsecond(args);
	}

	if ( ! IsA(other, Const) )
	{
		POSTGIS_DEBUG(3, " no constant arguments - returning a default selectivity");
		PG_RETURN_FLOAT8(DEFAULT_ND_SEL);
	}

	/* Convert the constant to a BOX */
	if( ! gserialized_datum_get_gbox_p(((Const*)other)->constvalue, &search_box) )
	{
		POSTGIS_DEBUG(3, "search box is EMPTY");
		PG_RETURN_FLOAT8(0.0);
	}
	POSTGIS_DEBUGF(4, " requested search box is: %s", gbox_to_string(&search_box));

	/* Get pg_statistic row */
	examine_variable(root, (Node*)self, 0, &vardata);
	if ( vardata.statsTuple ) {
		nd_stats = pg_nd_stats_from_tuple(vardata.statsTuple, mode);
	}
	ReleaseVariableStats(vardata);

	if ( ! nd_stats )
	{
		POSTGIS_DEBUG(3, " unable to load stats from syscache, not analyzed yet?");
		PG_RETURN_FLOAT8(FALLBACK_ND_SEL);
	}

	POSTGIS_DEBUGF(4, " got stats:\n%s", nd_stats_to_json(nd_stats));

	/* Do the estimation! */
	selectivity = estimate_selectivity(&search_box, nd_stats, mode);
	POSTGIS_DEBUGF(3, " returning computed value: %f", selectivity);

	pfree(nd_stats);
	PG_RETURN_FLOAT8(selectivity);
}



/**
 * Return the estimated extent of the table
 * looking at gathered statistics (or NULL if
 * no statistics have been gathered).
 */
PG_FUNCTION_INFO_V1(gserialized_estimated_extent);
Datum gserialized_estimated_extent(PG_FUNCTION_ARGS)
{
	char *nsp = NULL;
	char *tbl = NULL;
	text *col = NULL;
	char *nsp_tbl = NULL;
	Oid tbl_oid;
	ND_STATS *nd_stats;
	GBOX *gbox;

	if ( PG_NARGS() == 3 )
	{
		nsp = text2cstring(PG_GETARG_TEXT_P(0));
		tbl = text2cstring(PG_GETARG_TEXT_P(1));
		col = PG_GETARG_TEXT_P(2);
		nsp_tbl = palloc(strlen(nsp) + strlen(tbl) + 6);
		sprintf(nsp_tbl, "\"%s\".\"%s\"", nsp, tbl);
		tbl_oid = DatumGetObjectId(DirectFunctionCall1(regclassin, CStringGetDatum(nsp_tbl)));
		pfree(nsp_tbl);
	}
	else if ( PG_NARGS() == 2 )
	{
		tbl = text2cstring(PG_GETARG_TEXT_P(0));
		col = PG_GETARG_TEXT_P(1);
		nsp_tbl = palloc(strlen(tbl) + 3);
		sprintf(nsp_tbl, "\"%s\"", tbl);
		tbl_oid = DatumGetObjectId(DirectFunctionCall1(regclassin, CStringGetDatum(nsp_tbl)));
		pfree(nsp_tbl);
	}
	else
	{
		elog(ERROR, "estimated_extent() called with wrong number of arguments");
		PG_RETURN_NULL();
	}

	/* Estimated extent only returns 2D bounds, so use mode 2 */
	nd_stats = pg_get_nd_stats_by_name(tbl_oid, col, 2);
	
	/* Error out on no stats */
	if ( ! nd_stats ) 
		elog(ERROR, "stats for \"%s.%s\" do not exist", tbl, text2cstring(col));

	/* Construct the box */
	gbox = palloc(sizeof(GBOX));
	FLAGS_SET_GEODETIC(gbox->flags, 0);
	FLAGS_SET_Z(gbox->flags, 0);
	FLAGS_SET_M(gbox->flags, 0);
	gbox->xmin = nd_stats->extent.min[0];
	gbox->xmax = nd_stats->extent.max[0];
	gbox->ymin = nd_stats->extent.min[1];
	gbox->ymax = nd_stats->extent.max[1];

	pfree(nd_stats);
	PG_RETURN_POINTER(gbox);
}

/**
 * Return the estimated extent of the table
 * looking at gathered statistics (or NULL if
 * no statistics have been gathered).
 */
  
PG_FUNCTION_INFO_V1(geometry_estimated_extent);
Datum geometry_estimated_extent(PG_FUNCTION_ARGS)
{
	if ( PG_NARGS() == 3 )
	{
	    PG_RETURN_DATUM(
	    DirectFunctionCall3(gserialized_estimated_extent, 
	    PG_GETARG_DATUM(0), 
	    PG_GETARG_DATUM(1), 
        PG_GETARG_DATUM(2)));
	}
	else if ( PG_NARGS() == 2 )
	{
	    PG_RETURN_DATUM(
	    DirectFunctionCall2(gserialized_estimated_extent, 
	    PG_GETARG_DATUM(0), 
	    PG_GETARG_DATUM(1)));
	}

	elog(ERROR, "geometry_estimated_extent() called with wrong number of arguments");
	PG_RETURN_NULL();
}
