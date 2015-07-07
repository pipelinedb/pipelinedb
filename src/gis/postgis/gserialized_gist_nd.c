/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * Copyright 2009 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

/*
** R-Tree Bibliography
**
** [1] A. Guttman. R-tree: a dynamic index structure for spatial searching.
**     Proceedings of the ACM SIGMOD Conference, pp 47-57, June 1984.
** [2] C.-H. Ang and T. C. Tan. New linear node splitting algorithm for
**     R-Trees. Advances in Spatial Databases - 5th International Symposium,
**     1997
** [3] N. Beckmann, H.-P. Kriegel, R. Schneider, B. Seeger. The R*tree: an
**     efficient and robust access method for points and rectangles.
**     Proceedings of the ACM SIGMOD Conference. June 1990.
*/

#include "postgres.h"
#include "access/gist.h"    /* For GiST */
#include "access/itup.h"
#include "access/skey.h"

#include "../postgis_config.h"

/*#define POSTGIS_DEBUG_LEVEL 4*/

#include "liblwgeom.h"         /* For standard geometry types. */
#include "lwgeom_pg.h"       /* For debugging macros. */
#include "gserialized_gist.h"	     /* For utility functions. */
#include "geography.h"

#include <assert.h>


/* Fall back to older finite() if necessary */
#ifndef HAVE_ISFINITE
# ifdef HAVE_GNU_ISFINITE
#  define _GNU_SOURCE
# else
#  define isfinite finite
# endif
#endif

/*
** When is a node split not so good? If more than 90% of the entries
** end up in one of the children.
*/
#define LIMIT_RATIO 0.1

/*
** For debugging
*/
#if POSTGIS_DEBUG_LEVEL > 0
static int geog_counter_leaf = 0;
static int geog_counter_internal = 0;
#endif

/*
** ND Index key type stub prototypes
*/
Datum gidx_out(PG_FUNCTION_ARGS);
Datum gidx_in(PG_FUNCTION_ARGS);

/*
** ND GiST prototypes
*/
Datum gserialized_gist_consistent(PG_FUNCTION_ARGS);
Datum gserialized_gist_compress(PG_FUNCTION_ARGS);
Datum gserialized_gist_decompress(PG_FUNCTION_ARGS);
Datum gserialized_gist_penalty(PG_FUNCTION_ARGS);
Datum gserialized_gist_picksplit(PG_FUNCTION_ARGS);
Datum gserialized_gist_union(PG_FUNCTION_ARGS);
Datum gserialized_gist_same(PG_FUNCTION_ARGS);
Datum gserialized_gist_distance(PG_FUNCTION_ARGS);

/*
** ND Operator prototypes
*/
Datum gserialized_overlaps(PG_FUNCTION_ARGS);
Datum gserialized_contains(PG_FUNCTION_ARGS);
Datum gserialized_within(PG_FUNCTION_ARGS);
Datum gserialized_distance_box_nd(PG_FUNCTION_ARGS);
Datum gserialized_distance_centroid_nd(PG_FUNCTION_ARGS);

/*
** GIDX true/false test function type
*/
typedef bool (*gidx_predicate)(GIDX *a, GIDX *b);


/* Allocate a new copy of GIDX */
static GIDX* gidx_copy(GIDX *b)
{
	GIDX *c = (GIDX*)palloc(VARSIZE(b));
	POSTGIS_DEBUGF(5, "copied gidx (%p) to gidx (%p)", b, c);
	memcpy((void*)c, (void*)b, VARSIZE(b));
	return c;
}


/* Ensure all minimums are below maximums. */
static inline void gidx_validate(GIDX *b)
{
	int i;
	Assert(b);
	POSTGIS_DEBUGF(5,"validating gidx (%s)", gidx_to_string(b));
	for ( i = 0; i < GIDX_NDIMS(b); i++ )
	{
		if ( GIDX_GET_MIN(b,i) > GIDX_GET_MAX(b,i) )
		{
			float tmp;
			tmp = GIDX_GET_MIN(b,i);
			GIDX_SET_MIN(b,i,GIDX_GET_MAX(b,i));
			GIDX_SET_MAX(b,i,tmp);
		}
	}
	return;
}

/* An "unknown" GIDX is used to represent the bounds of an EMPTY
   geometry or other-wise unindexable geometry (like one with NaN
   or Inf bounds) */
static inline bool gidx_is_unknown(const GIDX *a)
{
	size_t size = VARSIZE(a) - VARHDRSZ;
	/* "unknown" gidx objects have a too-small size of one float */
	if ( size <= 0.0 )
		return TRUE;
	return FALSE;
}

static inline void gidx_set_unknown(GIDX *a)
{
	SET_VARSIZE(a, VARHDRSZ);
}

/* Enlarge b_union to contain b_new. If b_new contains more
   dimensions than b_union, expand b_union to contain those dimensions. */
static void gidx_merge(GIDX **b_union, GIDX *b_new)
{
	int i, dims_union, dims_new;
	Assert(b_union);
	Assert(*b_union);
	Assert(b_new);

	/* Can't merge an unknown into any thing */
	if( gidx_is_unknown(b_new) )
		return;
		
	/* Merge of unknown and known is known */
	if( gidx_is_unknown(*b_union) )
	{
		*b_union = b_new;
		return;
	}

	dims_union = GIDX_NDIMS(*b_union);
	dims_new = GIDX_NDIMS(b_new);

	POSTGIS_DEBUGF(4, "merging gidx (%s) into gidx (%s)", gidx_to_string(b_new), gidx_to_string(*b_union));

	if ( dims_new > dims_union )
	{
		POSTGIS_DEBUGF(5, "reallocating b_union from %d dims to %d dims", dims_union, dims_new);
		*b_union = (GIDX*)repalloc(*b_union, GIDX_SIZE(dims_new));
		SET_VARSIZE(*b_union, VARSIZE(b_new));
		dims_union = dims_new;
	}

	for ( i = 0; i < dims_new; i++ )
	{
		/* Adjust minimums */
		GIDX_SET_MIN(*b_union, i, Min(GIDX_GET_MIN(*b_union,i),GIDX_GET_MIN(b_new,i)));
		/* Adjust maximums */
		GIDX_SET_MAX(*b_union, i, Max(GIDX_GET_MAX(*b_union,i),GIDX_GET_MAX(b_new,i)));
	}

	POSTGIS_DEBUGF(5, "merge complete (%s)", gidx_to_string(*b_union));
	return;
}

/* Calculate the volume (in n-d units) of the GIDX */
static float gidx_volume(GIDX *a)
{
	float result;
	int i;
	if ( a == NULL || gidx_is_unknown(a) )
	{
		/* elog(ERROR, "gidx_volume received a null argument"); */
		return 0.0;
	}
	result = GIDX_GET_MAX(a,0) - GIDX_GET_MIN(a,0);
	for ( i = 1; i < GIDX_NDIMS(a); i++ )
		result *= (GIDX_GET_MAX(a,i) - GIDX_GET_MIN(a,i));
	POSTGIS_DEBUGF(5, "calculated volume of %s as %.8g", gidx_to_string(a), result);
	return result;
}

/* Ensure the first argument has the higher dimensionality. */
static void gidx_dimensionality_check(GIDX **a, GIDX **b)
{		
	if ( GIDX_NDIMS(*a) < GIDX_NDIMS(*b) )
	{
		GIDX *tmp = *b;
		*b = *a;
		*a = tmp;
	}
}

/* Calculate the volume of the union of the boxes. Avoids creating an intermediate box. */
static float gidx_union_volume(GIDX *a, GIDX *b)
{
	float result;
	int i;
	int ndims_a, ndims_b;

	POSTGIS_DEBUG(5,"entered function");

	if ( a == NULL && b == NULL )
	{
		elog(ERROR, "gidx_union_volume received two null arguments");
		return 0.0;
	}

	if ( a == NULL || gidx_is_unknown(a) )
		return gidx_volume(b);

	if ( b == NULL || gidx_is_unknown(b) )
		return gidx_volume(a);

	if ( gidx_is_unknown(a) && gidx_is_unknown(b) )
	{
		return 0.0;
	}

	/* Ensure 'a' has the most dimensions. */
	gidx_dimensionality_check(&a, &b);

	ndims_a = GIDX_NDIMS(a);
	ndims_b = GIDX_NDIMS(b);

	/* Initialize with maximal length of first dimension. */
	result = Max(GIDX_GET_MAX(a,0),GIDX_GET_MAX(b,0)) - Min(GIDX_GET_MIN(a,0),GIDX_GET_MIN(b,0));

	/* Multiply by maximal length of remaining dimensions. */
	for ( i = 1; i < ndims_b; i++ )
	{
		result *= (Max(GIDX_GET_MAX(a,i),GIDX_GET_MAX(b,i)) - Min(GIDX_GET_MIN(a,i),GIDX_GET_MIN(b,i)));
	}

	/* Add in dimensions of higher dimensional box. */
	for ( i = ndims_b; i < ndims_a; i++ )
	{
		result *= (GIDX_GET_MAX(a,i) - GIDX_GET_MIN(a,i));
	}

	POSTGIS_DEBUGF(5, "volume( %s union %s ) = %.12g", gidx_to_string(a), gidx_to_string(b), result);

	return result;
}

/* Calculate the volume of the intersection of the boxes. */
static float gidx_inter_volume(GIDX *a, GIDX *b)
{
	int i;
	float result;

	POSTGIS_DEBUG(5,"entered function");

	if ( a == NULL || b == NULL )
	{
		elog(ERROR, "gidx_inter_volume received a null argument");
		return 0.0;
	}

	if ( gidx_is_unknown(a) || gidx_is_unknown(b) )
	{
		return 0.0;
	}
		
	/* Ensure 'a' has the most dimensions. */
	gidx_dimensionality_check(&a, &b);

	/* Initialize with minimal length of first dimension. */
	result = Min(GIDX_GET_MAX(a,0),GIDX_GET_MAX(b,0)) - Max(GIDX_GET_MIN(a,0),GIDX_GET_MIN(b,0));

	/* If they are disjoint (max < min) then return zero. */
	if ( result < 0.0 ) return 0.0;

	/* Continue for remaining dimensions. */
	for ( i = 1; i < GIDX_NDIMS(b); i++ )
	{
		float width = Min(GIDX_GET_MAX(a,i),GIDX_GET_MAX(b,i)) - Max(GIDX_GET_MIN(a,i),GIDX_GET_MIN(b,i));
		if ( width < 0.0 ) return 0.0;
		/* Multiply by minimal length of remaining dimensions. */
		result *= width;
	}
	POSTGIS_DEBUGF(5, "volume( %s intersection %s ) = %.12g", gidx_to_string(a), gidx_to_string(b), result);
	return result;
}

/*
** Overlapping GIDX box test.
**
** Box(A) Overlaps Box(B) IFF for every dimension d:
**   min(A,d) <= max(B,d) && max(A,d) => min(B,d)
**
** Any missing dimension is assumed by convention to
** overlap whatever finite range available on the
** other operand. See
** http://lists.osgeo.org/pipermail/postgis-devel/2015-February/024757.html
**
** Empty boxes never overlap.
*/
static bool gidx_overlaps(GIDX *a, GIDX *b)
{
	int i;
	int ndims_b;
	POSTGIS_DEBUG(5, "entered function");
	
	if ( (a == NULL) || (b == NULL) ) return FALSE;

	if ( gidx_is_unknown(a) || gidx_is_unknown(b) )
		return FALSE;

	/* Ensure 'a' has the most dimensions. */
	gidx_dimensionality_check(&a, &b);

	ndims_b = GIDX_NDIMS(b);

	/* compare only up to dimensions of (b), missing dimensions always overlap */
	for ( i = 0; i < ndims_b; i++ )
	{
		if ( GIDX_GET_MIN(a,i) > GIDX_GET_MAX(b,i) )
			return FALSE;
		if ( GIDX_GET_MIN(b,i) > GIDX_GET_MAX(a,i) )
			return FALSE;
	}

	return TRUE;
}

/*
** Containment GIDX test.
**
** Box(A) CONTAINS Box(B) IFF (pt(A)LL < pt(B)LL) && (pt(A)UR > pt(B)UR)
*/
static bool gidx_contains(GIDX *a, GIDX *b)
{
	int i, dims_a, dims_b;

	POSTGIS_DEBUG(5, "entered function");

	if ( (a == NULL) || (b == NULL) ) return FALSE;

	if ( gidx_is_unknown(a) || gidx_is_unknown(b) )
		return FALSE;

	dims_a = GIDX_NDIMS(a);
	dims_b = GIDX_NDIMS(b);

	if ( dims_a < dims_b )
	{
		/*
		** If (b) is of higher dimensionality than (a) it can only be contained
		** if those higher dimensions are zeroes.
		*/
		for (i = dims_a; i < dims_b; i++)
		{
			if ( GIDX_GET_MIN(b,i) != 0 )
				return FALSE;
			if ( GIDX_GET_MAX(b,i) != 0 )
				return FALSE;
		}
	}

	/* Excess dimensions of (a), don't matter, it just has to contain (b) in (b)'s dimensions */
	for (i = 0; i < Min(dims_a, dims_b); i++)
	{
		if ( GIDX_GET_MIN(a,i) > GIDX_GET_MIN(b,i) )
			return FALSE;
		if ( GIDX_GET_MAX(a,i) < GIDX_GET_MAX(b,i) )
			return FALSE;
	}

	return TRUE;
}

/*
** Equality GIDX test.
**
** Box(A) EQUALS Box(B) IFF (pt(A)LL == pt(B)LL) && (pt(A)UR == pt(B)UR)
*/
static bool gidx_equals(GIDX *a, GIDX *b)
{
	int i;

	POSTGIS_DEBUG(5, "entered function");

	if ( (a == NULL) && (b == NULL) ) return TRUE;
	if ( (a == NULL) || (b == NULL) ) return FALSE;

	if ( gidx_is_unknown(a) && gidx_is_unknown(b) )
		return TRUE;

	if ( gidx_is_unknown(a) || gidx_is_unknown(b) )
		return FALSE;

	/* Ensure 'a' has the most dimensions. */
	gidx_dimensionality_check(&a, &b);

	/* For all shared dimensions min(a) == min(b), max(a) == max(b) */
	for (i = 0; i < GIDX_NDIMS(b); i++)
	{
		if ( GIDX_GET_MIN(a,i) != GIDX_GET_MIN(b,i) )
			return FALSE;
		if ( GIDX_GET_MAX(a,i) != GIDX_GET_MAX(b,i) )
			return FALSE;
	}
	/* For all unshared dimensions min(a) == 0.0, max(a) == 0.0 */
	for (i = GIDX_NDIMS(b); i < GIDX_NDIMS(a); i++)
	{
		if ( GIDX_GET_MIN(a,i) != 0.0 )
			return FALSE;
		if ( GIDX_GET_MAX(a,i) != 0.0 )
			return FALSE;
	}
	return TRUE;
}

/**
* Support function. Based on two datums return true if
* they satisfy the predicate and false otherwise.
*/
static int 
gserialized_datum_predicate(Datum gs1, Datum gs2, gidx_predicate predicate)
{
	/* Put aside some stack memory and use it for GIDX pointers. */
	char boxmem1[GIDX_MAX_SIZE];
	char boxmem2[GIDX_MAX_SIZE];
	GIDX *gidx1 = (GIDX*)boxmem1;
	GIDX *gidx2 = (GIDX*)boxmem2;

	POSTGIS_DEBUG(3, "entered function");

	/* Must be able to build box for each arguement (ie, not empty geometry)
	   and predicate function to return true. */
	if ( (gserialized_datum_get_gidx_p(gs1, gidx1) == LW_SUCCESS) &&
	     (gserialized_datum_get_gidx_p(gs2, gidx2) == LW_SUCCESS) &&
	      predicate(gidx1, gidx2) )
	{
		POSTGIS_DEBUGF(3, "got boxes %s and %s", gidx_to_string(gidx1), gidx_to_string(gidx2));
		return LW_TRUE;
	}
	return LW_FALSE;
}

/**
* Calculate the centroid->centroid distance between the boxes.
*/
static double gidx_distance_leaf_centroid(const GIDX *a, const GIDX *b)
{
  int ndims, i;
  double sum = 0;

  /* Base computation on least available dimensions */
  ndims = Min(GIDX_NDIMS(b), GIDX_NDIMS(a));
  for ( i = 0; i < ndims; ++i )
  {
    double ca, cb, d;
    double amin = GIDX_GET_MIN(a,i);
    double amax = GIDX_GET_MAX(a,i);
    double bmin = GIDX_GET_MIN(b,i);
    double bmax = GIDX_GET_MAX(b,i);
    ca = amin + ( ( amax - amin ) / 2.0 );
    cb = bmin + ( ( bmax - bmin ) / 2.0 );
    d = ca - cb;
    if ( ! isfinite(d) )
    {
      /* Can happen if a dimension was padded with FLT_MAX,
       * effectively meaning "infinite range". In that case
       * we take that dimension as adding 0 to the total
       * distance.
       */
      continue;
    }
    sum += d * d;
/*
    POSTGIS_DEBUGF(3, " centroid of A for dimension %d is %g", i, ca);
    POSTGIS_DEBUGF(3, " centroid of B for dimension %d is %g", i, cb);
    POSTGIS_DEBUGF(3, " distance on dimension %d is %g, squared as %g, grows sum to %g", i, d, d*d, sum);
*/
  }
  return sqrt(sum);
}

/**
* Calculate the box->box distance.
*/
static double gidx_distance(const GIDX *a, const GIDX *b)
{
  int ndims, i;
  double sum = 0;

  /* Base computation on least available dimensions */
  ndims = Min(GIDX_NDIMS(b), GIDX_NDIMS(a));
  for ( i = 0; i < ndims; ++i )
  {
    double d;
    double amin = GIDX_GET_MIN(a,i);
    double amax = GIDX_GET_MAX(a,i);
    double bmin = GIDX_GET_MIN(b,i);
    double bmax = GIDX_GET_MAX(b,i);
    POSTGIS_DEBUGF(3, "A %g - %g", amin, amax);
    POSTGIS_DEBUGF(3, "B %g - %g", bmin, bmax);

    if ( ( amin <= bmax && amax >= bmin ) )
    {
      /* overlaps */
      d = 0;
    }
    else if ( bmax < amin )
    {
      /* is "left" */
      d = amin - bmax;
    }
    else
    {
      /* is "right" */
      assert( bmin > amax );
      d = bmin - amax;
    }
    if ( ! isfinite(d) )
    {
      /* Can happen if coordinates are corrupted/NaN */
      continue;
    }
    sum += d * d;
    POSTGIS_DEBUGF(3, "dist %g, squared %g, grows sum to %g", d, d*d, sum);
  }
  return sqrt(sum);
}

static double gidx_distance_node_centroid(const GIDX *node, const GIDX *query)
{
  /* TODO: implement ! */
  return 0;
}

/**
* Return a #GSERIALIZED with an expanded bounding box.
*/
GSERIALIZED* 
gserialized_expand(GSERIALIZED *g, double distance)
{
	char boxmem[GIDX_MAX_SIZE];
	GIDX *gidx = (GIDX*)boxmem;
	float fdistance = (float)distance;

	/* Get our bounding box out of the geography, return right away if
	   input is an EMPTY geometry. */
	if ( gserialized_get_gidx_p(g, gidx) == LW_FAILURE )
	{
		return g;
	}
	
	gidx_expand(gidx, fdistance);

	return gserialized_set_gidx(g, gidx);
}

/***********************************************************************
* GiST N-D Index Operator Functions
*/

PG_FUNCTION_INFO_V1(gserialized_distance_box_nd);
Datum gserialized_distance_box_nd(PG_FUNCTION_ARGS)
{
	char bmem1[GIDX_MAX_SIZE];
	GIDX *b1 = (GIDX*)bmem1;
	char bmem2[GIDX_MAX_SIZE];
	GIDX *b2 = (GIDX*)bmem2;
	Datum gs1 = PG_GETARG_DATUM(0);
	Datum gs2 = PG_GETARG_DATUM(1);
	double distance;

	POSTGIS_DEBUG(3, "entered function");

	/* Must be able to build box for each argument (ie, not empty geometry). */
	if ( (gserialized_datum_get_gidx_p(gs1, b1) == LW_SUCCESS) &&
	     (gserialized_datum_get_gidx_p(gs2, b2) == LW_SUCCESS) )
	{
		distance = gidx_distance(b1, b2);
		POSTGIS_DEBUGF(3, "got boxes %s and %s", gidx_to_string(b1), gidx_to_string(b2));
		PG_RETURN_FLOAT8(distance);
	}
	PG_RETURN_FLOAT8(FLT_MAX);
}


PG_FUNCTION_INFO_V1(gserialized_distance_centroid_nd);
Datum gserialized_distance_centroid_nd(PG_FUNCTION_ARGS)
{
	char b1mem[GIDX_MAX_SIZE];
	GIDX *b1 = (GIDX*)b1mem;
	char b2mem[GIDX_MAX_SIZE];
	GIDX *b2 = (GIDX*)b2mem;
	Datum gs1 = PG_GETARG_DATUM(0);
	Datum gs2 = PG_GETARG_DATUM(1);
	double distance;

	POSTGIS_DEBUG(3, "entered function");

	/* Must be able to build box for each argument (ie, not empty geometry). */
	if ( (gserialized_datum_get_gidx_p(gs1, b1) == LW_SUCCESS) &&
			 (gserialized_datum_get_gidx_p(gs2, b2) == LW_SUCCESS) )
	{
		distance = gidx_distance_leaf_centroid(b1, b2);
		POSTGIS_DEBUGF(3, "got boxes %s and %s", gidx_to_string(b1), gidx_to_string(b2));
		PG_RETURN_FLOAT8(distance);
	}
	PG_RETURN_FLOAT8(FLT_MAX);
}

/*
** '~' and operator function. Based on two serialized return true if
** the first is contained by the second.
*/
PG_FUNCTION_INFO_V1(gserialized_within);
Datum gserialized_within(PG_FUNCTION_ARGS)
{
	if ( gserialized_datum_predicate(PG_GETARG_DATUM(1), PG_GETARG_DATUM(0), gidx_contains) == LW_TRUE )
	{
		PG_RETURN_BOOL(TRUE);
	}

	PG_RETURN_BOOL(FALSE);
}

/*
** '@' and operator function. Based on two serialized return true if
** the first contains the second.
*/
PG_FUNCTION_INFO_V1(gserialized_contains);
Datum gserialized_contains(PG_FUNCTION_ARGS)
{
	if ( gserialized_datum_predicate(PG_GETARG_DATUM(0),PG_GETARG_DATUM(1), gidx_contains) == LW_TRUE )
	{
		PG_RETURN_BOOL(TRUE);
	}

	PG_RETURN_BOOL(FALSE);
}

/*
** '&&' operator function. Based on two serialized return true if
** they overlap and false otherwise.
*/
PG_FUNCTION_INFO_V1(gserialized_overlaps);
Datum gserialized_overlaps(PG_FUNCTION_ARGS)
{
	if ( gserialized_datum_predicate(PG_GETARG_DATUM(0),PG_GETARG_DATUM(1), gidx_overlaps) == LW_TRUE )
	{
		PG_RETURN_BOOL(TRUE);
	}

	PG_RETURN_BOOL(FALSE);
}

/***********************************************************************
* GiST Index  Support Functions
*/

/*
** GiST support function. Given a geography, return a "compressed"
** version. In this case, we convert the geography into a geocentric
** bounding box. If the geography already has the box embedded in it
** we pull that out and hand it back.
*/
PG_FUNCTION_INFO_V1(gserialized_gist_compress);
Datum gserialized_gist_compress(PG_FUNCTION_ARGS)
{
	GISTENTRY *entry_in = (GISTENTRY*)PG_GETARG_POINTER(0);
	GISTENTRY *entry_out = NULL;
	char gidxmem[GIDX_MAX_SIZE];
	GIDX *bbox_out = (GIDX*)gidxmem;
	int result = LW_SUCCESS;
	int i;
	
	POSTGIS_DEBUG(4, "[GIST] 'compress' function called");

	/*
	** Not a leaf key? There's nothing to do.
	** Return the input unchanged.
	*/
	if ( ! entry_in->leafkey )
	{
		POSTGIS_DEBUG(4, "[GIST] non-leafkey entry, returning input unaltered");
		PG_RETURN_POINTER(entry_in);
	}

	POSTGIS_DEBUG(4, "[GIST] processing leafkey input");
	entry_out = palloc(sizeof(GISTENTRY));

	/*
	** Null key? Make a copy of the input entry and
	** return.
	*/
	if ( DatumGetPointer(entry_in->key) == NULL )
	{
		POSTGIS_DEBUG(4, "[GIST] leafkey is null");
		gistentryinit(*entry_out, (Datum) 0, entry_in->rel,
		              entry_in->page, entry_in->offset, FALSE);
		POSTGIS_DEBUG(4, "[GIST] returning copy of input");
		PG_RETURN_POINTER(entry_out);
	}

	/* Extract our index key from the GiST entry. */
	result = gserialized_datum_get_gidx_p(entry_in->key, bbox_out);

	/* Is the bounding box valid (non-empty, non-infinite) ?
	 * If not, use the "unknown" GIDX. */
	if ( result == LW_FAILURE )
	{
		POSTGIS_DEBUG(4, "[GIST] empty geometry!");
		gidx_set_unknown(bbox_out);
		gistentryinit(*entry_out, PointerGetDatum(gidx_copy(bbox_out)),
		              entry_in->rel, entry_in->page,
		              entry_in->offset, FALSE);
		PG_RETURN_POINTER(entry_out);
	}

	POSTGIS_DEBUGF(4, "[GIST] got entry_in->key: %s", gidx_to_string(bbox_out));

	/* Check all the dimensions for finite values.
	 * If not, use the "unknown" GIDX as a key */
	for ( i = 0; i < GIDX_NDIMS(bbox_out); i++ )
	{
		if ( ! isfinite(GIDX_GET_MAX(bbox_out, i))
		     || ! isfinite(GIDX_GET_MIN(bbox_out, i)) )
		{
			gidx_set_unknown(bbox_out);
			gistentryinit(*entry_out, 
			              PointerGetDatum(gidx_copy(bbox_out)),
			              entry_in->rel, entry_in->page,
			              entry_in->offset, FALSE);
			PG_RETURN_POINTER(entry_out);
		}
	}

	/* Enure bounding box has minimums below maximums. */
	gidx_validate(bbox_out);

	/* Prepare GISTENTRY for return. */
	gistentryinit(*entry_out, PointerGetDatum(gidx_copy(bbox_out)),
	              entry_in->rel, entry_in->page, entry_in->offset, FALSE);

	/* Return GISTENTRY. */
	POSTGIS_DEBUG(4, "[GIST] 'compress' function complete");
	PG_RETURN_POINTER(entry_out);
}

/*
** GiST support function.
** Decompress an entry. Unused for geography, so we return.
*/
PG_FUNCTION_INFO_V1(gserialized_gist_decompress);
Datum gserialized_gist_decompress(PG_FUNCTION_ARGS)
{
	POSTGIS_DEBUG(5, "[GIST] 'decompress' function called");
	/* We don't decompress. Just return the input. */
	PG_RETURN_POINTER(PG_GETARG_POINTER(0));
}

/*
** GiST support function. Called from gserialized_gist_consistent below.
*/
static inline bool gserialized_gist_consistent_leaf(GIDX *key, GIDX *query, StrategyNumber strategy)
{
	bool retval;

	POSTGIS_DEBUGF(4, "[GIST] leaf consistent, strategy [%d], count[%i]",
	               strategy, geog_counter_leaf++);

	switch (strategy)
	{
	case RTOverlapStrategyNumber:
		retval = (bool) gidx_overlaps(key, query);
		break;
	case RTSameStrategyNumber:
		retval = (bool) gidx_equals(key, query);
		break;
	case RTContainsStrategyNumber:
	case RTOldContainsStrategyNumber:
		retval = (bool) gidx_contains(key, query);
		break;
	case RTContainedByStrategyNumber:
	case RTOldContainedByStrategyNumber:
		retval = (bool) gidx_contains(query, key);
		break;
	default:
		retval = FALSE;
	}

	return (retval);
}

/*
** GiST support function. Called from gserialized_gist_consistent below.
*/
static inline bool gserialized_gist_consistent_internal(GIDX *key, GIDX *query, StrategyNumber strategy)
{
	bool		retval;

	POSTGIS_DEBUGF(4, "[GIST] internal consistent, strategy [%d], count[%i], query[%s], key[%s]",
	               strategy, geog_counter_internal++, gidx_to_string(query), gidx_to_string(key) );

	switch (strategy)
	{
	case RTOverlapStrategyNumber:
		retval = (bool) gidx_overlaps(key, query);
		break;
	case RTSameStrategyNumber:
	case RTContainsStrategyNumber:
	case RTOldContainsStrategyNumber:
		retval = (bool) gidx_contains(key, query);
		break;
	case RTContainedByStrategyNumber:
	case RTOldContainedByStrategyNumber:
		retval = (bool) gidx_overlaps(key, query);
		break;
	default:
		retval = FALSE;
	}	

	return (retval);
}

/*
** GiST support function. Take in a query and an entry and see what the
** relationship is, based on the query strategy.
*/
PG_FUNCTION_INFO_V1(gserialized_gist_consistent);
Datum gserialized_gist_consistent(PG_FUNCTION_ARGS)
{
	GISTENTRY *entry = (GISTENTRY*) PG_GETARG_POINTER(0);
	StrategyNumber strategy = (StrategyNumber) PG_GETARG_UINT16(2);
	bool result;
	char gidxmem[GIDX_MAX_SIZE];
	GIDX *query_gbox_index = (GIDX*)gidxmem;

	/* PostgreSQL 8.4 and later require the RECHECK flag to be set here,
	   rather than being supplied as part of the operator class definition */
	bool *recheck = (bool *) PG_GETARG_POINTER(4);

	/* We set recheck to false to avoid repeatedly pulling every "possibly matched" geometry
	   out during index scans. For cases when the geometries are large, rechecking
	   can make things twice as slow. */
	*recheck = false;

	POSTGIS_DEBUG(4, "[GIST] 'consistent' function called");

	/* Quick sanity check on query argument. */
	if ( DatumGetPointer(PG_GETARG_DATUM(1)) == NULL )
	{
		POSTGIS_DEBUG(4, "[GIST] null query pointer (!?!), returning false");
		PG_RETURN_BOOL(FALSE); /* NULL query! This is screwy! */
	}

	/* Quick sanity check on entry key. */
	if ( DatumGetPointer(entry->key) == NULL )
	{
		POSTGIS_DEBUG(4, "[GIST] null index entry, returning false");
		PG_RETURN_BOOL(FALSE); /* NULL entry! */
	}

	/* Null box should never make this far. */
	if ( gserialized_datum_get_gidx_p(PG_GETARG_DATUM(1), query_gbox_index) == LW_FAILURE )
	{
		POSTGIS_DEBUG(4, "[GIST] null query_gbox_index!");
		PG_RETURN_BOOL(FALSE);
	}

	/* Treat leaf node tests different from internal nodes */
	if (GIST_LEAF(entry))
	{
		result = gserialized_gist_consistent_leaf(
		             (GIDX*)DatumGetPointer(entry->key),
		             query_gbox_index, strategy);
	}
	else
	{
		result = gserialized_gist_consistent_internal(
		             (GIDX*)DatumGetPointer(entry->key),
		             query_gbox_index, strategy);
	}

	PG_RETURN_BOOL(result);
}


/*
** GiST support function. Calculate the "penalty" cost of adding this entry into an existing entry.
** Calculate the change in volume of the old entry once the new entry is added.
** TODO: Re-evaluate this in light of R*Tree penalty approaches.
*/
PG_FUNCTION_INFO_V1(gserialized_gist_penalty);
Datum gserialized_gist_penalty(PG_FUNCTION_ARGS)
{
	GISTENTRY *origentry = (GISTENTRY*) PG_GETARG_POINTER(0);
	GISTENTRY *newentry = (GISTENTRY*) PG_GETARG_POINTER(1);
	float *result = (float*) PG_GETARG_POINTER(2);
	GIDX *gbox_index_orig, *gbox_index_new;
	float size_union, size_orig;

	POSTGIS_DEBUG(4, "[GIST] 'penalty' function called");

	gbox_index_orig = (GIDX*)DatumGetPointer(origentry->key);
	gbox_index_new = (GIDX*)DatumGetPointer(newentry->key);

	/* Drop out if we're dealing with null inputs. Shouldn't happen. */
	if ( (gbox_index_orig == NULL) && (gbox_index_new == NULL) )
	{
		POSTGIS_DEBUG(4, "[GIST] both inputs NULL! returning penalty of zero");
		*result = 0.0;
		PG_RETURN_FLOAT8(*result);
	}

	/* Calculate the size difference of the boxes (volume difference in this case). */
	size_union = gidx_union_volume(gbox_index_orig, gbox_index_new);
	size_orig = gidx_volume(gbox_index_orig);
	*result = size_union - size_orig;

	/* All things being equal, we prefer to expand small boxes rather than large boxes.
	   NOTE: This code seemed to be causing badly balanced trees to be built
	   and therefore has been commented out. Not sure why it didn't work,
	   but it didn't.
	if( FP_IS_ZERO(*result) )
		if( FP_IS_ZERO(size_orig) )
			*result = 0.0;
		else
			*result = 1.0 - (1.0/(1.0 + size_orig));
	else
		*result += 1.0;
	*/

	POSTGIS_DEBUGF(4, "[GIST] union size (%.12f), original size (%.12f), penalty (%.12f)", size_union, size_orig, *result);

	PG_RETURN_POINTER(result);
}

/*
** GiST support function. Merge all the boxes in a page into one master box.
*/
PG_FUNCTION_INFO_V1(gserialized_gist_union);
Datum gserialized_gist_union(PG_FUNCTION_ARGS)
{
	GistEntryVector	*entryvec = (GistEntryVector *) PG_GETARG_POINTER(0);
	int *sizep = (int *) PG_GETARG_POINTER(1); /* Size of the return value */
	int	numranges, i;
	GIDX *box_cur, *box_union;

	POSTGIS_DEBUG(4, "[GIST] 'union' function called");

	numranges = entryvec->n;

	box_cur = (GIDX*) DatumGetPointer(entryvec->vector[0].key);

	box_union = gidx_copy(box_cur);

	for ( i = 1; i < numranges; i++ )
	{
		box_cur = (GIDX*) DatumGetPointer(entryvec->vector[i].key);
		gidx_merge(&box_union, box_cur);
	}

	*sizep = VARSIZE(box_union);

	POSTGIS_DEBUGF(4, "[GIST] union called, numranges(%i), pageunion %s", numranges, gidx_to_string(box_union));

	PG_RETURN_POINTER(box_union);

}

/*
** GiST support function. Test equality of keys.
*/
PG_FUNCTION_INFO_V1(gserialized_gist_same);
Datum gserialized_gist_same(PG_FUNCTION_ARGS)
{
	GIDX *b1 = (GIDX*)PG_GETARG_POINTER(0);
	GIDX *b2 = (GIDX*)PG_GETARG_POINTER(1);
	bool *result = (bool*)PG_GETARG_POINTER(2);

	POSTGIS_DEBUG(4, "[GIST] 'same' function called");

	*result = gidx_equals(b1, b2);

	PG_RETURN_POINTER(result);
}

/*
** GiST support function.
** Take in a query and an entry and return the "distance" between them.
**
** Given an index entry p and a query value q, this function determines the
** index entry's "distance" from the query value. This function must be
** supplied if the operator class contains any ordering operators. A query
** using the ordering operator will be implemented by returning index entries
** with the smallest "distance" values first, so the results must be consistent
** with the operator's semantics. For a leaf index entry the result just
** represents the distance to the index entry; for an internal tree node, the
** result must be the smallest distance that any child entry could have.
**
** Strategy 13 = centroid-based distance tests
** Strategy 14 = box-based distance tests (not implemented)
*/
PG_FUNCTION_INFO_V1(gserialized_gist_distance);
Datum gserialized_gist_distance(PG_FUNCTION_ARGS)
{
	GISTENTRY *entry = (GISTENTRY*) PG_GETARG_POINTER(0);
	StrategyNumber strategy = (StrategyNumber) PG_GETARG_UINT16(2);
	char query_box_mem[GIDX_MAX_SIZE];
	GIDX *query_box = (GIDX*)query_box_mem;
	GIDX *entry_box;
	double distance;

	POSTGIS_DEBUG(4, "[GIST] 'distance' function called");

	/* We are using '13' as the gist distance-betweeen-centroids strategy number
	 *  and '14' as the gist distance-between-boxes strategy number */
	if ( strategy != 13 && strategy != 14 ) {
		elog(ERROR, "unrecognized strategy number: %d", strategy);
		PG_RETURN_FLOAT8(FLT_MAX);
	}

	/* Null box should never make this far. */
	if ( gserialized_datum_get_gidx_p(PG_GETARG_DATUM(1), query_box) == LW_FAILURE )
	{
		POSTGIS_DEBUG(4, "[GIST] null query_gbox_index!");
		PG_RETURN_FLOAT8(FLT_MAX);
	}

	/* Get the entry box */
	entry_box = (GIDX*)DatumGetPointer(entry->key);

	/* Box-style distance test */
	if ( strategy == 14 )
	{
		distance = gidx_distance(entry_box, query_box);
		PG_RETURN_FLOAT8(distance);
	}

	/* Treat leaf node tests different from internal nodes */
	if (GIST_LEAF(entry))
	{
		/* Calculate distance to leaves */
		distance = (double)gidx_distance_leaf_centroid(entry_box, query_box);
	}
	else
	{
		/* Calculate distance for internal nodes */
		distance = (double)gidx_distance_node_centroid(entry_box, query_box);
	}

	PG_RETURN_FLOAT8(distance);
}


/*
** Utility function to add entries to the axis partition lists in the
** picksplit function.
*/
static void gserialized_gist_picksplit_addlist(OffsetNumber *list, GIDX **box_union, GIDX *box_current, int *pos, int num)
{
	if ( *pos )
		gidx_merge(box_union,  box_current);
	else
		memcpy((void*)(*box_union), (void*)box_current, VARSIZE(box_current));
	list[*pos] = num;
	(*pos)++;
}

/*
** Utility function check whether the number of entries two halves of the
** space constitute a "bad ratio" (poor balance).
*/
static int gserialized_gist_picksplit_badratio(int x, int y)
{
	POSTGIS_DEBUGF(4, "[GIST] checking split ratio (%d, %d)", x, y);
	if ( (y == 0) || (((float)x / (float)y) < LIMIT_RATIO) ||
	     (x == 0) || (((float)y / (float)x) < LIMIT_RATIO) )
		return TRUE;

	return FALSE;
}

static bool gserialized_gist_picksplit_badratios(int *pos, int dims)
{
	int i;
	for ( i = 0; i < dims; i++ )
	{
		if ( gserialized_gist_picksplit_badratio(pos[2*i],pos[2*i+1]) == FALSE )
			return FALSE;
	}
	return TRUE;
}


/*
** Where the picksplit algorithm cannot find any basis for splitting one way
** or another, we simply split the overflowing node in half.
*/
static void gserialized_gist_picksplit_fallback(GistEntryVector *entryvec, GIST_SPLITVEC *v)
{
	OffsetNumber i, maxoff;
	GIDX *unionL = NULL;
	GIDX *unionR = NULL;
	int nbytes;

	POSTGIS_DEBUG(4, "[GIST] in fallback picksplit function");

	maxoff = entryvec->n - 1;

	nbytes = (maxoff + 2) * sizeof(OffsetNumber);
	v->spl_left = (OffsetNumber*) palloc(nbytes);
	v->spl_right = (OffsetNumber*) palloc(nbytes);
	v->spl_nleft = v->spl_nright = 0;

	for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
	{
		GIDX *cur = (GIDX*)DatumGetPointer(entryvec->vector[i].key);

		if (i <= (maxoff - FirstOffsetNumber + 1) / 2)
		{
			v->spl_left[v->spl_nleft] = i;
			if (unionL == NULL)
			{
				unionL = gidx_copy(cur);
			}
			else
			{
				gidx_merge(&unionL, cur);
			}
			v->spl_nleft++;
		}
		else
		{
			v->spl_right[v->spl_nright] = i;
			if (unionR == NULL)
			{
				unionR = gidx_copy(cur);
			}
			else
			{
				gidx_merge(&unionR, cur);
			}
			v->spl_nright++;
		}
	}

	if (v->spl_ldatum_exists)
		gidx_merge(&unionL, (GIDX*)DatumGetPointer(v->spl_ldatum));

	v->spl_ldatum = PointerGetDatum(unionL);

	if (v->spl_rdatum_exists)
		gidx_merge(&unionR, (GIDX*)DatumGetPointer(v->spl_rdatum));

	v->spl_rdatum = PointerGetDatum(unionR);
	v->spl_ldatum_exists = v->spl_rdatum_exists = false;
}



static void gserialized_gist_picksplit_constructsplit(GIST_SPLITVEC *v, OffsetNumber *list1, int nlist1, GIDX **union1, OffsetNumber *list2, int nlist2, GIDX **union2)
{
	bool		firstToLeft = true;

	POSTGIS_DEBUG(4, "[GIST] picksplit in constructsplit function");

	if (v->spl_ldatum_exists || v->spl_rdatum_exists)
	{
		if (v->spl_ldatum_exists && v->spl_rdatum_exists)
		{
			GIDX *LRl = gidx_copy(*union1);
			GIDX *LRr = gidx_copy(*union2);
			GIDX *RLl = gidx_copy(*union2);
			GIDX *RLr = gidx_copy(*union1);
			double sizeLR, sizeRL;

			gidx_merge(&LRl, (GIDX*)DatumGetPointer(v->spl_ldatum));
			gidx_merge(&LRr, (GIDX*)DatumGetPointer(v->spl_rdatum));
			gidx_merge(&RLl, (GIDX*)DatumGetPointer(v->spl_ldatum));
			gidx_merge(&RLr, (GIDX*)DatumGetPointer(v->spl_rdatum));

			sizeLR = gidx_inter_volume(LRl,LRr);
			sizeRL = gidx_inter_volume(RLl,RLr);

			POSTGIS_DEBUGF(4, "[GIST] sizeLR / sizeRL == %.12g / %.12g", sizeLR, sizeRL);

			if (sizeLR > sizeRL)
				firstToLeft = false;

		}
		else
		{
			float p1, p2;
			GISTENTRY oldUnion,	addon;

			gistentryinit(oldUnion, (v->spl_ldatum_exists) ? v->spl_ldatum : v->spl_rdatum,
			              NULL, NULL, InvalidOffsetNumber, FALSE);

			gistentryinit(addon, PointerGetDatum(*union1), NULL, NULL, InvalidOffsetNumber, FALSE);
			DirectFunctionCall3(gserialized_gist_penalty, PointerGetDatum(&oldUnion), PointerGetDatum(&addon), PointerGetDatum(&p1));
			gistentryinit(addon, PointerGetDatum(*union2), NULL, NULL, InvalidOffsetNumber, FALSE);
			DirectFunctionCall3(gserialized_gist_penalty, PointerGetDatum(&oldUnion), PointerGetDatum(&addon), PointerGetDatum(&p2));

			POSTGIS_DEBUGF(4, "[GIST] p1 / p2 == %.12g / %.12g", p1, p2);

			if ((v->spl_ldatum_exists && p1 > p2) || (v->spl_rdatum_exists && p1 < p2))
				firstToLeft = false;
		}
	}

	POSTGIS_DEBUGF(4, "[GIST] firstToLeft == %d", firstToLeft);

	if (firstToLeft)
	{
		v->spl_left = list1;
		v->spl_right = list2;
		v->spl_nleft = nlist1;
		v->spl_nright = nlist2;
		if (v->spl_ldatum_exists)
			gidx_merge(union1, (GIDX*)DatumGetPointer(v->spl_ldatum));
		v->spl_ldatum = PointerGetDatum(*union1);
		if (v->spl_rdatum_exists)
			gidx_merge(union2, (GIDX*)DatumGetPointer(v->spl_rdatum));
		v->spl_rdatum = PointerGetDatum(*union2);
	}
	else
	{
		v->spl_left = list2;
		v->spl_right = list1;
		v->spl_nleft = nlist2;
		v->spl_nright = nlist1;
		if (v->spl_ldatum_exists)
			gidx_merge(union2, (GIDX*)DatumGetPointer(v->spl_ldatum));
		v->spl_ldatum = PointerGetDatum(*union2);
		if (v->spl_rdatum_exists)
			gidx_merge(union1, (GIDX*)DatumGetPointer(v->spl_rdatum));
		v->spl_rdatum = PointerGetDatum(*union1);
	}

	v->spl_ldatum_exists = v->spl_rdatum_exists = false;
}


#define BELOW(d) (2*(d))
#define ABOVE(d) ((2*(d))+1)

/*
** GiST support function. Split an overflowing node into two new nodes.
** Uses linear algorithm from Ang & Tan [2], dividing node extent into
** four quadrants, and splitting along the axis that most evenly distributes
** entries between the new nodes.
** TODO: Re-evaluate this in light of R*Tree picksplit approaches.
*/
PG_FUNCTION_INFO_V1(gserialized_gist_picksplit);
Datum gserialized_gist_picksplit(PG_FUNCTION_ARGS)
{

	GistEntryVector	*entryvec = (GistEntryVector*) PG_GETARG_POINTER(0);

	GIST_SPLITVEC *v = (GIST_SPLITVEC*) PG_GETARG_POINTER(1);
	OffsetNumber i;
	/* One union box for each half of the space. */
	GIDX **box_union;
	/* One offset number list for each half of the space. */
	OffsetNumber **list;
	/* One position index for each half of the space. */
	int *pos;
	GIDX *box_pageunion;
	GIDX *box_current;
	int direction = -1;
	bool all_entries_equal = true;
	OffsetNumber max_offset;
	int nbytes, ndims_pageunion, d;
	int posmin = entryvec->n;

	POSTGIS_DEBUG(4, "[GIST] 'picksplit' function called");

	/*
	** First calculate the bounding box and maximum number of dimensions in this page.
	*/

	max_offset = entryvec->n - 1;
	box_current = (GIDX*) DatumGetPointer(entryvec->vector[FirstOffsetNumber].key);
	box_pageunion = gidx_copy(box_current);

	/* Calculate the containing box (box_pageunion) for the whole page we are going to split. */
	for ( i = OffsetNumberNext(FirstOffsetNumber); i <= max_offset; i = OffsetNumberNext(i) )
	{
		box_current = (GIDX*) DatumGetPointer(entryvec->vector[i].key);

		if ( all_entries_equal == true && ! gidx_equals (box_pageunion, box_current) )
			all_entries_equal = false;

		gidx_merge( &box_pageunion, box_current );
	}

	POSTGIS_DEBUGF(3, "[GIST] box_pageunion: %s", gidx_to_string(box_pageunion));

	/* Every box in the page is the same! So, we split and just put half the boxes in each child. */
	if ( all_entries_equal )
	{
		POSTGIS_DEBUG(4, "[GIST] picksplit finds all entries equal!");
		gserialized_gist_picksplit_fallback(entryvec, v);
		PG_RETURN_POINTER(v);
	}

	/* Initialize memory structures. */
	nbytes = (max_offset + 2) * sizeof(OffsetNumber);
	ndims_pageunion = GIDX_NDIMS(box_pageunion);
	POSTGIS_DEBUGF(4, "[GIST] ndims_pageunion == %d", ndims_pageunion);
	pos = palloc(2*ndims_pageunion * sizeof(int));
	list = palloc(2*ndims_pageunion * sizeof(OffsetNumber*));
	box_union = palloc(2*ndims_pageunion * sizeof(GIDX*));
	for ( d = 0; d < ndims_pageunion; d++ )
	{
		list[BELOW(d)] = (OffsetNumber*) palloc(nbytes);
		list[ABOVE(d)] = (OffsetNumber*) palloc(nbytes);
		box_union[BELOW(d)] = gidx_new(ndims_pageunion);
		box_union[ABOVE(d)] = gidx_new(ndims_pageunion);
		pos[BELOW(d)] = 0;
		pos[ABOVE(d)] = 0;
	}

	/*
	** Assign each entry in the node to the volume partitions it belongs to,
	** such as "above the x/y plane, left of the y/z plane, below the x/z plane".
	** Each entry thereby ends up in three of the six partitions.
	*/
	POSTGIS_DEBUG(4, "[GIST] 'picksplit' calculating best split axis");
	for ( i = FirstOffsetNumber; i <= max_offset; i = OffsetNumberNext(i) )
	{
		box_current = (GIDX*) DatumGetPointer(entryvec->vector[i].key);

		for ( d = 0; d < ndims_pageunion; d++ )
		{
			if ( GIDX_GET_MIN(box_current,d)-GIDX_GET_MIN(box_pageunion,d) < GIDX_GET_MAX(box_pageunion,d)-GIDX_GET_MAX(box_current,d) )
			{
				gserialized_gist_picksplit_addlist(list[BELOW(d)], &(box_union[BELOW(d)]), box_current, &(pos[BELOW(d)]), i);
			}
			else
			{
				gserialized_gist_picksplit_addlist(list[ABOVE(d)], &(box_union[ABOVE(d)]), box_current, &(pos[ABOVE(d)]), i);
			}

		}

	}

	/*
	** "Bad disposition", too many entries fell into one octant of the space, so no matter which
	** plane we choose to split on, we're going to end up with a mostly full node. Where the
	** data is pretty homogeneous (lots of duplicates) entries that are equidistant from the
	** sides of the page union box can occasionally all end up in one place, leading
	** to this condition.
	*/
	if ( gserialized_gist_picksplit_badratios(pos,ndims_pageunion) == TRUE )
	{
		/*
		** Instead we split on center points and see if we do better.
		** First calculate the average center point for each axis.
		*/
		double *avgCenter = palloc(ndims_pageunion * sizeof(double));

		for ( d = 0; d < ndims_pageunion; d++ )
		{
			avgCenter[d] = 0.0;
		}

		POSTGIS_DEBUG(4, "[GIST] picksplit can't find good split axis, trying center point method");

		for ( i = FirstOffsetNumber; i <= max_offset; i = OffsetNumberNext(i) )
		{
			box_current = (GIDX*) DatumGetPointer(entryvec->vector[i].key);
			for ( d = 0; d < ndims_pageunion; d++ )
			{
				avgCenter[d] += (GIDX_GET_MAX(box_current,d) + GIDX_GET_MIN(box_current,d)) / 2.0;
			}
		}
		for ( d = 0; d < ndims_pageunion; d++ )
		{
			avgCenter[d] /= max_offset;
			pos[BELOW(d)] = pos[ABOVE(d)] = 0; /* Re-initialize our counters. */
			POSTGIS_DEBUGF(4, "[GIST] picksplit average center point[%d] = %.12g", d, avgCenter[d]);
		}

		/* For each of our entries... */
		for ( i = FirstOffsetNumber; i <= max_offset; i = OffsetNumberNext(i) )
		{
			double center;
			box_current = (GIDX*) DatumGetPointer(entryvec->vector[i].key);

			for ( d = 0; d < ndims_pageunion; d++ )
			{
				center = (GIDX_GET_MIN(box_current,d)+GIDX_GET_MAX(box_current,d))/2.0;
				if ( center < avgCenter[d] )
					gserialized_gist_picksplit_addlist(list[BELOW(d)], &(box_union[BELOW(d)]), box_current, &(pos[BELOW(d)]), i);
				else if ( FPeq(center, avgCenter[d]) )
					if ( pos[BELOW(d)] > pos[ABOVE(d)] )
						gserialized_gist_picksplit_addlist(list[ABOVE(d)], &(box_union[ABOVE(d)]), box_current, &(pos[ABOVE(d)]), i);
					else
						gserialized_gist_picksplit_addlist(list[BELOW(d)], &(box_union[BELOW(d)]), box_current, &(pos[BELOW(d)]), i);
				else
					gserialized_gist_picksplit_addlist(list[ABOVE(d)], &(box_union[ABOVE(d)]), box_current, &(pos[ABOVE(d)]), i);
			}

		}

		/* Do we have a good disposition now? If not, screw it, just cut the node in half. */
		if ( gserialized_gist_picksplit_badratios(pos,ndims_pageunion) == TRUE )
		{
			POSTGIS_DEBUG(4, "[GIST] picksplit still cannot find a good split! just cutting the node in half");
			gserialized_gist_picksplit_fallback(entryvec, v);
			PG_RETURN_POINTER(v);
		}

	}

	/*
	** Now, what splitting plane gives us the most even ratio of
	** entries in our child pages? Since each split region has been apportioned entries
	** against the same number of total entries, the axis that has the smallest maximum
	** number of entries in its regions is the most evenly distributed.
	** TODO: what if the distributions are equal in two or more axes?
	*/
	for ( d = 0; d < ndims_pageunion; d++ )
	{
		int posd = Max(pos[ABOVE(d)],pos[BELOW(d)]);
		if ( posd < posmin )
		{
			direction = d;
			posmin = posd;
		}
	}
	if ( direction == -1 || posmin == entryvec->n )
	{
		/* ERROR OUT HERE */
		elog(ERROR, "Error in building split, unable to determine split direction.");
	}

	POSTGIS_DEBUGF(3, "[GIST] 'picksplit' splitting on axis %d", direction);

	gserialized_gist_picksplit_constructsplit(v, list[BELOW(direction)],
	                                        pos[BELOW(direction)],
	                                        &(box_union[BELOW(direction)]),
	                                        list[ABOVE(direction)],
	                                        pos[ABOVE(direction)],
	                                        &(box_union[ABOVE(direction)]) );

	POSTGIS_DEBUGF(4, "[GIST] spl_ldatum: %s", gidx_to_string((GIDX*)v->spl_ldatum));
	POSTGIS_DEBUGF(4, "[GIST] spl_rdatum: %s", gidx_to_string((GIDX*)v->spl_rdatum));

	POSTGIS_DEBUGF(4, "[GIST] axis %d: parent range (%.12g, %.12g) left range (%.12g, %.12g), right range (%.12g, %.12g)",
	               direction,
	               GIDX_GET_MIN(box_pageunion, direction), GIDX_GET_MAX(box_pageunion, direction),
	               GIDX_GET_MIN((GIDX*)v->spl_ldatum, direction), GIDX_GET_MAX((GIDX*)v->spl_ldatum, direction),
	               GIDX_GET_MIN((GIDX*)v->spl_rdatum, direction), GIDX_GET_MAX((GIDX*)v->spl_rdatum, direction) );

	PG_RETURN_POINTER(v);

}

/*
** The GIDX key must be defined as a PostgreSQL type, even though it is only
** ever used internally. These no-op stubs are used to bind the type.
*/
PG_FUNCTION_INFO_V1(gidx_in); 
Datum gidx_in(PG_FUNCTION_ARGS)
{
	ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
	               errmsg("function gidx_in not implemented")));
	PG_RETURN_POINTER(NULL);
}

PG_FUNCTION_INFO_V1(gidx_out); 
Datum gidx_out(PG_FUNCTION_ARGS)
{
  GIDX *box = (GIDX *) PG_GETARG_POINTER(0);
  char *result = gidx_to_string(box);
  PG_RETURN_CSTRING(result);
}
