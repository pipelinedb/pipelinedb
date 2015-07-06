
/**********************************************************************
**  GIDX structure. 
**
**  This is an n-dimensional (practically, the 
**  implementation is 2-4 dimensions) box used for index keys. The 
**  coordinates are anonymous, so we can have any number of dimensions.
**  The sizeof a GIDX is 1 + 2 * ndims * sizeof(float).
**  The order of values in a GIDX is
**  xmin,xmax,ymin,ymax,zmin,zmax...
*/
typedef struct
{
	int32 varsize;
	float c[1];
} GIDX;

/*
** For some GiST support functions, it is more efficient to allocate
** memory for our GIDX off the stack and cast that memory into a GIDX.
** But, GIDX is variable length, what to do? We'll bake in the assumption
** that no GIDX is more than 4-dimensional for now, and ensure that much
** space is available.
** 4 bytes varsize + 4 dimensions * 2 ordinates * 4 bytes float size = 36 bytes
*/
#define GIDX_MAX_SIZE 36


/**********************************************************************
**  BOX2DF structure. 
**
**  This is a 2-dimensional key for simple cartesian indexes, 
**  with backwards compatible behavior to previous indexes in
**  PostGIS
*/

typedef struct
{
	float xmin, xmax, ymin, ymax;
} BOX2DF;


/*********************************************************************************
** GIDX support functions.
**
** We put the GIDX support here rather than libgeom because it is a specialized 
** type only used for indexing purposes. It also makes use of some PgSQL
** infrastructure like the VARSIZE() macros.
*/

/* allocate a new gidx object on the heap */
GIDX* gidx_new(int ndims) ;

/* Increase the size of a GIDX */
void gidx_expand(GIDX *a, float d);


/* Generate human readable form for GIDX. */
char* gidx_to_string(GIDX *a) ; 

/* typedef to correct array-bounds checking for casts to GIDX - do not
   use this ANYWHERE except in the casts below */
typedef float _gidx_float_array[sizeof(float) * 2 * 4];

/* Returns number of dimensions for this GIDX */
#define GIDX_NDIMS(gidx) ((VARSIZE((gidx)) - VARHDRSZ) / (2 * sizeof(float)))
/* Minimum accessor. */
#define GIDX_GET_MIN(gidx, dimension) (*((_gidx_float_array *)(&(gidx)->c)))[2*(dimension)] 
/* Maximum accessor. */
#define GIDX_GET_MAX(gidx, dimension) (*((_gidx_float_array *)(&(gidx)->c)))[2*(dimension)+1]
/* Minimum setter. */
#define GIDX_SET_MIN(gidx, dimension, value) ((gidx)->c[2*(dimension)] = (value))
/* Maximum setter. */
#define GIDX_SET_MAX(gidx, dimension, value) ((gidx)->c[2*(dimension)+1] = (value))
/* Returns the size required to store a GIDX of requested dimension */
#define GIDX_SIZE(dimensions) (sizeof(int32) + 2*(dimensions)*sizeof(float))

/*********************************************************************************
** GSERIALIZED support functions.
**
** Fast functions for pulling boxes out of serializations.
*/

/* Pull out the #GIDX bounding box with a absolute minimum system overhead */
int gserialized_datum_get_gidx_p(Datum gserialized_datum, GIDX *gidx);

/* Pull out the gidx bounding box from an already de-toasted geography */
int gserialized_get_gidx_p(GSERIALIZED *g, GIDX *gidx);
/* Copy a new bounding box into an existing gserialized */
GSERIALIZED* gserialized_set_gidx(GSERIALIZED *g, GIDX *gidx);

/* Given two datums, do they overlap? Computed very fast using embedded boxes. */
/* int gserialized_datum_overlaps(Datum gs1, Datum gs2); */
/* Remove the box from a disk serialization */
GSERIALIZED* gserialized_drop_gidx(GSERIALIZED *g);



