/*-------------------------------------------------------------------------
 *
 * bloom.c
 *	  Bloom Filter implementation.
 *
 * src/backend/pipeline/bloom.c
 *
 *-------------------------------------------------------------------------
 */
#include <math.h>
#include "pipeline/gcs.h"
#include "pipeline/miscutils.h"
#include "utils/elog.h"
#include "utils/palloc.h"

#define DEFAULT_P 0.02
#define DEFAULT_N (2 << 17)
