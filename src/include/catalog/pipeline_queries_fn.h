/*-------------------------------------------------------------------------
 *
 * pipeline_queries_fn.h
 *	 prototypes for functions in catalog/pipeline_queries.c
 *
 *
 * src/include/catalog/pipeline_queries_fn.h
 *
 *-------------------------------------------------------------------------
 */

#include "nodes/primnodes.h"

void AddQuery(const char *name, const char *query, char state);
char *GetQueryString(RangeVar *name, int *cqid, bool selectonly);
void SetQueryState(RangeVar *name, char state);
