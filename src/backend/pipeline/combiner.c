/*-------------------------------------------------------------------------
 *
 * combiner.c
 *
 *	  Combiner process functionality
 *
 * src/backend/pipeline/combiner.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pipeline/combiner.h"
#include "miscadmin.h"

extern void
ContinuousQueryCombinerRun(QueryDesc *queryDesc, ResourceOwner owner)
{

	elog(LOG, "running \"%s\" combiner with pid %d",
			queryDesc->plannedstmt->cq_target->relname, MyProcPid);
}
