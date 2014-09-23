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

#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"

typedef struct ContinuousViewState
{
	char		state;
	int8 		batchsize;
	int32 		maxwaitms;
	int32		emptysleepms;
	int16		parallelism;

} ContinuousViewState;

void RegisterContinuousView(RangeVar *name, const char *query_string);
void DeregisterContinuousView(RangeVar *name);
bool MarkContinuousViewAsActive(RangeVar *name, List *parameters, ContinuousViewState *cv_state);
bool MarkContinuousViewAsDeactivated(RangeVar *name);
void GetContinousViewState(RangeVar *name, ContinuousViewState *cv_state);
bool IsContinuousViewActive(RangeVar *name);
char *GetQueryString(RangeVar *name, int *cqid, bool selectonly);
bool IsContinuousViewActive(RangeVar *name);
