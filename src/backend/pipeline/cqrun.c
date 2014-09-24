/*-------------------------------------------------------------------------
 *
 * cqrun.c
 * 		Support for running process groups for continuous queries
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/cqrun.c
 *
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pipeline_queries.h"
#include "catalog/pipeline_queries_fn.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "pipeline/cqrun.h"
#include "pipeline/decode.h"
#include "postmaster/bgworker.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/portal.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/tqual.h"


/* Used to pass arguments to background workers spawned by the postmaster */
typedef struct RunCQArgs
{
	NameData name;
	NameData dbname;
	CQProcessType ptype;
	ContinuousViewState state;
} RunCQArgs;

static PlannedStmt*
get_plan(char *cvname, const char *sql, ContinuousViewState *state)
{
	List *parsetree_list;
	List *querytree_list;
	List *plantree_list;
	SelectStmt *selectparse;
	Query *query;
	PlannedStmt* plan;

	parsetree_list = pg_parse_query(sql);
	Assert(list_length(parsetree_list) == 1);

	selectparse = (SelectStmt *) linitial(parsetree_list);
	selectparse->forContinuousView = true;

	querytree_list = pg_analyze_and_rewrite((Node *) selectparse, sql, NULL, 0);
	Assert(list_length(querytree_list) == 1);

	query = linitial(querytree_list);
	query->is_continuous = true;
	query->cq_target = makeRangeVar(NULL, cvname, -1);
	query->cq_state = state;
	GetContinousViewState(query->cq_target, query->cq_state);

	plantree_list = pg_plan_queries(querytree_list, 0, NULL);
	Assert(list_length(plantree_list) == 1);
	plan = (PlannedStmt *) linitial(plantree_list);

	plan->cq_state = palloc(sizeof(ContinuousViewState));
	memcpy(plan->cq_state, query->cq_state, sizeof(ContinuousViewState));

	return plan;
}

/*
 * Run CQ combiner or worker in a background process with the postmaster as its parent
 */
static void
run_cq(Datum d, char *additional, Size additionalsize)
{
	MemoryContext oldcontext;
	CommandDest dest = DestRemote;
	DestReceiver *receiver;
	Portal portal;
	RunCQArgs args;
	int16 format;
	PlannedStmt *plan;
	ContinuousViewState state;
	char *sql;
	char completionTag[COMPLETION_TAG_BUFSIZE];
	char *cvname;

	MemoryContext planctxt = AllocSetContextCreate(TopMemoryContext,
																 "RunCQPlanContext",
																 ALLOCSET_DEFAULT_MINSIZE,
																 ALLOCSET_DEFAULT_INITSIZE,
																 ALLOCSET_DEFAULT_MAXSIZE);

	memcpy(&args, additional, additionalsize);

//	elog(LOG, "%d", MyProcPid);
//	pg_usleep(6*1000*1000);
	/*
	 * 0. Give this process access to the database
	 */
	BackgroundWorkerInitializeConnection(NameStr(args.dbname), NULL);
	InitDecoderCache();

	StartTransactionCommand();

	oldcontext = MemoryContextSwitchTo(planctxt);

	/*
	 * 1. Plan the continuous query
	 */
	cvname = NameStr(args.name);
	sql = GetQueryString(cvname, true);
	plan = get_plan(cvname, sql, &state);
	plan->cq_state->ptype = args.ptype;

	/*
	 * 2. Set up the portal to run it in
	 */
	portal = CreatePortal(cvname, true, true);
	portal->visible = false;
	PortalDefineQuery(portal,
					  NULL,
					  sql,
					  "SELECT",
					  list_make1(plan),
					  NULL);

	receiver = CreateDestReceiver(dest);
	format = 0;
	PortalStart(portal, NULL, 0, InvalidSnapshot);
	PortalSetResultFormat(portal, 1, &format);

	MemoryContextSwitchTo(oldcontext);

	/*
	 * 3. Run the continuous query until it's deactivated
	 */
	(void) PortalRun(portal,
					 FETCH_ALL,
					 true,
					 receiver,
					 receiver,
					 completionTag);

	CommitTransactionCommand();
}


int
RunContinuousQueryProcess(CQProcessType ptype, const char *cvname, ContinuousViewState state)
{
	BackgroundWorker worker;
	RunCQArgs args;

	memcpy(worker.bgw_name, cvname, strlen(cvname));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main = run_cq;
	worker.bgw_notify_pid = MyProcPid;
	worker.bgw_let_crash = true;
	worker.bgw_additional_size = sizeof(RunCQArgs);

	args.state = state;
	args.ptype = ptype;
	namestrcpy(&args.name, cvname);
	namestrcpy(&args.dbname, MyProcPort->database_name);

	memcpy(worker.bgw_additional_arg, &args, sizeof(RunCQArgs));

	RegisterDynamicBackgroundWorker(&worker, NULL);

	// wait to start
	// pg_usleep(10*1000)

	return 0;
}
