/*-------------------------------------------------------------------------
 *
 * config.c
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include <sys/stat.h>

#include "postgres.h"

#include "access/xact.h"
#include "analyzer.h"
#include "catalog.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_database.h"
#include "catalog/pg_namespace.h"
#include "commands.h"
#include "commands/extension.h"
#include "config.h"
#include "fmgr.h"
#include "pzmq.h"
#include "matrel.h"
#include "miscadmin.h"
#include "pipeline_query.h"
#include "pipeline_stream.h"
#include "planner.h"
#include "postmaster/bgworker.h"
#include "port.h"
#include "reaper.h"
#include "stats.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "stream_fdw.h"
#include "update.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"

PG_MODULE_MAGIC;

static shmem_startup_hook_type save_shmem_startup_hook;

static const struct config_enum_entry stream_insert_options[] = {
	{"async", STREAM_INSERT_ASYNCHRONOUS, false},
	{"sync_receive", STREAM_INSERT_SYNCHRONOUS_RECEIVE, false},
	{"sync_commit", STREAM_INSERT_SYNCHRONOUS_COMMIT, false},
	{NULL, 0, false}
};

char *pipeline_version_str = "unknown";
char *pipeline_revision_str = "unknown";

/*
 * pipeline_shmem_startup
 */
static void
pipeline_shmem_startup(void)
{
	if (save_shmem_startup_hook)
		save_shmem_startup_hook();

	RequestAddinShmemSpace(ContQuerySchedulerShmemSize());
	RequestAddinShmemSpace(MicrobatchAckShmemSize());
	RequestAddinShmemSpace(StatsShmemSize());

	ContQuerySchedulerShmemInit();
	MicrobatchAckShmemInit();
	StatsShmemInit();
}

/*
 * mkdir_p
 *
 * mkdir -p ...
 */
static int
mkdir_p(char *path, int omode)
{
	struct stat sb;
	mode_t		numask,
				oumask;
	int			last,
				retval;
	char	   *p;

	retval = 0;
	p = path;

#ifdef WIN32
	/* skip network and drive specifiers for win32 */
	if (strlen(p) >= 2)
	{
		if (p[0] == '/' && p[1] == '/')
		{
			/* network drive */
			p = strstr(p + 2, "/");
			if (p == NULL)
			{
				errno = EINVAL;
				return -1;
			}
		}
		else if (p[1] == ':' &&
				 ((p[0] >= 'a' && p[0] <= 'z') ||
				  (p[0] >= 'A' && p[0] <= 'Z')))
		{
			/* local drive */
			p += 2;
		}
	}
#endif

	/*
	 * POSIX 1003.2: For each dir operand that does not name an existing
	 * directory, effects equivalent to those caused by the following command
	 * shall occur:
	 *
	 * mkdir -p -m $(umask -S),u+wx $(dirname dir) && mkdir [-m mode] dir
	 *
	 * We change the user's umask and then restore it, instead of doing
	 * chmod's.  Note we assume umask() can't change errno.
	 */
	oumask = umask(0);
	numask = oumask & ~(S_IWUSR | S_IXUSR);
	(void) umask(numask);

	if (p[0] == '/')			/* Skip leading '/'. */
		++p;
	for (last = 0; !last; ++p)
	{
		if (p[0] == '\0')
			last = 1;
		else if (p[0] != '/')
			continue;
		*p = '\0';
		if (!last && p[1] == '\0')
			last = 1;

		if (last)
			(void) umask(oumask);

		/* check for pre-existing directory */
		if (stat(path, &sb) == 0)
		{
			if (!S_ISDIR(sb.st_mode))
			{
				if (last)
					errno = EEXIST;
				else
					errno = ENOTDIR;
				retval = -1;
				break;
			}
		}
		else if (mkdir(path, last ? omode : S_IRWXU | S_IRWXG | S_IRWXO) < 0)
		{
			retval = -1;
			break;
		}
		if (!last)
			*p = '/';
	}

	/* ensure we restored umask */
	(void) umask(oumask);

	return retval;
}

/*
 * IsCreatePipelineDBCommand
 */
bool
IsCreatePipelineDBCommand(CreateExtensionStmt *ext)
{
	return !pg_strcasecmp(ext->extname, PIPELINEDB_EXTENSION_NAME);
}

/*
 * IsDropPipelineDBCommand
 */
bool
IsDropPipelineDBCommand(DropStmt *stmt)
{
	ListCell *lc;

	if (stmt->removeType != OBJECT_EXTENSION)
		return false;

	foreach(lc, stmt->objects)
	{
		Node *n = (Node *) lfirst(lc);
		if (IsA(n, String) && !pg_strcasecmp(strVal(n), PIPELINEDB_EXTENSION_NAME))
			return true;
	}

	return false;
}

/*
 * CreatingPipelineDB
 */
bool
CreatingPipelineDB(void)
{
	if (!creating_extension)
		return false;

	if (!pg_strcasecmp(get_extension_name(CurrentExtensionObject), PIPELINEDB_EXTENSION_NAME))
		return true;
	return false;
}

/*
 * PipelineDBExists
 */
bool
PipelineDBExists(void)
{
	if (!IsTransactionState())
		return false;
	return OidIsValid(get_extension_oid(PIPELINEDB_EXTENSION_NAME, true));
}

/*
 * create_ipc_directory
 */
static void
create_ipc_directory(void)
{
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfo(&buf, "%s/pipeline/zmq", DataDir);

	if (mkdir_p(buf.data, S_IRWXU) != 0 && errno != EEXIST)
		elog(PANIC, "failed to check existence of IPC directory");

	pfree(buf.data);
}

/*
 * splash
 */
static void
splash(void)
{
	printf("    ____  _            ___            ____  ____\n");
	printf("   / __ \\(_)___  ___  / (_)___  ___  / __ \\/ __ )\n");
	printf("  / /_/ / / __ \\/ _ \\/ / / __ \\/ _ \\/ / / / __  |\n");
	printf(" / ____/ / /_/ /  __/ / / / / /  __/ /_/ / /_/ /\n");
	printf("/_/   /_/ .___/\\___/_/_/_/ /_/\\___/_____/_____/\n");
	printf("       /_/\n\n");
}

/*
 * _PG_init
 */
void
_PG_init(void)
{
	BackgroundWorker worker;

#define BACKTRACE_SEGFAULTS
#ifdef BACKTRACE_SEGFAULTS
	pqsignal(SIGSEGV, debug_segfault);
#endif

	if (!process_shared_preload_libraries_in_progress)
	{
		ereport(WARNING, (errmsg("pipelinedb must be loaded via shared_preload_libraries")));
		return;
	}

#if (PG_VERSION_NUM >= 100001 && PG_VERSION_NUM < 110000)
	if (PG_VERSION_NUM < 100001 || PG_VERSION_NUM >= 110000)
		ereport(ERROR, (errmsg("pipelinedb-postgresql-10 requires postgresql version 10.1 or higher")));
#elif (PG_VERSION_NUM >= 110000)
	if (PG_VERSION_NUM < 110000)
		ereport(ERROR, (errmsg("pipelinedb-postgresql-11 requires postgresql version 11.0 or higher")));
#endif

#ifdef PIPELINE_VERSION_STR
	pipeline_version_str = PIPELINE_VERSION_STR;
#endif
#ifdef PIPELINE_REVISION_STR
	pipeline_revision_str = PIPELINE_REVISION_STR;
#endif

	DefineCustomIntVariable("pipelinedb.num_workers",
			gettext_noop("Sets the number of parallel continuous query worker processes to use for each database."),
			NULL,
			&num_workers,
			1, 1, 1024,
			PGC_POSTMASTER, 0,
			NULL, NULL, NULL);

	DefineCustomIntVariable("pipelinedb.num_combiners",
			gettext_noop("Sets the number of parallel continuous query combiner processes to use for each database."),
			NULL,
			&num_combiners,
			1, 1, 1024,
			PGC_POSTMASTER, 0,
			NULL, NULL, NULL);

	DefineCustomIntVariable("pipelinedb.num_queues",
			gettext_noop("Sets the number of parallel continuous query IPC queues."),
			NULL,
			&num_queues,
			1, 1, 1024,
			PGC_POSTMASTER, 0,
			NULL, NULL, NULL);

	DefineCustomIntVariable("pipelinedb.num_reapers",
			gettext_noop("Sets the number of parallel continuous query reaper processes."),
			NULL,
			&num_reapers,
			1, 1, 1024,
			PGC_POSTMASTER, 0,
			NULL, NULL, NULL);

	DefineCustomEnumVariable("pipelinedb.stream_insert_level",
			gettext_noop("Sets the current transaction's stream write synchronization level."),
			NULL,
			&stream_insert_level,
			STREAM_INSERT_SYNCHRONOUS_RECEIVE,
			stream_insert_options,
			PGC_USERSET, 0,
			NULL, NULL, NULL);

	DefineCustomRealVariable("pipelinedb.sliding_window_step_factor",
			gettext_noop("Sets the default step size for a sliding window query as a percentage of the window size."),
			gettext_noop("A higher number will improve performance but tradeoff refresh interval."),
			&sliding_window_step_factor,
			5.0, 0.0, 50.0,
			PGC_POSTMASTER, 0,
			NULL, NULL, NULL);

	DefineCustomBoolVariable("pipelinedb.continuous_queries_enabled",
		   gettext_noop("If true, continuous queries should be be enabled upon creation."),
		   NULL,
		   &continuous_queries_enabled,
		   true,
			 PGC_POSTMASTER, 0,
		   NULL, NULL, NULL);

	DefineCustomStringVariable("pipelinedb.stream_targets",
			gettext_noop("List of continuous views that will be affected when inserting into an stream."),
			NULL,
			&stream_targets,
			NULL,
			PGC_USERSET, 0,
			NULL, NULL, NULL);

	DefineCustomIntVariable("pipelinedb.batch_mem",
			gettext_noop("Sets the maximum size of the batch of events to accumulate before executing a continuous query plan on them."),
			gettext_noop("A higher value usually yields less frequent continuous view updates."),
			&continuous_query_batch_mem,
			262144, 8192, MAX_KILOBYTES,
			PGC_POSTMASTER, GUC_UNIT_KB,
			NULL, NULL, NULL);

	DefineCustomIntVariable("pipelinedb.batch_size",
			gettext_noop("Sets the maximum size of the batch of events to accumulate before executing a continuous query plan on them."),
			gettext_noop("A higher value usually yields less frequent continuous view updates."),
			&continuous_query_batch_size,
			10000, 10, INT_MAX,
			PGC_POSTMASTER, 0,
			NULL, NULL, NULL);

	DefineCustomIntVariable("pipelinedb.commit_interval",
			gettext_noop("Sets the number of milliseconds that combiners will keep combining in memory before committing the result."),
			gettext_noop("A longer commit interval will increase performance at the expense of less frequent continuous view updates and more potential data loss."),
			&continuous_query_commit_interval,
			50, 0, 60000,
			PGC_POSTMASTER, GUC_UNIT_MS,
			NULL, NULL, NULL);

	DefineCustomIntVariable("pipelinedb.ipc_hwm",
			gettext_noop("Sets the high watermark for IPC between worker and combiner processes."),
			gettext_noop("A value will queue more IPC messages in memory."),
			&continuous_query_ipc_hwm,
			10, 1, INT_MAX,
			PGC_POSTMASTER, 0,
			NULL, NULL, NULL);

	DefineCustomIntVariable("pipelinedb.max_wait",
			gettext_noop("Sets the time a continuous query process will wait for a batch to accumulate."),
			gettext_noop("A higher value usually yields less frequent continuous view updates, but adversely affects latency."),
			&continuous_query_max_wait,
			50, 1, 60000,
			PGC_POSTMASTER, GUC_UNIT_MS,
			NULL, NULL, NULL);

	DefineCustomIntVariable("pipelinedb.ttl_expiration_batch_size",
			gettext_noop("Sets the maximum number of TTL-expired rows to delete at a time."),
			NULL,
			&ttl_expiration_batch_size,
			10000, 0, INT_MAX,
			PGC_POSTMASTER, 0,
			NULL, NULL, NULL);

	DefineCustomIntVariable("pipelinedb.ttl_expiration_threshold",
			gettext_noop("Sets the percentage of a relation's TTL that must have elapsed before attempting to expire rows again."),
			NULL,
			&ttl_expiration_threshold,
			5, 0, 100,
			PGC_POSTMASTER, 0,
			NULL, NULL, NULL);

	DefineCustomIntVariable("pipelinedb.queue_mem",
			gettext_noop("Sets the maximum amount of memory each queue process will use."),
			NULL,
			&continuous_query_queue_mem,
			256 * 1024, 8192, MAX_KILOBYTES,
			PGC_POSTMASTER, GUC_UNIT_KB,
			NULL, NULL, NULL);

	DefineCustomIntVariable("pipelinedb.fillfactor",
			gettext_noop("Sets the default fillfactor to use for continuous views."),
			NULL,
			&continuous_view_fillfactor,
			50, 1, 100,
			PGC_POSTMASTER, 0,
			NULL, NULL, NULL);

	DefineCustomIntVariable("pipelinedb.combiner_work_mem",
			gettext_noop("Sets the maximum memory to be used for combining partial results for continuous queries."),
			NULL,
			&continuous_query_combiner_work_mem,
			262144, 16384, MAX_KILOBYTES,
			PGC_POSTMASTER, GUC_UNIT_KB,
			NULL, NULL, NULL);

	DefineCustomBoolVariable("pipelinedb.anonymous_update_checks",
			gettext_noop("Anonymously check for available updates."),
			NULL,
			&anonymous_update_checks,
			true,
			PGC_USERSET, 0,
			NULL, NULL, NULL);

	DefineCustomBoolVariable("pipelinedb.matrels_writable",
		   gettext_noop("If true, allow changes to be made directly to materialization tables."),
		   NULL,
		   &matrels_writable,
		   false,
		   PGC_USERSET, 0,
		   NULL, NULL, NULL);

	splash();

	create_ipc_directory();

	StatsRequestLWLocks();

	save_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pipeline_shmem_startup;

	InstallCommandHooks();
	InstallAnalyzerHooks();
	InstallPlannerHooks();

	MemSet(&worker, 0, sizeof(BackgroundWorker));

	strcpy(worker.bgw_name, "scheduler");
	strcpy(worker.bgw_library_name, PIPELINEDB_EXTENSION_NAME);
	strcpy(worker.bgw_function_name, "ContQuerySchedulerMain");

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_notify_pid = 0;
	worker.bgw_restart_time = 1; /* recover in 1s */
	worker.bgw_main_arg = Int32GetDatum(0);

	RegisterBackgroundWorker(&worker);
}
