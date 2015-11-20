/*-------------------------------------------------------------------------
 *
 * assert.c
 *	  Assert code.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2013-2015, PipelineDB
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/error/assert.c
 *
 * NOTE
 *	  This should eventually work with elog()
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include "execinfo.h"
#include "miscadmin.h"
#include "tcop/tcopprot.h"

/*
 * ExceptionalCondition - Handles the failure of an Assert()
 */
void
ExceptionalCondition(const char *conditionName,
					 const char *errorType,
					 const char *fileName,
					 int lineNumber)
{
	void *array[32];
	size_t size;

	if (!PointerIsValid(conditionName)
		|| !PointerIsValid(fileName)
		|| !PointerIsValid(errorType))
		write_stderr("TRAP: ExceptionalCondition: bad arguments\n");
	else
	{
		write_stderr("TRAP: %s(\"%s\", File: \"%s\", Line: %d, PID: %d, Query: %s)\n",
					 errorType, conditionName,
					 fileName, lineNumber, getpid(), debug_query_string);
	}

	/* dump stack trace */
	size = backtrace(array, 32);
	fprintf(stderr, "Assertion failure (PID %d)\n", MyProcPid);
	fprintf(stderr, "version: %s\n", PIPELINE_VERSION_STR);
	fprintf(stderr, "backtrace:\n");
	backtrace_symbols_fd(array, size, STDERR_FILENO);

	/* Usually this shouldn't be needed, but make sure the msg went out */
	fflush(stderr);

#ifdef SLEEP_ON_ASSERT

	/*
	 * It would be nice to use pg_usleep() here, but only does 2000 sec or 33
	 * minutes, which seems too short.
	 */
	sleep(1000000);
#endif

	abort();
}
