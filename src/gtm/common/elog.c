/*-------------------------------------------------------------------------
 *
 * elog.c
 *	  error logging and reporting
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/utils/error/elog.c,v 1.212 2009/01/19 15:34:23 mha Exp $
 *
 *-------------------------------------------------------------------------
 */
#include <fcntl.h>
#include <time.h>
#include <unistd.h>
#include <signal.h>
#include <ctype.h>
#include "gtm/gtm_c.h"
#include "gtm/gtm.h"
#include "gtm/gtm_msg.h"
#include "gtm/stringinfo.h"
#include "gtm/memutils.h"
#include "gtm/elog.h"
#include "gtm/assert.h"
#include "gtm/gtm_ext.h"
#include "gtm/libpq.h"
#include "gtm/pqformat.h"

#undef _
#define _(x)	x

/*
 * Change this to something which is more appropriate.
 *
 * XXX The GTM should take command like argument to set the log file
 */
char *GTMLogFile = NULL;

/* GUC parameters */
int			Log_destination = LOG_DESTINATION_STDERR;

/* Macro for checking errordata_stack_depth is reasonable */
#define CHECK_STACK_DEPTH() \
	do { \
		if (errordata_stack_depth < 0) \
		{ \
			errordata_stack_depth = -1; \
			ereport(ERROR, (errmsg_internal("errstart was not called"))); \
		} \
	} while (0)


static void send_message_to_server_log(ErrorData *edata);
static void send_message_to_frontend(Port *myport, ErrorData *edata);
static char *expand_fmt_string(const char *fmt, ErrorData *edata);
static const char *useful_strerror(int errnum);
static const char *error_severity(int elevel);
static void append_with_tabs(StringInfo buf, const char *str);
static bool is_log_level_output(int elevel, int log_min_level);

int	log_min_messages = WARNING;
char	   *Log_line_prefix = "%l:%p:%m -";		/* format for extra log line info */

#define FORMATTED_TS_LEN 128
static char formatted_start_time[FORMATTED_TS_LEN];
static char formatted_log_time[FORMATTED_TS_LEN];

static void log_line_prefix(StringInfo buf);
static void setup_formatted_log_time(void);
/*
 * setup formatted_log_time, for consistent times between CSV and regular logs
 */
static void
setup_formatted_log_time(void)
{
	struct timeval tv;
	time_t	stamp_time;
	char		msbuf[8];

	gettimeofday(&tv, NULL);
	stamp_time = (time_t) tv.tv_sec;

	strftime(formatted_log_time, FORMATTED_TS_LEN,
				/* leave room for milliseconds... */
				"%Y-%m-%d %H:%M:%S     %Z",
				localtime(&stamp_time));

	/* 'paste' milliseconds into place... */
	sprintf(msbuf, ".%03d", (int) (tv.tv_usec / 1000));
	strncpy(formatted_log_time + 19, msbuf, 4);
}

/*
 * Format tag info for log lines; append to the provided buffer.
 */
static void
log_line_prefix(StringInfo buf)
{
	/* static counter for line numbers */
	static long log_line_number = 0;

	/* has counter been reset in current process? */
	static int	log_my_pid = 0;

	int			format_len;
	int			i;

	/*
	 * This is one of the few places where we'd rather not inherit a static
	 * variable's value from the postmaster.  But since we will, reset it when
	 * MyProcPid changes. MyStartTime also changes when MyProcPid does, so
	 * reset the formatted start timestamp too.
	 */
	if (log_my_pid != MyThreadID)
	{
		log_line_number = 0;
		log_my_pid = MyThreadID;
		formatted_start_time[0] = '\0';
	}
	log_line_number++;

	if (Log_line_prefix == NULL)
		return;					/* in case guc hasn't run yet */

	format_len = strlen(Log_line_prefix);

	for (i = 0; i < format_len; i++)
	{
		if (Log_line_prefix[i] != '%')
		{
			/* literal char, just copy */
			appendStringInfoChar(buf, Log_line_prefix[i]);
			continue;
		}
		/* go to char after '%' */
		i++;
		if (i >= format_len)
			break;				/* format error - ignore it */

		/* process the option */
		switch (Log_line_prefix[i])
		{
			case 'p':
				appendStringInfo(buf, "%lu", MyThreadID);
				break;
			case 'l':
				appendStringInfo(buf, "%ld", log_line_number);
				break;
			case 'm':
				setup_formatted_log_time();
				appendStringInfoString(buf, formatted_log_time);
				break;
			default:
				/* format error - ignore it */
				break;
		}
	}
}

/*
 * errstart --- begin an error-reporting cycle
 *
 * Create a stack entry and store the given parameters in it.  Subsequently,
 * errmsg() and perhaps other routines will be called to further populate
 * the stack entry.  Finally, errfinish() will be called to actually process
 * the error report.
 *
 * Returns TRUE in normal case.  Returns FALSE to short-circuit the error
 * report (if it's a warning or lower and not to be reported anywhere).
 */
bool
errstart(int elevel, const char *filename, int lineno,
		 const char *funcname, const char *domain)
{
	ErrorData	*edata;
	bool		output_to_server;
	bool		output_to_client = false;
	int			i;

	/*
	 * Check some cases in which we want to promote an error into a more
	 * severe error.  None of this logic applies for non-error messages.
	 */
	if (elevel >= ERROR)
	{
		/*
		 * If we are inside a critical section, all errors become PANIC
		 * errors.	See miscadmin.h.
		 */
		if (CritSectionCount > 0)
			elevel = PANIC;

		/*
		 * Check reasons for treating ERROR as FATAL:
		 *
		 * 1. we have no handler to pass the error to (implies we are in the
		 * postmaster or in backend startup).
		 *
		 * 2. ExitOnAnyError mode switch is set (initdb uses this).
		 *
		 * 3. the error occurred after proc_exit has begun to run.	(It's
		 * proc_exit's responsibility to see that this doesn't turn into
		 * infinite recursion!)
		 */
		if (elevel == ERROR)
		{
			if (PG_exception_stack == NULL)
				elevel = FATAL;
		}

		/*
		 * If the error level is ERROR or more, errfinish is not going to
		 * return to caller; therefore, if there is any stacked error already
		 * in progress it will be lost.  This is more or less okay, except we
		 * do not want to have a FATAL or PANIC error downgraded because the
		 * reporting process was interrupted by a lower-grade error.  So check
		 * the stack and make sure we panic if panic is warranted.
		 */
		for (i = 0; i <= errordata_stack_depth; i++)
			elevel = Max(elevel, errordata[i].elevel);
	}

	output_to_server = is_log_level_output(elevel, log_min_messages);
	output_to_client = (elevel >= ERROR);

	/* Skip processing effort if non-error message will not be output */
	if (elevel < ERROR && !output_to_server && !output_to_client)
		return false;

	/*
	 * Okay, crank up a stack entry to store the info in.
	 */

	if (recursion_depth++ > 0 && elevel >= ERROR)
	{
		/*
		 * Ooops, error during error processing.  Clear ErrorContext as
		 * discussed at top of file.  We will not return to the original
		 * error's reporter or handler, so we don't need it.
		 */
		MemoryContextReset(ErrorContext);
	}

	if (++errordata_stack_depth >= ERRORDATA_STACK_SIZE)
	{
		/*
		 * Wups, stack not big enough.	We treat this as a PANIC condition
		 * because it suggests an infinite loop of errors during error
		 * recovery.
		 */
		errordata_stack_depth = -1;		/* make room on stack */
		ereport(PANIC, (errmsg_internal("ERRORDATA_STACK_SIZE exceeded")));
	}
	/* Initialize data for this error frame */
	edata = &errordata[errordata_stack_depth];
	MemSet(edata, 0, sizeof(ErrorData));
	edata->elevel = elevel;
	edata->output_to_server = output_to_server;
	edata->output_to_client = output_to_client;
	edata->filename = filename;
	edata->lineno = lineno;
	edata->funcname = funcname;
	/* errno is saved here so that error parameter eval can't change it */
	edata->saved_errno = errno;

	recursion_depth--;
	return true;
}

/*
 * errfinish --- end an error-reporting cycle
 *
 * Produce the appropriate error report(s) and pop the error stack.
 *
 * If elevel is ERROR or worse, control does not return to the caller.
 * See elog.h for the error level definitions.
 */
void
errfinish(int dummy,...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	int			elevel = edata->elevel;

	MemoryContext oldcontext;
	recursion_depth++;
	CHECK_STACK_DEPTH();

	/*
	 * Do processing in ErrorContext, which we hope has enough reserved space
	 * to report an error.
	 */
	oldcontext = MemoryContextSwitchTo(ErrorContext);


	/*
	 * If ERROR (not more nor less) we pass it off to the current handler.
	 * Printing it and popping the stack is the responsibility of the handler.
	 */
	if (elevel == ERROR)
	{
		/*
		 * We do some minimal cleanup before longjmp'ing so that handlers can
		 * execute in a reasonably sane state.
		 */
		CritSectionCount = 0;	/* should be unnecessary, but... */

		/*
		 * Note that we leave CurrentMemoryContext set to ErrorContext. The
		 * handler should reset it to something else soon.
		 */

		recursion_depth--;
		PG_RE_THROW();
	}

	/* Emit the message to the right places */
	EmitErrorReport(MyPort);

	/* Now free up subsidiary data attached to stack entry, and release it */
	if (edata->message)
		pfree(edata->message);
	if (edata->detail)
		pfree(edata->detail);
	if (edata->detail_log)
		pfree(edata->detail_log);
	if (edata->hint)
		pfree(edata->hint);
	if (edata->context)
		pfree(edata->context);
	errordata_stack_depth--;

	/* Exit error-handling context */
	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;

	/*
	 * Perform error recovery action as specified by elevel.
	 */
	if (elevel == FATAL)
	{
		/*
		 * fflush here is just to improve the odds that we get to see the
		 * error message, in case things are so hosed that proc_exit crashes.
		 * Any other code you might be tempted to add here should probably be
		 * in an on_proc_exit or on_shmem_exit callback instead.
		 */
		fflush(stdout);
		fflush(stderr);

		/*
		 * Do normal process-exit cleanup, then return exit code 1 to indicate
		 * FATAL termination.  The postmaster may or may not consider this
		 * worthy of panic, depending on which subprocess returns it.
		 */
		if (IsMainThread())
			exit(1);
		else
			pthread_exit(NULL);
	}

	if (elevel >= PANIC)
	{
		fflush(stdout);
		fflush(stderr);
		abort();
	}

	/*
	 * We reach here if elevel <= WARNING. OK to return to caller.
	 */
}

/*
 * This macro handles expansion of a format string and associated parameters;
 * it's common code for errmsg(), errdetail(), etc.  Must be called inside
 * a routine that is declared like "const char *fmt, ..." and has an edata
 * pointer set up.	The message is assigned to edata->targetfield, or
 * appended to it if appendval is true.  The message is subject to translation
 * if translateit is true.
 *
 * Note: we pstrdup the buffer rather than just transferring its storage
 * to the edata field because the buffer might be considerably larger than
 * really necessary.
 */
#define EVALUATE_MESSAGE(targetfield, appendval, translateit)  \
	{ \
		char		   *fmtbuf; \
		StringInfoData	buf; \
		/* Expand %m in format string */ \
		fmtbuf = expand_fmt_string(fmt, edata); \
		initStringInfo(&buf); \
		if ((appendval) && edata->targetfield) \
			appendStringInfo(&buf, "%s\n", edata->targetfield); \
		/* Generate actual output --- have to use appendStringInfoVA */ \
		for (;;) \
		{ \
			va_list		args; \
			bool		success; \
			va_start(args, fmt); \
			success = appendStringInfoVA(&buf, fmtbuf, args); \
			va_end(args); \
			if (success) \
				break; \
			enlargeStringInfo(&buf, buf.maxlen); \
		} \
		/* Done with expanded fmt */ \
		pfree(fmtbuf); \
		/* Save the completed message into the stack item */ \
		if (edata->targetfield) \
			pfree(edata->targetfield); \
		edata->targetfield = pstrdup(buf.data); \
		pfree(buf.data); \
	}


/*
 * errmsg --- add a primary error message text to the current error
 *
 * In addition to the usual %-escapes recognized by printf, "%m" in
 * fmt is replaced by the error message for the caller's value of errno.
 *
 * Note: no newline is needed at the end of the fmt string, since
 * ereport will provide one for the output methods that need it.
 */
int
errmsg(const char *fmt,...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(ErrorContext);

	EVALUATE_MESSAGE(message, false, true);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;
	return 0;					/* return value does not matter */
}


/*
 * errmsg_internal --- add a primary error message text to the current error
 *
 * This is exactly like errmsg() except that strings passed to errmsg_internal
 * are not translated, and are customarily left out of the
 * internationalization message dictionary.  This should be used for "can't
 * happen" cases that are probably not worth spending translation effort on.
 * We also use this for certain cases where we *must* not try to translate
 * the message because the translation would fail and result in infinite
 * error recursion.
 */
int
errmsg_internal(const char *fmt,...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(ErrorContext);

	EVALUATE_MESSAGE(message, false, false);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;
	return 0;					/* return value does not matter */
}


/*
 * errdetail --- add a detail error message text to the current error
 */
int
errdetail(const char *fmt,...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(ErrorContext);

	EVALUATE_MESSAGE(detail, false, true);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;
	return 0;					/* return value does not matter */
}


/*
 * errdetail_log --- add a detail_log error message text to the current error
 */
int
errdetail_log(const char *fmt,...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(ErrorContext);

	EVALUATE_MESSAGE(detail_log, false, true);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;
	return 0;					/* return value does not matter */
}


/*
 * errhint --- add a hint error message text to the current error
 */
int
errhint(const char *fmt,...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(ErrorContext);

	EVALUATE_MESSAGE(hint, false, true);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;
	return 0;					/* return value does not matter */
}



/*
 * errfunction --- add reporting function name to the current error
 *
 * This is used when backwards compatibility demands that the function
 * name appear in messages sent to old-protocol clients.  Note that the
 * passed string is expected to be a non-freeable constant string.
 */
int
errfunction(const char *funcname)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];


	edata->funcname = funcname;
	edata->show_funcname = true;

	return 0;					/* return value does not matter */
}


/*
 * elog_start --- startup for old-style API
 *
 * All that we do here is stash the hidden filename/lineno/funcname
 * arguments into a stack entry.
 *
 * We need this to be separate from elog_finish because there's no other
 * portable way to deal with inserting extra arguments into the elog call.
 * (If macros with variable numbers of arguments were portable, it'd be
 * easy, but they aren't.)
 */
void
elog_start(const char *filename, int lineno, const char *funcname)
{
	ErrorData  *edata;

	if (++errordata_stack_depth >= ERRORDATA_STACK_SIZE)
	{
		/*
		 * Wups, stack not big enough.	We treat this as a PANIC condition
		 * because it suggests an infinite loop of errors during error
		 * recovery.  Note that the message is intentionally not localized,
		 * else failure to convert it to client encoding could cause further
		 * recursion.
		 */
		errordata_stack_depth = -1;		/* make room on stack */
		ereport(PANIC, (errmsg_internal("ERRORDATA_STACK_SIZE exceeded")));
	}

	edata = &errordata[errordata_stack_depth];
	edata->filename = filename;
	edata->lineno = lineno;
	edata->funcname = funcname;
	/* errno is saved now so that error parameter eval can't change it */
	edata->saved_errno = errno;
}

/*
 * elog_finish --- finish up for old-style API
 */
void
elog_finish(int elevel, const char *fmt,...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	CHECK_STACK_DEPTH();

	/*
	 * Do errstart() to see if we actually want to report the message.
	 */
	errordata_stack_depth--;
	errno = edata->saved_errno;
	if (!errstart(elevel, edata->filename, edata->lineno, edata->funcname, NULL))
		return;					/* nothing to do */

	/*
	 * Format error message just like errmsg_internal().
	 */
	recursion_depth++;
	oldcontext = MemoryContextSwitchTo(ErrorContext);

	EVALUATE_MESSAGE(message, false, false);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;

	/*
	 * And let errfinish() finish up.
	 */
	errfinish(0);
}

/*
 * Actual output of the top-of-stack error message
 *
 * In the ereport(ERROR) case this is called from GTM_ThreadMain(or not at all,
 * if the error is caught by somebody).  For all other severity levels this
 * is called by errfinish.
 */
void
EmitErrorReport(void *argp)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	Port *myport= (Port *)argp;
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(ErrorContext);

	/* Send to server log, if enabled */
	if (edata->output_to_server)
		send_message_to_server_log(edata);

	/* Send to client, if enabled */
	if ((edata->output_to_client) && (myport != NULL))
		send_message_to_frontend(myport, edata);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;
}

/*
 * FlushErrorState --- flush the error state after error recovery
 *
 * This should be called by an error handler after it's done processing
 * the error; or as soon as it's done CopyErrorData, if it intends to
 * do stuff that is likely to provoke another error.  You are not "out" of
 * the error subsystem until you have done this.
 */
void
FlushErrorState(void)
{
	/*
	 * Reset stack to empty.  The only case where it would be more than one
	 * deep is if we serviced an error that interrupted construction of
	 * another message.  We assume control escaped out of that message
	 * construction and won't ever go back.
	 */
	errordata_stack_depth = -1;
	recursion_depth = 0;
	/* Delete all data in ErrorContext */
	MemoryContextResetAndDeleteChildren(ErrorContext);
}



/*
 * pg_re_throw --- out-of-line implementation of PG_RE_THROW() macro
 */
void
pg_re_throw(void)
{
	/* If possible, throw the error to the next outer setjmp handler */
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	else
	{
		/*
		 * If we get here, elog(ERROR) was thrown inside a PG_TRY block, which
		 * we have now exited only to discover that there is no outer setjmp
		 * handler to pass the error to.  Had the error been thrown outside
		 * the block to begin with, we'd have promoted the error to FATAL, so
		 * the correct behavior is to make it FATAL now; that is, emit it and
		 * then call proc_exit.
		 */
		ErrorData  *edata = &errordata[errordata_stack_depth];

		Assert(errordata_stack_depth >= 0);
		Assert(edata->elevel == ERROR);
		edata->elevel = FATAL;

		/*
		 * At least in principle, the increase in severity could have changed
		 * where-to-output decisions, so recalculate.  This should stay in
		 * sync with errstart(), which see for comments.
		 */
		edata->output_to_server = is_log_level_output(FATAL,
													  log_min_messages);
		edata->output_to_client = true;
		errfinish(0);
	}

	/* We mustn't return... */
	ExceptionalCondition("pg_re_throw tried to return", "FailedAssertion",
						 __FILE__, __LINE__);

	/*
	 * Since ExceptionalCondition isn't declared noreturn because of
	 * TrapMacro(), we need this to keep gcc from complaining.
	 */
	abort();
}


/*
 * Initialization of error output file
 */
void
DebugFileOpen(void)
{
	int			fd,
				istty;

	if (GTMLogFile[0])
	{
		/*
		 * A debug-output file name was given.
		 *
		 * Make sure we can write the file, and find out if it's a tty.
		 */
		if ((fd = open(GTMLogFile, O_CREAT | O_APPEND | O_WRONLY,
					   0666)) < 0)
			ereport(FATAL,
					(errno,
				  errmsg("could not open file \"%s\": %m", GTMLogFile)));
		istty = isatty(fd);
		close(fd);

		/*
		 * Redirect our stderr to the debug output file.
		 */
		if (!freopen(GTMLogFile, "a", stderr))
			ereport(FATAL,
					(errno,
					 errmsg("could not reopen file \"%s\" as stderr: %m",
							GTMLogFile)));

		/*
		 * If the file is a tty and we're running under the postmaster, try to
		 * send stdout there as well (if it isn't a tty then stderr will block
		 * out stdout, so we may as well let stdout go wherever it was going
		 * before).
		 */
		if (istty)
			if (!freopen(GTMLogFile, "a", stdout))
				ereport(FATAL,
						(errno,
						 errmsg("could not reopen file \"%s\" as stdout: %m",
								GTMLogFile)));
	}
}

/*
 * Write error report to server's log
 */
static void
send_message_to_server_log(ErrorData *edata)
{
	StringInfoData buf;

	initStringInfo(&buf);

	formatted_log_time[0] = '\0';

	log_line_prefix(&buf);
	appendStringInfo(&buf, "%s:  ", error_severity(edata->elevel));

	if (edata->message)
		append_with_tabs(&buf, edata->message);
	else
		append_with_tabs(&buf, _("missing error text"));

	appendStringInfoChar(&buf, '\n');

	if (edata->detail_log)
	{
		log_line_prefix(&buf);
		appendStringInfoString(&buf, _("DETAIL:  "));
		append_with_tabs(&buf, edata->detail_log);
		appendStringInfoChar(&buf, '\n');
	}
	else if (edata->detail)
	{
		log_line_prefix(&buf);
		appendStringInfoString(&buf, _("DETAIL:  "));
		append_with_tabs(&buf, edata->detail);
		appendStringInfoChar(&buf, '\n');
	}
	if (edata->hint)
	{
		log_line_prefix(&buf);
		appendStringInfoString(&buf, _("HINT:  "));
		append_with_tabs(&buf, edata->hint);
		appendStringInfoChar(&buf, '\n');
	}
	if (edata->context)
	{
		log_line_prefix(&buf);
		appendStringInfoString(&buf, _("CONTEXT:  "));
		append_with_tabs(&buf, edata->context);
		appendStringInfoChar(&buf, '\n');
	}

	/* assume no newlines in funcname or filename... */
	if (edata->funcname && edata->filename)
	{
		appendStringInfo(&buf, _("LOCATION:  %s, %s:%d\n"),
						 edata->funcname, edata->filename,
						 edata->lineno);
	}
	else if (edata->filename)
	{
		appendStringInfo(&buf, _("LOCATION:  %s:%d\n"),
						 edata->filename, edata->lineno);
	}

	/* Write to stderr, if enabled */
	if (Log_destination & LOG_DESTINATION_STDERR)
		buf.len = write(fileno(stderr), buf.data, buf.len);

	pfree(buf.data);
}

/*
 * Write error report to client
 *
 * At present, this function is not used within GTM.   Because this flushes
 * message back to the client, GTM should consider to flush backup to the
 * standby.  However, we cannot simply refer to isGTM because this module
 * can be included in Coordinator backends.  If this can really be called
 * from any GTM module, we need a solution to determine that the Port is
 * in GTM or not, without direct reference to isGTM.
 *
 * K.Suzuki, Jan, 2012
 */
static void
send_message_to_frontend(Port *myport, ErrorData *edata)
{
	StringInfoData msgbuf;

	/* 'N' (Notice) is for nonfatal conditions, 'E' is for errors */
	pq_beginmessage(&msgbuf, (edata->elevel < ERROR) ? 'N' : 'E');

	if (myport->remote_type == GTM_NODE_GTM_PROXY)
	{
		GTM_ProxyMsgHeader proxyhdr;

		proxyhdr.ph_conid = myport->conn_id;
		/* Send the GTM Proxy header if we are dealing with a proxy */
		pq_sendbytes(&msgbuf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}

	pq_sendbyte(&msgbuf, PG_DIAG_SEVERITY);
	pq_sendstring(&msgbuf, error_severity(edata->elevel));

	/* M field is required per protocol, so always send something */
	pq_sendbyte(&msgbuf, PG_DIAG_MESSAGE_PRIMARY);
	if (edata->message)
		pq_sendstring(&msgbuf, edata->message);
	else
		pq_sendstring(&msgbuf, _("missing error text"));

	if (edata->detail)
	{
		pq_sendbyte(&msgbuf, PG_DIAG_MESSAGE_DETAIL);
		pq_sendstring(&msgbuf, edata->detail);
	}

	/* detail_log is intentionally not used here */

	if (edata->hint)
	{
		pq_sendbyte(&msgbuf, PG_DIAG_MESSAGE_HINT);
		pq_sendstring(&msgbuf, edata->hint);
	}

	pq_sendbyte(&msgbuf, '\0');		/* terminator */

	pq_endmessage(myport, &msgbuf);

	/*
	 * This flush is normally not necessary, since postgres.c will flush out
	 * waiting data when control returns to the main loop. But it seems best
	 * to leave it here, so that the client has some clue what happened if the
	 * backend dies before getting back to the main loop ... error/notice
	 * messages should not be a performance-critical path anyway, so an extra
	 * flush won't hurt much ...
	 */
	pq_flush(myport);
}

/*
 * Support routines for formatting error messages.
 */


/*
 * expand_fmt_string --- process special format codes in a format string
 *
 * We must replace %m with the appropriate strerror string, since vsnprintf
 * won't know what to do with it.
 *
 * The result is a palloc'd string.
 */
static char *
expand_fmt_string(const char *fmt, ErrorData *edata)
{
	StringInfoData buf;
	const char *cp;

	initStringInfo(&buf);

	for (cp = fmt; *cp; cp++)
	{
		if (cp[0] == '%' && cp[1] != '\0')
		{
			cp++;
			if (*cp == 'm')
			{
				/*
				 * Replace %m by system error string.  If there are any %'s in
				 * the string, we'd better double them so that vsnprintf won't
				 * misinterpret.
				 */
				const char *cp2;

				cp2 = useful_strerror(edata->saved_errno);
				for (; *cp2; cp2++)
				{
					if (*cp2 == '%')
						appendStringInfoCharMacro(&buf, '%');
					appendStringInfoCharMacro(&buf, *cp2);
				}
			}
			else
			{
				/* copy % and next char --- this avoids trouble with %%m */
				appendStringInfoCharMacro(&buf, '%');
				appendStringInfoCharMacro(&buf, *cp);
			}
		}
		else
			appendStringInfoCharMacro(&buf, *cp);
	}

	return buf.data;
}


/*
 * A slightly cleaned-up version of strerror()
 */
static const char *
useful_strerror(int errnum)
{
	/* this buffer is only used if errno has a bogus value */
	static char errorstr_buf[48];
	const char *str;

	str = strerror(errnum);

	/*
	 * Some strerror()s return an empty string for out-of-range errno. This is
	 * ANSI C spec compliant, but not exactly useful.
	 */
	if (str == NULL || *str == '\0')
	{
		snprintf(errorstr_buf, sizeof(errorstr_buf),
		/*------
		  translator: This string will be truncated at 47
		  characters expanded. */
				 _("operating system error %d"), errnum);
		str = errorstr_buf;
	}

	return str;
}


/*
 * error_severity --- get localized string representing elevel
 */
static const char *
error_severity(int elevel)
{
	const char *prefix;

	switch (elevel)
	{
		case DEBUG1:
		case DEBUG2:
		case DEBUG3:
		case DEBUG4:
		case DEBUG5:
			prefix = _("DEBUG");
			break;
		case LOG:
		case COMMERROR:
			prefix = _("LOG");
			break;
		case INFO:
			prefix = _("INFO");
			break;
		case NOTICE:
			prefix = _("NOTICE");
			break;
		case WARNING:
			prefix = _("WARNING");
			break;
		case ERROR:
			prefix = _("ERROR");
			break;
		case ERROR2:
			prefix = _("ERROR2");
			break;
		case FATAL:
			prefix = _("FATAL");
			break;
		case PANIC:
			prefix = _("PANIC");
			break;
		default:
			prefix = "???";
			break;
	}

	return prefix;
}


/*
 *	append_with_tabs
 *
 *	Append the string to the StringInfo buffer, inserting a tab after any
 *	newline.
 */
static void
append_with_tabs(StringInfo buf, const char *str)
{
	char		ch;

	while ((ch = *str++) != '\0')
	{
		appendStringInfoCharMacro(buf, ch);
		if (ch == '\n')
			appendStringInfoCharMacro(buf, '\t');
	}
}


/*
 * Write errors to stderr (or by equal means when stderr is
 * not available). Used before ereport/elog can be used
 * safely (memory context, GUC load etc)
 */
void
write_stderr(const char *fmt,...)
{
	va_list		ap;

	fmt = _(fmt);

	va_start(ap, fmt);

	/* On Unix, we just fprintf to stderr */
	vfprintf(stderr, fmt, ap);
	fflush(stderr);
	va_end(ap);
}


/*
 * is_log_level_output -- is elevel logically >= log_min_level?
 *
 * We use this for tests that should consider LOG to sort out-of-order,
 * between ERROR and FATAL.  Generally this is the right thing for testing
 * whether a message should go to the postmaster log, whereas a simple >=
 * test is correct for testing whether the message should go to the client.
 */
static bool
is_log_level_output(int elevel, int log_min_level)
{
	if (elevel == LOG || elevel == COMMERROR)
	{
		if (log_min_level == LOG || log_min_level <= ERROR)
			return true;
	}
	else if (log_min_level == LOG)
	{
		/* elevel != LOG */
		if (elevel >= FATAL)
			return true;
	}
	/* Neither is LOG */
	else if (elevel >= log_min_level)
		return true;

	return false;
}
