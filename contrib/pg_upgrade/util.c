/*
 *	util.c
 *
 *	utility functions
 *
 *	Copyright (c) 2010-2013, PostgreSQL Global Development Group
 *	contrib/pg_upgrade/util.c
 */

#include "postgres_fe.h"

#include "pg_upgrade.h"

#include <signal.h>


LogOpts		log_opts;

/*
 * report_status()
 *
 *	Displays the result of an operation (ok, failed, error message,...)
 */
void
report_status(eLogType type, const char *fmt,...)
{
	va_list		args;
	char		message[MAX_STRING];

	va_start(args, fmt);
	vsnprintf(message, sizeof(message), fmt, args);
	va_end(args);

	pg_log(type, "%s\n", message);
}


/* force blank output for progress display */
void
end_progress_output(void)
{
	/*
	 * In case nothing printed; pass a space so gcc doesn't complain about
	 * empty format string.
	 */
	prep_status(" ");
}


/*
 * prep_status
 *
 *	Displays a message that describes an operation we are about to begin.
 *	We pad the message out to MESSAGE_WIDTH characters so that all of the "ok" and
 *	"failed" indicators line up nicely.
 *
 *	A typical sequence would look like this:
 *		prep_status("about to flarb the next %d files", fileCount );
 *
 *		if(( message = flarbFiles(fileCount)) == NULL)
 *		  report_status(PG_REPORT, "ok" );
 *		else
 *		  pg_log(PG_FATAL, "failed - %s\n", message );
 */
void
prep_status(const char *fmt,...)
{
	va_list		args;
	char		message[MAX_STRING];

	va_start(args, fmt);
	vsnprintf(message, sizeof(message), fmt, args);
	va_end(args);

	if (strlen(message) > 0 && message[strlen(message) - 1] == '\n')
		pg_log(PG_REPORT, "%s", message);
	else
		/* trim strings that don't end in a newline */
		pg_log(PG_REPORT, "%-*s", MESSAGE_WIDTH, message);
}


void
pg_log(eLogType type, char *fmt,...)
{
	va_list		args;
	char		message[MAX_STRING];

	va_start(args, fmt);
	vsnprintf(message, sizeof(message), fmt, args);
	va_end(args);

	/* PG_VERBOSE and PG_STATUS are only output in verbose mode */
	/* fopen() on log_opts.internal might have failed, so check it */
	if (((type != PG_VERBOSE && type != PG_STATUS) || log_opts.verbose) &&
		log_opts.internal != NULL)
	{
		if (type == PG_STATUS)
			/* status messages need two leading spaces and a newline */
			fprintf(log_opts.internal, "  %s\n", message);
		else
			fprintf(log_opts.internal, "%s", message);
		fflush(log_opts.internal);
	}

	switch (type)
	{
		case PG_VERBOSE:
			if (log_opts.verbose)
				printf("%s", _(message));
			break;

		case PG_STATUS:
			/* for output to a display, do leading truncation and append \r */
			if (isatty(fileno(stdout)))
				/* -2 because we use a 2-space indent */
				printf("  %s%-*.*s\r",
				/* prefix with "..." if we do leading truncation */
					   strlen(message) <= MESSAGE_WIDTH - 2 ? "" : "...",
					   MESSAGE_WIDTH - 2, MESSAGE_WIDTH - 2,
				/* optional leading truncation */
					   strlen(message) <= MESSAGE_WIDTH - 2 ? message :
					   message + strlen(message) - MESSAGE_WIDTH + 3 + 2);
			else
				printf("  %s\n", _(message));
			break;

		case PG_REPORT:
		case PG_WARNING:
			printf("%s", _(message));
			break;

		case PG_FATAL:
			printf("\n%s", _(message));
			printf("Failure, exiting\n");
			exit(1);
			break;

		default:
			break;
	}
	fflush(stdout);
}


void
check_ok(void)
{
	/* all seems well */
	report_status(PG_REPORT, "ok");
	fflush(stdout);
}


/*
 * quote_identifier()
 *		Properly double-quote a SQL identifier.
 *
 * The result should be pg_free'd, but most callers don't bother because
 * memory leakage is not a big deal in this program.
 */
char *
quote_identifier(const char *s)
{
	char	   *result = pg_malloc(strlen(s) * 2 + 3);
	char	   *r = result;

	*r++ = '"';
	while (*s)
	{
		if (*s == '"')
			*r++ = *s;
		*r++ = *s;
		s++;
	}
	*r++ = '"';
	*r++ = '\0';

	return result;
}


/*
 * get_user_info()
 * (copied from initdb.c) find the current user
 */
int
get_user_info(char **user_name)
{
	int			user_id;

#ifndef WIN32
	struct passwd *pw = getpwuid(geteuid());

	user_id = geteuid();
#else							/* the windows code */
	struct passwd_win32
	{
		int			pw_uid;
		char		pw_name[128];
	}			pass_win32;
	struct passwd_win32 *pw = &pass_win32;
	DWORD		pwname_size = sizeof(pass_win32.pw_name) - 1;

	GetUserName(pw->pw_name, &pwname_size);

	user_id = 1;
#endif

	*user_name = pg_strdup(pw->pw_name);

	return user_id;
}


/*
 * getErrorText()
 *
 *	Returns the text of the error message for the given error number
 *
 *	This feature is factored into a separate function because it is
 *	system-dependent.
 */
const char *
getErrorText(int errNum)
{
#ifdef WIN32
	_dosmaperr(GetLastError());
#endif
	return pg_strdup(strerror(errNum));
}


/*
 *	str2uint()
 *
 *	convert string to oid
 */
unsigned int
str2uint(const char *str)
{
	return strtoul(str, NULL, 10);
}


/*
 *	pg_putenv()
 *
 *	This is like putenv(), but takes two arguments.
 *	It also does unsetenv() if val is NULL.
 */
void
pg_putenv(const char *var, const char *val)
{
	if (val)
	{
#ifndef WIN32
		char	   *envstr = (char *) pg_malloc(strlen(var) +
												strlen(val) + 2);

		sprintf(envstr, "%s=%s", var, val);
		putenv(envstr);

		/*
		 * Do not free envstr because it becomes part of the environment on
		 * some operating systems.	See port/unsetenv.c::unsetenv.
		 */
#else
		SetEnvironmentVariableA(var, val);
#endif
	}
	else
	{
#ifndef WIN32
		unsetenv(var);
#else
		SetEnvironmentVariableA(var, "");
#endif
	}
}
