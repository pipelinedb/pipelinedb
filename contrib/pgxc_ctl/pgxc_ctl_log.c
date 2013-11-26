/*-------------------------------------------------------------------------
 *
 * pgxc_ctl_log.c
 *
 *    Logging module of Postgres-XC configuration and operation tool.
 *
 *
 * Portions Copyright (c) 2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */
/*
 * To allow mutiple pgxc_ctl to run in parallel and write a log to the same file,
 * this module uses fctl to lock log I/O.  You can lock/unlock in stack.   Anyway
 * actual lock will be captured/released at the bottom level of this stack.
 * If you'd like to have a block of the logs to be in a single block, not interrupted
 * bo other pgxc_ctl log, you should be careful to acquire the lock and release it
 * reasonablly.
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <sys/types.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <fcntl.h>

#include "pgxc_ctl.h"
#include "pgxc_ctl_log.h"
#include "varnames.h"
#include "variables.h"
#include "config.h"
#include "utils.h"

FILE *logFile = NULL;
char logFileName[MAXPATH+1];
static char *pgxcCtlGetTime(void);
static int lockStack = 0;
#define lockStackLimit 8

int logMsgLevel = INFO;
int printMsgLevel = WARNING;
int	printLocation = FALSE;
int logLocation = FALSE;


/*
 * Path is NULL if name is effective.
 * Path is valid if name is NULL
 */
static void set_msgLogLevel(void)
{
	if (sval(VAR_logMessage) == NULL)
		logMsgLevel = WARNING;
	else if (strcasecmp(sval(VAR_logMessage), "panic") == 0)
		logMsgLevel = PANIC;
	else if (strcasecmp(sval(VAR_logMessage), "error") == 0)
		logMsgLevel = ERROR;
	else if (strcasecmp(sval(VAR_logMessage), "warning") == 0)
		logMsgLevel = WARNING;
	else if (strcasecmp(sval(VAR_logMessage), "notice") == 0)
		logMsgLevel = NOTICE;
	else if (strcasecmp(sval(VAR_logMessage), "info") == 0)
		logMsgLevel = INFO;
	else if (strcasecmp(sval(VAR_logMessage), "debug1") == 0)
		logMsgLevel = DEBUG1;
	else if (strcasecmp(sval(VAR_logMessage), "debug2") == 0)
		logMsgLevel = DEBUG2;
	else if (strcasecmp(sval(VAR_logMessage), "debug3") == 0)
		logMsgLevel = DEBUG3;
	else
		logMsgLevel = INFO;
}

static void set_printLogLevel(void)
{
	if (sval(VAR_printMessage) == NULL)
		printMsgLevel = ERROR;
	else if (strcasecmp(sval(VAR_printMessage), "panic") == 0)
		printMsgLevel = PANIC;
	else if (strcasecmp(sval(VAR_printMessage), "error") == 0)
		printMsgLevel = ERROR;
	else if (strcasecmp(sval(VAR_printMessage), "warning") == 0)
		printMsgLevel = WARNING;
	else if (strcasecmp(sval(VAR_printMessage), "notice") == 0)
		printMsgLevel = NOTICE;
	else if (strcasecmp(sval(VAR_printMessage), "info") == 0)
		printMsgLevel = INFO;
	else if (strcasecmp(sval(VAR_printMessage), "debug1") == 0)
		printMsgLevel = DEBUG1;
	else if (strcasecmp(sval(VAR_printMessage), "debug2") == 0)
		printMsgLevel = DEBUG2;
	else if (strcasecmp(sval(VAR_printMessage), "debug3") == 0)
		printMsgLevel = DEBUG3;
	else
		printMsgLevel = WARNING;
}

void initLog(char *path, char *name)
{
	if(logFile)
		return;
	if(name)
		strncat(logFileName, name, MAXPATH);
	else
		snprintf(logFileName, MAXPATH, "%s/%d_pgxc_ctl.log", path, getpid());
	if ((logFile = fopen(logFileName, "a")) == NULL)
		fprintf(stderr, "Could not open log file %s, %s\n", logFileName, strerror(errno));
	/* Setup log/print message level */
	set_msgLogLevel();
	set_printLogLevel();
	printLocation = (isVarYes(VAR_printLocation)) ? TRUE : FALSE;
	logLocation = (isVarYes(VAR_logLocation)) ? TRUE : FALSE;
	lockStack = 0;
}

void closeLog()
{
	fclose(logFile);
	logFile = NULL;
}

static char *fname;
static char *funcname;
static int lineno;

void elog_start(const char *file, const char *func, int line)
{
	fname = Strdup(file);
	funcname = Strdup(func);
	lineno = line;
}

static void clean_location(void)
{
	freeAndReset(fname);
	freeAndReset(funcname);
	lineno = -1;
}


static void elogMsgRaw0(int level, const char *msg, int flag)
{
	if (logFile && level >= logMsgLevel)
	{
		if (logLocation && flag)
			fprintf(logFile, "%s(%d):%s %s:%s(%d) %s", progname, getpid(), pgxcCtlGetTime(), 
					fname, funcname, lineno, msg); 
		else
			fprintf(logFile, "%s(%d):%s %s", progname, getpid(), pgxcCtlGetTime(), msg); 
		fflush(logFile);
	}
	if (level >= printMsgLevel)
	{
		if (printLocation && flag)
			fprintf(((outF) ? outF : stderr), "%s:%s(%d) %s", fname, funcname, lineno, msg); 
		else
			fputs(msg, (outF) ? outF : stderr);
		fflush((outF) ? outF : stderr);
	}
	clean_location();
}

void elogMsgRaw(int level, const char *msg)
{
	lockLogFile();
	elogMsgRaw0(level, msg, TRUE);
	unlockLogFile();
}

void elogFinish(int level, const char *fmt, ...)
{
	char msg[MAXLINE+1];
	va_list arg;

	lockLogFile();
	if ((level >= logMsgLevel) || (level >= printMsgLevel))
	{
		va_start(arg, fmt);
		vsnprintf(msg, MAXLINE, fmt, arg);
		va_end(arg);
		elogMsgRaw(level, msg);
	}
	unlockLogFile();
}

void elogFileRaw(int level, char *path)
{
	FILE *f;
	char s[MAXLINE+1];

	lockLogFile();
	if ((f = fopen(path, "r")))
	{
		while(fgets(s, MAXLINE, f))
			elogMsgRaw0(level, s, FALSE);
		fclose(f);
	}
	else
		elog(ERROR, "ERROR: Cannot open \"%s\" for read, %s\n", path, strerror(errno));
	unlockLogFile();
}

static char timebuf[MAXTOKEN+1];

/*
 * Please note that this routine is not reentrant
 */
static char *pgxcCtlGetTime(void)
{
	struct tm *tm_s;
	time_t now;

	now = time(NULL);
	tm_s = localtime(&now);
/*	tm_s = gmtime(&now); */

	snprintf(timebuf, MAXTOKEN, "%02d%02d%02d%02d%02d_%02d", 
			 ((tm_s->tm_year+1900) >= 2000) ? (tm_s->tm_year + (1900 - 2000)) : tm_s->tm_year, 
			 tm_s->tm_mon+1, tm_s->tm_mday, tm_s->tm_hour, tm_s->tm_min, tm_s->tm_sec);
	return timebuf;
}

void writeLogRaw(const char *fmt, ...)
{
	char msg[MAXLINE+1];
	va_list arg;

	va_start(arg, fmt);
	vsnprintf(msg, MAXLINE, fmt, arg);
	va_end(arg);
	if (logFile)
	{
		lockLogFile();
		fprintf(logFile, "%s(%d):%s %s", progname, getpid(), pgxcCtlGetTime(), msg); 
		fflush(logFile);
		unlockLogFile();
	}
	fputs(msg, logFile ? logFile : stderr);
	fflush(outF ? outF : stderr);
}

void writeLogOnly(const char *fmt, ...)
{
	char msg[MAXLINE+1];
	va_list arg;

	if (logFile)
	{
		va_start(arg, fmt);
		vsnprintf(msg, MAXLINE, fmt, arg);
		va_end(arg);
		lockLogFile();
		fprintf(logFile, "%s(%d):%s %s", progname, getpid(), pgxcCtlGetTime(), msg); 
		fflush(logFile);
		unlockLogFile();
	}
}

int setLogMsgLevel(int newLevel)
{
	int rc;

	rc = logMsgLevel;
	logMsgLevel = newLevel;
	return rc;
}

int getLogMsgLevel(void)
{
	return logMsgLevel;
}

int setPrintMsgLevel(int newLevel)
{
	int rc;

	rc = printMsgLevel;
	printMsgLevel = newLevel;
	return rc;
}

int getPrintMsgLevel(void)
{
	return printMsgLevel;
}

void lockLogFile(void)
{
	struct flock lock1;

	if (logFile == NULL)
		return;
	if (lockStack > lockStackLimit)
	{
		fprintf(stderr, "Log file lock stack exceeded the limit %d. Something must be wrong.\n", lockStackLimit);
		return;
	}
	if (lockStack == 0)
	{
		lock1.l_type = F_WRLCK;
		lock1.l_start = 0;
		lock1.l_len = 0;
		lock1.l_whence = SEEK_SET;
		fcntl(fileno(logFile), F_SETLKW, &lock1);
	}
	lockStack++;
}


void unlockLogFile(void)
{
	struct flock lock1;

	if (logFile == NULL)
		return;
	lockStack--;
	if (lockStack < 0)
	{
		fprintf(stderr, "Log file stack is below zero.  Something must be wrong.\n");
		return;
	}
	if (lockStack == 0)
	{
		lock1.l_type = F_UNLCK;
		lock1.l_start = 0;
		lock1.l_len = 0;
		lock1.l_whence = SEEK_SET;
		fcntl(fileno(logFile), F_SETLKW, &lock1);
	}
}
