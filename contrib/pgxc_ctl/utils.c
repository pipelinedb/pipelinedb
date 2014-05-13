/*-------------------------------------------------------------------------
 *
 * utils.c
 *
 *    Utility module of Postgres-XC configuration and operation tool.
 *
 * Copyright (c) 2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */
/*
 * Variable useful tools/small routines.
 */
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <time.h>
#include <stdio.h>

#include "../../src/interfaces/libpq/libpq-fe.h"
#include "utils.h"
#include "pgxc_ctl.h"
#include "pgxc_ctl_log.h"
#include "do_shell.h"
#include "config.h"
#include "variables.h"
#include "varnames.h"
#include "sys/time.h"

static int Malloc_ed = 0;
static int Strdup_ed = 0;
static int Freed = 0;
static void myUsleep(long microsec);

void
*Malloc(size_t size)
{
	void *rv = malloc(size);

	Malloc_ed++;
	if (rv == NULL)
	{
		elog(PANIC, "PANIC: No more memory.  See core file for details.\n");
		abort();
	}
	return(rv);
}

char **
addToList(char **List, char *val)
{
	char **rv;
	int ii;
	
	for (ii = 0; List[ii]; ii++);
	rv = Realloc(List, sizeof(char *) * ii);
	rv[ii - 1] = NULL;
	return rv;
}

void *
Malloc0(size_t size)
{
	void *rv = malloc(size);

	Malloc_ed++;
	if (rv == NULL)
	{
		elog(PANIC, "PANIC: No more memory.  See core file for details.\n");
		abort();
	}
	memset(rv, 0, size);
	return(rv);
}

void *
Realloc(void *ptr, size_t size)
{
	void *rv = realloc(ptr, size);

	if (rv == NULL)
	{
		elog(PANIC, "PANIC: No more memory.  See core file for details.\n");
		abort();
	}
	return(rv);
}

void
Free(void *ptr)
{
	Freed++;
	if (ptr)
		free(ptr);
}

/*
 * If flag is TRUE and chdir fails, then exit(1)
 */
int
Chdir(char *path, int flag)
{
	if (chdir(path))
	{
		elog(ERROR, "ERROR: Could not change work directory to \"%s\". %s%s\n", 
			 path, 
			 flag == TRUE ? "Exiting. " : "",
			 strerror(errno));
		if (flag == TRUE)
			exit(1);
		else
			return -1;
	}
	return 0;
}

FILE *
Fopen(char *path, char *mode)
{
	FILE *rv;

	if ((rv = fopen(path, mode)) == NULL)
		elog(ERROR, "ERROR: Could not open the file \"%s\" in \"%s\", %s\n", path, mode, strerror(errno));
	return(rv);
}


char *
Strdup(const char *s)
{
	char *rv;

	Strdup_ed++;
	rv = strdup(s);
	if (rv == NULL)
	{
		elog(PANIC, "PANIC: No more memory. See core file for details.\n");
		abort();
	}
	return(rv);
}

void
appendFiles(FILE *f, char **fileList)
{
	FILE *src;
	int ii;
	char buf[MAXLINE+1];

	if (fileList)
		for (ii = 0; fileList[ii]; ii++)
		{
			if (!is_none(fileList[ii]))
			{
				if ((src = fopen(fileList[ii], "r")) == 0)
				{
					elog(ERROR, "ERROR: could not open file %s for read, %s\n", fileList[ii], strerror(errno));
					continue;
				}
				while (fgets(buf, MAXLINE, src))
					fputs(buf, f);
				fclose(src);
			}
		}
}

FILE *
prepareLocalStdin(char *buf, int len, char **fileList)
{
	FILE *f;
	if ((f = fopen(createLocalFileName(STDIN, buf, len), "w")) == NULL)
	{
		elog(ERROR, "ERROR: could not open file %s for write, %s\n", buf, strerror(errno));
		return(NULL);
	}
	appendFiles(f, fileList);
	return(f);
}

char *
timeStampString(char *buf, int len)
{
	time_t nowTime;
	struct tm nowTm;

	nowTime = time(NULL);
	localtime_r(&nowTime, &nowTm);

	snprintf(buf, len, "%04d%02d%02d_%02d:%02d:%02d",
			 nowTm.tm_year+1900, nowTm.tm_mon+1, nowTm.tm_mday,
			 nowTm.tm_hour, nowTm.tm_min, nowTm.tm_sec);
	return(buf);
}

char **
makeActualNodeList(char **nodeList)
{
	char **actualNodeList;
	int ii, jj;

	for (ii = 0, jj = 0; nodeList[ii]; ii++)
	{
		if (!is_none(nodeList[ii]))
			jj++;
	}
	actualNodeList = Malloc0(sizeof(char *) * (jj + 1));
	for (ii = 0, jj = 0; nodeList[ii]; ii++)
	{
		if (!is_none(nodeList[ii]))
		{
			actualNodeList[jj] = Strdup(nodeList[ii]);
			jj++;
		}
	}
	return actualNodeList;
}

int
gtmProxyIdx(char *gtmProxyName)
{
	int ii;

	for (ii = 0; aval(VAR_gtmProxyNames)[ii]; ii++)
	{
		if (strcmp(aval(VAR_gtmProxyNames)[ii], gtmProxyName) == 0)
			return ii;
	}
	return -1;
}

int coordIdx(char *coordName)
{
	int ii;

	if (is_none(coordName))
		return -1;
	for (ii = 0; aval(VAR_coordNames)[ii]; ii++)
	{
		if (strcmp(aval(VAR_coordNames)[ii], coordName) == 0)
			return ii;
	}
	return -1;
}

int datanodeIdx(char *datanodeName)
{
	int ii;

	if (is_none(datanodeName))
		return -1;
	for (ii = 0; aval(VAR_datanodeNames)[ii]; ii++)
	{
		if (strcmp(aval(VAR_datanodeNames)[ii], datanodeName) == 0)
			return ii;
	}
	return -1;
}

int getEffectiveGtmProxyIdxFromServerName(char *serverName)
{
	int ii;

	if (serverName == NULL)
		return (-1);
	for (ii = 0; aval(VAR_gtmProxyNames)[ii]; ii++)
	{
		if (strcmp(aval(VAR_gtmProxyServers)[ii], serverName) == 0)
			return ii;
	}
	return -1;
}



/*
 * Please note that this function deeply depend upon
 * the environment.
 *
 * It works find with CentOS/Ubuntu/ReadHat Linux but
 * may need another tweak for other operation systems
 * such as Solaris, FreeBSD, MacOS.
 */
pid_t
get_prog_pid(char *host, char *progname, char *dir)
{
	char cmd[MAXLINE+1];
	char pid_s[MAXLINE+1];
	int ii;
	FILE *wkf;
	char *token;
	char *line;

	snprintf(cmd, MAXLINE,
			 "ssh %s@%s "
			 "\"ps -f -C %s | grep %s\"",
			 sval(VAR_pgxcUser), host, progname, dir);
	wkf = popen(cmd, "r");
	if (wkf == NULL)
	{
		elog(ERROR, "ERROR: cannot obtain pid value of the remote postmaster, host \"%s\" dir \"%s\", %s\n",
					host, dir, strerror(errno));
		return(-1);
	}
	fgets(pid_s, MAXLINE, wkf);
	fclose(wkf);
	/* Get the second token */
	line = pid_s;
	if ((line = get_word(line, &token)) == NULL)
		return 0;
	get_word(line, &token);
	if (token == NULL)
		return 0;
	for (ii = 0; token[ii]; ii++)
		if (token[ii] < '0' || token[ii] > '9')
			return 0;
	return(atoi(token));
}

int
pingNode(char *host, char *port)
{
	PGPing status;
	char conninfo[MAXLINE+1];
	char editBuf[MAXPATH+1];
#define RETRY 3
#define sleepMicro 100*1000	/* 100 millisec */
	int retry;

	conninfo[0] = 0;
	if (host)
	{
		snprintf(editBuf, MAXPATH, "host = '%s' ", host);
		strncat(conninfo, editBuf, MAXLINE);
	}
	if (port)
	{
		snprintf(editBuf, MAXPATH, "port = %d ", atoi(port));
		strncat(conninfo, editBuf, MAXLINE);
	}
	if (conninfo[0])
	{
		for (retry = RETRY; retry; retry--){
			status = PQping(conninfo);
			if (status == PQPING_OK)
				return 0;
			else
			{
				myUsleep(sleepMicro);
				continue;
			}
		}
		return 1;
	}
	else
		return -1;
#undef RETRY
#undef sleepMicro
}

void
trimNl(char *s)
{
	for (;*s && *s != '\n'; s++);
	*s = 0;
}

char *
getChPidList(char *host, pid_t ppid)
{
	FILE *wkf;
	char cmd[MAXLINE+1];
	char line[MAXLINE+1];
	char *rv = Malloc(MAXLINE+1);

	rv[0] = 0;
	snprintf(cmd, MAXLINE, "ssh %s@%s pgrep -P %d",
			 sval(VAR_pgxcUser), host, ppid);
	wkf = popen(cmd, "r");
	if (wkf == NULL)
		return NULL;
	while (fgets(line, MAXLINE, wkf))
	{
		trimNl(line);
		strncat(rv, line, MAXLINE);
		strncat(rv, " ", MAXLINE);
	}
	return rv;
}
	
char *
getIpAddress(char *hostName)
{
	char command[MAXLINE+1];
	char *ipAddr;
	FILE *f;

	snprintf(command, MAXLINE, "ping -c1 %s | head -n 1 | sed 's/^[^(]*(\\([^)]*\\).*$/\\1/'", hostName);
	if ((f = popen(command, "r")) == NULL)
	{
		elog(ERROR, "ERROR: could not open the command, \"%s\", %s\n", command, strerror(errno));
		return NULL;
	}
	ipAddr = Malloc(MAXTOKEN+1);
	fgets(ipAddr, MAXTOKEN, f);
	fclose(f);
	trimNl(ipAddr);
	return ipAddr;
}

static void
myUsleep(long microsec)
{
	struct timeval delay;

	if (microsec <= 0)
		return;

	delay.tv_sec = microsec / 1000000L;
	delay.tv_usec = microsec % 1000000L;
	(void) select(0, NULL, NULL, NULL, &delay);
}
