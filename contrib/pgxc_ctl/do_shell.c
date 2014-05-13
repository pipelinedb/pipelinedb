/*-------------------------------------------------------------------------
 *
 * do_shell.c
 *
 *    Shell control module of Postgres-XC configuration and operation tool.
 *
 * Copyright (c) 2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */

/*
 * This module provides a basic infrastructure to run various shell script.
 *
 * Basically, for a single operation, when more than one server are involved,
 * they can be run in parallel.  Within each parallel execution, we can have
 * more than one command to be run in series.
 *
 * cmdList_t contains more than one command trains can be done in parallel.
 * cmd_t will be contained in cmdList_t structure which represents a train
 * of shell script.
 *
 * For each command, stdout will be handled automatically in this module.
 * Stdin can be provided by callers.
 */
#include <stdio.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <setjmp.h>
#include <string.h>
#include <readline/readline.h>
#include <readline/history.h>

#include "pgxc_ctl.h"
#include "variables.h"
#include "varnames.h"
#include "pgxc_ctl_log.h"
#include "config.h"
#include "do_shell.h"
#include "utils.h"

typedef unsigned int xc_status;
static int file_sn = 0;
static int nextSize(int size);
static char *getCleanHostname(char *buf, int len);
#if 0
static void waitTypeReturn(void);
static void echoPid(pid_t pid);
#endif
static char *allocActualCmd(cmd_t *cmd);
static void prepareStdout(cmdList_t *cmdList);

/*
 * SIGINT handler
 */
jmp_buf *whereToJumpDoShell = NULL;
jmp_buf dcJmpBufDoShell;
pgxcsigfunc old_HandlerDoShell = NULL;
void do_shell_SigHandler(int signum);

/*
 * Signal handler (SIGINT only)
 */
void
do_shell_SigHandler(int signum)
{
	if (whereToJumpDoShell)
		longjmp(*whereToJumpDoShell, 1);
	else
		signal(SIGINT,do_shell_SigHandler);
}

/*
 * Stdout/stderr/stdin will be created at $LocalTmpDir.
 * 
 */
char
*createLocalFileName(FileType type, char *buf, int len)
{
	/* 
	 * Filename is $LocalTmpDir/type_pid_serno.
	 */
	switch (type)
	{
		case STDIN:
			snprintf(buf, len-1, "%s/STDIN_%d_%d", sval(VAR_localTmpDir), getpid(), file_sn++);
			break;
		case STDOUT:
			snprintf(buf, len-1, "%s/STDOUT_%d_%d", sval(VAR_localTmpDir), getpid(), file_sn++);
			break;
		case STDERR:
			snprintf(buf, len-1, "%s/STDERR_%d_%d", sval(VAR_localTmpDir), getpid(), file_sn++);
			break;
		case GENERAL:
			snprintf(buf, len-1, "%s/GENERAL_%d_%d", sval(VAR_localTmpDir), getpid(), file_sn++);
		default:
			return NULL;
	}
	return buf;
}

/*
 * Please note that remote stdout is not in pgxc_ctl so far.  It will directly be written
 * to local stdout.
 */
char
*createRemoteFileName(FileType type, char *buf, int len)
{
	char hostname[MAXPATH+1];
	/* 
	 * Filename is $TmpDir/hostname_type_serno.
	 */
	getCleanHostname(hostname, MAXPATH);
	switch (type)
	{
		case STDIN:
			snprintf(buf, len-1, "%s/%s_STDIN_%d_%d", sval(VAR_tmpDir), hostname, getpid(), file_sn++);
			break;
		case STDOUT:
			snprintf(buf, len-1, "%s/%s_STDOUT_%d_%d", sval(VAR_tmpDir), hostname, getpid(), file_sn++);
			break;
		case STDERR:
			snprintf(buf, len-1, "%s/%s_STDERR_%d_%d", sval(VAR_tmpDir), hostname, getpid(), file_sn++);
			break;
		case GENERAL:
			snprintf(buf, len-1, "%s/%s_GENERAL_%d_%d", sval(VAR_tmpDir), hostname, getpid(), file_sn++);
			break;
		default:
			return NULL;
	}
	return buf;
}

/*
 * ==============================================================================================
 *
 * Tools to run a command foreground.
 *
 * ==============================================================================================
 */
/*
 * Run any command foreground locally.  No more redirection.
 * Return value same as system();
 * Stdout will be set to outF.  The content will also be written to log if specified.
 * If stdIn is NULL or stdiIn[0] == 0, then stdin will not be used.
 * If host == NULL or host[0] == 0, then the command will be run locally.
 */

/* Does not handle stdin/stdout.  If needed, they should be included in the cmd. */
int
doImmediateRaw(const char *cmd_fmt, ...)
{
	char actualCmd[MAXLINE+1];
	va_list arg;
	va_start(arg, cmd_fmt);
	vsnprintf(actualCmd, MAXLINE, cmd_fmt, arg);
	va_end(arg);
	return(system(actualCmd));
}

FILE *
pgxc_popen_wRaw(const char *cmd_fmt, ...)
{
	va_list arg;
	char actualCmd[MAXLINE+1];

	va_start(arg, cmd_fmt);
	vsnprintf(actualCmd, MAXLINE, cmd_fmt, arg);
	va_end(arg);
	return(popen(actualCmd, "w"));
}

FILE *
pgxc_popen_w(char *host, const char *cmd_fmt, ...)
{
	FILE *f;
	va_list arg;
	char actualCmd[MAXLINE+1];
	char sshCmd[MAXLINE+1];

	va_start(arg, cmd_fmt);
	vsnprintf(actualCmd, MAXLINE, cmd_fmt, arg);
	va_end(arg);
	snprintf(sshCmd, MAXLINE, "ssh %s@%s \" %s \"", sval(VAR_pgxcUser), host, actualCmd);
	if ((f = popen(sshCmd, "w")) == NULL)
		elog(ERROR, "ERROR: could not open the command \"%s\" to write, %s\n", sshCmd, strerror(errno));
	return f;
}
	
int
doImmediate(char *host, char *stdIn, const char *cmd_fmt, ...)
{
	char cmd_wk[MAXLINE+1];
	char actualCmd[MAXLINE+1];
	char remoteStdout[MAXPATH+1];
	char localStdout[MAXPATH+1];
	va_list arg;
	int rc;

	va_start(arg, cmd_fmt);
	vsnprintf(cmd_wk, MAXLINE, cmd_fmt, arg);
	va_end(arg);
	if (host == NULL || host[0] == '\0')
	{
		/* Local case */
		snprintf(actualCmd, MAXLINE, "( %s ) < %s > %s 2>&1",
				 cmd_wk,
				 ((stdIn == NULL) || (stdIn[0] == 0)) ? "/dev/null" : stdIn,
				 createLocalFileName(STDOUT, localStdout, MAXPATH));
		elog(DEBUG1, "Actual command: %s\n", actualCmd);
		rc = system(actualCmd);
	}
	else
	{
		int rc1;
		/* Remote case */
		snprintf(actualCmd, MAXLINE, "ssh %s@%s \"( %s ) > %s 2>&1\" < %s > /dev/null 2>&1",
				 sval(VAR_pgxcUser), host, cmd_wk, 
				 createRemoteFileName(STDOUT, remoteStdout, MAXPATH),
				 ((stdIn == NULL) || (stdIn[0] == 0)) ? "/dev/null" : stdIn);
		elog(INFO, "Actual Command: %s\n", actualCmd);
		rc = system(actualCmd);
		snprintf(actualCmd, MAXLINE, "scp %s@%s:%s %s > /dev/null 2>&1",
				 sval(VAR_pgxcUser), host, remoteStdout,
				 createLocalFileName(STDOUT, localStdout, MAXPATH));
		elog(INFO, "Bring remote stdout: %s\n", actualCmd);
		rc1 = system(actualCmd);
		if (WEXITSTATUS(rc1) != 0)
			elog(WARNING, "WARNING: Stdout transfer not successful, file: %s:%s->%s\n",
				 host, remoteStdout, localStdout);
		doImmediateRaw("ssh %s@%s \"rm -f %s < /dev/null > /dev/null\" < /dev/null > /dev/null",
					   sval(VAR_pgxcUser), host, remoteStdout);
	}
	elogFile(INFO, localStdout);
	unlink(localStdout);
	if (stdIn && stdIn[0])
		unlink(stdIn);
	return((rc));
}

/*
 * =======================================================================================
 *
 * Command list handlers
 *
 * =======================================================================================
 */
cmdList_t *
initCmdList(void)
{
	cmdList_t *rv = (cmdList_t *)Malloc0(sizeof(cmdList_t));

	rv->allocated = 1;
	return(rv);
}

cmd_t *
initCmd(char *host)
{
	cmd_t *rv = (cmd_t *)Malloc0(sizeof(cmd_t));
	if (host)
		rv->host = Strdup(host);
	return rv;
}

static void
clearStdin(cmd_t *cmd)
{
	unlink(cmd->localStdin);
	freeAndReset(cmd->localStdin);
}

static void
touchStdout(cmd_t *cmd)
{
	if (cmd->remoteStdout)
		if (cmd->remoteStdout)
			doImmediateRaw("(ssh %s@%s touch %s) < /dev/null > /dev/null 2>&1", 
						   sval(VAR_pgxcUser), cmd->host,
						   cmd->remoteStdout);
	if (cmd->localStdout)
		doImmediateRaw("(touch %s) < /dev/null > /dev/null", cmd->localStdout);
}

#if 0
static void setStdout(cmd_t *cmd)
{
	if (cmd->host != NULL)
	{
		if (cmd->remoteStdout == NULL)
			/* Remote cmd */
			cmd->remoteStdout = createRemoteFileName(STDOUT, Malloc(MAXPATH+1), MAXPATH);
		else
			freeAndReset(cmd->remoteStdout);
	}
	if (cmd->localStdout == NULL)
		cmd->localStdout = createLocalFileName(STDOUT, Malloc(MAXPATH+1), MAXPATH);
}
#endif
	
int
doCmd(cmd_t *cmd)
{
	int rc = 0;

	cmd_t *curr;

	for(curr = cmd; curr; curr = curr->next)
	{
		rc = doCmdEl(curr);
	}
	return rc;
}

static char *
allocActualCmd(cmd_t *cmd)
{
		return (cmd->actualCmd) ? cmd->actualCmd : (cmd->actualCmd = Malloc(MAXLINE+1));
}

/* localStdout has to be set by the caller */
int
doCmdEl(cmd_t *cmd)
{
	if (cmd->isInternal)
	{
		if (*cmd->callback)
			(*cmd->callback)(cmd->callback_parm);
		else
			elog(ERROR, "ERROR: no function entry was found in cmd_t.\n");
		freeAndReset(cmd->callback_parm);
		return 0;
	}
	if (cmd->host)
	{
		/* Build actual command */
		snprintf(allocActualCmd(cmd), MAXLINE,
				 "ssh %s@%s \"( %s ) > %s 2>&1\" < %s > /dev/null 2>&1",
				 sval(VAR_pgxcUser),
				 cmd->host,
				 cmd->command,
				 cmd->remoteStdout ? cmd->remoteStdout : "/dev/null",
				 cmd->localStdin ? cmd->localStdin : "/dev/null");
		/* Do it */
		elog(DEBUG1, "Remote command: \"%s\", actual: \"%s\"\n", cmd->command, cmd->actualCmd);
		cmd->excode = system(cmd->actualCmd);
		/* Handle stdout */
		clearStdin(cmd);
		touchStdout(cmd);
		doImmediateRaw("(scp %s@%s:%s %s; ssh %s@%s rm -rf %s) < /dev/null > /dev/null",
					   sval(VAR_pgxcUser), cmd->host, cmd->remoteStdout, cmd->localStdout,
					   sval(VAR_pgxcUser), cmd->host, cmd->remoteStdout);
		freeAndReset(cmd->remoteStdout);
		/* Handle stdin */
		return (cmd->excode);
	}
	else
	{
		freeAndReset(cmd->remoteStdout);
		/* Build actual command */
		snprintf(allocActualCmd(cmd), MAXLINE,
				 "( %s ) > %s 2>&1 < %s",
				 cmd->command,
				 cmd->localStdout ? cmd->localStdout : "/dev/null",
				 cmd->localStdin ? cmd->localStdin : "/dev/null");
		/* Do it */
		elog(DEBUG1, "Local command: \"%s\", actual: \"%s\"\n", cmd->command, cmd->actualCmd);
		cmd->excode = system(cmd->actualCmd);
		/* Handle stdout */
		clearStdin(cmd);
		touchStdout(cmd);
		/* Handle stdin */
		return (cmd->excode);
	}
}

/*
 * Here, we should handle exit code.
 *
 * If each command ran and exit normally, maximum (worst) value of the status code
 * will be returned.
 *
 * If SIGINT is detected, then the status will be set with EC_IFSTOPPED flag, as well as
 * EC_STOPSIG to SIGINT.  In this case, EC_IFSTOPPED will be set and EC_SIGNAL will be
 * set to SIGKILL as well.  Exit status will be set to 2.
 */
int
doCmdList(cmdList_t *cmds)
{
	int ii, jj;
	xc_status rc = 0;

	dump_cmdList(cmds);
	if (cmds->cmds == NULL)
		return(0);
	old_HandlerDoShell = signal(SIGINT, do_shell_SigHandler);
	whereToJumpDoShell = &dcJmpBufDoShell;
	/*
	 * Invoke remote command with SSH
	 */
	prepareStdout(cmds);
	if (setjmp(dcJmpBufDoShell) == 0)
	{
		for (ii = 0; cmds->cmds[ii]; ii++)
		{
			if (!isVarYes(VAR_debug))
			{
				if ((cmds->cmds[ii]->pid = fork()) != 0)
				{
					if (cmds->cmds[ii]->pid == -1)
					{
						elog(ERROR, "Process for \"%s\" failed to start. %s\n",
									cmds->cmds[ii]->actualCmd,
									strerror(errno));
						cmds->cmds[ii]->pid = 0;
					}
					continue;
				}
				else
					exit(doCmd(cmds->cmds[ii]));
			}
			else
			{
				cmds->cmds[ii]->excode = doCmd(cmds->cmds[ii]);
				rc = WEXITSTATUS(cmds->cmds[ii]->excode);
			}
		}
	}
	else
	{
		/* Signal exit here */
		for (ii = 0; cmds->cmds[ii]; ii++)
		{
			if (!isVarYes(VAR_debug))
			{
				if (cmds->cmds[ii]->pid)
				{
					/*
					 * We don't care if the process is alive or not.
					 * Try to kill anyway.  Then handle remote/local
					 * stdin/stdout in the next step.
					 *
					 * If it's bothering to wait for printing, the user can
					 * issue a SIGINT again.
					 */
					kill(cmds->cmds[ii]->pid, SIGKILL);
					cmds->cmds[ii]->pid = 0;
				}
			}
			else
			{
				/* Something to do at non-parallel execution */
			}
		}
		elog(NOTICE, "%s:%d Finish by interrupt\n", __FUNCTION__, __LINE__);
		return 2;
	}
	/*
	 * Handle remote/local stdin/stdout
	 */
	signal(SIGINT, do_shell_SigHandler);
	if (setjmp(dcJmpBufDoShell) == 0)
	{
		for (ii = 0; cmds->cmds[ii]; ii++)
		{
			int status;
			cmd_t *cur;

			if (!isVarYes(VAR_debug))
			{
				if (cmds->cmds[ii]->pid)
				{
					int rc_new;
					
					rc_new = waitpid(cmds->cmds[ii]->pid, &status, 0);
					rc = WEXITSTATUS(rc_new);
				}
			}
			cmds->cmds[ii]->pid = 0;
			for (cur = cmds->cmds[ii]; cur; cur = cur->next)
			{
				elogFile(MANDATORY, cur->localStdout);
				doImmediateRaw("(rm -f %s) < /dev/null > /dev/null", cur->localStdout);
				freeAndReset(cur->actualCmd);
				freeAndReset(cur->localStdout);
				freeAndReset(cur->msg);
			}
		}
	}
	else
	{
		/* Captured SIGINT */
		signal(SIGINT, old_HandlerDoShell);

		for (jj = 0; cmds->cmds[jj]; jj++)
		{
			/* Need to handle the case with non-parallel execution */
			if (cmds->cmds[jj]->pid)
			{
				kill(cmds->cmds[jj]->pid, SIGKILL);
				cmds->cmds[jj]->pid = 0;
			}
			if (cmds->cmds[jj]->localStdout)
				doImmediate(NULL, NULL, "rm -f %s", cmds->cmds[jj]->localStdout);
			if (cmds->cmds[jj]->remoteStdout)		/* Note that remote stdout will be removed anyway */
				doImmediate(cmds->cmds[jj]->host, NULL, "rm -f %s",
							cmds->cmds[jj]->remoteStdout);
			freeAndReset(cmds->cmds[jj]->actualCmd);
			freeAndReset(cmds->cmds[jj]->localStdout);
			freeAndReset(cmds->cmds[jj]->msg);
			freeAndReset(cmds->cmds[jj]->remoteStdout);
		}
		elog(NOTICE, "%s:%d Finish by interrupt\n", __FUNCTION__, __LINE__);
		return(2);
	}
	signal(SIGINT, old_HandlerDoShell);
	whereToJumpDoShell = NULL;
	return(rc);
}

void
appendCmdEl(cmd_t *src, cmd_t *new)
{
	cmd_t *curr;

	for(curr = src; src->next; src = src->next);
	src->next = new;
}

void
do_cleanCmdEl(cmd_t *cmd)
{
	if (cmd)
	{
		if (cmd->localStdout)
			unlink(cmd->localStdout);
		Free(cmd->localStdout);
		Free(cmd->msg);
		if (cmd->localStdin)
			unlink(cmd->localStdin);
		Free(cmd->localStdin);
		if (cmd->remoteStdout)
			doImmediateRaw("ssh %s@%s \"rm -f %s > /dev/null 2>&1\"", sval(VAR_pgxcUser), cmd->host, cmd->remoteStdout);
		Free(cmd->remoteStdout);
		Free(cmd->actualCmd);
		Free(cmd->command);
		Free(cmd->host);
	}
}

void
do_cleanCmd(cmd_t *cmd)
{
	if (cmd == NULL)
		return;
	if (cmd->next == NULL)
		do_cleanCmdEl(cmd);
	else
	{
		do_cleanCmd(cmd->next);
		freeAndReset(cmd->next);
	}
}
	
void
do_cleanCmdList(cmdList_t *cmdList)
{
	int ii;
	
	if (cmdList->cmds)
	{
		for (ii = 0; cmdList->cmds[ii]; ii++)
		{
			cleanCmd(cmdList->cmds[ii]);
			Free(cmdList->cmds[ii]);
		}
	}
	Free(cmdList);
}

void
addCmd(cmdList_t *cmds, cmd_t *cmd)
{
	cmd->pid = 0;
	cmd->actualCmd = cmd->remoteStdout = cmd->msg = cmd->localStdout = NULL;
	if (cmds->used + 1 >= cmds->allocated)
	{
		int newsize = nextSize(cmds->allocated);
		cmds->cmds = (cmd_t **)Realloc(cmds->cmds, sizeof(cmd_t *) * newsize);
		cmds->allocated = newsize;
	}
	cmds->cmds[cmds->used++] = cmd;
	cmds->cmds[cmds->used] = NULL;
}

void
cleanLastCmd(cmdList_t *cmdList)
{
	int ii;

	if ((cmdList == NULL) || (cmdList->cmds[0] == NULL))
		return;
	for (ii = 0; cmdList->cmds[ii+1]; ii++);
	cleanCmd(cmdList->cmds[ii]);
}

/*
 * ====================================================================================
 *
 * Miscellaneous
 *
 * ====================================================================================
 */
static int
nextSize(int size)
{
	if (size == 0)
		return 1;
	if (size < 128)
		return (size*2);
	return (size + 32);
}

/*
 * Get my hostname to prevent remote file name conflist
 * Take only the first part of the hostname and ignore
 * domain part
 */
static char *
getCleanHostname(char *buf, int len)
{
	char hostname[MAXPATH+1];
	int ii;

	gethostname(hostname, MAXPATH);
	for (ii = 0; hostname[ii] && hostname[ii] != '.'; ii++);
	if (hostname[ii])
		hostname[ii] = 0;
	strncpy(buf, hostname, len);
	return buf;
}

/*
 * Wait for typing something only when debug option is specified.
 * Used to synchronize child processes to start to help gdb.
 *
 * May be not useful if input file is not stdin.
 */
#if 0
static void waitTypeReturn(void)
{
	char buf[MAXLINE+1];

		fputs("Type Return: ", outF);
		fgets(buf, MAXLINE, inF);
}

static void echoPid(pid_t pid)
{
		fprintf(outF, "INFO: pid = %d\n", pid);
}
#endif

static void
prepareStdout(cmdList_t *cmdList)
{
	int ii;

	if (cmdList == NULL)
		return;
	if (cmdList->cmds == NULL)
		return;
	for (ii = 0; cmdList->cmds[ii]; ii++)
	{
		cmd_t *curr;
		for (curr = cmdList->cmds[ii]; curr; curr = curr->next)
		{
			if (curr->localStdout == NULL)
				createLocalFileName(STDOUT, (curr->localStdout = Malloc(sizeof(char) * (MAXPATH+1))), MAXPATH);
			if (curr->host)
			{
				if (curr->remoteStdout == NULL)
					createRemoteFileName(STDOUT, (curr->remoteStdout = Malloc(sizeof(char) * (MAXPATH+1))), MAXPATH);
			}
			else
				freeAndReset(curr->remoteStdout);
		}
	}
}

cmd_t *
makeConfigBackupCmd(void)
{
	cmd_t *rv = Malloc0(sizeof(cmd_t));
	snprintf((rv->command = Malloc(MAXLINE+1)), MAXLINE,
			 "ssh %s@%s mkdir -p %s;scp %s %s@%sp:%s",
			 sval(VAR_pgxcUser), sval(VAR_configBackupHost), sval(VAR_configBackupDir),
			 pgxc_ctl_config_path, sval(VAR_pgxcUser), sval(VAR_configBackupHost),
			 sval(VAR_configBackupFile));
	return(rv);
}

int
doConfigBackup(void)
{
	int rc;

	rc = doImmediateRaw("ssh %s@%s mkdir -p %s;scp %s %s@%sp:%s",
						sval(VAR_pgxcUser), sval(VAR_configBackupHost), sval(VAR_configBackupDir),
						pgxc_ctl_config_path, sval(VAR_pgxcUser), sval(VAR_configBackupHost),
						sval(VAR_configBackupFile));
	return(rc);
}

void
dump_cmdList(cmdList_t *cmdList)
{
	int ii, jj;
	cmd_t *cur;

	lockLogFile();	/* We don't like this output interrupted by other process log */
	elog(DEBUG1,
		 "*** cmdList Dump *******************************\n"
		 "allocated = %d, used = %d\n", cmdList->allocated, cmdList->used);
	if (cmdList->cmds == NULL)
	{
		elog(DEBUG1, "=== No command dfined. ===\n");
		return;
	}
	for (ii = 0; cmdList->cmds[ii]; ii++)
	{
		elog(DEBUG1,
			 "=== CMD: %d ===\n", ii);
		for (cur = cmdList->cmds[ii], jj=0; cur; cur = cur->next, jj++)
		{
			elog(DEBUG1,
				 "   --- CMD-EL: %d:"
				 "host=\"%s\", command=\"%s\", localStdin=\"%s\", localStdout=\"%s\"\n",
				 jj, cur->host ? cur->host : "NULL",
				 cur->command ? cur->command : "NULL",
				 cur->localStdin ? cur->localStdin : "NULL",
				 cur->localStdout ? cur->localStdout : "NULL");
			if (cur->localStdin)
			{
				elogFile(DEBUG1, cur->localStdin);
				elog(DEBUG1, "   ----------\n");
			}
		}
	}
	unlockLogFile();
}
