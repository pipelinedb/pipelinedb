/*-------------------------------------------------------------------------
 *
 * do_shell.h
 *
 *    Shell control module of Postgres-XC configuration and operation tool.
 *
 * Copyright (c) 2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef DO_SHELL_H
#define DO_SHELL_H

extern void dcSigHandler(int signum);
typedef enum FileType { STDIN, STDOUT, STDERR, GENERAL } FileType;
typedef void (*pgxcsigfunc) (int signo);
extern char *createLocalFileName(FileType type, char *buf, int len);
extern char *createRemoteFileName(FileType type, char *buf, int len);
extern int doImmediate(char *host, char *stdIn, const char *cmd_fmt, ...) __attribute__((format(printf, 3, 4)));
extern int doImmediateRaw(const char *cmd_fmt, ...) __attribute__((format(printf, 1,2)));
extern FILE *pgxc_popen_wRaw(const char *cmd_fmt, ...) __attribute__((format(printf, 1,2)));
extern FILE *pgxc_popen_w(char *host, const char *cmd_fmt, ...) __attribute__((format(printf, 2,3)));

/*
 * Flags
 */
#define PrintLog	0x01
#define PrintErr	0x02
#define LogOnly PrintLog
#define ErrOnly PrintErr
#define LogErr (PrintLog | PrintErr)
#define LeaveRemoteStdin	0x04
#define LeaveLocalStdin		0x08
#define LeaveStdout	0x10
#define InternalFunc(cmd, func, parm) \
	do \
	    {(cmd)->isInternal = TRUE; (cmd)->callback = (func); (cmd)->callback_parm = (parm);} \
	while(0)
#define ShellCall(cmd) \
	do \
        {(cmd)->isInternal = FALSE; (cmd)->callback = NULL; (cmd)->callback_parm = NULL;} \
	while(0)



typedef struct cmd_t
{
	struct cmd_t *next;	/* Next to do --> done in the same shell */
	int  isInternal;	/* If true, do not invoke shell.  Call internal function */
	void (*callback)(char *line);	/* Callback function */
	char *callback_parm;/* Argument to the callback function.  Will be freed here. */
	char *host;			/* target host  -> If null, then local command */
	char *command;		/* Will be double-quoted.   Double-quote has to be escaped by the caller */
	char *localStdin;	/* Local stdin name --> Supplied by the caller. */
	char *actualCmd;	/* internal use --> local ssh full command. */
	char *localStdout;	/* internal use, local stdout name --> Generated. Stderr will be copied here too */
						/* Messages from the child process may be printed to this file too. */
	pid_t pid;			/* internal use: valid only for cmd at the head of the list */
	int	flag;			/* flags */
	int excode;			/* exit code -> not used in parallel execution.  */
	char *msg;			/* internal use: messages to write.  Has to be comsumed only by child process. */
	char *remoteStdout;	/* internal use: remote stdout name.  Generated for remote case */
} cmd_t;

typedef struct cmdList_t
{
	int		allocated;
	int		used;
	cmd_t 	**cmds;
} cmdList_t;

extern cmdList_t *initCmdList(void);
extern cmd_t *initCmd(char *host);
#define newCommand(a) ((a)->command=Malloc(sizeof(char) * (MAXLINE+1)))
#define newCmd(a) ((a)=initCmd())
#define newFilename(a) ((a)=Malloc(sizeof(char) *(MAXPATH+1)))

/*
 * Return valie from doCmd() and doCmdList():  This include
 * exit code from the shell (and their command), as well as
 * other status of the code.
 *
 * Exit status should include WIFSIGNALED() and their signal information,
 * as well as other seen in wait(2).  Such information should be composed
 * using individual command status.  Because functions to compose them is
 * not available, we provide corresponding local implementation for them.
 */

extern int doCmdEl(cmd_t *cmd);
extern int doCmd(cmd_t *cmd);
extern int doCmdList(cmdList_t *cmds);
extern void do_cleanCmdList(cmdList_t *cmds);
#define cleanCmdList(x) do{do_cleanCmdList(x); (x) = NULL;}while(0)
extern void do_cleanCmd(cmd_t *cmd);
#define cleanCmd(x) do{do_cleanCmd(x); (x) = NULL;}while(0)
extern void do_cleanCmdEl(cmd_t *cmd);
#define cleanCmdEl(x) do{do_cleanCmeEl(x); (x) = NULL;}while(0)
extern void addCmd(cmdList_t *cmdList, cmd_t *cmd);
extern void appendCmdEl(cmd_t *src, cmd_t *new);
extern void cleanLastCmd(cmdList_t *cmdList);
extern cmd_t *makeConfigBackupCmd(void);
extern int doConfigBackup(void);
extern void dump_cmdList(cmdList_t *cmdList);

#endif /* DO_SHELL_H */

