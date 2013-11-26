/*-------------------------------------------------------------------------
 *
 * pgxc_ctl_log.h
 *
 *    Logging module of Postgres-XC configuration and operation tool.
 *
 * Portions Copyright (c) 2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOG_H
#define LOG_H
#include "pgxc_ctl.h"

#define MAXMSG 4096

/* Control verbosity */

#define	DEBUG3  10
#define	DEBUG2  11
#define DEBUG1  12
#define INFO    13		/* Default for logMsgLevel */
#define NOTICE2 14
#define NOTICE  15		/* Default for printMsgLevel */
#define WARNING 16
#define ERROR   17
#define PANIC   18
#define MANDATORY 19

extern FILE *logFile;
extern void elog_start(const char *file, const char *func, int line);
extern void elogFinish(int level, const char *fmt,...) __attribute__((format(printf, 2, 3)));
extern void elogMsgRaw(int level, const char *msg);
extern void elogFileRaw(int level, char *fn);
extern void initLog(char *path, char *name);
extern void closeLog(void);
extern void writeLogRaw(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
extern void writeLogOnly(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
extern int  setLogMsgLevel(int newLevel);
extern int  getLogMsgLevel(void);
extern int  setPrintMsgLevel(int newLevel);
extern int  getPrintMsgLevel(void);
extern void lockLogFile(void);
extern void unlockLogFile(void);

#define elog	elog_start(__FILE__, __FUNCTION__, __LINE__), elogFinish
#define elogMsg	elog_start(__FILE__, __FUNCTION__, __LINE__), elogMsgRaw
#define elogFile	elog_start(__FILE__, __FUNCTION__, __LINE__), elogFileRaw
/*
#define elog elogFinish
#define elogMsg elogMsgRaw
#define elogFile elogFileRaw
*/

extern char logFileName[MAXPATH+1];


extern int logMsgLevel;
extern int printMsgLevel;
extern int printLocation;
extern int logLocation;

#endif /* LOG_H */
