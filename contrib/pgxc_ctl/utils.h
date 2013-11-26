/*-------------------------------------------------------------------------
 *
 * utils.h
 *
 *    Utilty module of Postgres-XC configuration and operation tool.
 *
 * Copyright (c) 2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */
#include <stdlib.h>
#include <stdio.h>

extern void *Malloc(size_t size);
extern void *Malloc0(size_t size);
extern void *Realloc(void *ptr, size_t size);
extern void Free(void *ptr);
extern int Chdir(char *path, int flag);
extern FILE *Fopen(char *path, char *mode);
extern char *Strdup(const char *s);
extern char **addToList(char **List, char *val);
extern void appendFiles(FILE *f, char **fileList);
extern FILE *prepareLocalStdin(char *buf, int len, char **fileList);
extern char *timeStampString(char *buf, int len);
extern char **makeActualNodeList(char **nodeList);
extern int gtmProxyIdx(char *gtmProxyName);
extern int coordIdx(char *coordName);
extern int datanodeIdx(char *datanodeName);
extern int getEffectiveGtmProxyIdxFromServerName(char *serverName);
extern pid_t get_prog_pid(char *host, char *progname, char *dir);
extern int pingNode(char *host, char *port);
extern void trimNl(char *s);
extern char *getChPidList(char *host, pid_t ppid);
extern char *getIpAddress(char *hostName);

#define get_postmaster_pid(host, dir) get_prog_pid(host, "postgres", dir)
#define get_gtm_pid(host, dir) get_prog_pid(host, "gtm", dir)
#define get_gtmProxy_pid(host, dir) get_prog_pid(host, "gtm_proxy", dir)
#define freeAndReset(x) do{Free(x);(x)=NULL;}while(0)
#define myWEXITSTATUS(rc) ((rc) & 0x000000FF)

/* Printout variable in bash format */
#define svalFormat "%s=%s\n"
#define expandSval(name) name, sval(name)
#define avalFormat "%s=( %s )\n"
#define expandAval(name) name, listValue(name)
#define fprintAval(f, name) do{fprintf(f, avalFormat, expandAval(name));}while(0)
#define fprintSval(f, name) do{fprintf(f, svalFormat, expandSval(name));}while(0)
