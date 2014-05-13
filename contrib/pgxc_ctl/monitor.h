/*-------------------------------------------------------------------------
 *
 * monitor.h
 *
 *    Monitoring module of Postgres-XC configuration and operation tool.
 *
 *
 * Copyright (c) 2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef MONITOR_H
#define MONITOR_H

extern void do_monitor_command(char *line);
extern int do_gtm_ping(char *host, int port);

#endif /* MONITOR_H */
