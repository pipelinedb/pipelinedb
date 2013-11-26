/*-------------------------------------------------------------------------
 *
 * gtm_standby.h
 *
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/gtm/gtm_standby.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef GTM_STANDBY_H
#define GTM_STANDBY_H

#include "c.h"
#include "gtm/gtm_c.h"
#include "gtm/libpq-fe.h"
#include "gtm/register.h"

/*
 * Variables to interact with GTM active under GTM standby mode.
 */
bool gtm_is_standby(void);
void gtm_set_standby(bool standby);
void gtm_set_active_conninfo(const char *addr, int port);

int gtm_standby_start_startup(void);
int gtm_standby_finish_startup(void);

int gtm_standby_restore_next_gxid(void);
int gtm_standby_restore_gxid(void);
int gtm_standby_restore_sequence(void);
int gtm_standby_restore_node(void);

int gtm_standby_register_self(const char *node_name, int port, const char *datadir);
int gtm_standby_activate_self(void);

GTM_Conn *gtm_standby_connect_to_standby(void);
void gtm_standby_disconnect_from_standby(GTM_Conn *conn);
GTM_Conn *gtm_standby_reconnect_to_standby(GTM_Conn *old_conn, int retry_max);
bool gtm_standby_check_communication_error(int *retry_count, GTM_Conn *oldconn);

GTM_PGXCNodeInfo *find_standby_node_info(void);

int gtm_standby_begin_backup(void);
int gtm_standby_end_backup(void);
void gtm_standby_closeActiveConn(void);

void gtm_standby_finishActiveConn(void);

/* Functions to process backup */
void ProcessGTMBeginBackup(Port *myport, StringInfo message);
void ProcessGTMEndBackup(Port *myport, StringInfo message);

/*
 * Startup mode
 */
#define GTM_ACT_MODE 0
#define GTM_STANDBY_MODE 1

#endif /* GTM_STANDBY_H */
