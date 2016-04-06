/*-------------------------------------------------------------------------
 *
 * alert_server.h
 *	  Interface for alert server
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#ifndef ALERT_SERVER_H
#define ALERT_SERVER_H

#include "lib/stringinfo.h"

typedef struct AlertServer AlertServer;

AlertServer *
create_alert_server(void *data);

void destroy_alert_server(AlertServer *);
void alert_server_handle(AlertServer *server);

void alert_server_add(AlertServer *server, Oid oid, const char *name);
void alert_server_remove(AlertServer *server, Oid oid);
void alert_server_push(AlertServer *server, Oid oid, StringInfo msg);

void alert_server_flush(AlertServer *server);

#endif
