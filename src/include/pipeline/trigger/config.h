/*-------------------------------------------------------------------------
 *
 * config.h
 *	  Interface for config
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */

#ifndef TRIGGERS_CONFIG_H
#define TRIGGERS_CONFIG_H

extern int alert_socket_mem;
extern int alert_server_port;
extern char *alert_server_address;
extern bool continuous_triggers_enabled;

#endif
