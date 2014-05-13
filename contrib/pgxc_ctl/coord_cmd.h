/*-------------------------------------------------------------------------
 *
 * cood_cmd.h
 *
 *    Coordinator command module of Postgres-XC configuration and operation tool.
 *
 * Copyright (c) 2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef COORD_CMD_H
#define COORD_CMD_H

#include "utils.h"

extern int init_coordinator_master(char **nodeList);
extern int init_coordinator_slave(char **nodeList);
extern int init_coordinator_master_all(void);
extern int init_coordinator_slave_all(void);

extern int configure_nodes(char **nodeList);
extern int configure_nodes_all(void);

extern int kill_coordinator_master(char **nodeList);
extern int kill_coordinator_master_all(void);
extern int kill_coordinator_slave(char **nodeList);
extern int kill_coordinator_slave_all(void);

extern int clean_coordinator_master(char **nodeList);
extern int clean_coordinator_master_all(void);
extern int clean_coordinator_slave(char **nodeList);
extern int clean_coordinator_slave_all(void);
extern cmd_t *prepare_cleanCoordinatorMaster(char *nodeName);
extern cmd_t *prepare_cleanCoordinatorSlave(char *nodeName);

extern int start_coordinator_master(char **nodeList);
extern int start_coordinator_master_all(void);
extern int start_coordinator_slave(char **nodeList);
extern int start_coordinator_slave_all(void);

extern int stop_coordinator_master(char **nodeList, char *immediate);
extern int stop_coordinator_master_all(char *immediate);
extern int stop_coordinator_slave(char **nodeList, char *immediate);
extern int stop_coordinator_slave_all(char *immediate);

extern int add_coordinatorMaster(char *name, char *host, int port, int pooler, char *dir);
extern int add_coordinatorSlave(char *name, char *host, char *dir, char *archDir);
extern int remove_coordinatorMaster(char *name, int clean_opt);
extern int remove_coordinatorSlave(char *name, int clean_opt);

extern int failover_coordinator(char **nodeList);

extern int show_config_coordMasterSlaveMulti(char **nodeList);
extern int show_config_coordMasterMulti(char **nodeList);
extern int show_config_coordSlaveMulti(char **nodeList);
extern int show_config_coordMaster(int flag, int idx, char *hostname);
extern int show_config_coordSlave(int flag, int idx, char *hostname);
extern int check_AllCoordRunning(void);


#endif /* COORD_CMD_H */
