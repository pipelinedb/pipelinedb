/*-------------------------------------------------------------------------
 *
 * datanode_cmd.h
 *
 *    Datanode command module of Postgres-XC configuration and operation tool.
 *
 * Copyright (c) 2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef DATANODE_CMD_H
#define DATANODE_CMD_H

#include "utils.h"

extern int init_datanode_master(char **nodeList);
extern int init_datanode_master_all(void);
extern int init_datanode_slave(char **nodeList);
extern int init_datanode_slave_all(void);


extern int start_datanode_master(char **nodeList);
extern int start_datanode_master_all(void);
extern int start_datanode_slave(char **nodeList);
extern int start_datanode_slave_all(void);

extern int stop_datanode_master(char **nodeList, char *immediate);
extern int stop_datanode_master_all(char *immediate);
extern int stop_datanode_slave(char **nodeList, char *immediate);
extern int stop_datanode_slave_all(char *immediate);

extern int failover_datanode(char **nodeList);

extern int kill_datanode_master(char **nodeList);
extern int kill_datanode_master_all(void);
extern int kill_datanode_slave(char **nodeList);
extern int kill_datanode_slave_all(void);

extern int clean_datanode_master(char **nodeList);
extern int clean_datanode_master_all(void);
extern int clean_datanode_slave(char **nodeList);
extern int clean_datanode_slave_all(void);
extern cmd_t *prepare_cleanDatanodeMaster(char *nodeName);
extern cmd_t *prepare_cleanDatanodeSlave(char *nodeName);

extern int add_datanodeMaster(char *name, char *host, int port, char *dir);
extern int add_datanodeSlave(char *name, char *host, char *dir, char *archDir);
extern int remove_datanodeMaster(char *name, int clean_opt);
extern int remove_datanodeSlave(char *name, int clean_opt);

extern int show_config_datanodeMasterSlaveMulti(char **nodeList);
extern int show_config_datanodeMasterMulti(char **nodeList);
extern int show_config_datanodeSlaveMulti(char **nodeList);
extern int show_config_datanodeMaster(int flag, int idx, char *hostname);
extern int show_config_datanodeSlave(int flag, int idx, char *hostname);

extern int check_AllDatanodeRunning(void);

#endif /* DATANODE_CMD_H */
