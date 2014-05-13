/*-------------------------------------------------------------------------
 *
 * config.h
 *
 *    Configuration module of Postgres-XC configuration and operation tool.
 *
 * Copyright (c) 2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONFIG_H
#define CONFIG_H

#include <stdio.h>
#include <string.h>

typedef enum NodeType {
	NodeType_UNDEF = 0, 
	NodeType_GTM, 
	NodeType_GTM_PROXY, 
	NodeType_COORDINATOR, 
	NodeType_DATANODE, 
	NodeType_SERVER} NodeType;

void read_vars(FILE *conf);
void install_conf_prototype(char *path);
void check_configuration(void);
void read_selected_vars(FILE *conf, char *selectThis[]);
char *get_word(char *line, char **token);
int is_none(char *s);
int backup_configuration(void);
NodeType getNodeType(char *nodeName);
int checkSpecificResourceConflict(char *name, char *host, int port, char *dir, int is_gtm);
int checkNameConflict(char *name, int is_gtm);
int checkPortConflict(char *host, int port);
int checkDirConflict(char *host, char *dir);
void makeServerList(void);
int getDefaultWalSender(int isCoord);

#define DEBUG() (strcasecmp(sval(VAR_debug), "y") == 0)
#define VERBOSE() (strcasecmp(sval(VAR_verbose), "y") == 0)
#define isVarYes(x) ((sval(x) != NULL) && (strcasecmp(sval(x), "y") == 0))

void handle_no_slaves(void);

#endif /* CONFIG_H */
