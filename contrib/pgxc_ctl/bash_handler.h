/*-------------------------------------------------------------------------
 *
 * bash_handler.h
 *
 *    Bash script handling  module of Postgres-XC configuration and operation tool.
 *
 * Copyright (c) 2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef BASH_HANDLER_H
#define BASH_HANDLER_H

void install_pgxc_ctl_bash(char *path);
void read_config_file(char *path, char *conf);
void uninstall_pgxc_ctl_bash(char *path);

#endif /* BASH_HANDLER_H */
