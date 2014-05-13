/*-------------------------------------------------------------------------
 *
 * bash_handler.c
 *
 *    Bash script handler module of Postgres-XC configuration and operation tool.
 *
 * Copyright (c) 2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include "bash_handler.h"
#include "config.h"
#include "pgxc_ctl.h"
#include "pgxc_ctl_log.h"

extern char *pgxc_ctl_bash_script[];
extern char *pgxc_ctl_conf_prototype[];

/*
 * Install bash script.
 */
void
install_pgxc_ctl_bash(char *path)
{
	char cmd[1024];
	FILE *pgxc_ctl_bash = fopen(path, "w");
	int i;

	elog(NOTICE, "Installing pgxc_ctl_bash script as %s.\n", path);
	if (!pgxc_ctl_bash)
	{
		elog(ERROR, "ERROR: Could not open pgxc_ctl bash script, %s, %s\n", path, strerror(errno));
	}
	for (i=0; pgxc_ctl_conf_prototype[i]; i++)
		fprintf(pgxc_ctl_bash, "%s\n", pgxc_ctl_conf_prototype[i]);
	for (i=0; pgxc_ctl_bash_script[i]; i++)
		fprintf(pgxc_ctl_bash, "%s\n", pgxc_ctl_bash_script[i]);
	fclose(pgxc_ctl_bash);
	sprintf(cmd, "chmod +x %s", path);
	system(cmd);
}

/*
 * Uninstall bash script.
 */
void uninstall_pgxc_ctl_bash(char *path)
{
	if (path)
		unlink(path);
}

/*
 * Run the bash script and read its output, which consists of variables needed to configure
 * postgres-xc cluster in pgxc_ctl.
 *
 * Be careful that pgxc_ctl changes its working directory to pgxc home directory,
 * typically $HOME/pgxc_ctl, which can be changed with pgxc_ctl options.
 * See pgxc_ctl.c or pgxc_ctl document for details.
 */
void
read_config_file(char *path, char *conf)
{
	FILE *vars;
	char cmd[1024];

	if (conf)
		sprintf(cmd, "bash %s/pgxc_ctl_bash --configure %s print_values", path, conf);
	else
		sprintf(cmd, "bash %s/pgxc_ctl_bash print_values", path);
	vars = popen(cmd, "r");
	read_vars(vars);
	fclose(vars);
}
