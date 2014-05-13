/*-------------------------------------------------------------------------
 *
 * gtm_cmd.c
 *
 *    GTM command module of Postgres-XC configuration and operation tool.
 *
 * Copyright (c) 2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */
/*
 * This module provides various gtm-related pgxc_operation.
 */
#include <stdio.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <setjmp.h>
#include <string.h>
#include <readline/readline.h>
#include <readline/history.h>

#include "pgxc_ctl.h"
#include "do_command.h"
#include "variables.h"
#include "varnames.h"
#include "pgxc_ctl_log.h"
#include "config.h"
#include "do_shell.h"
#include "utils.h"
#include "gtm_cmd.h"
#include "monitor.h"

static char date[MAXTOKEN+1];

static cmd_t * prepare_initGtmMaster(void);
static cmd_t * prepare_initGtmSlave(void);
static cmd_t *prepare_startGtmMaster(void);
static cmd_t *prepare_startGtmSlave(void);
static cmd_t *prepare_stopGtmMaster(void);
static cmd_t *prepare_stopGtmSlave(void);
static cmd_t *prepare_killGtmMaster(void);
static cmd_t *prepare_killGtmSlave(void);
static cmd_t *prepare_initGtmProxy(char *nodeName);
static cmd_t *prepare_startGtmProxy(char *nodeName);
static cmd_t *prepare_stopGtmProxy(char *nodeName);
static cmd_t *prepare_killGtmProxy(char *nodeName);
static cmd_t *prepare_reconnectGtmProxy(char *nodeName);

/*  ======================================================================================
 *
 * GTM Staff
 *
 * =======================================================================================
 */
/*
 * Init gtm master -----------------------------------------------------------------
 */
static cmd_t *
prepare_initGtmMaster(void)
{
	cmd_t *cmdInitGtmMaster, *cmdGtmConf, *cmdGxid;
	char date[MAXTOKEN+1];
	FILE *f;
	char **fileList = NULL;

	/* Kill current gtm, bild work directory and run initgtm */
	cmdInitGtmMaster = initCmd(sval(VAR_gtmMasterServer));
	snprintf(newCommand(cmdInitGtmMaster), MAXLINE,
			 "killall -u %s -9 gtm; rm -rf %s; mkdir -p %s;initgtm -Z gtm -D %s",
			 sval(VAR_pgxcUser), 
			 sval(VAR_gtmMasterDir), sval(VAR_gtmMasterDir), sval(VAR_gtmMasterDir));

	/* Then prepare gtm.conf file */

	/* Prepare local Stdin */
	appendCmdEl(cmdInitGtmMaster, (cmdGtmConf = initCmd(sval(VAR_gtmMasterServer))));
	if ((f = prepareLocalStdin(newFilename(cmdGtmConf->localStdin), MAXPATH, NULL)) == NULL)
	{
		cleanCmd(cmdInitGtmMaster);
		return(NULL);
	}
	fprintf(f, 
			"#===============================================\n"
			"# Added at initialization, %s\n"
			"listen_addresses = '*'\n",
			timeStampString(date, MAXTOKEN));
	if (!is_none(sval(VAR_gtmExtraConfig)))
		AddMember(fileList, sval(VAR_gtmExtraConfig));
	if (!is_none(sval(VAR_gtmMasterSpecificExtraConfig)))
		AddMember(fileList, sval(VAR_gtmMasterSpecificExtraConfig));
	appendFiles(f, fileList);
	CleanArray(fileList);
	fprintf(f,
			"port = %s\n"
			"nodename = '%s'\n"
			"startup = ACT\n"
			"# End of addition\n",
			sval(VAR_gtmMasterPort), sval(VAR_gtmName));
	fclose(f);
	/* other options */
	snprintf(newCommand(cmdGtmConf), MAXLINE,
			 "cat >> %s/gtm.conf",
			 sval(VAR_gtmMasterDir));

	/* Setup GTM with appropriate GXID value */
	
	appendCmdEl(cmdGtmConf, (cmdGxid = initCmd(sval(VAR_gtmMasterServer))));
	snprintf(newCommand(cmdGxid), MAXLINE,
			 "(gtm -x 2000 -D %s &); sleep 1; gtm_ctl stop -Z gtm -D %s",
			 sval(VAR_gtmMasterDir), sval(VAR_gtmMasterDir));

	return cmdInitGtmMaster;
}

int
init_gtm_master(void)
{
	int rc;
	cmdList_t *cmdList;

	elog(INFO, "Initialize GTM master\n");
	cmdList = initCmdList();

	/* Kill current gtm, build work directory and run initgtm */

	addCmd(cmdList, prepare_initGtmMaster());
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	elog(INFO, "Done.\n");
	return(rc);
}

/*
 * Add gtm slave: to be used after all the configuration is done.
 *
 * This function only maintains internal configuration, updte configuration file,
 * and make backup if configured.   You should run init_gtm_slave and stat_gtm_slave
 * separately.
 */
int
add_gtmSlave(char *name, char *host, int port, char *dir)
{
	char port_s[MAXTOKEN+1];
	char date[MAXTOKEN+1];
	FILE *f;
	int	rc;

	if (isVarYes(VAR_gtmSlave))
	{
		elog(ERROR, "ERROR: GTM slave is already configured.\n");
		return 1;
	}
	if (is_none(host))
	{
		elog(ERROR, "ERROR: Cannot add gtm slave with the name \"none\".\n");
		return 1;
	}
	if (is_none(dir))
	{
		elog(ERROR, "ERROR: Cannot add gtm slave with the directory \"none\".\n");
		return 1;
	}
	if (checkSpecificResourceConflict(name, host, port, dir, TRUE))
	{
		elog(ERROR, "ERROR: New specified name:%s, host:%s, port:%d and dir:\"%s\" conflicts with existing node.\n",
			 name, host, port, dir);
		return 1;
	}
	assign_sval(VAR_gtmSlave, Strdup("y"));
	assign_sval(VAR_gtmSlaveServer, Strdup(host));
	snprintf(port_s, MAXTOKEN, "%d", port);
	assign_sval(VAR_gtmSlavePort, Strdup(port_s));
	assign_sval(VAR_gtmSlaveDir, Strdup(dir));
	makeServerList();
	if ((f = fopen(pgxc_ctl_config_path, "a")) == NULL)
	{
		/* Should it be panic? */
		elog(ERROR, "ERROR: cannot open configuration file \"%s\", %s\n", pgxc_ctl_config_path, strerror(errno));
		return 1;
	}
	fprintf(f, 
			"#===================================================\n"
			"# pgxc configuration file updated due to GTM slave addition\n"
			"#        %s\n", 
			timeStampString(date, MAXTOKEN+1));
	fprintSval(f, VAR_gtmSlave);
	fprintSval(f, VAR_gtmSlaveServer);
	fprintSval(f, VAR_gtmSlavePort);
	fprintSval(f, VAR_gtmSlaveDir);
	fprintf(f, "%s","#----End of reconfiguration -------------------------\n");
	fclose(f);
	backup_configuration();
	if ((rc = init_gtm_slave()) != 0)
		return rc;
	return(start_gtm_slave());
}

int
remove_gtmSlave(bool clean_opt)
{
	FILE *f;

	/* Check if gtm_slave is configured */
	if (!isVarYes(VAR_gtmSlave) || !sval(VAR_gtmSlaveServer) || is_none(sval(VAR_gtmSlaveServer)))
	{
		elog(ERROR, "ERROR: GTM slave is not configured.\n");
		return 1;
	}
	/* Check if gtm_slave is not running */
	if (!do_gtm_ping(sval(VAR_gtmSlaveServer), atoi(sval(VAR_gtmSlavePort))))
	{
		elog(ERROR, "ERROR: GTM slave is now running. Cannot remove it.\n");
		return 1;
	}
	elog(NOTICE, "Removing gtm slave.\n");
	/* Clean */
	if (clean_opt)
		clean_gtm_slave();
	/* Reconfigure */
	reset_var(VAR_gtmSlave);
	assign_sval(VAR_gtmSlave, Strdup("n"));
	reset_var(VAR_gtmSlaveServer);
	assign_sval(VAR_gtmSlaveServer, Strdup("none"));
	reset_var(VAR_gtmSlavePort);
	assign_sval(VAR_gtmSlavePort, Strdup("-1"));
	reset_var(VAR_gtmSlaveDir);
	assign_sval(VAR_gtmSlaveDir, Strdup("none"));
	/* Write the configuration file and bakup it */
	if ((f = fopen(pgxc_ctl_config_path, "a")) == NULL)
	{
		/* Should it be panic? */
		elog(ERROR, "ERROR: cannot open configuration file \"%s\", %s\n", pgxc_ctl_config_path, strerror(errno));
		return 1;
	}
	fprintf(f, 
			"#===================================================\n"
			"# pgxc configuration file updated due to GTM slave removal\n"
			"#        %s\n",
			timeStampString(date, MAXTOKEN+1));
	fprintSval(f, VAR_gtmSlave);
	fprintSval(f, VAR_gtmSlaveServer);
	fprintSval(f, VAR_gtmSlavePort);
	fprintSval(f, VAR_gtmSlaveDir);
	fprintf(f, "%s", "#----End of reconfiguration -------------------------\n");
	fclose(f);
	backup_configuration();
	elog(NOTICE, "Done.\n");
	return 0;
}


/*
 * Init gtm slave -------------------------------------------------------------
 */

/* 
 * Assumes Gtm Slave is configured.
 * Caller should check this.
 */
static cmd_t *
prepare_initGtmSlave(void)
{
	char date[MAXTOKEN+1];
	cmd_t *cmdInitGtm, *cmdGtmConf;
	FILE *f;
	char **fileList = NULL;

	if (!isVarYes(VAR_gtmSlave) || (sval(VAR_gtmSlaveServer) == NULL) || is_none(sval(VAR_gtmSlaveServer)))
	{
		elog(ERROR, "ERROR: GTM slave is not configured.\n");
		return(NULL);
	}
	/* Kill current gtm, build work directory and run initgtm */
	cmdInitGtm = initCmd(sval(VAR_gtmSlaveServer));
	snprintf(newCommand(cmdInitGtm), MAXLINE,
			 "killall -u %s -9 gtm; rm -rf %s; mkdir -p %s; initgtm -Z gtm -D %s",
			 sval(VAR_pgxcUser),
			 sval(VAR_gtmSlaveDir), sval(VAR_gtmSlaveDir), sval(VAR_gtmSlaveDir));

	/* Prepare gtm.conf file */

	/* Prepare local Stdin */
	appendCmdEl(cmdInitGtm, (cmdGtmConf = initCmd(sval(VAR_gtmSlaveServer))));
	snprintf(newCommand(cmdGtmConf), MAXLINE,
			 "cat >> %s/gtm.conf",
			 sval(VAR_gtmSlaveDir));
	if ((f = prepareLocalStdin(newFilename(cmdGtmConf->localStdin), MAXPATH, NULL)) == NULL)
	{
		cleanCmd(cmdInitGtm);
		return(NULL);
	}
	fprintf(f, 
			"#===============================================\n"
			"# Added at initialization, %s\n"
			"listen_addresses = '*'\n",
			timeStampString(date, MAXPATH+1));
	if (!is_none(sval(VAR_gtmExtraConfig)))
		AddMember(fileList, sval(VAR_gtmExtraConfig));
	if (!is_none(sval(VAR_gtmMasterSpecificExtraConfig)))
		AddMember(fileList, sval(VAR_gtmMasterSpecificExtraConfig));
	appendFiles(f, fileList);
	CleanArray(fileList);
	fprintf(f,
			"port = %s\n"
			"nodename = '%s'\n"
			"startup = STANDBY\n"
			"active_host = '%s'\n"
			"active_port = %d\n"
			"# End of addition\n",
			sval(VAR_gtmSlavePort), sval(VAR_gtmName),
			sval(VAR_gtmMasterServer), atoi(sval(VAR_gtmMasterPort)));
	fclose(f);
	return (cmdInitGtm);
}

int
init_gtm_slave(void)
{
	cmdList_t *cmdList;
	cmd_t *cmdInitGtm;
	int rc;

	elog(INFO, "Initialize GTM slave\n");
	cmdList = initCmdList();
	if ((cmdInitGtm = prepare_initGtmSlave()))
	{
		addCmd(cmdList, cmdInitGtm);
		/* Do all the commands and clean */
		rc = doCmdList(cmdList);
		cleanCmdList(cmdList);
		elog(INFO, "Done.\n");
		return(rc);
	}
	return 1;
}

/*
 * Start gtm master -----------------------------------------------------
 */
static cmd_t *
prepare_startGtmMaster(void)
{
	cmd_t *cmdGtmCtl;

	cmdGtmCtl = initCmd(sval(VAR_gtmMasterServer));
	snprintf(newCommand(cmdGtmCtl), MAXLINE, 
			 "gtm_ctl stop -Z gtm -D %s;"
			 "rm -f %s/register.node;"
			 "gtm_ctl start -Z gtm -D %s",
			 sval(VAR_gtmMasterDir), sval(VAR_gtmMasterDir), sval(VAR_gtmMasterDir));
	return cmdGtmCtl;
}

int
start_gtm_master(void)
{
	cmdList_t *cmdList;
	int rc;

	elog(INFO, "Start GTM master\n");
	cmdList = initCmdList();
	addCmd(cmdList, prepare_startGtmMaster());
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	return(rc);
}

/*
 * Start gtm slave ----------------------------------------------------
 */
static cmd_t *
prepare_startGtmSlave(void)
{
	cmd_t *cmdGtmCtl;

	if (!isVarYes(VAR_gtmSlave) || (sval(VAR_gtmSlaveServer) == NULL) || is_none(sval(VAR_gtmSlaveServer)))
	{
		elog(ERROR, "ERROR: GTM slave is not configured.\n");
		return(NULL);
	}
	cmdGtmCtl = initCmd(sval(VAR_gtmSlaveServer));
	snprintf(newCommand(cmdGtmCtl), MAXLINE, 
			 "gtm_ctl stop -Z gtm -D %s;"
			 "rm -rf %s/register.node;"
			 "gtm_ctl start -Z gtm -D %s",
			 sval(VAR_gtmSlaveDir), sval(VAR_gtmSlaveDir), sval(VAR_gtmSlaveDir));
	return (cmdGtmCtl);
}

int
start_gtm_slave(void)
{
	cmdList_t *cmdList;
	cmd_t *cmd;
	int rc;

	elog(INFO, "Start GTM slave");
	cmdList = initCmdList();
	if ((cmd = prepare_startGtmSlave()))
	{
		addCmd(cmdList, cmd);
		rc = doCmdList(cmdList);
		cleanCmdList(cmdList);
		elog(INFO, "Done.\n");
		return(rc);
	}
	return 1;
}

/*
 * Stop gtm master ---------------------------------------------------------
 */
static cmd_t *
prepare_stopGtmMaster(void)
{
	cmd_t *cmdGtmCtl;

	cmdGtmCtl = initCmd(sval(VAR_gtmMasterServer));
	snprintf(newCommand(cmdGtmCtl), MAXLINE,
			 "gtm_ctl stop -Z gtm -D %s",
			 sval(VAR_gtmMasterDir));
	return(cmdGtmCtl);
}

int
stop_gtm_master(void)
{
	cmdList_t *cmdList;
	int rc;

	elog(INFO, "Stop GTM master\n");
	cmdList = initCmdList();
	addCmd(cmdList, prepare_stopGtmMaster());
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	return(rc);
}

/*
 * Stop gtm slave ---------------------------------------------------------------
 */
static cmd_t *
prepare_stopGtmSlave(void)
{
	cmd_t *cmdGtmCtl;

	if (!isVarYes(VAR_gtmSlave) || (sval(VAR_gtmSlaveServer) == NULL) || is_none(sval(VAR_gtmSlaveServer)))
	{
		elog(ERROR, "ERROR: GTM slave is not configured.\n");
		return(NULL);
	}
	cmdGtmCtl = initCmd(sval(VAR_gtmSlaveServer));
	snprintf(newCommand(cmdGtmCtl), MAXLINE,
			 "gtm_ctl stop -Z gtm -D %s",
			 sval(VAR_gtmSlaveDir));
	return(cmdGtmCtl);
}

int
stop_gtm_slave(void)
{
	cmdList_t *cmdList;
	cmd_t *cmd;
	int rc;

	elog(INFO, "Stop GTM slave\n");
	cmdList = initCmdList();
	if ((cmd = prepare_stopGtmSlave()))
	{
		addCmd(cmdList, cmd);
		rc = doCmdList(cmdList);
		cleanCmdList(cmdList);
		return(rc);
	}
	return 1;
}

/*
 * Kill gtm master -----------------------------------------------------
 *
 * You should not kill gtm master in this way.   This may discard the latest
 * gtm status.  This is just in case.  You must try to stop gtm master
 * gracefully.
 */
static cmd_t *
prepare_killGtmMaster(void)
{
	cmd_t *cmdKill;
	pid_t gtmPid;


	if (is_none(sval(VAR_gtmMasterServer)))
		return(NULL);
	cmdKill = initCmd(sval(VAR_gtmMasterServer));
	gtmPid = get_gtm_pid(sval(VAR_gtmMasterServer), sval(VAR_gtmMasterDir));
	if (gtmPid > 0)
		snprintf(newCommand(cmdKill), MAXLINE,
				 "kill -9 %d; rm -rf /tmp/.s.'*'%d'*' %s/gtm.pid",
				 gtmPid, atoi(sval(VAR_gtmMasterPort)), sval(VAR_gtmMasterDir));
	else
		snprintf(newCommand(cmdKill), MAXLINE,
				 "killall -u %s -9 gtm; rm -rf /tmp/.s.'*'%d'*' %s/gtm.pid",
				 sval(VAR_pgxcUser), atoi(sval(VAR_gtmMasterPort)), sval(VAR_gtmMasterDir));
	return(cmdKill);
}


int
kill_gtm_master(void)
{
	cmdList_t *cmdList;
	cmd_t *cmd_killGtmMaster;
	int rc;

	elog(INFO, "Kill GTM master\n");
	cmdList = initCmdList();
	if ((cmd_killGtmMaster = prepare_killGtmMaster()))
	{
		addCmd(cmdList, cmd_killGtmMaster);
		rc = doCmdList(cmdList);
		cleanCmdList(cmdList);
		return(rc);
	}
	return 1;
}

/*
 * Kill gtm slave --------------------------------------------------------
 *
 * GTM slave has no significant informaion to carry over.  But it is a good
 * habit to stop gtm slave gracefully with stop command.
 */
static cmd_t *
prepare_killGtmSlave(void)
{
	cmd_t *cmdKill;
	pid_t gtmPid;

	if (!isVarYes(VAR_gtmSlave) || (sval(VAR_gtmSlaveServer) == NULL) || is_none(sval(VAR_gtmSlaveServer)))
	{
		elog(ERROR, "ERROR: GTM slave is not configured.\n");
		return(NULL);
	}
	cmdKill = initCmd(sval(VAR_gtmSlaveServer));
	gtmPid = get_gtm_pid(sval(VAR_gtmSlaveServer), sval(VAR_gtmSlaveDir));
	if (gtmPid > 0)
		snprintf(newCommand(cmdKill), MAXLINE,
				 "kill -9 %d; rm -rf /tmp/.s.'*'%d'*' %s/gtm.pid",
				 gtmPid, atoi(sval(VAR_gtmSlavePort)), sval(VAR_gtmSlaveDir));
	else
		snprintf(newCommand(cmdKill), MAXLINE,
				 "killall -u %s -9 gtm; rm -rf /tmp/.s.'*'%d'*' %s/gtm.pid",
				 sval(VAR_pgxcUser), atoi(sval(VAR_gtmSlavePort)), sval(VAR_gtmSlaveDir));
	return(cmdKill);
}


int
kill_gtm_slave(void)
{
	cmdList_t *cmdList;
	cmd_t *cmdKill;
	int rc;

	elog(INFO, "Kill GTM slave\n");
	cmdList = initCmdList();
	if ((cmdKill = prepare_killGtmSlave()))
	{
		addCmd(cmdList, cmdKill);
		rc = doCmdList(cmdList);
		cleanCmdList(cmdList);
		return(rc);
	}
	else return 1;
}

/*
 * Failover the gtm ------------------------------------------------------
 */
int
failover_gtm(void)
{
	char date[MAXTOKEN+1];
	char *stdIn;
	int rc;
	FILE *f;
	
	elog(INFO, "Failover gtm\n");
	if (!isVarYes(VAR_gtmSlave) || (sval(VAR_gtmSlaveServer) == NULL) || is_none(sval(VAR_gtmSlaveServer)))
	{
		elog(ERROR, "ERROR: GTM slave is not configured. Cannot failover.\n");
		return(1);
	}
	
	if (do_gtm_ping(sval(VAR_gtmSlaveServer), atoi(sval(VAR_gtmSlavePort))) != 0)
	{
		elog(ERROR, "ERROR: GTM slave is not running\n");
		return(1);
	}

	/* Promote the slave */
	elog(NOTICE, "Running \"gtm_ctl promote -Z gtm -D %s\"\n", sval(VAR_gtmSlaveDir));
	rc = doImmediate(sval(VAR_gtmSlaveServer), NULL, 
					 "gtm_ctl promote -Z gtm -D %s", sval(VAR_gtmSlaveDir));
	if (WEXITSTATUS(rc) != 0)
	{
		elog(ERROR, "ERROR: could not promote gtm (host:%s, dir:%s)\n", sval(VAR_gtmSlaveServer), sval(VAR_gtmSlaveDir));
		return 1;
	}

	/* Configure promoted gtm */
	if ((f = prepareLocalStdin(newFilename(stdIn), MAXPATH, NULL)) == NULL)
		return(1);
	fprintf(f,
			"#===================================================\n"
			"# Updated due to GTM failover\n"
			"#        %s\n"
			"startup = ACT\n"
			"#----End of reconfiguration -------------------------\n",
			timeStampString(date, MAXTOKEN+1));
	fclose(f);
	elog(NOTICE, "Updating gtm.conf at %s:%s\n", sval(VAR_gtmSlaveServer), sval(VAR_gtmSlaveDir));
	rc = doImmediate(sval(VAR_gtmSlaveServer), stdIn, "cat >> %s/gtm.conf", sval(VAR_gtmSlaveDir));
	if (WEXITSTATUS(rc) != 0)
	{
		elog(ERROR, "ERROR: could not update gtm.conf (host: %s, dir:%s)\n", sval(VAR_gtmSlaveServer), sval(VAR_gtmSlaveDir));
		return 1;
	}

	/* Update and backup configuration file */
	if ((f = prepareLocalStdin(stdIn, MAXPATH, NULL)) == NULL)
		return(1);
	fprintf(f,
			"#===================================================\n"
			"# pgxc configuration file updated due to GTM failover\n"
			"#        %s\n"
			"gtmMasterServer=%s\n"
			"gtmMasterPort=%s\n"
			"gtmMasterDir=%s\n"
			"gtmSlave=n\n"
			"gtmSlaveServer=none\n"
			"gtmSlavePort=0\n"
			"gtmSlaveDir=none\n"
			"#----End of reconfiguration -------------------------\n",
			timeStampString(date, MAXTOKEN+1),
			sval(VAR_gtmSlaveServer),
			sval(VAR_gtmSlavePort),
			sval(VAR_gtmSlaveDir));
	fclose(f);
	rc = doImmediate(NULL, stdIn, "cat >> %s", pgxc_ctl_config_path);
	if (WEXITSTATUS(rc) != 0)
	{
		elog(ERROR, "ERROR: could not update gtm.conf (host: %s, dir:%s)\n", sval(VAR_gtmSlaveServer), sval(VAR_gtmSlaveDir));
		return 1;
	}
	freeAndReset(stdIn);
	backup_configuration();

	/* Reconfigure myself */
	assign_val(VAR_gtmMasterServer, VAR_gtmSlaveServer); reset_var(VAR_gtmSlaveServer);
	assign_val(VAR_gtmMasterPort, VAR_gtmSlavePort); reset_var(VAR_gtmSlavePort);
	assign_val(VAR_gtmMasterDir, VAR_gtmSlaveDir); reset_var(VAR_gtmSlaveDir);
	assign_sval(VAR_gtmSlaveServer, "none");
	assign_sval(VAR_gtmSlavePort, "0");
	assign_sval(VAR_gtmSlaveDir, "none");
	assign_sval(VAR_gtmSlave, "n");

	return 0;
}

/*
 * Clean gtm master resources -- directory and socket --------------------------
 */
cmd_t *
prepare_cleanGtmMaster(void)
{
	cmd_t *cmd;

	/* Remote work dir and clean the socket */
	cmd = initCmd(sval(VAR_gtmMasterServer));
	snprintf(newCommand(cmd), MAXLINE, 
			 "rm -rf %s; mkdir -p %s; chmod 0700 %s;rm -f /tmp/.s.*%d*",
			 sval(VAR_gtmMasterDir), sval(VAR_gtmMasterDir), sval(VAR_gtmMasterDir),
			 atoi(VAR_gtmMasterPort));
	return cmd;
}

int
clean_gtm_master(void)
{
	cmdList_t *cmdList;
	int rc;

	elog(INFO, "Clearing gtm master directory and socket.\n");
	cmdList = initCmdList();
	addCmd(cmdList, prepare_cleanGtmMaster());
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	return(rc);
}

/*
 * Clean gtm master resources -- direcotry and socket --------------------------
 */
/*
 * Null will be retruend if gtm slave is not configured.
 * Be careful.   If you configure gtm slave and gtm master on a same server,
 * bott slave amd master process will be killed.
 */
cmd_t *
prepare_cleanGtmSlave(void)
{
	cmd_t *cmd;
	
	if (!isVarYes(VAR_gtmSlave) || is_none(VAR_gtmSlaveServer))
		return(NULL);
	cmd = initCmd(sval(VAR_gtmSlaveServer));
	snprintf(newCommand(cmd), MAXLINE,
			 "rm -rf %s; mkdir -p %s; chmod 0700 %s;rm -f /tmp/.s.*%d*",
			 sval(VAR_gtmSlaveDir), sval(VAR_gtmSlaveDir), sval(VAR_gtmMasterDir),
			 atoi(VAR_gtmSlavePort));
	return cmd;
}

int
clean_gtm_slave(void)
{
	cmdList_t *cmdList;
	int rc;

	elog(NOTICE, "Clearing gtm slave resources.\n");
	if (!isVarYes(VAR_gtmSlave) || is_none(VAR_gtmSlaveServer))
	{
		elog(ERROR, "ERROR: gtm slave is not configured.\n");
		return 1;
	}
	cmdList = initCmdList();
	addCmd(cmdList, prepare_cleanGtmSlave());
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	elog(NOTICE, "Done.\n");
	return(rc);
}

/*
 * ==================================================================================
 *
 * Gtm Proxy Staff
 *
 * ==================================================================================
 */

/*
 * Add gtm proxy: to be used after all the configuration is done.
 *
 * This function only maintains internal configuration, updte configuration file,
 * and make backup if configured.   You should run init and start it separately.
 */
int
add_gtmProxy(char *name, char *host, int port, char *dir)
{
	char port_s[MAXTOKEN+1];
	char date[MAXTOKEN+1];
	FILE *f;
	char **nodelist = NULL;
	int rc;

	if (is_none(host))
	{
		elog(ERROR, "ERROR: Cannot add gtm proxy with the name \"none\".\n");
		return 1;
	}
	if (is_none(dir))
	{
		elog(ERROR, "ERROR: Cannot add gtm proxy with the directory \"none\".\n");
		return 1;
	}
	if (checkSpecificResourceConflict(name, host, port, dir, TRUE))
	{
		elog(ERROR, "ERROR: New specified name:%s, host:%s, port:%d and dir:\"%s\" conflicts with existing node.\n",
			 name, host, port, dir);
		return 1;
	}
	if (!isVarYes(VAR_gtmProxy))
	{
		assign_sval(VAR_gtmProxy, Strdup("y"));
		reset_var(VAR_gtmProxyNames);
	}
	add_val(find_var(VAR_gtmProxyNames), Strdup(name));
	add_val(find_var(VAR_gtmProxyServers), Strdup(host));
	snprintf(port_s, MAXTOKEN, "%d", port);
	add_val(find_var(VAR_gtmProxyPorts), Strdup(port_s));
	add_val(find_var(VAR_gtmProxyDirs), Strdup(dir));
	add_val(find_var(VAR_gtmPxySpecificExtraConfig), Strdup("none"));
	makeServerList();
	if ((f = fopen(pgxc_ctl_config_path, "a")) == NULL)
	{
		/* Should it be panic? */
		elog(ERROR, "ERROR: cannot open configuration file \"%s\", %s\n", pgxc_ctl_config_path, strerror(errno));
		return 1;
	}
	fprintf(f, 
			"#===================================================\n"
			"# pgxc configuration file updated due to GTM proxy (%s) addition\n"
			"#        %s\n",
			name,
			timeStampString(date, MAXTOKEN+1));
	fprintSval(f, VAR_gtmProxy);
	fprintAval(f, VAR_gtmProxyNames);
	fprintAval(f, VAR_gtmProxyServers);
	fprintAval(f, VAR_gtmProxyPorts);
	fprintAval(f, VAR_gtmProxyDirs);
	fprintAval(f, VAR_gtmPxySpecificExtraConfig);
	fprintf(f, "%s", "#----End of reconfiguration -------------------------\n");
	fclose(f);
	AddMember(nodelist, name);
	init_gtm_proxy(nodelist);
	rc = start_gtm_proxy(nodelist);
	CleanArray(nodelist);
	return rc;
}

int
remove_gtmProxy(char *name, bool clean_opt)
{
	FILE *f;
	int idx;

	/* Check if gtm proxy exists */
	if ((idx = gtmProxyIdx(name)) < 0)
	{
		elog(ERROR, "ERROR: %s is not a gtm proxy.\n", name);
		return 1;
	}
	/* Check if it is in use */
	if (ifExists(VAR_coordMasterServers, aval(VAR_gtmProxyServers)[idx]) ||
		ifExists(VAR_coordSlaveServers, aval(VAR_gtmProxyServers)[idx]) ||
		ifExists(VAR_datanodeMasterServers, aval(VAR_gtmProxyServers)[idx]) ||
		ifExists(VAR_datanodeSlaveServers, aval(VAR_gtmProxyServers)[idx]))
	{
		elog(ERROR, "ERROR: GTM Proxy %s is in use\n", name);
		return 1;
	}
	elog(NOTICE, "NOTICE: removing gtm_proxy %s\n", name);
	/* Clean */
	if (clean_opt)
	{
		char **nodelist = NULL;

		elog(NOTICE, "NOTICE: cleaning target resources.\n");
		AddMember(nodelist, name);
		clean_gtm_proxy(nodelist);
		CleanArray(nodelist);
	}
	/* Reconfigure */
	var_assign(&aval(VAR_gtmProxyNames)[idx], Strdup("none"));
	var_assign(&aval(VAR_gtmProxyServers)[idx], Strdup("none"));
	var_assign(&aval(VAR_gtmProxyPorts)[idx], Strdup("-1"));
	var_assign(&aval(VAR_gtmProxyDirs)[idx], Strdup("none"));
	handle_no_slaves();
	makeServerList();
	/* Update configuration file and backup it */
	if ((f = fopen(pgxc_ctl_config_path, "a")) == NULL)
	{
		/* Should it be panic? */
		elog(ERROR, "ERROR: cannot open configuration file \"%s\", %s\n", pgxc_ctl_config_path, strerror(errno));
		return 1;
	}
	fprintf(f, 
			"#===================================================\n"
			"# pgxc configuration file updated due to GTM proxy addition\n"
			"#        %s\n"
			"%s=%s\n"					/* gtmProxy */
			"%s=( %s )\n"				/* gtmProxyNames */
			"%s=( %s )\n"				/* gtmProxyServers */
			"%s=( %s )\n"				/* gtmProxyPorts */
			"%s=( %s )\n"				/* gtmProxyDirs */
			"#----End of reconfiguration -------------------------\n",
			timeStampString(date, MAXTOKEN+1),
			VAR_gtmProxy, sval(VAR_gtmProxy),
			VAR_gtmProxyNames, listValue(VAR_gtmProxyNames),
			VAR_gtmProxyServers, listValue(VAR_gtmProxyServers),
			VAR_gtmProxyPorts, listValue(VAR_gtmProxyPorts),
			VAR_gtmProxyDirs, listValue(VAR_gtmProxyDirs));
	fclose(f);
	backup_configuration();
	elog(NOTICE, "Done.\n");
	return 0;
}

/* 
 * Does not check if node name is valid.
 */
static cmd_t *
prepare_initGtmProxy(char *nodeName)
{
	cmd_t *cmdInitGtm, *cmdGtmProxyConf;
	int idx;
	FILE *f;
	char timestamp[MAXTOKEN+1];
	char **fileList = NULL;

	if ((idx = gtmProxyIdx(nodeName)) < 0)
	{
		elog(ERROR, "ERROR: Specified name %s is not GTM Proxy configuration.\n", nodeName);
		return NULL;
	}

	/* Build directory and run initgtm */
	cmdInitGtm = initCmd(aval(VAR_gtmProxyServers)[idx]);
	snprintf(newCommand(cmdInitGtm), MAXLINE,
			 "killall -u %s -9 gtm_proxy;"
			 "rm -rf %s;"
			 "mkdir -p %s;"
			 "initgtm -Z gtm_proxy -D %s",
			 sval(VAR_pgxcUser),
			 aval(VAR_gtmProxyDirs)[idx],
			 aval(VAR_gtmProxyDirs)[idx],
			 aval(VAR_gtmProxyDirs)[idx]);

	/* Configure gtm_proxy.conf */
	appendCmdEl(cmdInitGtm, (cmdGtmProxyConf = initCmd(aval(VAR_gtmProxyServers)[idx])));
	if ((f = prepareLocalStdin(newFilename(cmdGtmProxyConf->localStdin), MAXPATH, NULL)) == NULL)
	{
		cleanCmd(cmdInitGtm);
		return NULL;
	}
	fprintf(f,
			"#===========================\n"
			"# Added at initialization, %s\n"
			"listen_addresses = '*'\n"
			"worker_threads = 1\n"
			"gtm_connect_retry_interval = 1\n",
			timeStampString(timestamp, MAXTOKEN));;
	if (!is_none(sval(VAR_gtmPxyExtraConfig)))
		AddMember(fileList, sval(VAR_gtmPxyExtraConfig));
	if (!is_none(aval(VAR_gtmPxySpecificExtraConfig)[idx]))
		AddMember(fileList, aval(VAR_gtmPxySpecificExtraConfig)[idx]);
	appendFiles(f, fileList);
	CleanArray(fileList);
	fprintf(f,			
			"nodename = '%s'\n"
			"port = %s\n"
			"gtm_host = '%s'\n"
			"gtm_port = %s\n"
			"# End of addition\n",
			aval(VAR_gtmProxyNames)[idx],
			aval(VAR_gtmProxyPorts)[idx],
			sval(VAR_gtmMasterServer),
			sval(VAR_gtmMasterPort));
	fclose(f);
	snprintf(newCommand(cmdGtmProxyConf), MAXLINE,
			 "cat >> %s/gtm_proxy.conf", aval(VAR_gtmProxyDirs)[idx]);
	return(cmdInitGtm);
}

/*
 * Initialize gtm proxy -------------------------------------------------------
 */
int
init_gtm_proxy(char **nodeList)
{
	char **actualNodeList;
	int ii;
	int rc;
	cmdList_t *cmdList;
	cmd_t *cmdInitGtmPxy;

	if (!isVarYes(VAR_gtmProxy))
	{
		elog(ERROR, "ERROR: GTM Proxy is not configured.\n");
		return 1;
	}
	actualNodeList = makeActualNodeList(nodeList);
	/* Init and run initgtm */
	cmdList = initCmdList();
	for (ii = 0; actualNodeList[ii]; ii++)
	{
		elog(NOTICE, "Initializing gtm proxy %s.\n", actualNodeList[ii]);
		if ((cmdInitGtmPxy = prepare_initGtmProxy(actualNodeList[ii])))
			addCmd(cmdList, cmdInitGtmPxy);
		else
			elog(WARNING, "WARNING: %s is not a gtm proxy.\n", actualNodeList[ii]);
	}
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	CleanArray(actualNodeList);
	elog(NOTICE, "Done.\n");
	return(rc);
}


int
init_gtm_proxy_all(void)
{
	elog(NOTICE, "Initialize all the gtm proxies.\n");
	if (!isVarYes(VAR_gtmProxy))
	{
		elog(ERROR, "ERROR: GTM Proxy is not configured.\n");
		return(1);
	}
	return(init_gtm_proxy(aval(VAR_gtmProxyNames)));
}

/*
 * Start gtm proxy -----------------------------------------------------------
 */
static cmd_t *
prepare_startGtmProxy(char *nodeName)
{
	cmd_t *cmd;
	int idx;

	if ((idx = gtmProxyIdx(nodeName)) < 0)
	{
		elog(ERROR, "ERROR: Specified name %s is not GTM Proxy\n", nodeName);
		return(NULL);
	}
	cmd = initCmd(aval(VAR_gtmProxyServers)[idx]);
	snprintf(newCommand(cmd), MAXLINE,
			 "killall -u %s -9 gtm_proxy;"
			 "gtm_ctl start -Z gtm_proxy -D %s",
			 sval(VAR_pgxcUser),
			 aval(VAR_gtmProxyDirs)[idx]);
	return(cmd);
}

int
start_gtm_proxy(char **nodeList)
{
	char **actualNodeList;
	int ii;
	int rc;
	cmdList_t *cmdList;

	if (!isVarYes(VAR_gtmProxy))
	{
		elog(ERROR, "ERROR: GTM Proxy is not configured.\n");
		return(1);
	}
	actualNodeList = makeActualNodeList(nodeList);
	/* Init and run initgtm */
	cmdList = initCmdList();
	for (ii = 0; actualNodeList[ii]; ii++)
	{
		cmd_t *cmd;
		elog(NOTICE, "Starting gtm proxy %s.\n", actualNodeList[ii]);
		if ((cmd = prepare_startGtmProxy(actualNodeList[ii])))
			addCmd(cmdList, cmd);
		else
			elog(WARNING, "WARNING: %s is not a gtm proxy.\n", actualNodeList[ii]);
	}
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	CleanArray(actualNodeList);
	elog(NOTICE, "Done.\n");
	return(rc);
}

int
start_gtm_proxy_all(void)
{
	elog(NOTICE, "Starting all the gtm proxies.\n");
	return(start_gtm_proxy(aval(VAR_gtmProxyNames)));
}

/*
 * Stop gtm proxy -------------------------------------------------------------
 */
static cmd_t *
prepare_stopGtmProxy(char *nodeName)
{
	cmd_t *cmd;
	int idx;

	if ((idx = gtmProxyIdx(nodeName)) < 0)
	{
		elog(ERROR, "ERROR: Specified name %s is not GTM Proxy\n", nodeName);
		return NULL;
	}
	cmd = initCmd(aval(VAR_gtmProxyServers)[idx]);
	snprintf(newCommand(cmd), MAXLINE,
			 "gtm_ctl stop -Z gtm_proxy -D %s",
			 aval(VAR_gtmProxyDirs)[idx]);
	return(cmd);
}


int
stop_gtm_proxy(char **nodeList)
{
	char **actualNodeList;
	int ii;
	int rc;
	cmdList_t *cmdList;

	if (!isVarYes(VAR_gtmProxy))
	{
		elog(ERROR, "ERROR: GTM Proxy is not configured.\n");
		return(1);
	}
	actualNodeList = makeActualNodeList(nodeList);
	/* Init and run initgtm */
	cmdList = initCmdList();
	for (ii = 0; actualNodeList[ii]; ii++)
	{
		cmd_t *cmd;

		elog(NOTICE, "Stopping gtm proxy %s.\n", actualNodeList[ii]);
		if ((cmd = prepare_stopGtmProxy(actualNodeList[ii])))
			addCmd(cmdList, cmd);
		else
			elog(WARNING, "WARNING: %s is not a gtm proxy.\n", actualNodeList[ii]);
	}
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	CleanArray(actualNodeList);
	elog(NOTICE, "Done.\n");
	return(rc);
}

int
stop_gtm_proxy_all(void)
{
	elog(NOTICE, "Stopping all the gtm proxies.\n");
	return(stop_gtm_proxy(aval(VAR_gtmProxyNames)));
}

/*
 * Kill gtm proxy -------------------------------------------------------------------
 *
 * Although gtm proxy does not have significant resources to carry over to the next
 * run, it is a good habit to stop gtm proxy with stop command gracefully.
 */
static cmd_t *
prepare_killGtmProxy(char *nodeName)
{
	cmd_t *cmd;
	int idx;
	pid_t gtmPxyPid;

	if ((idx = gtmProxyIdx(nodeName)) < 0)
	{
		elog(ERROR, "ERROR: Specified name %s is not GTM Proxy\n", nodeName);
		return NULL;
	}
	cmd = initCmd(aval(VAR_gtmProxyServers)[idx]);
	gtmPxyPid = get_gtmProxy_pid(aval(VAR_gtmProxyServers)[idx], aval(VAR_gtmProxyDirs)[idx]);
	if (gtmPxyPid > 0)
		snprintf(newCommand(cmd), MAXLINE,
				 "kill -9 %d; rm -rf /tmp/.s.'*'%d'*' %s/gtm_proxy.pid",
				 gtmPxyPid, atoi(aval(VAR_gtmProxyPorts)[idx]), aval(VAR_gtmProxyDirs)[idx]);
	else
		snprintf(newCommand(cmd), MAXLINE,
				 "killall -u %s -9 gtm; rm -rf /tmp/.s.'*'%d'*' %s/gtm_proxy.pid",
				 sval(VAR_pgxcUser), atoi(aval(VAR_gtmProxyPorts)[idx]), aval(VAR_gtmProxyDirs)[idx]);
	return(cmd);
}

int
kill_gtm_proxy(char **nodeList)
{
	char **actualNodeList;
	int ii;
	int rc;
	cmdList_t *cmdList;

	if (!isVarYes(VAR_gtmProxy))
	{
		elog(ERROR, "ERROR: GTM Proxy is not configured.\n");
		return(1);
	}
	actualNodeList = makeActualNodeList(nodeList);
	/* Init and run initgtm */
	cmdList = initCmdList();
	for (ii = 0; actualNodeList[ii]; ii++)
	{
		cmd_t *cmd;

		elog(NOTICE, "Killing process of gtm proxy %s.\n", actualNodeList[ii]);
		if ((cmd = prepare_killGtmProxy(actualNodeList[ii])))
			addCmd(cmdList, cmd);
		else
			elog(WARNING, "WARNING: %s is not a gtm proxy.\n", actualNodeList[ii]);
	}
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	CleanArray(actualNodeList);
	return(rc);
}

int
kill_gtm_proxy_all(void)
{
	elog(NOTICE, "Killing all the gtm proxy processes.\n");
	return(kill_gtm_proxy(aval(VAR_gtmProxyNames)));
}

/*
 * Reconnect to the current GTM master --------------------------------------------------
 *
 * When failed over, the current Master must have been updated.
 * Remember to update gtm_proxy configuration file so that it 
 * connects to the new master at the next start.
 * Please note that we assume GTM has already been failed over.
 * First argument is gtm_proxy nodename
 */
static cmd_t *
prepare_reconnectGtmProxy(char *nodeName)
{
	cmd_t *cmdGtmCtl, *cmdGtmProxyConf;
	int idx;
	FILE *f;
	char date[MAXTOKEN+1];

	if ((idx = gtmProxyIdx(nodeName)) < 0)
	{
		elog(ERROR, "ERROR: Specified name %s is not GTM Proxy\n", nodeName);
		return(NULL);
	}

	/* gtm_ctl reconnect */
	cmdGtmCtl = initCmd(aval(VAR_gtmProxyServers)[idx]);
	snprintf(newCommand(cmdGtmCtl), MAXLINE,
			 "gtm_ctl reconnect -Z gtm_proxy -D %s -o \\\"-s %s -t %s\\\"",
			 aval(VAR_gtmProxyDirs)[idx], sval(VAR_gtmMasterServer), sval(VAR_gtmMasterPort));

	/* gtm_proxy.conf */
	appendCmdEl(cmdGtmCtl, (cmdGtmProxyConf = initCmd(aval(VAR_gtmProxyServers)[idx])));
	if ((f = prepareLocalStdin(newFilename(cmdGtmProxyConf->localStdin), MAXPATH, NULL)) == NULL)
	{
		cleanCmd(cmdGtmCtl);
		return(NULL);
	}
	fprintf(f, 
			"#===================================================\n"
			"# Updated due to GTM Proxy reconnect\n"
			"#        %s\n"
			"gtm_host = '%s'\n"
			"gtm_port = %s\n"
			"#----End of reconfiguration -------------------------\n",
			timeStampString(date, MAXTOKEN),
			sval(VAR_gtmMasterServer),
			sval(VAR_gtmMasterPort));
	fclose(f);
	snprintf(newCommand(cmdGtmProxyConf), MAXLINE,
			 "cat >> %s/gtm_proxy.conf", aval(VAR_gtmProxyDirs)[idx]);
	return(cmdGtmCtl);
}


int
reconnect_gtm_proxy(char **nodeList)
{
	char **actualNodeList;
	int ii;
	int rc;
	cmdList_t *cmdList;

	if (!isVarYes(VAR_gtmProxy))
	{
		elog(ERROR, "GTM Proxy is not configured.\n");
		return(1);
	}
	actualNodeList = makeActualNodeList(nodeList);
	/* Init and run initgtm */
	cmdList = initCmdList();
	for (ii = 0; actualNodeList[ii]; ii++)
	{
		cmd_t *cmd;

		elog(NOTICE, "Reconnecting gtm proxy %s.\n", actualNodeList[ii]);
		if((cmd = prepare_reconnectGtmProxy(actualNodeList[ii])))
			addCmd(cmdList, cmd);
		else
			elog(WARNING, "WARNING: %s is not a gtm proxy.\n", actualNodeList[ii]);
	}
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	CleanArray(actualNodeList);
	elog(NOTICE, "Done.\n");
	return(rc);
}

int
reconnect_gtm_proxy_all(void)
{
	elog(NOTICE, "Reconnecting all the gtm proxies to the new one.\n");
	return(reconnect_gtm_proxy(aval(VAR_gtmProxyNames)));
}

/*
 * Cleanup -- nodeName must be valid.   Instead, NULL will bereturned.
 */
cmd_t *
prepare_cleanGtmProxy(char *nodeName)
{
	cmd_t *cmd;
	int   idx;
	
	if ((idx = gtmProxyIdx(nodeName)) < 0)
		return NULL;
	cmd = initCmd(aval(VAR_gtmProxyServers)[idx]);
	snprintf(newCommand(cmd), MAXLINE,
			 "rm -rf %s; mkdir -p %s; chmod 0700 %s;rm -f /tmp/.s.*%d*",
			 aval(VAR_gtmProxyDirs)[idx], aval(VAR_gtmProxyDirs)[idx], aval(VAR_gtmProxyDirs)[idx], 
			 atoi(aval(VAR_gtmProxyPorts)[idx]));
	return cmd;
}

int
clean_gtm_proxy(char **nodeList)
{
	char **actualNodeList;
	cmdList_t *cmdList;
	cmd_t *cmd;
	int ii;
	int rc;

	actualNodeList = makeActualNodeList(nodeList);
	cmdList = initCmdList();
	for (ii = 0; actualNodeList[ii]; ii++)
	{
		elog(NOTICE, "Clearing resources for gtm_proxy %s.\n", actualNodeList[ii]);
		if ((cmd = prepare_cleanGtmProxy(actualNodeList[ii])))
			addCmd(cmdList, cmd);
		else
			elog(WARNING, "%s is not a gtm proxy.\n", actualNodeList[ii]);
	}
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	CleanArray(actualNodeList);
	elog(NOTICE, "Done.\n");
	return(rc);
}

int
clean_gtm_proxy_all(void)
{
	elog(NOTICE, "Clearing all the gtm_proxy resources.\n");
	return(clean_gtm_proxy(aval(VAR_gtmProxyNames)));
}

/*
 * configuration --------------------------------------------------------------------
 */
int
show_config_gtmMaster(int flag, char *hostname)
{
	char lineBuf[MAXLINE+1];
	char editBuf[MAXPATH+1];

	lineBuf[0] = 0;
	if (flag)
		strncat(lineBuf, "GTM Master: ", MAXLINE);
	if (hostname)
	{
		snprintf(editBuf, MAXPATH, "host: %s", hostname);
		strncat(lineBuf, editBuf, MAXLINE);
	}
	if (flag || hostname)
		strncat(lineBuf, "\n", MAXLINE);
	lockLogFile();
	if (lineBuf[0])
		elog(NOTICE, "%s", lineBuf);
	print_simple_node_info(sval(VAR_gtmName), sval(VAR_gtmMasterPort), sval(VAR_gtmMasterDir),
						   sval(VAR_gtmExtraConfig), sval(VAR_gtmMasterSpecificExtraConfig));
	unlockLogFile();
	return 0;
}

int
show_config_gtmSlave(int flag, char *hostname)
{
	char lineBuf[MAXLINE+1];
	char editBuf[MAXPATH+1];

	if (!isVarYes(VAR_gtmSlave) || is_none(VAR_gtmSlaveServer))
	{
		elog(ERROR, "ERROR: gtm slave is not configured.\n");
		return 0;
	}
	lineBuf[0] = 0;
	if (flag)
		strncat(lineBuf, "GTM Slave: ", MAXLINE);
	if (hostname)
	{
		snprintf(editBuf, MAXPATH, "host: %s", hostname);
		strncat(lineBuf, editBuf, MAXLINE);
	}
	if (flag || hostname)
		strncat(lineBuf, "\n", MAXLINE);
	lockLogFile();
	elog(NOTICE, "%s", lineBuf);
	print_simple_node_info(sval(VAR_gtmName), sval(VAR_gtmSlavePort), sval(VAR_gtmSlaveDir),
						   sval(VAR_gtmExtraConfig), sval(VAR_gtmSlaveSpecificExtraConfig));
	unlockLogFile();
	return 0;
}

int
show_config_gtmProxies(char **nameList)
{
	int ii;

	lockLogFile();
	for(ii = 0; nameList[ii]; ii++)
		show_config_gtmProxy(TRUE, ii, aval(VAR_gtmProxyServers)[ii]);
	unlockLogFile();
	return 0;
}

int
show_config_gtmProxy(int flag, int idx, char *hostname)
{
	char lineBuf[MAXLINE+1];
	char editBuf[MAXPATH+1];

	lineBuf[0] = 0;
	if (flag)
		strncat(lineBuf, "GTM Proxy: ", MAXLINE);
	if (hostname)
	{
		snprintf(editBuf, MAXPATH, "host: %s", hostname);
		strncat(lineBuf, editBuf, MAXLINE);
	}
	if (flag || hostname)
		strncat(lineBuf, "\n", MAXLINE);
	lockLogFile();
	if (lineBuf[0])
		elog(NOTICE, "%s", lineBuf);
	print_simple_node_info(aval(VAR_gtmProxyNames)[idx], aval(VAR_gtmProxyPorts)[idx],
						   aval(VAR_gtmProxyDirs)[idx], sval(VAR_gtmPxyExtraConfig),
						   aval(VAR_gtmPxySpecificExtraConfig)[idx]);
	unlockLogFile();
	return 0;
}
