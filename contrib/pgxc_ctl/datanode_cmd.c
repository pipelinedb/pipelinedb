/*-------------------------------------------------------------------------
 *
 * datanode_cmd.c
 *
 *    Datanode command module of Postgres-XC configuration and operation tool.
 *
 * Copyright (c) 2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
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
#include "datanode_cmd.h"
#include "gtm_util.h"
#include "coord_cmd.h"

static char date[MAXTOKEN+1];

/* Static functions */
static cmd_t *prepare_initDatanodeMaster(char *nodeName);
static cmd_t *prepare_initDatanodeSlave(char *nodeName);
static cmd_t *prepare_startDatanodeMaster(char *nodeName);
static cmd_t *prepare_startDatanodeSlave(char *nodeName);
static cmd_t *prepare_stopDatanodeMaster(char *nodeName, char *immediate);
static cmd_t *prepare_stopDatanodeSlave(char *nodeName, char *immediate);
static cmd_t *prepare_killDatanodeMaster(char *nodeName);
static cmd_t *prepare_killDatanodeSlave(char *nodeName);

/*
 *======================================================================
 *
 * Datanode staff
 *
 *=====================================================================
 */
static int failover_oneDatanode(int datanodeIdx);

/*
 * Initialize datanode master ------------------------------------
 */
int
init_datanode_master_all(void)
{
	elog(NOTICE, "Initialize all the datanode masters.\n");
	return(init_datanode_master(aval(VAR_datanodeNames)));
}

static cmd_t *
prepare_initDatanodeMaster(char *nodeName)
{
	int idx;
	int jj;
	cmd_t *cmd, *cmdInitdb, *cmdPgConf, *cmdPgHba;
	char *gtmHost;
	char *gtmPort;
	int gtmIdx;
	char **fileList = NULL;
	FILE *f;
	char timeStamp[MAXTOKEN+1];

	if ((idx = datanodeIdx(nodeName)) < 0)
		return(NULL);
	/* Build each coordinator's initialize command */
	cmd = cmdInitdb = initCmd(aval(VAR_datanodeMasterServers)[idx]);
	snprintf(newCommand(cmdInitdb), MAXLINE,
			 "rm -rf %s; mkdir -p %s; initdb --nodename %s -D %s",
			 aval(VAR_datanodeMasterDirs)[idx], aval(VAR_datanodeMasterDirs)[idx],
			 aval(VAR_datanodeNames)[idx], aval(VAR_datanodeMasterDirs)[idx]);
		
	/* Initialize postgresql.conf */
	appendCmdEl(cmdInitdb, (cmdPgConf = initCmd(aval(VAR_datanodeMasterServers)[idx])));
	snprintf(newCommand(cmdPgConf), MAXLINE,
			 "cat >> %s/postgresql.conf",
			 aval(VAR_datanodeMasterDirs)[idx]);
	if ((f = prepareLocalStdin((cmdPgConf->localStdin = Malloc(MAXPATH+1)), MAXPATH, NULL)) == NULL)
	{
		cleanCmd(cmd);
		return(NULL);
	}
	fprintf(f,
			"#===========================================\n"
			"# Added at initialization. %s\n"
			"log_destination = 'stderr'\n"
			"logging_collector = on\n"
			"log_directory = 'pg_log'\n"
			"listen_addresses = '*'\n"
			"max_connections = 100\n",
			timeStampString(timeStamp, MAXTOKEN));
	if (!is_none(sval(VAR_datanodeExtraConfig)))
		AddMember(fileList, sval(VAR_datanodeExtraConfig));
	if (!is_none(aval(VAR_datanodeSpecificExtraConfig)[idx]))
		AddMember(fileList, aval(VAR_datanodeSpecificExtraConfig)[idx]);
	appendFiles(f, fileList);
	CleanArray(fileList);
	freeAndReset(fileList);
	gtmIdx = getEffectiveGtmProxyIdxFromServerName(aval(VAR_datanodeMasterServers)[idx]);
	gtmHost = (gtmIdx < 0) ? sval(VAR_gtmMasterServer) : aval(VAR_gtmProxyServers)[gtmIdx];
	gtmPort = (gtmIdx < 0) ? sval(VAR_gtmMasterPort) : aval(VAR_gtmProxyPorts)[gtmIdx];
	fprintf(f,
			"port = %s\n"
			"gtm_host = '%s'\n"
			"gtm_port = %s\n",
			aval(VAR_datanodePorts)[idx],
			gtmHost, gtmPort);
	fclose(f);
	
	/* Additional Initialization for log_shipping */
	if (isVarYes(VAR_datanodeSlave) && !is_none(aval(VAR_datanodeSlaveServers)[idx]))
	{
		cmd_t *cmd_cleanDir, *cmd_PgConf;
		/* This coordinator has a slave */

		/* Build archive log target */
		appendCmdEl(cmdInitdb, (cmd_cleanDir = initCmd(aval(VAR_datanodeSlaveServers)[idx])));
		snprintf(newCommand(cmd_cleanDir), MAXLINE,
				 "rm -rf %s;mkdir -p %s; chmod 0700 %s",
				 aval(VAR_datanodeArchLogDirs)[idx], aval(VAR_datanodeArchLogDirs)[idx],
				 aval(VAR_datanodeArchLogDirs)[idx]);

		/* postgresql.conf */
		appendCmdEl(cmdInitdb, (cmd_PgConf = initCmd(aval(VAR_datanodeMasterServers)[idx])));
		snprintf(newCommand(cmd_PgConf), MAXLINE,
				 "cat >> %s/postgresql.conf", aval(VAR_datanodeMasterDirs)[idx]);
		if ((f = prepareLocalStdin(newFilename(cmd_PgConf->localStdin), MAXPATH, NULL)) == NULL)
		{
			cleanCmd(cmd);
			return(NULL);
		}
		fprintf(f,
				"wal_level = hot_standby\n"
				"archive_mode = on\n"
				"archive_command = 'rsync %%p %s@%s:%s/%%f'\n"
				"max_wal_senders = %s\n"
				"# End of Addition\n",
				sval(VAR_pgxcUser), aval(VAR_datanodeSlaveServers)[idx], aval(VAR_datanodeArchLogDirs)[idx],
				aval(VAR_datanodeMaxWALSenders)[idx]);
		fclose(f);
	}
	else
	{
		cmd_t *cmd_PgConf;
		appendCmdEl(cmdInitdb, (cmd_PgConf = initCmd(aval(VAR_datanodeMasterServers)[idx])));
		snprintf(newCommand(cmd_PgConf), MAXLINE, 
				 "cat >> %s/postgresql.conf", aval(VAR_datanodeMasterDirs)[idx]);
		if ((f = prepareLocalStdin(newFilename(cmd_PgConf->localStdin), MAXPATH, NULL)) == NULL)
		{
			cleanCmd(cmd);
			return(NULL);
		}
		fprintf(f, "# End of Addition\n");
		fclose(f);
	}
		
	/* pg_hba.conf */
	appendCmdEl(cmdInitdb, (cmdPgHba = initCmd(aval(VAR_datanodeMasterServers)[idx])));
	snprintf(newCommand(cmdPgHba), MAXLINE,
			 "cat >> %s/pg_hba.conf", aval(VAR_datanodeMasterDirs)[idx]);
	if ((f = prepareLocalStdin(newFilename(cmdPgHba->localStdin), MAXPATH, NULL)) == NULL)
	{
		cleanCmd(cmd);
		return(NULL);
	}
	fprintf(f,
			"#=================================================\n"
			"# Addition at initialization, %s\n",
			timeStampString(timeStamp, MAXTOKEN));
	if (!is_none(sval(VAR_datanodeExtraPgHba)))
		AddMember(fileList, sval(VAR_datanodeExtraPgHba));
	if (!is_none(aval(VAR_datanodeSpecificExtraPgHba)[idx]))
		AddMember(fileList, aval(VAR_datanodeSpecificExtraPgHba)[idx]);
	appendFiles(f, fileList);
	CleanArray(fileList);
	for (jj = 0; aval(VAR_datanodePgHbaEntries)[jj]; jj++)
	{
		fprintf(f, 
				"host all %s %s trust\n", 
				sval(VAR_pgxcOwner), aval(VAR_datanodePgHbaEntries)[jj]);
		if (isVarYes(VAR_datanodeSlave))
			if (!is_none(aval(VAR_datanodeSlaveServers)[idx]))
				fprintf(f,
						"host replication %s %s trust\n",
						sval(VAR_pgxcOwner), aval(VAR_datanodePgHbaEntries)[jj]);
	}
	fprintf(f, "# End of additon\n");
	fclose(f);
	return(cmd);
}


int
init_datanode_master(char **nodeList)
{
	int ii;
	cmdList_t *cmdList;
	cmd_t *cmd;
	char **actualNodeList;
	int rc;

	actualNodeList = makeActualNodeList(nodeList);
	cmdList = initCmdList();
	for(ii = 0; actualNodeList[ii]; ii++)
	{
		elog(INFO, "Initialize the datanode master %s.\n", actualNodeList[ii]);
		if ((cmd = prepare_initDatanodeMaster(actualNodeList[ii])))
			addCmd(cmdList, cmd);
	}
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	CleanArray(actualNodeList);
	elog(NOTICE, "Done.\n");
	return(rc);
}

/*
 * Initialize datanode slave ----------------------------------------------------
 */
int
init_datanode_slave_all(void)
{
	elog(INFO, "Initialize all the datanode slaves.\n");
	return(init_datanode_slave(aval(VAR_datanodeNames)));
}

static cmd_t *
prepare_initDatanodeSlave(char *nodeName)
{
	cmd_t *cmd, *cmdBuildDir, *cmdStartMaster, *cmdBaseBkup, *cmdRecovConf, *cmdPgConf, *cmdStopMaster;
	FILE *f;
	int idx;
	int startMaster;
	char timestamp[MAXTOKEN+1];
	
	if ((idx = datanodeIdx(nodeName)) < 0)
	{
		elog(WARNING, "WARNING: specified node %s is not datanode. skipping.\n", nodeName);
		return(NULL);
	}
	startMaster = FALSE;
	/* Check if the datanode master is running */
	if (pingNode(aval(VAR_datanodeMasterServers)[idx], aval(VAR_datanodePorts)[idx]) != 0)
		startMaster = TRUE;

	/* Build slave's directory -1- */
	cmd = cmdBuildDir = initCmd(aval(VAR_datanodeSlaveServers)[idx]);
	snprintf(newCommand(cmdBuildDir), MAXLINE,
			 "rm -rf %s;mkdir -p %s; chmod 0700 %s",
			 aval(VAR_datanodeSlaveDirs)[idx], aval(VAR_datanodeSlaveDirs)[idx],
			 aval(VAR_datanodeSlaveDirs)[idx]);

	/* Start datanode master if it is not running -2- */
	if (startMaster)
	{
		appendCmdEl(cmdBuildDir, (cmdStartMaster = prepare_startDatanodeMaster(nodeName)));
	}

	/* Obtain base backup of the master */
	appendCmdEl(cmdBuildDir, (cmdBaseBkup = initCmd(aval(VAR_datanodeSlaveServers)[idx])));
	snprintf(newCommand(cmdBaseBkup), MAXLINE, 
			 "pg_basebackup -p %s -h %s -D %s -x",
			 aval(VAR_datanodePorts)[idx], aval(VAR_datanodeMasterServers)[idx],
			 aval(VAR_datanodeSlaveDirs)[idx]);

	/* Configure recovery.conf of the slave */
	appendCmdEl(cmdBuildDir, (cmdRecovConf = initCmd(aval(VAR_datanodeSlaveServers)[idx])));
	snprintf(newCommand(cmdRecovConf), MAXLINE, 
			 "cat >> %s/recovery.conf",
			 aval(VAR_datanodeSlaveDirs)[idx]);
	if ((f = prepareLocalStdin(newFilename(cmdRecovConf->localStdin), MAXPATH, NULL)) == NULL)
	{
		cleanCmd(cmd);
		return(NULL);
	}
	fprintf(f,
			"#==========================================\n"
			"# Added to initialize the slave, %s\n"
			"standby_mode = on\n"
			"primary_conninfo = 'host = %s port = %s user = %s application_name = %s'\n"
			"restore_command = 'cp %s/%%f %%p'\n"
			"archive_cleanup_command = 'pg_archivecleanup %s %%r'\n",		
			timeStampString(timestamp, MAXTOKEN),
			aval(VAR_datanodeMasterServers)[idx], aval(VAR_datanodePorts)[idx], 
			sval(VAR_pgxcOwner), aval(VAR_datanodeNames)[idx],
			aval(VAR_datanodeArchLogDirs)[idx],
			aval(VAR_datanodeArchLogDirs)[idx]);
	fclose(f);

	/* Configure slave's postgresql.conf */
	appendCmdEl(cmdBuildDir, (cmdPgConf = initCmd(aval(VAR_datanodeSlaveServers)[idx])));
	snprintf(newCommand(cmdPgConf), MAXPATH, 
			 "cat >> %s/postgresql.conf",
			 aval(VAR_datanodeSlaveDirs)[idx]);
	if ((f = prepareLocalStdin(newFilename(cmdPgConf->localStdin), MAXPATH, NULL)) == NULL)
	{
		cleanCmd(cmd);
		return(NULL);
	}
	fprintf(f,
			"#==========================================\n"
			"# Added to startup the slave, %s\n"
			"hot_standby = on\n"
			"port = %s\n"
			"# End of addition\n",
			timeStampString(timestamp, MAXTOKEN),
			aval(VAR_datanodePorts)[idx]);
	fclose(f);

	/* Stp datanode master if needed */
	if (startMaster == TRUE)
		appendCmdEl(cmdBuildDir, (cmdStopMaster = prepare_stopDatanodeMaster(aval(VAR_datanodeNames)[idx], FAST)));
	return(cmd);
}

int
init_datanode_slave(char **nodeList)
{
	int ii;
	int rc;
	cmdList_t *cmdList;
	cmd_t *cmd;
	char **actualNodeList;

	if (!isVarYes(VAR_datanodeSlave))
	{
		elog(ERROR, "ERROR: datanode slave is not configured.\n");
		return 1;
	}
	actualNodeList = makeActualNodeList(nodeList);
	cmdList = initCmdList();
	for (ii = 0; actualNodeList[ii]; ii++)
	{
		elog(INFO, "Initialize datanode slave %s\n", actualNodeList[ii]);
		if ((cmd = prepare_initDatanodeSlave(actualNodeList[ii])))
			addCmd(cmdList, cmd);
	}
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	CleanArray(actualNodeList);
	return(rc);
}

/*
 * Start datanode master --------------------------------------------------
 */
int
start_datanode_master_all(void)
{
	elog(INFO, "Starting all the datanode masters.\n");
	return(start_datanode_master(aval(VAR_datanodeNames)));
}

static cmd_t *
prepare_startDatanodeMaster(char *nodeName)
{
	cmd_t *cmdStartDatanodeMaster = NULL;
	int idx;

	if ((idx = datanodeIdx(nodeName)) < 0)
	{
		elog(WARNING, "WARNING: %s is not a datanode, skipping\n", nodeName);
		return(NULL);
	}
	/* Check if the target is running */
	if (pingNode(aval(VAR_datanodeMasterServers)[idx], aval(VAR_datanodePorts)[idx]) == 0)
	{
		elog(WARNING, "WARNING: datanode master %s is running now. Skipping.\n",
			 aval(VAR_datanodeNames)[idx]);
		cleanCmd(cmdStartDatanodeMaster);
		return(NULL);
	}
	cmdStartDatanodeMaster = initCmd(aval(VAR_datanodeMasterServers)[idx]);
	snprintf(newCommand(cmdStartDatanodeMaster), MAXLINE,
			 "pg_ctl start -Z datanode -D %s -o -i -w", aval(VAR_datanodeMasterDirs)[idx]);
	return(cmdStartDatanodeMaster);
}

int
start_datanode_master(char **nodeList)
{
	int ii;
	int rc;
	cmdList_t *cmdList;
	cmd_t *cmd;
	char **actualNodeList;

	actualNodeList = makeActualNodeList(nodeList);

	cmdList = initCmdList();
	for (ii = 0; actualNodeList[ii]; ii++)
	{
		elog(INFO, "Starting datanode master %s.\n", actualNodeList[ii]);
		if ((cmd = prepare_startDatanodeMaster(actualNodeList[ii])))
			addCmd(cmdList, cmd);
	}
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	CleanArray(actualNodeList);
	elog(NOTICE, "Done.\n");
	return(rc);
}

/*
 * Start datanode slave --------------------------------------------------
 */
int
start_datanode_slave_all(void)
{
	elog(INFO, "Starting all the datanode slaves.\n");
	return(start_datanode_slave(aval(VAR_datanodeNames)));
}

static cmd_t *
prepare_startDatanodeSlave(char *nodeName)
{
	cmd_t *cmd, *cmdStartDatanodeSlave, *cmdMasterToSyncMode;
	FILE *f;
	int idx;
	char timestamp[MAXTOKEN+1];
	
	/* If the node really a datanode? */
	if((idx = datanodeIdx(nodeName)) < 0)
	{
		elog(WARNING, "WARNING: node %s is not a datanode. Skipping\n", nodeName);
		return(NULL);
	}
	/* Check if the datanode master is running */
	if (pingNode(aval(VAR_datanodeMasterServers)[idx], aval(VAR_datanodePorts)[idx]) != 0)
	{
		elog(WARNING, "WARNING: master of the datanode %s is not running. Skipping\n", nodeName);
		return(NULL);
	}
	
	cmd = cmdStartDatanodeSlave = initCmd(aval(VAR_datanodeSlaveServers)[idx]);
	snprintf(newCommand(cmdStartDatanodeSlave), MAXLINE,
			 "pg_ctl start -Z datanode -D %s -w",
			 aval(VAR_datanodeSlaveDirs)[idx]);
	
	/* Change the master to synchronous mode */
	appendCmdEl(cmdStartDatanodeSlave, (cmdMasterToSyncMode = initCmd(aval(VAR_datanodeMasterServers)[idx])));
	snprintf(newCommand(cmdMasterToSyncMode), MAXLINE,
			 "cat >> %s/postgresql.conf",
			 aval(VAR_datanodeMasterDirs)[idx]);
	if ((f = prepareLocalStdin(newFilename(cmdMasterToSyncMode->localStdin), MAXPATH, NULL)) == NULL)
	{
		cleanCmd(cmd);
		return(NULL);
	}
	fprintf(f,
			"#==========================================================\n"
			"# Added to start the slave in sync. mode, %s\n"
			"synchronous_commit = on\n"
			"synchronous_standby_names = '%s'\n"
			"# End of the addition\n",
			timeStampString(timestamp, MAXTOKEN),
			aval(VAR_datanodeNames)[idx]);
	fclose(f);
	return(cmd);
}

int
start_datanode_slave(char **nodeList)
{
	int ii;
	int rc;
	cmdList_t *cmdList;
	char **actualNodeList;

	if (!isVarYes(VAR_datanodeSlave))
	{
		elog(ERROR, "ERROR: datanode slave is not configured.\n");
		return 1;
	}
	cmdList = initCmdList();
	actualNodeList = makeActualNodeList(nodeList);
	for (ii = 0; actualNodeList[ii]; ii++)
	{
		cmd_t *cmd;

		elog(INFO, "Starting datanode slave %s.\n", actualNodeList[ii]);
		if ((cmd = prepare_startDatanodeSlave(actualNodeList[ii])))
			addCmd(cmdList, cmd);
	}
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	CleanArray(actualNodeList);
	elog(NOTICE, "Done.\n");
	return(rc);
}


/*
 * Stop datanode master ------------------------------------------------
 */
static cmd_t *
prepare_stopDatanodeMaster(char *nodeName, char *immediate)
{
	cmd_t *cmdStopDatanodeMaster;
	int idx;
	
	if ((idx = datanodeIdx(nodeName)) < 0)
	{
		elog(WARNING, "WARNING: %s is not a datande. Skipping\n", nodeName);
		return(NULL);
	}
	cmdStopDatanodeMaster = initCmd(aval(VAR_datanodeMasterServers)[idx]);
	if (immediate)
		snprintf(newCommand(cmdStopDatanodeMaster), MAXLINE,
				 "pg_ctl stop -Z datanode -D %s -m %s",
				 aval(VAR_datanodeMasterDirs)[idx], immediate);
	else
		snprintf(newCommand(cmdStopDatanodeMaster), MAXLINE,
				 "pg_ctl stop -Z datanode -D %s",
				 aval(VAR_datanodeMasterDirs)[idx]);
	return(cmdStopDatanodeMaster);
}


int
stop_datanode_master_all(char *immediate)
{
	elog(INFO, "Stopping all the datanode masters.\n");
	return(stop_datanode_master(aval(VAR_datanodeNames), immediate));
}


int
stop_datanode_master(char **nodeList, char *immediate)
{
	int ii;
	int rc;
	cmdList_t *cmdList;
	cmd_t *cmd;
	char **actualNodeList;

	actualNodeList = makeActualNodeList(nodeList);
	cmdList = initCmdList();
	for(ii = 0; actualNodeList[ii]; ii++)
	{
		elog(INFO, "Stopping datanode master %s.\n", actualNodeList[ii]);
		if ((cmd = prepare_stopDatanodeMaster(actualNodeList[ii], immediate)))
			addCmd(cmdList, cmd);
	}
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	CleanArray(actualNodeList);
	elog(NOTICE, "Done.\n");
	return(rc);
}


/*
 * Stop datanode slave --------------------------------------------------------
 */
static cmd_t *
prepare_stopDatanodeSlave(char *nodeName, char *immediate)
{
	int idx;
	cmd_t *cmd, *cmdMasterToAsyncMode, *cmdStopSlave;
	FILE *f;
	char timestamp[MAXTOKEN+1];

	if ((idx = datanodeIdx(nodeName)) < 0)
	{
		elog(WARNING, "%s is not a datanode. Skipping\n", nodeName);
		return(NULL);
	}
	if (!doesExist(VAR_datanodeSlaveServers, idx) || is_none(aval(VAR_datanodeSlaveServers)[idx]))
	{
		elog(WARNING, "datanode %s does not have a slave. Skipping.\n", nodeName);
		return(NULL);
	}
	/* Set the master to asynchronous mode */
	cmd = cmdMasterToAsyncMode = initCmd(aval(VAR_datanodeMasterServers)[idx]);
	snprintf(newCommand(cmdMasterToAsyncMode), MAXLINE,
			 "cat >> %s/postgresql.conf",
			 aval(VAR_datanodeMasterDirs)[idx]);
	if ((f = prepareLocalStdin(newFilename(cmdMasterToAsyncMode->localStdin), MAXPATH, NULL)) == NULL)
	{
		cleanCmd(cmd);
		return(NULL);
	}
	fprintf(f,
			"#=======================================\n"
			"# Updated to trun off the slave %s\n"
			"synchronous_standby_names = ''\n"
			"# End of the update\n",
			timeStampString(timestamp, MAXTOKEN));
	fclose(f);

	/* Reload new config file if the master is running */
	/* The next step might need improvement.  When GTM is dead, the following may
	 * fail even though the master is running.
	 */
	if (pingNode(aval(VAR_datanodeSlaveServers)[idx], aval(VAR_datanodePorts)[idx]) == 0)
	{
		cmd_t *cmdReloadMaster;

		appendCmdEl(cmdMasterToAsyncMode, (cmdReloadMaster = initCmd(aval(VAR_datanodeMasterServers)[idx])));
		snprintf(newCommand(cmdReloadMaster), MAXLINE,
				 "pg_ctl reload -Z datanode -D %s",
				 aval(VAR_datanodeMasterDirs)[idx]);
	}

	/* Stop the slave */
	appendCmdEl(cmdMasterToAsyncMode, (cmdStopSlave = initCmd(aval(VAR_datanodeSlaveServers)[idx])));
	if (immediate)
		snprintf(newCommand(cmdStopSlave), MAXLINE,
				 "pg_ctl stop -Z datanode -D %s -m %s", aval(VAR_datanodeSlaveDirs)[idx], immediate);
	else
		snprintf(newCommand(cmdStopSlave), MAXLINE,
				 "pg_ctl stop -Z datanode -D %s", aval(VAR_datanodeSlaveDirs)[idx]);
	return(cmd);
}


int
stop_datanode_slave_all(char *immediate)
{
	elog(INFO, "Stopping all the datanode slaves.\n");
	return(stop_datanode_slave(aval(VAR_datanodeNames), immediate));
}

int
stop_datanode_slave(char **nodeList, char *immediate)
{
	int ii;
	int rc;
	cmdList_t *cmdList;
	cmd_t *cmd;
	char **actualNodeList;

	if (!isVarYes(VAR_datanodeSlave))
	{
		elog(ERROR, "Datanode slave is not configured.  Returning.\n");
		return 1;
	}
	actualNodeList = makeActualNodeList(nodeList);
	cmdList = initCmdList();
	for (ii = 0; actualNodeList[ii]; ii++)
	{
		elog(INFO, "Stopping datanode slave %s.\n", actualNodeList[ii]);
		if ((cmd = prepare_stopDatanodeSlave(actualNodeList[ii], immediate)))
			addCmd(cmdList, cmd);
	}
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	CleanArray(actualNodeList);
	return(rc);
}

/*
 * Failover datanode ---------------------------------------------------------
 */
int
failover_datanode(char **nodeList)
{
	int ii;
	char **actualNodeList;
	int rc = 0;

	elog(INFO, "Failover specified datanodes.\n");
	if (!isVarYes(VAR_datanodeSlave))
	{
		elog(ERROR, "ERROR: datnaode slave is not configured.\n");
		return 1;
	}
	actualNodeList = makeActualNodeList(nodeList);
	for (ii = 0; actualNodeList[ii]; ii++)
	{
		int idx;
		int rc_local;

		elog(INFO, "Failover the datanode %s.\n", actualNodeList[ii]);
		if ((idx = datanodeIdx(actualNodeList[ii])) < 0)
		{
			elog(ERROR, "ERROR: %s is not a datanode. Skipping.\n", actualNodeList[ii]);
			continue;
		}
		if (is_none(aval(VAR_datanodeSlaveServers)[idx]))
		{
			elog(ERROR, "ERROR: slave of the datanode %s is not configured. Skipping\n",
				 actualNodeList[ii]);
			continue;
		}
		rc_local = failover_oneDatanode(idx);
		if (rc_local < 0)
			return(rc_local);
		else
			if (rc_local > rc)
				rc = rc_local;
	}
	elog(INFO, "Done.\n");
	return(rc);
}

static int
failover_oneDatanode(int datanodeIdx)
{
	int rc = 0;
	int rc_local;
	int jj;
	char *gtmHost;
	char *gtmPort;
	int gtmPxyIdx;
	FILE *f;
	char timestamp[MAXTOKEN+1];

#	define checkRc() do{if(WEXITSTATUS(rc_local) > rc) rc = WEXITSTATUS(rc_local);}while(0)

	/*
	 * Determine the target GTM
	 */
	gtmPxyIdx = getEffectiveGtmProxyIdxFromServerName(aval(VAR_datanodeSlaveServers)[datanodeIdx]);
	gtmHost = (gtmPxyIdx >= 0) ? aval(VAR_gtmProxyServers)[gtmPxyIdx] : sval(VAR_gtmMasterServer);
	gtmPort = (gtmPxyIdx >= 0) ? aval(VAR_gtmProxyPorts)[gtmPxyIdx] : sval(VAR_gtmMasterPort);
	if (gtmPxyIdx >= 0)
		elog(NOTICE, "Failover datanode %s using gtm %s\n",
			 aval(VAR_datanodeNames)[datanodeIdx], aval(VAR_gtmProxyNames)[gtmPxyIdx]);
	else
		elog(NOTICE, "Filover database %s using GTM itself\n",
			 aval(VAR_datanodeNames)[datanodeIdx]);

	/* Unregister the datanode */
	unregister_datanode(aval(VAR_datanodeNames)[datanodeIdx]);

	/* Promote the slave */
	rc_local = doImmediate(aval(VAR_datanodeSlaveServers)[datanodeIdx], NULL,
						   "pg_ctl promote -Z datanode -D %s",
						   aval(VAR_datanodeSlaveDirs)[datanodeIdx]);
	checkRc();

	/* Reconfigure new datanode master with new gtm_proxy or gtm */
	if ((f =  pgxc_popen_w(aval(VAR_datanodeSlaveServers)[datanodeIdx],
						   "cat >> %s/postgresql.conf",
						   aval(VAR_datanodeSlaveDirs)[datanodeIdx])) == NULL)
	{
		elog(ERROR, "ERROR: Could not prepare to update postgresql.conf, %s", strerror(errno));
		return(-1);
	}
	fprintf(f,
			"#=================================================\n"
			"# Added to promote, %s\n"
			"gtm_host = '%s'\n"
			"gtm_port = %s\n"
			"# End of addition\n",
			timeStampString(timestamp, MAXTOKEN),
			gtmHost, gtmPort);
	fclose(f);

	/* Restart datanode slave (as the new master) */
	rc_local = doImmediate(aval(VAR_datanodeSlaveServers)[datanodeIdx], NULL,
						   "pg_ctl restart -w -Z datanode -D %s -o -i; sleep 1",
						   aval(VAR_datanodeSlaveDirs)[datanodeIdx]);
	checkRc();
	/*
	 * Update the configuration variable
	 */
	var_assign(&(aval(VAR_datanodeMasterServers)[datanodeIdx]), Strdup(aval(VAR_datanodeSlaveServers)[datanodeIdx]));
	var_assign(&(aval(VAR_datanodeSlaveServers)[datanodeIdx]), Strdup("none"));
	var_assign(&(aval(VAR_datanodeMasterDirs)[datanodeIdx]), Strdup(aval(VAR_datanodeSlaveDirs)[datanodeIdx]));
	var_assign(&(aval(VAR_datanodeSlaveDirs)[datanodeIdx]), Strdup("none"));

	/*
	 * Update the configuration file
	 */
	if ((f = fopen(pgxc_ctl_config_path, "a")) == NULL)
	{
		elog(ERROR, "ERROR: Failed to open configuration file %s, %s\n", pgxc_ctl_config_path, strerror(errno));
		return(-1);
	}
	fprintf(f,
			"#=====================================================\n"
			"# Updated due to the datanode failover, %s, %s\n"
			"datanodeMasterServers=( %s )\n"
			"datanodeMasterDirs=( %s )\n"
			"datanodeSlaveServers=( %s )\n"
			"datanodeSlaveDirs=( %s )\n"
			"# End of the update\n",
			aval(VAR_datanodeNames)[datanodeIdx], timeStampString(timestamp, MAXTOKEN),
			listValue(VAR_datanodeMasterServers),
			listValue(VAR_datanodeMasterDirs),
			listValue(VAR_datanodeSlaveServers),
			listValue(VAR_datanodeSlaveDirs));
	fclose(f);

	/* Backup the configuration file */
	if (isVarYes(VAR_configBackup))
	{
		rc_local = doConfigBackup();
		checkRc();
	}

	/*
	 * Reconfigure coordinators with new datanode
	 */
	for (jj = 0; aval(VAR_coordNames)[jj]; jj++)
	{
		if (is_none(aval(VAR_coordMasterServers)[jj]))
			continue;
			
		if (pingNode(aval(VAR_coordMasterServers)[jj], aval(VAR_coordPorts)[jj]) != 0)
		{
			elog(ERROR, "Coordinator %s is not running.  Skip reconfiguration for this coordinator.\n",
				 aval(VAR_coordNames)[jj]);
			continue;
		}
		if ((f = pgxc_popen_wRaw("psql -p %d -h %s %s %s",
								 atoi(aval(VAR_coordPorts)[jj]),
								 aval(VAR_coordMasterServers)[jj],
								 sval(VAR_defaultDatabase),
								 sval(VAR_pgxcOwner)))
			== NULL)
		{
			elog(ERROR, "ERROR: failed to start psql for coordinator %s, %s\n", aval(VAR_coordNames)[jj], strerror(errno));
			continue;
		}
		fprintf(f,
#if 0 /* Current alter node does't work well in this context */
				"ALTER NODE %s WITH (HOST='%s', PORT=%s);\n"
#else
				"DROP NODE %s;\n"
				"CREATE NODE %s WITH (type = datanode, HOST='%s', PORT=%s);\n"
#endif
				"select pgxc_pool_reload();\n"
				"\\q\n",
				aval(VAR_datanodeNames)[datanodeIdx],
				aval(VAR_datanodeNames)[datanodeIdx], aval(VAR_datanodeMasterServers)[datanodeIdx], aval(VAR_datanodePorts)[datanodeIdx]);
		fclose(f);
	}
	return rc;

#	undef checkRc

}

/*------------------------------------------------------------------------
 *
 * Add command
 *
 *-----------------------------------------------------------------------*/
int
add_datanodeMaster(char *name, char *host, int port, char *dir)
{
	FILE *f, *lockf;
	int size, idx;
	char port_s[MAXTOKEN+1];
	int gtmPxyIdx;
	char *gtmHost;
	char *gtmPort;
	char pgdumpall_out[MAXPATH+1];
	char **nodelist = NULL;
	int ii, jj;
	char **confFiles = NULL;

	/* Check if all the coordinators are running */
	if (!check_AllDatanodeRunning())
	{
		elog(ERROR, "ERROR: Some of the coordinator masters are not running. Cannot add new one.\n");
		return 1;
	}
	/* Check if there's no conflict with the current configuration */
	if (checkNameConflict(name, FALSE))
	{
		elog(ERROR, "ERROR: Node name %s duplicate.\n", name);
		return 1;
	}
	if (checkPortConflict(host, port))
	{
		elog(ERROR, "ERROR: port numbrer (%d) at host %s conflicts.\n", port, host);
		return 1;
	}
	if (checkDirConflict(host, dir))
	{
		elog(ERROR, "ERROR: directory \"%s\" conflicts at host %s.\n", dir, host);
		return 1;
	}
	/*
	 * Check if datanode masgter configuration is consistent
	 */
	idx = size = arraySizeName(VAR_datanodeNames);
	if ((arraySizeName(VAR_datanodePorts) != size) ||
		(arraySizeName(VAR_datanodeMasterServers) != size) ||
		(arraySizeName(VAR_datanodeMasterDirs) != size) ||
		(arraySizeName(VAR_datanodeMaxWALSenders) != size) ||
		(arraySizeName(VAR_datanodeSpecificExtraConfig) != size) ||
		(arraySizeName(VAR_datanodeSpecificExtraPgHba) != size))
	{
		elog(ERROR, "ERROR: sorry found some inconflicts in datanode master configuration.\n");
		return 1;
	}
	/*
	 * Now reconfigure
	 */
	/*
	 * 000 We need another way to configure specific pg_hba.conf and max_wal_senders.
	 */
	snprintf(port_s, MAXTOKEN, "%d", port);
	assign_arrayEl(VAR_datanodeNames, idx, name, NULL);
	assign_arrayEl(VAR_datanodeMasterServers, idx, host, NULL);
	assign_arrayEl(VAR_datanodePorts, idx, port_s, "-1");
	assign_arrayEl(VAR_datanodeMasterDirs, idx, dir, NULL);
	assign_arrayEl(VAR_datanodeMaxWALSenders, idx, aval(VAR_datanodeMaxWALSenders)[0], NULL);	/* Could be vulnerable */
	assign_arrayEl(VAR_datanodeSlaveServers, idx, "none", NULL);
	assign_arrayEl(VAR_datanodeSlaveDirs, idx, "none", NULL);
	assign_arrayEl(VAR_datanodeArchLogDirs, idx, "none", NULL);
	assign_arrayEl(VAR_datanodeSpecificExtraConfig, idx, "none", NULL);
	assign_arrayEl(VAR_datanodeSpecificExtraPgHba, idx, "none", NULL);
	/*
	 * Update the configuration file and backup it
	 */
	/*
	 * Take care of exrtra conf file
	 */
	if (doesExist(VAR_datanodeExtraConfig, 0) && !is_none(sval(VAR_coordExtraConfig)))
		AddMember(confFiles, sval(VAR_datanodeExtraConfig));
	if (doesExist(VAR_datanodeSpecificExtraConfig, idx) && !is_none(aval(VAR_datanodeSpecificExtraConfig)[idx]))
		AddMember(confFiles, aval(VAR_datanodeSpecificExtraConfig)[idx]);
	/*
	 * Main part
	 */
	if ((f = fopen(pgxc_ctl_config_path, "a")) == NULL)
	{
		/* Should it be panic? */
		elog(ERROR, "ERROR: cannot open configuration file \"%s\", %s\n", pgxc_ctl_config_path, strerror(errno));
		return 1;
	}
	fprintf(f, 
			"#===================================================\n"
			"# pgxc configuration file updated due to datanode master addition\n"
			"#        %s\n",
			timeStampString(date, MAXTOKEN+1));
	fprintAval(f, VAR_datanodeNames);
	fprintAval(f, VAR_datanodeMasterServers);
	fprintAval(f, VAR_datanodePorts);
	fprintAval(f, VAR_datanodeMasterDirs);
	fprintAval(f, VAR_datanodeMaxWALSenders);
	fprintAval(f, VAR_datanodeSlaveServers);
	fprintAval(f, VAR_datanodeSlaveDirs);
	fprintAval(f, VAR_datanodeArchLogDirs);
	fprintAval(f, VAR_datanodeSpecificExtraConfig);
	fprintAval(f, VAR_datanodeSpecificExtraPgHba);
	fprintf(f, "%s", "#----End of reconfiguration -------------------------\n");
	fclose(f);
	backup_configuration();

	/* Now add the master */

	gtmPxyIdx = getEffectiveGtmProxyIdxFromServerName(name);
	gtmHost = (gtmPxyIdx > 0) ? aval(VAR_gtmProxyServers)[gtmPxyIdx] : sval(VAR_gtmMasterServer);
	gtmPort = (gtmPxyIdx > 0) ? aval(VAR_gtmProxyPorts)[gtmPxyIdx] : sval(VAR_gtmMasterPort);

	/* initdb */
	doImmediate(host, NULL, "initdb -D %s --nodename %s", dir, name);

	/* Edit configurations */
	if ((f = pgxc_popen_w(host, "cat >> %s/postgresql.conf", dir)))
	{
		appendFiles(f, confFiles);
		fprintf(f,
				"#===========================================\n"
				"# Added at initialization. %s\n"
				"port = %d\n"
				"gtm_host = '%s'\n"
				"gtm_port = %s\n"
				"# End of Additon\n",
				timeStampString(date, MAXTOKEN+1),
				port, gtmHost, gtmPort);
		fclose(f);
	}
	CleanArray(confFiles);
	jj = datanodeIdx(name);
	if ((f = pgxc_popen_w(host, "cat >> %s/pg_hba.conf", dir)))
	{
		int kk;
		for (kk = 0; aval(VAR_datanodePgHbaEntries)[kk]; kk++)
		{
			fprintf(f,"host all %s %s trust\n",	sval(VAR_pgxcOwner), aval(VAR_datanodePgHbaEntries)[kk]);
			if (isVarYes(VAR_datanodeSlave))
				if (!is_none(aval(VAR_datanodeSlaveServers)[jj]))
					fprintf(f, "host replication %s %s trust\n",
							sval(VAR_pgxcOwner), aval(VAR_datanodePgHbaEntries)[kk]);
		}
		fprintf(f, "# End of addition\n");
		fclose(f);
	}

	/* Lock ddl */
	if ((lockf = pgxc_popen_wRaw("psql -h %s -p %d %s", aval(VAR_datanodeMasterServers)[0], atoi(aval(VAR_datanodePorts)[0]), sval(VAR_defaultDatabase))) == NULL)
	{
		elog(ERROR, "ERROR: could not open psql command, %s\n", strerror(errno));
		return 1;
	}
	fprintf(lockf, "select pgxc_lock_for_backup();\n");	/* Keep open until the end of the addition. */
	fflush(lockf);

	/* pg_dumpall */
	createLocalFileName(GENERAL, pgdumpall_out, MAXPATH);
	doImmediateRaw("pg_dumpall -p %s -h %s -s --include-nodes --dump-nodes --file=%s",
				   aval(VAR_datanodePorts)[0], aval(VAR_datanodeMasterServers)[0], pgdumpall_out);

	/* Start the new datanode */
	doImmediate(host, NULL, "pg_ctl start -Z restoremode -D %s -o -i -w", dir);

	/* Restore the backup */
	doImmediateRaw("psql -h %s -p %d -d %s -f %s", host, port, sval(VAR_defaultDatabase), pgdumpall_out);
	doImmediateRaw("rm -f %s", pgdumpall_out);

	/* Quit the new datanode */
	doImmediate(host, NULL, "pg_ctl stop -Z restoremode -D %s", dir);

	/* Start the new datanode with --datanode option */
	AddMember(nodelist, name);
	start_datanode_master(nodelist);
	CleanArray(nodelist);

	/* Issue CREATE NODE */
	for (ii = 0; aval(VAR_coordNames)[ii]; ii++)
	{
		if (!is_none(aval(VAR_coordNames)[ii]))
		{
			if ((f = pgxc_popen_wRaw("psql -h %s -p %s %s", aval(VAR_coordMasterServers)[ii], aval(VAR_coordPorts)[ii], sval(VAR_defaultDatabase))) == NULL)
			{
				elog(ERROR, "ERROR: cannot connect to the datanode master %s.\n", aval(VAR_coordNames)[ii]);
				continue;
			}
			fprintf(f, "CREATE NODE %s WITH (TYPE = 'datanode', host='%s', PORT=%d);\n", name, host, port);
			fprintf(f, "\\q\n");
			fclose(f);
		}
	}

	/* Quit DDL lokkup session */
	fprintf(lockf, "\\q\n");
	fclose(lockf);
	return 0;

}


int
add_datanodeSlave(char *name, char *host, char *dir, char *archDir)
{
	int idx;
	FILE *f;

	/* Check if the name is valid datanode */
	if ((idx = datanodeIdx(name)) < 0)
	{
		elog(ERROR, "ERROR: Specified datanodeiantor %s is not configured.\n", name);
		return 1;
	}
	/* Check if the datanode slave is not configred */
	if (isVarYes(VAR_datanodeSlave) && doesExist(VAR_datanodeSlaveServers, idx) && !is_none(aval(VAR_datanodeSlaveServers)[idx]))
	{
		elog(ERROR, "ERROR: Slave for the datanode %s has already been condigired.\n", name);
		return 1;
	}
	/* Check if the resource does not conflict */
	if (strcmp(dir, archDir) == 0)
	{
		elog(ERROR, "ERROR: working directory is the same as WAL archive directory.\n");
		return 1;
	}
	/*
	 * We dont check the name conflict here because acquiring datanode index means that
	 * there's no name conflict.
	 */
	if (checkPortConflict(host, atoi(aval(VAR_datanodePorts)[idx])))
	{
		elog(ERROR, "ERROR: the port %s has already been used in the host %s.\n",  aval(VAR_datanodePorts)[idx], host);
		return 1;
	}
	if (checkDirConflict(host, dir) || checkDirConflict(host, archDir))
	{
		elog(ERROR, "ERROR: directory %s or %s has already been used by other node.\n", dir, archDir);
		return 1;
	}
	/* Check if the datanode master is running */
	if (pingNode(aval(VAR_datanodeMasterServers)[idx], aval(VAR_datanodePorts)[idx]) != 0)
	{
		elog(ERROR, "ERROR: Datanode master %s is not running.\n", name);
		return 1;
	}
	/* Prepare the resources (directories) */
	doImmediate(host, NULL, "rm -rf %s; mkdir -p %s;chmod 0700 %s", dir, dir, dir);
	doImmediate(host, NULL, "rm -rf %s; mkdir -p %s;chmod 0700 %s", archDir, archDir, archDir);
	/* Reconfigure the master with WAL archive */
	/* Update the configuration and backup the configuration file */
	if ((f = pgxc_popen_w(aval(VAR_datanodeMasterServers)[idx], "cat >> %s/postgresql.conf", aval(VAR_datanodeMasterDirs)[idx])) == NULL)
	{
		elog(ERROR, "ERROR: Cannot open datanodenator master's configuration file, %s/postgresql.conf",
			 aval(VAR_datanodeMasterDirs)[idx]);
		return 1;
	}
	fprintf(f, 
			"#========================================\n"
			"# Addition for log shipping, %s\n"
			"wal_level = hot_standby\n"
			"archive_mode = on\n"
			"archive_command = 'rsync %%p %s@%s:%s/%%f'\n"
			"max_wal_senders = %d\n"
			"# End of Addition\n",
			timeStampString(date, MAXPATH),
			sval(VAR_pgxcUser), host, archDir,
			getDefaultWalSender(FALSE));
	fclose(f);
	/* pg_hba.conf for replication */
	if ((f = pgxc_popen_w(aval(VAR_datanodeMasterServers)[idx], "cat >> %s/pg_hba.conf", aval(VAR_datanodeMasterDirs)[idx])) == NULL)
	{
		elog(ERROR, "ERROR: Cannot open datanode master's pg_hba.conf file, %s/pg_hba.conf, %s\n", 
			 aval(VAR_datanodeMasterDirs)[idx], strerror(errno));
		return 1;
	}
	fprintf(f, 
			"#================================================\n"
			"# Additional entry by adding the slave, %s\n"
			"host replication %s %s/32 trust\n"
			"# End of addition ===============================\n",
			timeStampString(date, MAXPATH),
			sval(VAR_pgxcOwner), getIpAddress(host));
	fclose(f);
	/* Reconfigure pgxc_ctl configuration with the new slave */
#if 0
	/* Need an API to expand the array to desired size */
	if ((extendVar(VAR_datanodeSlaveServers, idx, "none") != 0) ||
		(extendVar(VAR_datanodeSlaveDirs, idx, "none")  != 0) ||
		(extendVar(VAR_datanodeArchLogDirs, idx, "none") != 0))
	{
		elog(PANIC, "PANIC: Internal error, inconsitent datanode information\n");
		return 1;
	}
#endif
	if (!isVarYes(VAR_datanodeSlave))
		assign_sval(VAR_datanodeSlave, "y");
	assign_arrayEl(VAR_datanodeSlaveServers, idx, host, NULL);
	assign_arrayEl(VAR_datanodeSlaveDirs, idx, dir, NULL);
	assign_arrayEl(VAR_datanodeArchLogDirs, idx, archDir, NULL);
	/* Update the configuration file and backup it */
	if ((f = fopen(pgxc_ctl_config_path, "a")) == NULL)
	{
		/* Should it be panic? */
		elog(ERROR, "ERROR: cannot open configuration file \"%s\", %s\n", pgxc_ctl_config_path, strerror(errno));
		return 1;
	}
	fprintf(f, 
			"#===================================================\n"
			"# pgxc configuration file updated due to datanode slave addition\n"
			"#        %s\n",
			timeStampString(date, MAXTOKEN+1));
	fprintSval(f, VAR_datanodeSlave);
	fprintAval(f, VAR_datanodeSlaveServers);
	fprintAval(f, VAR_datanodeArchLogDirs);
	fprintAval(f, VAR_datanodeSlaveDirs);
	fprintf(f, "%s", "#----End of reconfiguration -------------------------\n");
	fclose(f);
	backup_configuration();

	/* Restart the master */
	/*
	 * It's not a good idea to use "restart" here because some connection from other coordinators
	 * may be alive.   They are posessed by the pooler and we have to reload the pool to release them,
	 * which aborts all the transactions.
	 *
	 * Beacse we need to issue pgxc_pool_reload() at all the coordinators, we need to give up all the
	 * transactions in the whole cluster.
	 *
	 * It is much better to shutdow the target coordinator master fast because it does not affect
	 * transactions this coordinator is not involved.
	 */
	doImmediate(aval(VAR_coordMasterServers)[idx], NULL, 
				"pg_ctl stop -Z datanode -D %s -m fast", aval(VAR_datanodeMasterDirs)[idx]);
	doImmediate(aval(VAR_coordMasterServers)[idx], NULL, 
				"pg_ctl start -Z datanode -D %s -w", aval(VAR_datanodeMasterDirs)[idx]);
	/* pg_basebackup */
	doImmediate(host, NULL, "pg_basebackup -p %s -h %s -D %s -x",
				aval(VAR_datanodePorts)[idx], aval(VAR_datanodeMasterServers)[idx], dir);
	/* Update the slave configuration with hot standby and port */
	if ((f = pgxc_popen_w(host, "cat >> %s/postgresql.conf", dir)) == NULL)
	{
		elog(ERROR, "ERROR: Cannot open the new slave's postgresql.conf, %s\n", strerror(errno));
		return 1;
	}
	fprintf(f,
			"#==========================================\n"
			"# Added to initialize the slave, %s\n"
			"hot_standby = on\n"
			"port = %s\n"
			"archive_command = ''\n"	/* No archive mode */
			"# End of Addition\n",
			timeStampString(date, MAXTOKEN), aval(VAR_datanodePorts)[idx]);
	fclose(f);
	/* Update the slave recovery.conf */
	if ((f = pgxc_popen_w(host, "cat >> %s/recovery.conf", dir)) == NULL)
	{
		elog(ERROR, "ERROR: Cannot open the slave's recovery.conf, %s\n", strerror(errno));
		return 1;
	}
	fprintf(f,
			"#==========================================\n"
			"# Added to add the slave, %s\n"
			"standby_mode = on\n"
			"primary_conninfo = 'host = %s port = %s "
			"user = %s application_name = %s'\n"
			"restore_command = 'cp %s/%%f %%p'\n"
			"archive_cleanup_command = 'pg_archivecleanup %s %%r'\n"
			"# End of addition\n",
			timeStampString(date, MAXTOKEN), aval(VAR_datanodeMasterServers)[idx], aval(VAR_datanodePorts)[idx],
			sval(VAR_pgxcOwner), aval(VAR_datanodeNames)[idx], 
			aval(VAR_datanodeArchLogDirs)[idx], aval(VAR_datanodeArchLogDirs)[idx]);
	fclose(f);
	/* Start the slave */
	doImmediate(host, NULL, "pg_ctl start -Z datanode -D %s -w", dir);
	return 0;
}


/*------------------------------------------------------------------------
 *
 * Remove command
 *
 *-----------------------------------------------------------------------*/
int
remove_datanodeMaster(char *name, int clean_opt)
{
	/*
	  1. Transfer the data from the datanode to be removed to the rest of the datanodes for all the tables in all the databases.
         For example to shift data of the table rr_abc to the
         rest of the nodes we can use command

		 ALTER TABLE rr_abc DELETE NODE (DATA_NODE_3);

		 This step is not included in remove_datanodeMaster() function.

	  2. Confirm that there is no data left on the datanode to be removed.
         For example to confirm that there is no data left on DATA_NODE_3

		 select c.pcrelid from pgxc_class c, pgxc_node n where
		 n.node_name = 'DATA_NODE_3' and n.oid = ANY (c.nodeoids);

		 This step is not included in this function either.

	  3. Stop the datanode server to be removed.
 	     Now any SELECTs that involve the datanode to be removed would start failing
		 and DMLs have already been blocked, so essentially the cluster would work
		 only partially.

		 If datanode slave is also configured, we need to remove it first.

	  4. Connect to any of the coordinators.
	     In our example assuming COORD_1 is running on port 5432,
		 the following command would connect to COORD_1

		 psql postgres -p 5432

	  5. Drop the datanode to be removed.
	     For example to drop datanode DATA_NODE_3 use command

		 DROP NODE DATA_NODE_3;

      6. Update the connection information cached in pool.

	     SELECT pgxc_pool_reload();

      7. Repeat steps 4,5 & 6 for all the coordinators in the cluster.
	 */

	int idx;
	int ii;
	FILE *f;
	char **namelist = NULL;
	char date[MAXTOKEN+1];

	/* Check if the datanodeinator is configured */
	if ((idx = datanodeIdx(name)) < 0)
	{
		elog(ERROR, "ERROR: Coordinator %s is not configured.\n", name);
		return 1;
	}
	/* Check if all the other datanodeinators are running */
	for (ii = 0; aval(VAR_datanodeNames)[ii]; ii++)
	{
		if ((ii != idx) && !is_none(aval(VAR_datanodeNames)[ii]) && (pingNode(aval(VAR_datanodeMasterServers)[ii], aval(VAR_datanodePorts)[ii]) != 0))
		{
			elog(ERROR, "ERROR: Datanode master %s is not running.\n", aval(VAR_datanodeNames)[ii]);
			return 1;
		}
	}
	/* Check if there's a slave configured */
	if (doesExist(VAR_datanodeSlaveServers, idx) && !is_none(aval(VAR_datanodeSlaveServers)[idx]))
		remove_datanodeSlave(name, clean_opt);
#if 0
	/* Stop the datanodeinator master if running */
	if (pingNode(aval(VAR_datanodeMasterServers)[idx], aval(VAR_datanodePorts)[idx]) == 0)
	{
		AddMember(namelist, name);
		stop_datanode_master(namelist, "fast");
		CleanArray(namelist);
	}
	/* Cleanup the datanodeinator master resource if specified */
	if (clean_opt)
		doImmediate(aval(VAR_datanodeMasterServers)[idx], NULL, "rm -rf %s", aval(VAR_datanodeMasterDirs)[idx]);
#endif
	/* Issue "drop node" at all the other datanodeinators */
	for (ii = 0; aval(VAR_coordNames)[ii]; ii++)
	{
		if (doesExist(VAR_coordNames, ii) && !is_none(aval(VAR_coordNames)[ii]))
		{
			f = pgxc_popen_wRaw("psql -p %d -h %s %s", atoi(aval(VAR_coordPorts)[ii]), aval(VAR_coordMasterServers)[ii], sval(VAR_defaultDatabase));
			if (f == NULL)
			{
				elog(ERROR, "ERROR: cannot begin psql for the coordinator master %s\n", aval(VAR_coordNames)[ii]);
				continue;
			}
			fprintf(f, "DROP NODE %s;\n", name);
			fprintf(f, "\\q");
			fclose(f);
		}
	}
#if 1
	/* Stop the datanodeinator master if running */
	if (pingNode(aval(VAR_datanodeMasterServers)[idx], aval(VAR_datanodePorts)[idx]) == 0)
	{
		AddMember(namelist, name);
		stop_datanode_master(namelist, "fast");
		CleanArray(namelist);
	}
	/* Cleanup the datanodeinator master resource if specified */
	if (clean_opt)
		doImmediate(aval(VAR_datanodeMasterServers)[idx], NULL, "rm -rf %s", aval(VAR_datanodeMasterDirs)[idx]);
#endif
	/* Update configuration and backup --> should cleanup "none" entries here */
	assign_arrayEl(VAR_datanodeNames, idx, "none", NULL);
	assign_arrayEl(VAR_datanodeMasterDirs, idx, "none", NULL);
	assign_arrayEl(VAR_datanodePorts, idx, "-1", "-1");
	assign_arrayEl(VAR_datanodeMasterServers, idx, "none", NULL);
	assign_arrayEl(VAR_datanodeMaxWALSenders, idx, "0", "0");
	assign_arrayEl(VAR_datanodeSlaveServers, idx, "none", NULL);
	assign_arrayEl(VAR_datanodeSlaveDirs, idx, "none", NULL);
	assign_arrayEl(VAR_datanodeArchLogDirs, idx, "none", NULL);
	assign_arrayEl(VAR_datanodeSpecificExtraConfig, idx, "none", NULL);
	handle_no_slaves();
	/*
	 * Write config files
	 */
	if ((f = fopen(pgxc_ctl_config_path, "a")) == NULL)
	{
		/* Should it be panic? */
		elog(ERROR, "ERROR: cannot open configuration file \"%s\", %s\n", pgxc_ctl_config_path, strerror(errno));
		return 1;
	}
	fprintf(f, 
			"#================================================================\n"
			"# pgxc configuration file updated due to coodinator master removal\n"
			"#        %s\n",
			timeStampString(date, MAXTOKEN+1));
	fprintSval(f, VAR_datanodeSlave);
	fprintAval(f, VAR_datanodeNames);
	fprintAval(f, VAR_datanodeMasterDirs);
	fprintAval(f, VAR_datanodePorts);
	fprintAval(f, VAR_datanodeMasterServers);
	fprintAval(f, VAR_datanodeMaxWALSenders);
	fprintAval(f, VAR_datanodeSlaveServers);
	fprintAval(f, VAR_datanodeSlaveDirs);
	fprintAval(f, VAR_datanodeArchLogDirs);
	fprintAval(f, VAR_datanodeSpecificExtraConfig);
	fclose(f);
	backup_configuration();
	return 0;
}

int
remove_datanodeSlave(char *name, int clean_opt)
{
	int idx;
	char **nodelist = NULL;
	FILE *f;

	if (!isVarYes(VAR_datanodeSlave))
	{
		elog(ERROR, "ERROR: datanode slave is not configured.\n");
		return 1;
	}
	idx = datanodeIdx(name);
	if (idx < 0)
	{
		elog(ERROR, "ERROR: datanode %s is not configured.\n", name);
		return 1;
	}
	if (!doesExist(VAR_datanodeSlaveServers, idx) || is_none(aval(VAR_datanodeSlaveServers)[idx]))
	{
		elog(ERROR, "ERROR: datanode slave %s is not configured.\n", name);
		return 1;
	}
	AddMember(nodelist, name);
	if (pingNode(aval(VAR_datanodeSlaveServers)[idx], aval(VAR_datanodePorts)[idx]) == 0)
		stop_datanode_slave(nodelist, "immediate");
	{
		FILE *f;
		if ((f = pgxc_popen_w(aval(VAR_datanodeMasterServers)[idx], "cat >> %s/postgresql.conf", aval(VAR_datanodeMasterDirs)[idx])) == NULL)
		{
			elog(ERROR, "ERROR: cannot open %s/postgresql.conf at %s, %s\n", aval(VAR_datanodeMasterDirs)[idx], aval(VAR_datanodeMasterServers)[idx], strerror(errno));
			return 1;
		}
		fprintf(f,
				"#=======================================\n"
				"# Updated to remove the slave %s\n"
				"synchronous_standby_names = ''\n"
				"archive_command = ''\n"
				"# End of the update\n",
				timeStampString(date, MAXTOKEN));
		fclose(f);
	}
	doImmediate(aval(VAR_datanodeMasterServers)[idx], NULL, "pg_ctl restart -Z datanode -D %s", aval(VAR_datanodeMasterDirs)[idx]);

	if (clean_opt)
		clean_datanode_slave(nodelist);
	/*
	 * Maintain variables
	 */
	assign_arrayEl(VAR_datanodeSlaveServers, idx, "none", NULL);
	assign_arrayEl(VAR_datanodeSlaveDirs, idx, "none", NULL);
	assign_arrayEl(VAR_datanodeArchLogDirs, idx, "none", NULL);
	handle_no_slaves();
	/*
	 * Maintain configuration file
	 */
	if ((f = fopen(pgxc_ctl_config_path, "a")) == NULL)
	{
		/* Should it be panic? */
		elog(ERROR, "ERROR: cannot open configuration file \"%s\", %s\n", pgxc_ctl_config_path, strerror(errno));
		return 1;
	}
	fprintf(f, 
			"#================================================================\n"
			"# pgxc configuration file updated due to coodinator slave removal\n"
			"#        %s\n",
			timeStampString(date, MAXTOKEN+1));
	fprintSval(f, VAR_datanodeSlave);
	fprintAval(f, VAR_datanodeSlaveServers);
	fprintAval(f, VAR_datanodeSlaveDirs);
	fprintAval(f, VAR_datanodeArchLogDirs);
	fclose(f);
	backup_configuration();
	CleanArray(nodelist);
	return 0;

}

/*
 * Clean datanode master resources -- directory and port -----------------------------
 */
cmd_t *
prepare_cleanDatanodeMaster(char *nodeName)
{
	cmd_t *cmd;
	int idx;
	
	if ((idx = datanodeIdx(nodeName)) <  0)
	{
		elog(ERROR, "ERROR: %s is not a datanode\n", nodeName);
		return(NULL);
	}
	cmd = initCmd(aval(VAR_datanodeMasterServers)[idx]);
	snprintf(newCommand(cmd), MAXLINE,
			 "rm -rf %s; mkdir -p %s; chmod 0700 %s",
			 aval(VAR_datanodeMasterDirs)[idx], aval(VAR_datanodeMasterDirs)[idx], aval(VAR_datanodeMasterDirs)[idx]);
	return(cmd);
}

int
clean_datanode_master_all(void)
{
	elog(INFO, "Cleaning all the datanode master resources.\n");
	return(clean_datanode_master(aval(VAR_datanodeNames)));
}

int
clean_datanode_master(char **nodeList)
{
	char **actualNodeList;
	cmdList_t *cmdList;
	cmd_t *cmd;
	int ii;
	int rc;

	cmdList = initCmdList();
	actualNodeList = makeActualNodeList(nodeList);
	for (ii = 0; actualNodeList[ii]; ii++)
	{
		elog(INFO, "Cleaning datanode %s maseter resources.\n", actualNodeList[ii]);
		if ((cmd = prepare_cleanDatanodeMaster(actualNodeList[ii])))
			addCmd(cmdList, cmd);
	}
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	CleanArray(actualNodeList);
	return(rc);
}

/*
 * Cleanup datanode slave resources -- directory and the socket ------------------
 */
cmd_t *
prepare_cleanDatanodeSlave(char *nodeName)
{
	cmd_t *cmd;
	int idx;
	
	if ((idx = datanodeIdx(nodeName)) <  0)
	{
		elog(ERROR, "ERROR: %s is not a datanode\n", nodeName);
		return(NULL);
	}
	if (!doesExist(VAR_datanodeSlaveServers, idx) || is_none(aval(VAR_datanodeSlaveServers)[idx]))
		return NULL;
	cmd = initCmd(aval(VAR_datanodeSlaveServers)[idx]);
	snprintf(newCommand(cmd), MAXLINE,
			 "rm -rf %s; mkdir -p %s; chmod 0700 %s",
			 aval(VAR_datanodeSlaveDirs)[idx], aval(VAR_datanodeSlaveDirs)[idx], aval(VAR_datanodeSlaveDirs)[idx]);
	return(cmd);
}

int
clean_datanode_slave_all(void)
{
	elog(INFO, "Cleaning all the datanode slave resouces.\n");
	return(clean_datanode_slave(aval(VAR_datanodeNames)));
}

int
clean_datanode_slave(char **nodeList)
{
	char **actualNodeList;
	cmdList_t *cmdList;
	cmd_t *cmd;
	int ii;
	int rc;

	cmdList = initCmdList();
	actualNodeList = makeActualNodeList(nodeList);
	for (ii = 0; actualNodeList[ii]; ii++)
	{
		elog(INFO, "Cleaning datanode %s slave resources.\n", actualNodeList[ii]);
		if ((cmd = prepare_cleanDatanodeSlave(actualNodeList[ii])))
			addCmd(cmdList, cmd);
		else
			elog(WARNING, "WARNING: datanode slave %s not found.\n", actualNodeList[ii]);
	}
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	CleanArray(actualNodeList);
	return(rc);
}

/*
 * Show configuration of datanodes -------------------------------------------------
 */
int
show_config_datanodeMaster(int flag, int idx, char *hostname)
{
	int ii;
	char outBuf[MAXLINE+1];
	char editBuf[MAXPATH+1];

	outBuf[0] = 0;
	if (flag)
		strncat(outBuf, "Datanode Master: ", MAXLINE);
	if (hostname)
	{
		snprintf(editBuf, MAXPATH, "host: %s", hostname);
		strncat(outBuf, editBuf, MAXLINE);
	}
	if (flag || hostname)
		strncat(outBuf, "\n", MAXLINE);
	lockLogFile();
	if (outBuf[0])
		elog(NOTICE, "%s", outBuf);
	elog(NOTICE, "    Nodename: '%s', port: %s\n",
		 aval(VAR_datanodeNames)[idx], aval(VAR_datanodePorts)[idx]);
	elog(NOTICE, "    MaxWALSenders: %s, Dir: '%s'\n",
		 aval(VAR_datanodeMaxWALSenders)[idx], aval(VAR_datanodeMasterDirs)[idx]);
	elog(NOTICE, "    ExtraConfig: '%s', Specific Extra Config: '%s'\n",
		 sval(VAR_datanodeExtraConfig), aval(VAR_datanodeSpecificExtraConfig)[idx]);
	strncpy(outBuf, "    pg_hba entries ( ", MAXLINE);
	for (ii = 0; aval(VAR_datanodePgHbaEntries)[ii]; ii++)
	{
		snprintf(editBuf, MAXPATH, "'%s' ", aval(VAR_datanodePgHbaEntries)[ii]);
		strncat(outBuf, editBuf, MAXLINE);
	}
	elog(NOTICE, "%s)\n", outBuf);
	elog(NOTICE, "    Extra pg_hba: '%s', Specific Extra pg_hba: '%s'\n",
		 sval(VAR_datanodeExtraPgHba), aval(VAR_datanodeSpecificExtraPgHba)[idx]);
	unlockLogFile();
	return 0;
}

int
show_config_datanodeSlave(int flag, int idx, char *hostname)
{
	char outBuf[MAXLINE+1];
	char editBuf[MAXPATH+1];

	outBuf[0] = 0;
	if (flag)
		strncat(outBuf, "Datanode Slave: ", MAXLINE);
	if (hostname)
	{
		snprintf(editBuf, MAXPATH, "host: %s", hostname);
		strncat(outBuf, editBuf, MAXLINE);
	}
	if (flag || hostname)
		strncat(outBuf, "\n", MAXLINE);
	lockLogFile();
	if (outBuf[0])
		elog(NOTICE, "%s", outBuf);
	elog(NOTICE, "    Nodename: '%s', port: %s\n",
		 aval(VAR_datanodeNames)[idx], aval(VAR_datanodePorts)[idx]);
	elog(NOTICE,"    Dir: '%s', Archive Log Dir: '%s'\n",
		 aval(VAR_datanodeSlaveDirs)[idx], aval(VAR_datanodeArchLogDirs)[idx]);
	unlockLogFile();
	return 0;
}

int show_config_datanodeMasterSlaveMulti(char **nodeList)
{
	int ii;
	int idx;

	lockLogFile();
	for (ii = 0; nodeList[ii]; ii++)
	{
		if ((idx = datanodeIdx(nodeList[ii])) < 0)
		{
			elog(WARNING, "WARNING: %s is not a datanode, skipping.\n", nodeList[ii]);
			continue;
		}
		else
		{
			show_config_datanodeMaster(TRUE, idx, aval(VAR_datanodeMasterServers)[idx]);
			if (isVarYes(VAR_datanodeSlave))
				show_config_datanodeSlave(TRUE, idx, aval(VAR_datanodeSlaveServers)[idx]);
		}
	}
	unlockLogFile();
	return 0;
}

int
show_config_datanodeMasterMulti(char **nodeList)
{
	int ii;
	int idx;

	lockLogFile();
	for (ii = 0; nodeList[ii]; ii++)
	{
		if ((idx = datanodeIdx(nodeList[ii])) < 0)
		{
			elog(WARNING, "WARNING: %s is not a datanode. skipping\n", nodeList[ii]);
			continue;
		}
		else
			show_config_datanodeMaster(TRUE, idx, aval(VAR_datanodeMasterServers)[idx]);
	}
	unlockLogFile();
	return 0;
}

int
show_config_datanodeSlaveMulti(char **nodeList)
{
	int ii;
	int idx;

	if (!isVarYes(VAR_datanodeSlave))
	{
		elog(ERROR, "ERROR: datanode slave is not configured.\n");
		return 1;
	}
	lockLogFile();
	for (ii = 0; nodeList[ii]; ii++)
	{
		if ((idx = datanodeIdx(nodeList[ii])) < 0)
		{
			elog(WARNING, "WARNING: %s is not a datanode, skipping.\n", nodeList[ii]);
			continue;
		}
		else
			show_config_datanodeSlave(TRUE, idx, aval(VAR_datanodeSlaveServers)[idx]);
	}
	unlockLogFile();
	return(0);
}

/*
 * Kill datanode master ---------------------------------------------------------------
 *
 * Normally, you should not kill masters in such a manner.   It is just for
 * emergence.
 */
static cmd_t *
prepare_killDatanodeMaster(char *nodeName)
{
	pid_t postmasterPid;
	int dnIndex;
	cmd_t *cmd = NULL;

	if (is_none(nodeName))
		return(NULL);
	if ((dnIndex = datanodeIdx(nodeName)) < 0)
	{
		elog(WARNING, "WARNING: \"%s\" is not a datanode name\n", nodeName);
		return(NULL);
	}
	if (is_none(aval(VAR_datanodeMasterServers)[dnIndex]))
		return(NULL);
	cmd = initCmd(aval(VAR_datanodeMasterServers)[dnIndex]);
	if ((postmasterPid = get_postmaster_pid(aval(VAR_datanodeMasterServers)[dnIndex], aval(VAR_datanodeMasterDirs)[dnIndex])) > 0)
	{
		char *pidList = getChPidList(aval(VAR_datanodeMasterServers)[dnIndex], postmasterPid);

		snprintf(newCommand(cmd), MAXLINE,
				 "kill -9 %d %s;"	/* Kill the postmaster and all its children */
				 "rm -rf /tmp/.s.'*'%d'*'",		/* Remove the socket */
				 postmasterPid, 
				 pidList,
				 atoi(aval(VAR_datanodePorts)[dnIndex]));
		freeAndReset(pidList);
	}
	else
		snprintf(newCommand(cmd), MAXLINE,
				 "killall -u %s -9 postgres;"	/* Kill the postmaster and all its children */
				 "rm -rf /tmp/.s.'*'%d'*'",		/* Remove the socket */
				 sval(VAR_pgxcUser), atoi(aval(VAR_datanodePorts)[dnIndex]));
	return(cmd);
}

int
kill_datanode_master_all(void)
{
	return(kill_datanode_master(aval(VAR_datanodeNames)));
}

int
kill_datanode_master(char **nodeList)
{
	int ii;
	int rc;
	char **actualNodeList;
	cmdList_t *cmdList = NULL;
	cmd_t *cmd;

	actualNodeList = makeActualNodeList(nodeList);
	cmdList = initCmdList();
	for (ii = 0; actualNodeList[ii]; ii++)
	{
		if ((cmd = prepare_killDatanodeMaster(actualNodeList[ii])))
			addCmd(cmdList, cmd);
	}
	if (cmdList)
	{
		rc = doCmdList(cmdList);
		cleanCmdList(cmdList);
		CleanArray(actualNodeList);
	}
	else
		rc = 0;
	return(rc);
}

/*
 * Kill datanode slaves -----------------------------------------------------
 *
 * You should not kill datanodes in such a manner.  It is just for emergence.
 * You should try to stop it gracefully.
 */
static cmd_t *
prepare_killDatanodeSlave(char *nodeName)
{
	pid_t postmasterPid;
	int dnIndex;
	cmd_t *cmd;

	if (is_none(nodeName))
		return(NULL);
	if ((dnIndex = datanodeIdx(nodeName)) < 0)
	{
		elog(WARNING, "WARNING: \"%s\" is not a datanode name, skipping.\n", nodeName);
		return(NULL);
	}
	if (is_none(aval(VAR_datanodeSlaveServers)[dnIndex]))
		return(NULL);
	if (!doesExist(VAR_datanodeSlaveServers, dnIndex) || is_none(aval(VAR_datanodeSlaveServers)[dnIndex]))
	{
		elog(WARNING, "WARNING: datanode slave %s is not found.\n", nodeName);
		return NULL;
	}
	cmd = initCmd(aval(VAR_datanodeSlaveServers)[dnIndex]);
	postmasterPid = get_postmaster_pid(aval(VAR_datanodeSlaveServers)[dnIndex], aval(VAR_datanodeSlaveDirs)[dnIndex]);
	if (postmasterPid == -1)
	{
		/* No postmaster pid found */
		elog(WARNING, "WARNING: pid for datanode slave \"%s\" slave was not found.  Remove socket only.\n", nodeName);
		snprintf(newCommand(cmd), MAXLINE,
				 "rm -rf /tmp/.s.'*'%s'*'",		/* Remove the socket */
				 aval(VAR_datanodePorts)[dnIndex]);
	}
	else
	{
		char *pidList = getChPidList(aval(VAR_datanodeSlaveServers)[dnIndex], postmasterPid);

		snprintf(newCommand(cmd), MAXLINE,
				 "kill -9 %d %s;"	/* Kill the postmaster and all its children */
				 "rm -rf /tmp/.s.'*'%d'*'",		/* Remove the socket */
				 postmasterPid, 
				 pidList,
				 atoi(aval(VAR_datanodePorts)[dnIndex]));
		freeAndReset(pidList);
	}
	return(cmd);
}

int
kill_datanode_slave_all(void)
{
	return(kill_datanode_slave(aval(VAR_datanodeNames)));
}

int
kill_datanode_slave(char **nodeList)
{
	int ii;
	int rc;
	char **actualNodeList;
	cmdList_t *cmdList;
	cmd_t *cmd;

	cmdList = initCmdList();
	actualNodeList = makeActualNodeList(nodeList);
	if (!isVarYes(VAR_datanodeSlave))
	{
		elog(ERROR, "ERROR: Datanode slave is not configured.\n");
		return 1;
	}
	for (ii = 0; actualNodeList[ii]; ii++)
	{
		if ((cmd = prepare_killDatanodeSlave(actualNodeList[ii])))
			addCmd(cmdList, cmd);
	}
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	return(rc);
}

/*
 * Checks if all the coordinators are running
 *
 * Returns FALSE if any of them are not running.
 */
int
check_AllDatanodeRunning(void)
{
	int ii;

	for (ii = 0; aval(VAR_datanodeMasterServers)[ii]; ii++)
	{
		if (!is_none(aval(VAR_datanodeMasterServers)[ii]))
			if (pingNode(aval(VAR_datanodeMasterServers)[ii], aval(VAR_datanodePorts)[ii]) != 0)
				return FALSE;
	}
	return TRUE;
}


