/*-------------------------------------------------------------------------
 *
 * config.c
 *
 *    Configuration module of Postgres-XC configuration and operation tool.
 *
 * Copyright (c) 2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */
/*
 * This module handles pgxc_ctl configuration.  This file includes
 * parser of incoming variable inforation which is installed to
 * pgxc_ctl variable system.
 *
 * This module also checks if there's any conficts in the resources
 * among different nodes.
 */
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <readline/readline.h>
#include <readline/history.h>
#include "variables.h"
#include "varnames.h"
#include "config.h"
#include "pgxc_ctl_log.h"
#include "pgxc_ctl.h"
#include "utils.h"
#include "do_shell.h"

extern char *pgxc_ctl_conf_prototype[];

static void addServer(char **name);
static void verifyResource(void);

enum Quote { UNQUOTED, SINGLEQUOTED, DOUBLEQUOTED };

/*====================================================================
 *
 * Configuration parser
 *
 * The following functions read, parse and constract variables.
 * 
 * As you see pgxc_ctl_bash_script[] in pgxc_ctl_bash.c, each variable
 * will be read in the form of
 * varname value value ....
 *
 * Each variable is basically an array.  Sometimes, only the first
 * element is needed.
 *
 * Please note that pgxc_ctl_bash.c is build by make_signature script
 * using pgxc_ctl_bash_2 and pgxc_cont_conf_part dynamically.
 *
 * You can change detailed behaviors of the script by editing these
 * files.  Be careful! Please maintain the interface to this module.
 *
 *===================================================================
 */
/*
 * Get a token from the line
 */
char *
get_word(char *line, char **token)
{
	enum Quote quoted = UNQUOTED;

	*token = NULL;

	if (!line)
		return NULL;
	for(;*line == ' ' || *line == '\t'; line++);
	if (!*line)
	{
		*token = NULL;
		return NULL;
	}
	if (*line == '"')
	{
		quoted = DOUBLEQUOTED;
		line++;
	}
	else if (*line == '\'')
	{
		quoted = SINGLEQUOTED;
		line++;
	}
	else if (*line == '#')
	{
		*line = 0;
		*token = NULL;
		return NULL;
	}
	*token = line;
	if (quoted == DOUBLEQUOTED)
	{
		for (; *line && *line != '\n' && *line != '"'; line++);
		if (*line == '"')
		{
			*line = 0;
			line++;
		}
	}
	else if (quoted == SINGLEQUOTED)
	{
		for (; *line && *line != '\n' && *line != '\''; line++);
		if (*line == '\'')
		{
			*line = 0;
			line++;
		}
	}
 	else
		for (; *line && *line != ' ' && *line != '\t' && *line != '\n' && *line != '#'; line++);
	if (*line == '#')
	{
		*line = 0;
		return(line);
	}
	else if (*line)
	{
		*line = 0;
		return(line+1);
	}
	else
		return(line);
}

/*
 * Parse line and create/update a variable.
 */
static void
parse_line(char *line)
{
	char *varname;
	char *val;
	pgxc_ctl_var *newv;

	line = get_word(line, &varname);
	if (!varname)
		return;
	if (!(newv = confirm_var(varname)))
		return;
	reset_value(newv);
	while((line = get_word(line, &val)))
	{
		if (val)
		{
			add_val(newv, val);
		}
	}
}

/*
 * Parse line and filter only pre-defined variables.
 *
 * This blocks any unknow variables to be build within pgxc_ctl structure.
 */
static void
parse_line_select(char *line, char *selectThis[])
{
	char *varname;
	char *val;
	pgxc_ctl_var *newv;
	int ii;

	line = get_word(line, &varname);
	if (!varname || varname[0] == '#')
		return;
	for (ii = 0; selectThis[ii]; ii++)
	{
		if (strcmp(varname, selectThis[ii]) == 0)
		{
			if (!(newv = confirm_var(varname)))
				return;
			while((line = get_word(line, &val)))
			{
				if (val)
					add_val(newv, val);
			}
		}
	}
}

/*
 * Configuration file I/F
 */
void
read_vars(FILE *conf)
{
	char line[MAXLINE+1];

	while (fgets(line, MAXLINE, conf))
		parse_line(line);
}

/*
 * Configuration file I/F
 */
void
read_selected_vars(FILE *conf, char *selectThis[])
{
	char line[MAXLINE+1];

	while (fgets(line, MAXLINE, conf))
		parse_line_select(line, selectThis);
}

/*
 * Build the configuraiton file prototype.
 */
void
install_conf_prototype(char *path)
{
	char cmd[MAXPATH+1];
	FILE *pgxc_config_proto = fopen(path, "w");
	int i;

	if (!pgxc_config_proto)
	{
		elog(ERROR, "ERROR Could not open configuration prototype to %s. %s\n", path, strerror(errno));
		return;
	}
	for (i = 0; pgxc_ctl_conf_prototype[i]; i++)
		fprintf(pgxc_config_proto, "%s\n", pgxc_ctl_conf_prototype[i]);
	fclose(pgxc_config_proto);
	snprintf(cmd, MAXPATH, "chmod +x %s", path);
	system(cmd);
}

/*
 * Get all the servers --> VAR_allServers
 */
static void
addServer(char **name)
{
	int ii, jj;
	int flag;

	confirm_var(VAR_allServers);
	
	for (ii = 0; name[ii]; ii++)
	{
		flag = TRUE;
		for (jj = 0; aval(VAR_allServers)[jj]; jj++)
		{
			if (strcmp(name[ii], aval(VAR_allServers)[jj]) != 0)
				continue;
			else
			{
				flag = FALSE;
				break;
			}
		}
		if (flag)
			add_val(find_var(VAR_allServers), name[ii]);
	}
}

/*
 * Test each node and build target server list
 */
void
makeServerList(void)
{
	/* Initialize */
	reset_var(VAR_allServers);
	/* GTM Master */
	addServer(aval(VAR_gtmMasterServer));
	/* GTM Slave */
	if (isVarYes(VAR_gtmSlave))
		addServer(aval(VAR_gtmSlaveServer));
	/* GTM_Proxy */
	if (isVarYes(VAR_gtmProxy))
		addServer(aval(VAR_gtmProxyServers));
	/* Coordinator Master */
	if (find_var(VAR_coordMasterServers))
		addServer(aval(VAR_coordMasterServers));
	/* Coordinator Slave */
	if (isVarYes(VAR_coordSlave))
		addServer(aval(VAR_coordSlaveServers));
	/* Datanode Master */
	addServer(aval(VAR_datanodeMasterServers));
	/* Datanode Slave */
	if (isVarYes(VAR_datanodeSlave))
		addServer(aval(VAR_datanodeSlaveServers));
	/* Should add secondary slaves */
}


/*
 * Take care of "no" slaves and build server list.   At present, we don't support
 * Cascaded or multi slaves.   This will be supported in the future.
 *
 * Please note that log archive backup site is not counted as this server list
 * because such servers are not likely to provide XC node operation.
 *
 * Log archive backup will be implemented in the future.
 */
int
is_none(char *s)
{
	if (strcmp(s, "none") == 0)
		return TRUE;
	if (strcmp(s, "N/A") == 0)
		return TRUE;
	return FALSE;
}

/*
 * Remove gtm slave.  Used at failover.
 */
static void
emptyGtmSlave()
{
	reset_var_val(VAR_gtmSlaveServer, "none");
	reset_var_val(VAR_gtmSlavePort, "0");
	reset_var_val(VAR_gtmSlaveDir, "none");
}

/*
 * Remove gtm proxies.   Used when a node crashes.
 * Because gtm_proxy is expected to be running at any target
 * server, we don't have gtm_proxy slaves.
 * We can just initialize gtm_proy using gtminit, configure and
 * run it.
 */
static void
emptyGtmProxies()
{
	int ii;

	reset_var_val(VAR_gtmProxy, "n");
	reset_var(VAR_gtmProxyServers);
	reset_var(VAR_gtmProxyNames);
	reset_var(VAR_gtmProxyPorts);
	reset_var(VAR_gtmProxyDirs);
	reset_var_val(VAR_gtmPxyExtraConfig, "none");
	reset_var(VAR_gtmPxySpecificExtraConfig);
	for (ii = 0; ii < arraySizeName(VAR_allServers); ii++)
	{
		add_val(find_var(VAR_gtmProxyServers), "none");
		add_val(find_var(VAR_gtmProxyNames), "none");
		add_val(find_var(VAR_gtmProxyPorts), "-1");
		add_val(find_var(VAR_gtmProxyDirs), "none");
		add_val(find_var(VAR_gtmPxyExtraConfig), "none");
	}
}

/*
 * Removes coordinator slaves from pgxc_ctl configuration.
 * This is needed when a slave promotes and becomes a new
 * master.
 */
static void
emptyCoordSlaves()
{
	int ii;

	reset_var_val(VAR_coordSlave, "n");
	reset_var(VAR_coordSlaveServers);
	reset_var(VAR_coordSlaveDirs);
	reset_var(VAR_coordArchLogDirs);
	for (ii = 0; ii < arraySizeName(VAR_coordNames); ii++)
	{
		add_val(find_var(VAR_coordSlaveServers), "none");
		add_val(find_var(VAR_coordSlaveDirs), "none");
		add_val(find_var(VAR_coordArchLogDirs), "none");
	}
}

/*
 * Removes datanode slave from pgxc_ctl configuration.
 */
static void
emptyDatanodeSlaves()
{
	int ii;

	reset_var_val(VAR_datanodeSlave, "n");
	reset_var(VAR_datanodeSlaveServers);
	reset_var(VAR_datanodeSlaveDirs);
	reset_var(VAR_datanodeArchLogDirs);
	for (ii = 0; ii < arraySizeName(VAR_datanodeSlaveServers); ii++)
	{
		add_val(find_var(VAR_datanodeSlaveServers), "none");
		add_val(find_var(VAR_coordSlaveDirs), "none");
		add_val(find_var(VAR_coordArchLogDirs), "none");
	}
}

/*
 * Scans initial configuration and set up "not configured" things.
 *
 * If, for example, gtm proxy is not configured,
 * we set gtmProxy variable to "n".
 *
 * When gtmProxy varieble is already set to "n", remove gtm_proxy
 * configuration information.
 *
 * Similar handling will be done for gtm slave, coordinator slaves
 * and datanode slaves.
 */
void
handle_no_slaves()
{
	int is_empty;
	int ii;

	/* GTM Slave */
	if (!find_var(VAR_gtmSlave))
		reset_var_val(VAR_gtmSlave, "n");
	if (!isVarYes(VAR_gtmSlave))
		emptyGtmSlave();
	else
	{
		confirm_var(VAR_gtmSlaveServer);
		if (!sval(VAR_gtmSlaveServer) || is_none(sval(VAR_gtmSlaveServer)))
		{
			emptyGtmSlave();
			reset_var_val(VAR_gtmSlaveServer, "n");
		}
	}

	/* GTM Proxy */
	if (!find_var(VAR_gtmProxy))
		reset_var_val(VAR_gtmProxy, "n");
	if (!isVarYes(VAR_gtmProxy))
		emptyGtmProxies();
	else
	{
		is_empty = TRUE;
		for (ii = 0; aval(VAR_gtmProxyServers)[ii]; ii++)
		{
			if (is_none(aval(VAR_gtmProxyServers)[ii]))
				continue;
			else
			{
				is_empty = FALSE;
				break;
			}
		}
		if (is_empty)
		{
			reset_var_val(VAR_gtmProxy, "n");
			emptyGtmProxies();
		}
	}
	/* Coordinator Slaves */
	if (!find_var(VAR_coordSlave))
		reset_var_val(VAR_coordSlave, "n");
	if (!isVarYes(VAR_coordSlave))
		emptyCoordSlaves();
	else
	{
		is_empty = TRUE;
		if (find_var(VAR_coordSlaveServers))
		{
			for (ii = 0; aval(VAR_coordSlaveServers)[ii]; ii++)
			{
				if (is_none(aval(VAR_coordSlaveServers)[ii]))
					continue;
				else
				{
					is_empty = FALSE;
					break;
				}
			}
			if (is_empty)
			{
				reset_var_val(VAR_coordSlave, "n");
				emptyCoordSlaves();
			}
		}
		else
		{
			elog(WARNING, "WARNING: coordSlaveServers variable not found where coordSlave is set to \"y\"\n");
			reset_var_val(VAR_coordSlave, "n");
			emptyCoordSlaves();
		}
	}
	/* Datanode Slaves */
	if (!find_var(VAR_datanodeSlave))
		reset_var_val(VAR_datanodeSlave, "n");
	if (!isVarYes(VAR_datanodeSlave))
		emptyDatanodeSlaves();
	else
	{
		is_empty = TRUE;
		if (find_var(VAR_datanodeSlaveServers))
		{
			for (ii = 0; aval(VAR_datanodeSlaveServers)[ii]; ii++)
			{
				if (is_none(aval(VAR_datanodeSlaveServers)[ii]))
					continue;
				else
				{
					is_empty = FALSE;
					break;
				}
			}
			if (is_empty)
			{
				reset_var_val(VAR_datanodeSlave, "n");
				emptyDatanodeSlaves();
			}
		}
		else
		{
			elog(WARNING, "WARNING: datanodeSlaveServers variable not found where datanodeSlave is set to \"y\"\n");
			reset_var_val(VAR_datanodeSlave, "n");
			emptyDatanodeSlaves();
		}
	}
}

/*
 * Check if there's no overlap in the resource --> Port, host and directory.
 */
static void
reportMissingVar(char *name)
{
	elog(ERROR, "ERROR: %s is not configured.\n", name);
}

static int anyConfigErrors = FALSE;

static void
checkIfVarIsConfigured(char *name)
{
	if (!find_var(name) || !sval(name))
	{
		anyConfigErrors = TRUE;
		reportMissingVar(name);
	}
}

static void
checkIfConfigured(char *names[])
{
	int ii;
	for(ii = 0; names[ii]; ii++)
		checkIfVarIsConfigured(names[ii]);
}

static void
checkConfiguredAndSize(char *names[], char *msg)
{
	int ii;
	int sz0;

	for (ii = 0; names[ii]; ii++)
	{
		checkIfVarIsConfigured(names[ii]);
		confirm_var(names[ii]);
	}
	sz0 = arraySizeName(names[0]);
	for (ii = 1; names[ii]; ii++)
	{
		if (arraySizeName(names[ii]) != sz0)
		{
			anyConfigErrors = TRUE;
			elog(ERROR, "ERROR: Number of elements in %s definitions are different %s and %s.  Check your configuration\n", msg, names[0], names[ii]);
		}
	}
}

int
checkSpecificResourceConflict(char *name, char *host, int port, char *dir, int is_gtm)
{
	if (checkNameConflict(name, is_gtm))
		return 1;
	if (checkPortConflict(host, port))
		return 1;
	if (checkDirConflict(host, dir))
		return 1;
	return 0;
}
/*
 * Note that 1 will be returned when a conflict is found
 */
int
checkNameConflict(char *name, int is_gtm)
{
	int ii;

	/*
	 * GTM Master
	 */
	if (!is_gtm && strcasecmp(name, sval(VAR_gtmName)) == 0)
		return 1;
	/*
	 * GTM Proxy
	 */
	if (isVarYes(VAR_gtmProxy))
		for (ii = 0; aval(VAR_gtmProxyNames)[ii]; ii++)
			if (strcasecmp(name, aval(VAR_gtmProxyNames)[ii]) == 0)
				return 1;
	/*
	 * Coordinator
	 */
	for (ii = 0; aval(VAR_coordNames)[ii]; ii++)
		if (strcasecmp(name, aval(VAR_coordNames)[ii]) == 0)
			return 1;
	/*
	 * Datanode
	 */
	for (ii = 0; aval(VAR_datanodeNames)[ii]; ii++)
		if (strcasecmp(name, aval(VAR_datanodeNames)[ii]) == 0)
			return 1;
	return 0;
}

/*
 * Note that 1 will be returned when a conflict is found.
 */
int
checkPortConflict(char *host, int port)
{
	int ii;

	/* GTM Master */
	if ((strcasecmp(host, sval(VAR_gtmMasterServer)) == 0) && (atoi(sval(VAR_gtmMasterPort)) == port))
		return 1;
	/* GTM Slave */
	if (isVarYes(VAR_gtmSlave) && (strcasecmp(host, sval(VAR_gtmSlaveServer)) == 0) && (atoi(sval(VAR_gtmSlavePort)) == port))
		return 1;
	/* GTM Proxy */
	if (isVarYes(VAR_gtmProxy))
		for (ii = 0; aval(VAR_gtmProxyNames)[ii]; ii++)
			if ((strcasecmp(host, aval(VAR_gtmProxyServers)[ii]) == 0) && (atoi(aval(VAR_gtmProxyPorts)[ii]) == port))
				return 1;
	/* Coordinator Master */
	for (ii = 0; aval(VAR_coordNames)[ii]; ii++)
		if ((strcasecmp(host, aval(VAR_coordMasterServers)[ii]) == 0) && 
			((atoi(aval(VAR_coordPorts)[ii]) == port) || (atoi(aval(VAR_poolerPorts)[ii])) == port))
			return 1;
	/* Coordinator Slave */
	if (isVarYes(VAR_coordSlave))
		for (ii = 0; aval(VAR_coordNames)[ii]; ii++)
			if (doesExist(VAR_coordSlaveServers, ii) && !is_none(aval(VAR_coordSlaveServers)[ii]) && 
				(strcasecmp(host, aval(VAR_coordSlaveServers)[ii]) == 0) && (atoi(aval(VAR_coordPorts)[ii]) == port))
				return 1;
	/* Datanode Master */
	for (ii = 0; aval(VAR_datanodeNames)[ii]; ii++)
		if ((strcasecmp(host, aval(VAR_datanodeMasterServers)[ii]) == 0) && (atoi(aval(VAR_datanodePorts)[ii]) == port))
			return 1;
	/* Datanode Slave */
	if (isVarYes(VAR_datanodeSlave))
		for (ii = 0; aval(VAR_datanodeNames)[ii]; ii++)
			if (doesExist(VAR_datanodeSlaveServers, ii) && !is_none(aval(VAR_datanodeSlaveServers)[ii]) && 
				(strcasecmp(host, aval(VAR_datanodeSlaveServers)[ii]) == 0) && (atoi(aval(VAR_datanodePorts)[ii]) == port))
				return 1;
	return 0;
}

int
checkDirConflict(char *host, char *dir)
{
	int ii;

	/* GTM Master */
	if ((strcasecmp(host, sval(VAR_gtmMasterServer)) == 0) && (strcmp(dir, sval(VAR_gtmMasterDir)) == 0))
		return 1;
	/* GTM Slave */
	if (isVarYes(VAR_gtmSlave) && (strcasecmp(host, sval(VAR_gtmSlaveServer)) == 0) && (strcmp(dir, sval(VAR_gtmSlaveDir)) == 0))
		return 1;
	/* GTM Proxy */
	if (isVarYes(VAR_gtmProxy))
		for (ii = 0; aval(VAR_gtmProxyNames)[ii]; ii++)
			if ((strcasecmp(host, aval(VAR_gtmProxyServers)[ii]) == 0) && (strcmp(dir, aval(VAR_gtmProxyDirs)[ii]) == 0))
				return 1;
	/* Coordinator Master */
	for (ii = 0; aval(VAR_coordNames)[ii]; ii++)
		if ((strcasecmp(host, aval(VAR_coordMasterServers)[ii]) == 0) && (strcmp(dir, aval(VAR_coordMasterDirs)[ii]) == 0))
			return 1;
	/* Coordinator Slave */
	if (isVarYes(VAR_coordSlave))
		for (ii = 0; aval(VAR_coordNames)[ii]; ii++)
			if ((strcasecmp(host, aval(VAR_coordSlaveServers)[ii]) == 0) && (strcmp(dir, aval(VAR_coordSlaveDirs)[ii]) == 0))
				return 1;
	/* Datanode Master */
	for (ii = 0; aval(VAR_datanodeNames)[ii]; ii++)
		if ((strcasecmp(host, aval(VAR_datanodeMasterServers)[ii]) == 0) && (strcmp(dir, aval(VAR_datanodeMasterDirs)[ii]) == 0))
			return 1;
	/* Datanode Slave */
	if (isVarYes(VAR_datanodeSlave))
		if (doesExist(VAR_datanodeSlaveServers, ii) && doesExist(VAR_datanodeSlaveDirs, ii) &&
			(strcasecmp(host, aval(VAR_datanodeSlaveServers)[ii]) == 0) && (strcmp(dir, aval(VAR_datanodeSlaveDirs)[ii]) == 0))
			return 1;
	return 0;
}

/*
 * Check if there's any conflict among src and dest, checks duplicate in names, servers, ports and directories.
 *
 * The rules are:
 *
 * 1) Each node (gtm, gtm_proxy, coordinator, datanode) must have unique name.
 *
 * 2) A port, in a given host, must be owned (listed to) only by single node.
 *
 * 3) A directory, in a given host, must be owned (used) only by single node.
 */
static void
checkResourceConflict(char *srcNames, char *srcServers, char *srcPorts, char *srcPoolers, char *srcDirs,
					  char *destNames, char *destServers, char *destPorts, char *destPoolers, char *destDirs, 
					  int destOnly, int checkName)
{
	int ii, jj;

	if (!srcNames || !find_var(srcNames))
	{
		/* No source specified */
		return;
	}
	if (!destOnly)
	{
		/* Check conflict among the source first */
		for (ii = 0; aval(srcNames)[ii]; ii++)
		{
			if (is_none(aval(srcNames)[ii]))
				continue;
			/* Pooler and the port in the same name */
			if (srcPoolers && (atoi(aval(srcPorts)[ii]) == atoi(aval(srcPoolers)[ii])))
			{
				if (atoi(aval(srcPorts)[ii]) > 0)
				{
					anyConfigErrors = TRUE;
					elog(ERROR, "ERROR: Conflict in between port and pooler within %s variable.\n", srcNames);
				}
			}
			if (checkName && srcNames && !doesExist(srcNames, ii))
				assign_arrayEl(srcNames, ii, "none", NULL);
			if (srcServers && !doesExist(srcServers, ii))
				assign_arrayEl(srcServers, ii, "none", NULL);
			if (srcPoolers && !doesExist(srcPoolers, ii))
				assign_arrayEl(srcPoolers, ii, "-1", "-1");
			if (srcPorts && !doesExist(srcPorts, ii))
				assign_arrayEl(srcPorts, ii, "-1", "-1");
			if (srcDirs && !doesExist(srcDirs, ii))
				assign_arrayEl(srcDirs, ii, "none", NULL);
			for (jj = ii+1; aval(srcNames)[jj]; jj++)
			{
				/* Name conflict */
				if (checkName && srcNames && !doesExist(srcNames, jj))
					assign_arrayEl(srcNames, jj, "none", NULL);
				if (checkName && srcNames && (strcmp(aval(srcNames)[ii], aval(srcNames)[jj]) == 0))
				{
					anyConfigErrors = TRUE;
					elog(ERROR, "ERROR: Conflict in resource name within %s variable.\n", srcNames);
				}
				if (srcServers && is_none(aval(srcServers)[ii]))
					continue;
				if (srcServers && !doesExist(srcServers, jj))
					assign_arrayEl(srcServers, jj, "none", NULL);
				if (srcServers && strcmp(aval(srcServers)[ii], aval(srcServers)[jj]) == 0)
				{
					/* Ports and Poolers */
					if (srcPorts && !doesExist(srcPorts, jj))
						assign_arrayEl(srcPorts, jj, "-1", "-1");
					if (srcPoolers && !doesExist(srcPoolers, jj))
						assign_arrayEl(srcPoolers, jj, "-1", "-1");
					if((srcPorts && (atoi(aval(srcPorts)[ii]) > 0) && (atoi(aval(srcPorts)[ii]) == atoi(aval(srcPorts)[jj]))) ||
					   (srcPorts && srcPoolers && (atoi(aval(srcPorts)[ii]) > 0) && (atoi(aval(srcPorts)[ii]) == atoi(aval(srcPoolers)[jj]))) ||
					   (srcPoolers && (atoi(aval(srcPoolers)[ii]) > 0) && (atoi(aval(srcPoolers)[ii]) == atoi(aval(srcPoolers)[jj]))))
					{
						anyConfigErrors = TRUE;
						elog(ERROR, "ERROR: Conflict in port and pooler numbers within  %s variable.\n", srcNames);
					}
					/* Directories */
					if (srcDirs && !doesExist(srcDirs, jj))
						assign_arrayEl(srcDirs, jj, "none", NULL);
					if (srcDirs && strcmp(aval(srcDirs)[ii], aval(srcDirs)[jj]) == 0)
					{
						if (!is_none(aval(srcDirs)[ii]))
						{
							anyConfigErrors = TRUE;
							elog(ERROR, "ERROR: Conflict in directories within  %s variable.\n", srcNames);
						}
					}
				}
			}
		}
	}
	/* Check between src and destination */
	if (destNames)
	{
		for (ii = 0; aval(srcNames)[ii]; ii++)
		{
			if (is_none(aval(srcNames)[ii]))
				continue;
			for (jj = 0; aval(destNames)[jj]; jj++)
			{
				/* Resource names */
				if (checkName && (strcmp(aval(srcNames)[ii], aval(destNames)[jj]) == 0))
				{
					anyConfigErrors = TRUE;
					elog(ERROR, "ERROR: Conflict in names between  %s and %s variable.\n", srcNames, destNames);
				}
				if (destServers && !doesExist(destServers, jj))
					assign_arrayEl(destServers, jj, "none", NULL);
				if (srcServers && destServers && (strcmp(aval(srcServers)[ii], aval(destServers)[jj]) == 0) && !is_none(aval(srcServers)[ii]))
				{
					/* Ports and poolers */
					if (destPorts && !doesExist(destPorts, jj))
						assign_arrayEl(destPorts, jj, "-1", "-1");
					if (destPoolers && !doesExist(destPoolers, jj))
						assign_arrayEl(destPoolers, jj, "-1", "-1");
					if ((srcPorts && destPorts && (atoi(aval(srcPorts)[ii]) == atoi(aval(destPorts)[jj])) && (atoi(aval(srcPorts)[ii]) > 0)) ||
						(destPoolers && srcPorts && (destPoolers && (atoi(aval(srcPorts)[ii]) == atoi(aval(destPoolers)[jj]))) && (atoi(aval(srcPorts)[ii]) > 0))  ||
						(srcPoolers && destPorts && (atoi(aval(srcPoolers)[ii]) == atoi(aval(destPorts)[jj])) && (atoi(aval(srcPoolers)[ii]) > 0)) ||
						(srcPoolers && destPoolers && (atoi(aval(srcPoolers)[ii]) == atoi(aval(destPoolers)[jj])) && (atoi(aval(srcPoolers)[ii]) > 0)))
					{
						anyConfigErrors = TRUE;
						elog(ERROR, "ERROR: Conflict in port/pooler in %s and %s variable.\n", srcNames, destNames);
					}
					/* Dir Names */
					if (srcDirs && destDirs && !is_none(aval(srcDirs)[ii]) && (strcmp(aval(srcDirs)[ii], aval(destDirs)[jj]) == 0))
					{
						anyConfigErrors = TRUE;
						elog(ERROR, "ERROR: Conflict in directory names in %s and %s variable.\n", srcNames, destNames);
					}
				}
			}
		}
	}
}

/* 
 * Check if each node resource is configured properly
 * Again, finding an error will not make the program stop.
 */
static void
verifyResource(void)
{
	char *GtmVars[] = {VAR_gtmName, 
					   VAR_gtmMasterServer, 
					   VAR_gtmMasterPort, 
					   VAR_gtmMasterDir, 
					   NULL};
	char *GtmSlaveVars[] = {VAR_gtmSlaveServer, 
							VAR_gtmSlavePort, 
							VAR_gtmSlaveDir, 
							NULL};
	char *gtmProxyVars[] = {VAR_gtmProxyNames, 
							VAR_gtmProxyServers, 
							VAR_gtmProxyPorts, 
							VAR_gtmProxyDirs, 
							NULL};
	char *coordMasterVars[] = {VAR_coordNames, 
							   VAR_coordPorts, 
							   VAR_poolerPorts, 
							   VAR_coordMasterServers,
							   VAR_coordMasterDirs, 
							   VAR_coordMaxWALSenders, 
							   NULL};
	char *coordSlaveVars[] = {VAR_coordNames, 
							  VAR_coordSlaveServers, 
							  VAR_coordSlaveDirs, 
							  VAR_coordArchLogDirs, 
							  NULL};
#if 0
	/*
	 * Please note that at present, pgxc_ctl supports only synchronous replication
	 * between {coordinator|datanode} master and server.
	 *
	 * Start/stop operation of the master and failover operation is affected by this
	 * settings.  Will be improved soon.
	 */
	char *coordSlaveSVars[] = {VAR_coordSlaveSync, NULL};	/* For extension */
#endif
	char *datanodeMasterVars[] = {VAR_datanodeNames, 
								  VAR_datanodePorts, 
								  VAR_datanodeMasterServers,
								  VAR_datanodeMasterDirs, 
								  VAR_datanodeMaxWALSenders, 
								  NULL};
	char *datanodeSlaveVars[] = {VAR_datanodeNames,
								 VAR_datanodeSlaveServers,
								 VAR_datanodeSlaveDirs,
								 VAR_datanodeArchLogDirs,
								 NULL};
#if 0
	char *datanodeSlaveSVars[] = {VAR_datanodeSlaveSync, NULL};	/* For extension, see above */
#endif
	
	/*
	 * -------------- Fundamental check -------------------
	 */
	anyConfigErrors = FALSE;
	/* GTM */
	checkIfConfigured(GtmVars);
	/* GTM slave */
	if (isVarYes(VAR_gtmSlave))
		checkIfConfigured(GtmSlaveVars);
	/* GTM proxy */
	if (isVarYes(VAR_gtmProxy))
		checkConfiguredAndSize(gtmProxyVars, "GTM Proxy");
	/* Coordinator Master */
	checkIfConfigured(coordMasterVars);
	checkConfiguredAndSize(coordMasterVars, "coordinator master");
	/* Coordinator Slave */
	if (isVarYes(VAR_coordSlave))
	{
#if 0
		checkIfConfigured(coordSlaveSVars);	/* For extension, see above */

#endif
		checkConfiguredAndSize(coordSlaveVars, "coordinator slave");
	}
	/* Datanode Master */
	checkConfiguredAndSize(datanodeMasterVars, "datanode master");
	/* Datanode Slave */
	if (sval(VAR_datanodeSlave) && strcmp(sval(VAR_datanodeSlave), "y") == 0)
	{
#if 0
		checkIfConfigured(datanodeSlaveSVars);	/* For extension, see above */
#endif
		checkConfiguredAndSize(datanodeSlaveVars, "datanode slave");
	}
	if (anyConfigErrors)
		elog(ERROR, "ERROR: Found fundamental configuration error.\n");
	/*
	 * --------------- Resource Conflict Check ---------------------
	 */
	/* 
	 * GTM Master and others ----------------
	 */
	anyConfigErrors = FALSE;
	/* GTM and GTM slave */
	if (isVarYes(VAR_gtmSlave))
		checkResourceConflict(VAR_gtmName, VAR_gtmMasterServer, VAR_gtmMasterPort, NULL, VAR_gtmMasterDir,
							  VAR_gtmName, VAR_gtmSlaveServer, VAR_gtmSlavePort, NULL, VAR_gtmSlaveDir, TRUE, FALSE);
	/* GTM and GTM Proxy, if any */
	if (isVarYes(VAR_gtmProxy))
		checkResourceConflict(VAR_gtmName, VAR_gtmMasterServer, VAR_gtmMasterPort, NULL, VAR_gtmMasterDir,
							  VAR_gtmProxyNames, VAR_gtmProxyServers, VAR_gtmProxyPorts, NULL, VAR_gtmProxyDirs, TRUE, TRUE);
	/* GTM and coordinator masters */
	checkResourceConflict(VAR_gtmName, VAR_gtmMasterServer, VAR_gtmMasterPort, NULL, VAR_gtmMasterDir,
						  VAR_coordNames, VAR_coordMasterServers, VAR_coordPorts, VAR_poolerPorts, VAR_coordMasterDirs,
						  TRUE, TRUE);
	/* GTM and coordinator slaves, if any */
	if (isVarYes(VAR_coordSlave))
		checkResourceConflict(VAR_gtmName, VAR_gtmMasterServer, VAR_gtmMasterPort, NULL, VAR_gtmMasterDir,
							  VAR_coordNames, VAR_coordSlaveServers, VAR_coordPorts, NULL, VAR_coordSlaveDirs, TRUE, TRUE);
	/* GTM and datanode masters */
	checkResourceConflict(VAR_gtmName, VAR_gtmMasterServer, VAR_gtmMasterPort, NULL, VAR_gtmMasterDir,
						  VAR_datanodeNames, VAR_datanodeMasterServers, VAR_datanodePorts, NULL, VAR_datanodeMasterDirs, TRUE, TRUE);
	/* GTM and datanode slaves, if any */
	if(isVarYes(VAR_datanodeSlave))
		checkResourceConflict(VAR_gtmName, VAR_gtmMasterServer, VAR_gtmMasterPort, NULL, VAR_gtmMasterDir,
							  VAR_datanodeNames, VAR_datanodeMasterServers, VAR_datanodePorts, NULL, VAR_datanodeSlaveDirs,
							  TRUE, TRUE);
	/* 
	 * GTM slave and others ------------
	 */
	if (isVarYes(VAR_gtmSlave))
	{
		/* GTM slave and GTM Proxy, if any */
		if (isVarYes(VAR_gtmProxy))
			checkResourceConflict(VAR_gtmName, VAR_gtmSlaveServer, VAR_gtmSlavePort, NULL, VAR_gtmSlaveDir,
								  VAR_gtmProxyNames, VAR_gtmProxyServers, VAR_gtmProxyPorts, NULL, VAR_gtmProxyDirs, 
								  TRUE, TRUE);
		/* GTM slave and coordinator masters */
		checkResourceConflict(VAR_gtmName, VAR_gtmSlaveServer, VAR_gtmSlavePort, NULL, VAR_gtmSlaveDir,
							  VAR_coordNames, VAR_coordMasterServers, VAR_coordPorts, VAR_poolerPorts, VAR_coordMasterDirs,
							  TRUE, TRUE);
		/* GTM slave and coordinator slaves, if any */
		if (isVarYes(VAR_coordSlave))
			checkResourceConflict(VAR_gtmName, VAR_gtmSlaveServer, VAR_gtmSlavePort, NULL, VAR_gtmSlaveDir,
								  VAR_coordNames, VAR_coordSlaveServers, VAR_coordPorts, VAR_poolerPorts, VAR_coordSlaveDirs,
								  TRUE, TRUE);
		/* GTM slave and datanode masters */
		checkResourceConflict(VAR_gtmName, VAR_gtmSlaveServer, VAR_gtmSlavePort, NULL, VAR_gtmSlaveDir,
							  VAR_datanodeNames, VAR_datanodeMasterServers, VAR_datanodePorts, NULL, VAR_datanodeMasterDirs,
							  TRUE, TRUE);
		/* GTM slave and datanode slave, if any */
		if (isVarYes(VAR_datanodeSlave))
			checkResourceConflict(VAR_gtmName, VAR_gtmSlaveServer, VAR_gtmSlavePort, NULL, VAR_gtmSlaveDir,
								  VAR_datanodeNames, VAR_datanodeSlaveServers, VAR_datanodePorts, NULL, VAR_datanodeSlaveDirs,
								  TRUE, TRUE);
	}
	/* 
	 * GTM proxy and others ---------
	 */
	if (isVarYes(VAR_gtmProxy))
	{
		/* GTM proxy and coordinator masters */
		checkResourceConflict(VAR_gtmProxyNames, VAR_gtmProxyServers, VAR_gtmProxyPorts, NULL, VAR_gtmProxyDirs,
							  VAR_coordNames, VAR_coordMasterServers, VAR_coordPorts, VAR_poolerPorts, VAR_coordMasterDirs,
							  FALSE, TRUE);
		/* GTM proxy and coordinator slaves, if any */
		if (sval(VAR_coordSlave) && (strcmp(sval(VAR_coordSlave), "y") == 0))
			checkResourceConflict(VAR_gtmProxyNames, VAR_gtmProxyServers, VAR_gtmProxyPorts, NULL, VAR_gtmProxyDirs,
								  VAR_coordNames, VAR_coordSlaveServers, VAR_coordPorts, VAR_poolerPorts, VAR_coordSlaveDirs,
								  TRUE, TRUE);
		/* GTM proxy and datanode masters */
			checkResourceConflict(VAR_gtmProxyNames, VAR_gtmProxyServers, VAR_gtmProxyPorts, NULL, VAR_gtmProxyDirs,
							  VAR_datanodeNames, VAR_datanodeMasterServers, VAR_datanodePorts, NULL, VAR_datanodeMasterDirs,
							  TRUE, TRUE);
		/* GTM proxy and datanode slave, if any */
		if (sval(VAR_datanodeSlave) && (strcmp(sval(VAR_datanodeSlave), "y") == 0))
			checkResourceConflict(VAR_gtmProxyNames, VAR_gtmProxyServers, VAR_gtmProxyPorts, NULL, VAR_gtmProxyDirs,
								  VAR_datanodeNames, VAR_datanodeSlaveServers, VAR_datanodePorts, NULL, VAR_datanodeSlaveDirs,
								  TRUE, TRUE);
	}
	/* 
	 * Coordinator Masters and others
	 */
	/* Coordinator master and coordinator slaves, if any */
	if (isVarYes(VAR_coordSlave))
		checkResourceConflict(VAR_coordNames, VAR_coordMasterServers, VAR_coordPorts, VAR_poolerPorts, VAR_coordMasterDirs,
							  VAR_coordNames, VAR_coordSlaveServers, VAR_coordPorts, VAR_poolerPorts, VAR_coordSlaveDirs,
							  TRUE, FALSE);
	/* Coordinator masters and datanode masters */
	checkResourceConflict(VAR_coordNames, VAR_coordMasterServers, VAR_coordPorts, VAR_poolerPorts, VAR_coordMasterDirs,
						  VAR_datanodeNames, VAR_datanodeMasterServers, VAR_datanodePorts, NULL, VAR_datanodeMasterDirs,
						  FALSE, TRUE);
	/* Coordinator masters and datanode slave, if any */
	if (isVarYes(VAR_datanodeSlave))
		checkResourceConflict(VAR_coordNames, VAR_coordMasterServers, VAR_coordPorts, VAR_poolerPorts, VAR_coordMasterDirs,
							  VAR_datanodeNames, VAR_datanodeSlaveServers, VAR_datanodePorts, NULL, VAR_datanodeSlaveDirs,
							  TRUE, TRUE);
	/* 
	 * Coordinator slaves and others
	 */
	if (isVarYes(VAR_coordSlave))
	{
		/* Coordinator slave and datanode masters */
		checkResourceConflict(VAR_coordNames, VAR_coordSlaveServers, VAR_coordPorts, VAR_poolerPorts, VAR_coordSlaveDirs,
							  VAR_datanodeNames, VAR_datanodeSlaveServers, VAR_datanodePorts, NULL, VAR_datanodeSlaveDirs,
							  FALSE, TRUE);
		/* Coordinator slave and datanode slave, if any */
		if (isVarYes(VAR_datanodeSlave))
			checkResourceConflict(VAR_coordNames, VAR_coordSlaveServers, VAR_coordPorts, VAR_poolerPorts, VAR_coordSlaveDirs,
								  VAR_datanodeNames, VAR_datanodeSlaveServers, VAR_datanodePorts, NULL, VAR_datanodeSlaveDirs,
								  TRUE, TRUE);
	}
	/* 
	 * Datanode masters and others ---
	 */
	/* Datanode master self */
	checkResourceConflict(VAR_datanodeNames, VAR_datanodeMasterServers, VAR_datanodePorts, NULL, VAR_datanodeMasterDirs,
						  NULL, NULL, NULL, NULL, NULL,
						  FALSE, TRUE);
	/* Datanode master and datanode slave, if any */
	if (sval(VAR_datanodeSlave) && (strcmp(sval(VAR_datanodeSlave), "y") == 0))
		checkResourceConflict(VAR_datanodeNames, VAR_datanodeMasterServers, VAR_datanodePorts, NULL, VAR_datanodeMasterDirs,
							  VAR_datanodeNames, VAR_datanodeSlaveServers, VAR_datanodePorts, NULL, VAR_datanodeSlaveDirs,
							  TRUE, FALSE);
	if (anyConfigErrors)
	{
		elog(ERROR, "ERROR: Found conflicts among resources.  Exiting.\n");
		exit(1);
	}
}

/*
 * Check if the minimum components are configured --- gtm master, coordinator master and datanode master.
 */
void
check_configuration(void)
{
	/*
	 * See if mandatory configuration is defined.  Will continue if error is detected
	 * to check all the errors at a time.
	 */
	/* GTM Master */
	if (!find_var(VAR_gtmName) || !find_var(VAR_gtmMasterServer) || !find_var(VAR_gtmMasterPort) || !find_var(VAR_gtmMasterDir))
		elog(ERROR, "ERROR: GTM master configuration is missing. gtmName, gtmMasterServer, gtmMasterPort or gtmMasterDir\n");
	/* Coordinator Master */
	if (!find_var(VAR_coordNames) || !find_var(VAR_coordPorts) || !find_var(VAR_poolerPorts) ||
	    !find_var(VAR_coordMasterServers) || !find_var(VAR_coordMasterDirs))
		elog(ERROR, "ERROR: Coordinator master configuration is missing. coordNames, coodPorts, poolerPorts, coordMasterPorts or coordMasterDirs\n");
	/* Datanode Master */
	if (!find_var(VAR_datanodeNames) || !find_var(VAR_datanodePorts) || !find_var(VAR_datanodeMasterServers) ||
		!find_var(VAR_datanodeMasterDirs))
		elog(ERROR, "ERROR: Datanode master configuration is missing. datanodeNames, datanodePorts, datanodeMasterPorts or datanodeMasterDirs\n");
	handle_no_slaves();
	verifyResource();
	makeServerList();
}

/*
 * Backup configuration files to a remote site as specified.
 */
int
backup_configuration(void)
{
	if ((strcasecmp(sval(VAR_configBackup), "y") != 0) || is_none(sval(VAR_configBackupHost)) || 
		is_none(sval(VAR_configBackupDir)) || is_none(sval(VAR_configBackupFile)))
		return (2);
	return(doImmediate(NULL, NULL, "scp %s %s@%s:%s/%s",
					   pgxc_ctl_config_path,
					   sval(VAR_pgxcUser), sval(VAR_configBackupHost),
					   sval(VAR_configBackupDir), sval(VAR_configBackupFile)));
}

NodeType
getNodeType(char *nodeName)
{
	int ii;

	/* Check GTM */
	if (strcmp(nodeName, sval(VAR_gtmName)) == 0)
		return NodeType_GTM;
	/* GTM_Proxy */
	for (ii = 0; aval(VAR_gtmProxyNames)[ii]; ii++)
		if (strcmp(nodeName, aval(VAR_gtmProxyNames)[ii]) == 0)
			return NodeType_GTM_PROXY;
	/* Coordinator */
	for (ii = 0; aval(VAR_coordNames)[ii]; ii++)
		if (strcmp(nodeName, aval(VAR_coordNames)[ii]) == 0)
			return NodeType_COORDINATOR;
	/* Datanode */
	for (ii = 0; aval(VAR_datanodeNames)[ii]; ii++)
		if (strcmp(nodeName, aval(VAR_datanodeNames)[ii]) == 0)
			return NodeType_DATANODE;
	/* Nodename */
	for (ii = 0; aval(VAR_allServers)[ii]; ii++)
		if (strcmp(nodeName, aval(VAR_allServers)[ii]) == 0)
			return NodeType_SERVER;
	return NodeType_UNDEF;

}

int
getDefaultWalSender(int isCoord)
{
	int ii;

	char *names = isCoord ? VAR_coordNames : VAR_datanodeNames;
	char *walSender = isCoord ? VAR_coordMaxWALSenders : VAR_datanodeMaxWALSenders;
	
	for (ii = 0; aval(names)[ii]; ii++)
	{
		if (doesExist(names, ii) && !is_none(aval(names)[ii]) && (atoi(aval(walSender)[ii]) >= 0))
			return atoi(aval(walSender)[ii]);
	}
	return 0;
}
