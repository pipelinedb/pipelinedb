/*-------------------------------------------------------------------------
 *
 * gtm_util.c
 *
 *    GTM utility module of Postgres-XC configuration and operation tool.
 *
 * Copyright (c) 2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */
/*
 * This module was imported from Koichi's personal development.
 *
 * Provides unregistration of the nodes from gtm.  This operation might be
 * needed after some node crashes and its registration information remains
 * in GTM.
 */
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
/*
#include "gtm/gtm_c.h"
*/
#include "gtm/gtm_client.h"
#include "gtm/libpq-fe.h"
#include "utils.h"
#include "variables.h"
/* This is an ugly hack to avoid conflict between gtm_c.h and pgxc_ctl.h */
#undef true
#undef false
#include "pgxc_ctl_log.h"
#include "varnames.h"
#include "config.h"
#include "gtm_util.h"

typedef enum command_t
{
	CMD_INVALID = 0,
	CMD_UNREGISTER
} command_t;

static char	*nodename = NULL;
static char *myname = NULL;
static GTM_PGXCNodeType nodetype = 0;	/* Invalid */
#define GetToken() (line = get_word(line, &token))
#define testToken(word) ((token != NULL) && (strcmp(token, word) == 0))
#define TestToken(word) ((token != NULL) && (strcasecmp(token, word) == 0))

static int inputError(char *msg)
{
	elog(ERROR, "%s\n", msg);
	return -1;
}

int
unregisterFromGtm(char *line)
{
	char *token;
	int rc;

	for(;GetToken();)
	{
		if (testToken("-n"))
		{
			if (!GetToken())
				return(inputError("No -n option value was found."));
			Free(myname);
			myname = Strdup(token);
			continue;
		}
		else if (testToken("-Z"))
		{
			if (!GetToken())
				return(inputError("No -Z option value was found."));
			if (testToken("gtm"))
			{
				nodetype = GTM_NODE_GTM;
				continue;
			}
			else if (testToken("gtm_proxy"))
			{
				nodetype = GTM_NODE_GTM_PROXY;
				break;
			}
			else if (testToken("gtm_proxy_postmaster"))
			{
				nodetype = GTM_NODE_GTM_PROXY_POSTMASTER;
				break;
			}
			else if (testToken("coordinator"))
			{
				nodetype = GTM_NODE_COORDINATOR;
				break;
			}
			else if (testToken("datanode"))
			{
				nodetype = GTM_NODE_DATANODE;
				break;
			}
			else
			{
				elog(ERROR, "ERROR: Invalid -Z option value, %s\n", token);
				return(-1);
			}
			continue;
		}
		else
			break;
	}
	if (nodetype == 0)
	{
		elog(ERROR, "ERROR: no node type was specified.\n");
		return(-1);
	}

	if (myname == NULL)
		myname = Strdup(DefaultName);
	
	if (!token)
	{
		fprintf(stderr,"%s: No command specified.\n", progname);
		exit(2);
	}
	if (!GetToken())
	{
		elog(ERROR, "ERROR: unregister: no node name was found to unregister.\n");
		return(-1);
	}
	nodename = Strdup(token);
	rc = process_unregister_command(nodetype, nodename);
	Free(nodename);
	return(rc);
}

static
GTM_Conn *connectGTM()
{
	char connect_str[MAXLINE+1];

	snprintf(connect_str, MAXLINE, "host=%s port=%d node_name=%s remote_type=%d postmaster=0",
			 sval(VAR_gtmMasterServer), atoi(sval(VAR_gtmMasterPort)), (myname == NULL) ? DefaultName : myname, GTM_NODE_COORDINATOR);
	return(PQconnectGTM(connect_str));
}

int
process_unregister_command(GTM_PGXCNodeType type, char *nodename)
{
	GTM_Conn *conn;
	int res;
	
	conn = connectGTM();
	if (conn == NULL)
	{
		elog(ERROR, "ERROR: failed to connect to GTM\n");
		return -1;
	}
	res = node_unregister(conn, type, nodename);
	if (res == GTM_RESULT_OK){
		elog(NOTICE, "unregister %s from GTM.\n", nodename);
		GTMPQfinish(conn);
		return 0;
	}
	else
	{
		elog(ERROR, "ERROR: Failed to unregister %s from GTM.\n", nodename);
		GTMPQfinish(conn);
		return res;
	}
}
