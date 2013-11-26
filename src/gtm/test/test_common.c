#include "test_common.h"

pthread_key_t     threadinfo_key;

GTM_ThreadID      TopMostThreadID;

GTM_Conn *conn = NULL;
GTM_Conn *conn2 = NULL;
GTM_Timestamp *timestamp = NULL;
char connect_string[100];

void
print_nodeinfo(GTM_PGXCNodeInfo d)
{
	client_log(("type=%d, nodename=%s, proxyname=%s, ipaddress=%s, port=%d, datafolder=%s, status=%d\n",
		    d.type,
		    d.nodename,
		    d.proxyname,
		    d.ipaddress,
		    d.port,
		    d.datafolder,
		    d.status));
}


/*
 * Connect to active GTM.
 */
void
connect1()
{
	sprintf(connect_string, "host=localhost port=6666 node_name=one_zero_one remote_type=%d",
		GTM_NODE_GTM);
	
	conn = PQconnectGTM(connect_string);
	if (conn == NULL)
	{
		client_log(("Error in connection\n"));
		exit(1);
	}
	client_log(("PGconnectGTM() ok.\n"));
}

/*
 * Connect to standby GTM.
 */
void
connect2()
{
	sprintf(connect_string, "host=localhost port=6667 node_name=one_zero_two remote_type=%d",
		GTM_NODE_GTM);
	
	conn = PQconnectGTM(connect_string);
	if (conn == NULL)
	{
		client_log(("Error in connection\n"));
		exit(1);
	}
	client_log(("PGconnectGTM() ok.\n"));
}


/*
 * Get a word count with using grep command in a log file.
 */
int
grep_count(const char *file, const char *key)
{
	FILE *fp;
	int count;
	char cmd[1024];

	snprintf(cmd, sizeof(cmd), "grep -c '%s' %s", key, file);

	fp = popen(cmd, "r");
	fscanf(fp, "%d", &count);
	pclose(fp);

	return count;
}
