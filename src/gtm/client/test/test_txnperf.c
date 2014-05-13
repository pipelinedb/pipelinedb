/*
 * Copyright (c) 2010-2012 Postgres-XC Development Group
 */
#include <sys/types.h>
#include <unistd.h>

#include "gtm/gtm_c.h"
#include "gtm/libpq-fe.h"
#include "gtm/gtm_client.h"
#include <sys/time.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/wait.h>

#define client_log(x)

extern int      optind;
extern char *optarg;

/* Calculate time difference */
static void
diffTime(struct timeval *t1, struct timeval *t2, struct timeval *result)
{
    int sec = t1->tv_sec - t2->tv_sec;
    int usec = t1->tv_usec - t2->tv_usec;
    if (usec < 0)
    {
        usec += 1000000;
        sec--;
    }
    result->tv_sec = sec;
    result->tv_usec = usec;
}

/*
 * Help display should match 
 */
static void
help(const char *progname)
{
	printf(_("Usage:\n  %s [OPTION]...\n\n"), progname);
	printf(_("Options:\n"));
	printf(_("  -h hostname     GTM proxy/server hostname/IP\n"));
	printf(_("  -p port         GTM proxy/serevr port number\n"));
	printf(_("  -c count        Number of clients\n"));
	printf(_("  -n count        Number of transactions per client\n"));
	printf(_("  -s count        Number of statements per transaction\n"));
	printf(_("  -i id           Coordinator ID\n"));
}

int
main(int argc, char *argv[])
{
	int ii;
	int jj;
	int kk;
	char connect_string[100];
	int gtmport;
	char *tmp_name;
	int nclients;
	int ntxns_per_cli;
	int nstmts_per_txn;
	char *gtmhost;
	char opt;
	struct timeval starttime, endtime, diff;
	FILE *fp;
	FILE *fp2;
	char buf[1024];
	int testid, this_testid, max_testid;
	int snapsize = 0;
	float avg_sanpsize = 0;
	pid_t child_pids[1024];
	pid_t parent_pid;

#define TXN_COUNT		1000

	GlobalTransactionId gxid[TXN_COUNT];
	GTM_Conn *conn;
	char test_output[256], test_end[256], test_output_csv[256];
	char system_cmd[1024];

	/*
	 * Catch standard options before doing much else
	 */
	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			help(argv[0]);
			exit(0);
		}
	}

	 /*
         * Parse the command like options and set variables
         */
	while ((opt = getopt(argc, argv, "h:p:c:n:s:i:")) != -1)
        {
		switch (opt)
		{
			case 'h':
				gtmhost = strdup(optarg);
				break;

			case 'p':
				gtmport = atoi(optarg);
				break;

			case 'c':
				nclients = atoi(optarg);
				break;

			case 'n':
				ntxns_per_cli = atoi(optarg);
				break;

			case 's':
				nstmts_per_txn = atoi(optarg);
				break;

			case 'i':
				tmp_name = strdup(optarg);
				sprintf(test_output, "TEST_OUTPUT_%s\0", tmp_name);
				sprintf(test_end, "TEST_END_%s\0", tmp_name);
				sprintf(test_output_csv, "TEST_OUTPUT_%s.CSV\0", tmp_name);
				break;

			default:
				fprintf(stderr, "Unrecognized option %c\n", opt);
				help(argv[0]);
				exit(0);
		}
	}
		
	sprintf(connect_string, "host=%s port=%d node_name=%s remote_type=%d", gtmhost, gtmport, tmp_name, GTM_NODE_COORDINATOR);

	sprintf(system_cmd, "echo -------------------------------------------------------- >> %s", test_output);
	system(system_cmd);
	sprintf(system_cmd, "date >> %s", test_output);
	system(system_cmd);
	sprintf(system_cmd, "echo -------------------------------------------------------- >> %s", test_output);
	system(system_cmd);

	fp = fopen(test_output, "a+");
	fp2 = fopen(test_output_csv, "a+");
	
	max_testid = 0;
	while (fgets(buf, 1024, fp) != NULL)
	{
		if (sscanf(buf, "TEST-ID: %d", &testid) == 1)
		{
			if (max_testid < testid)
				max_testid = testid;
		}
	}
	
	this_testid = max_testid + 1;

	fprintf(fp, "TEST-ID: %d", this_testid);
	fprintf(fp, "\n\n");
	fflush(fp);

	parent_pid = getpid();

	gettimeofday(&starttime, NULL);

	/*
	 * Start as many clients 
	 */
	for (ii = 1; ii < nclients; ii++)
	{
		int cpid;
		if ((cpid = fork()) == 0)
			break;
		else
			child_pids[ii-1] = cpid;
	}

	if (getpid() == parent_pid)
		fprintf(stderr, "started %d clients\n", nclients);

	conn = PQconnectGTM(connect_string);
	if (conn == NULL)
	{
		client_log(("Error in connection\n"));
		exit(1);
	}

	if (getpid() != parent_pid)
		gettimeofday(&starttime, NULL);

	snapsize = 0;

	for (jj = 0; jj <= ntxns_per_cli / TXN_COUNT; jj++)
	{
		for (ii = 0; ii < TXN_COUNT; ii++)
		{
			if ((jj * TXN_COUNT) + ii >= ntxns_per_cli)
				break;

			gxid[ii] = begin_transaction(conn, GTM_ISOLATION_RC);
			if (gxid[ii] != InvalidGlobalTransactionId)
				client_log(("Started a new transaction (GXID:%u)\n", gxid[ii]));
			else
				client_log(("BEGIN transaction failed for ii=%d\n", ii));

			for (kk = 0; kk < nstmts_per_txn; kk++)
			{
				GTM_Snapshot snapshot = get_snapshot(conn, gxid[ii], true);
				snapsize += snapshot->sn_xcnt;
			}

			if (!prepare_transaction(conn, gxid[ii]))
				client_log(("PREPARE successful (GXID:%u)\n", gxid[ii]));
			else
				client_log(("PREPARE failed (GXID:%u)\n", gxid[ii]));

			if (ii % 2 == 0)
			{
				if (!abort_transaction(conn, gxid[ii]))
					client_log(("ROLLBACK successful (GXID:%u)\n", gxid[ii]));
				else
					client_log(("ROLLBACK failed (GXID:%u)\n", gxid[ii]));
			}
			else
			{
				if (!commit_transaction(conn, gxid[ii]))
					client_log(("COMMIT successful (GXID:%u)\n", gxid[ii]));
				else
					client_log(("COMMIT failed (GXID:%u)\n", gxid[ii]));
			}
		}

		fprintf(stderr, "client [%d] finished %d transactions\n", getpid(), (jj * TXN_COUNT) + ii);
	}
	
	GTMPQfinish(conn);

	if (parent_pid == getpid())
	{
		for (ii = 1; ii < nclients; ii++)
			wait(NULL);

		gettimeofday(&endtime, NULL);
		diffTime(&endtime, &starttime, &diff);
		avg_sanpsize =  ((float) snapsize) / (ntxns_per_cli * nstmts_per_txn);

		fprintf(fp, "\n");
		fprintf(fp, "Num of client: %d\n", nclients);
		fprintf(fp, "Num of txns/client: %d\n", ntxns_per_cli);
		fprintf(fp, "Num of statements/txn: %d\n", nstmts_per_txn);
		fprintf(fp, "TPS: %2f\n", (ntxns_per_cli * nclients) / ((float)((diff.tv_sec * 1000000) + diff.tv_usec)/1000000));
		fprintf(fp, "Total snapshot size: %d\n", snapsize);
		fprintf(fp, "Average snapshot size: %f\n", avg_sanpsize);
	
		fprintf(fp, "Time: %d.%d\n", diff.tv_sec, diff.tv_usec);
		fprintf(fp, "\n");

		sprintf(system_cmd, "touch %s\0", test_end);
		system(system_cmd);
	}
	else
	{
		gettimeofday(&endtime, NULL);
		diffTime(&endtime, &starttime, &diff);
		avg_sanpsize =  ((float) snapsize) / (ntxns_per_cli * nstmts_per_txn);
	}

	flock(fileno(fp2), LOCK_EX);
	if (parent_pid != getpid())
		fprintf(fp2, "%d,%d,%d,%d,%d,%d,%d,%f,false\n", this_testid, nclients, ntxns_per_cli, nstmts_per_txn, diff.tv_sec, diff.tv_usec, snapsize, avg_sanpsize);
	else
		fprintf(fp2, "%d,%d,%d,%d,%d,%d,%d,%f,true\n", this_testid, nclients, ntxns_per_cli, nstmts_per_txn, diff.tv_sec, diff.tv_usec, snapsize, avg_sanpsize);
		
	flock(fileno(fp2), LOCK_UN);
	fclose(fp2);

	fclose(fp);
	
	return 0;
}
