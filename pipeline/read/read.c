#include <time.h>
#include "postgres_fe.h"
#include "libpq-fe.h"
#include "libpq-int.h"

#define MAX_BUF_SIZE 1024 * 1024

static void usage(void);

static void
usage(void)
{
	printf("Usage:\n\
  read <host> <port> <database name> <user> <query id>\n");
}


/*
 * Sends raw events to the backend
 */
int main(int argc, char* argv[])
{
	char *host;
	char *dbname;
	char *user;
	char *port;
	char connectstr[64];
  int status = 0;
  int maxevents;
  int queryid;
	PGconn *conn;
	PGresult *res;

	if (argc < 7)
	{
		usage();
		exit(1);
	}

	host = argv[1];
	port = argv[2];
	dbname = argv[3];
	user = argv[4];
	sprintf(connectstr, "host='%s' dbname='%s' user='%s' port=%s", host, dbname, user, port);
	conn = PQconnectdb(connectstr);
	if (conn == NULL)
	{
		printf("could not connect\n");
		exit(1);
	}
	switch (PQstatus(conn))
	{
		case CONNECTION_OK:
			break;
		default:
			printf("error connecting to database\n");
			PQfinish(conn);
			exit(1);
	}
	queryid = atoi(argv[5]);
	maxevents = atoi(argv[6]);

	PQreadStreamBuffer(queryid, maxevents, conn);

	PQfinish(conn);

	return status;
}
