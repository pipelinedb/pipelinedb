#include <time.h>
#include "postgres_fe.h"
#include "libpq-fe.h"
#include "libpq-int.h"


static void usage(void);

static void
usage(void)
{
	printf("Usage:\n\
  pipeline_send <host> <port> <database name> <user> <stream>\n");
}


/*
 * Sends raw events to the backend. Message format is:
 *
 * Event (F)
 *   Byte1('V')
 *       Identifies the message as an event.
 *   Int32
 *       Specifies the number of bytes in the raw payload
 *   ByteN
 *       Raw payload
 */
int main(int argc, char* argv[])
{
	char *host;
	char *dbname;
	char *user;
	char *stream;
	char *port;
	char connectstr[64];
	char *line = NULL;
  int status = 0;
	int bytes = 0;
	int len = 0;
	int events = 0;
	float eps;
	float bps;
	float elapsed;
	PGconn *conn;
	PGresult *res;
	size_t size;
	clock_t start;

	if (argc < 6)
	{
		usage();
		exit(1);
	}

	host = argv[1];
	port = argv[2];
	dbname = argv[3];
	user = argv[4];
	stream = argv[5];
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

  res = PQexec(conn, "BEGIN");
  if (PQresultStatus(res) != PGRES_COMMAND_OK)
  {
  	printf("error beginning transaction\n");
  	PQfinish(conn);
  	PQclear(res);
  	exit(1);
  }

	/* Read in event data from stdin. Each line is a separate event */
  start = clock();
	while (getline(&line, &size, stdin) > 0)
	{
		len = strlen(line);
		bytes += len;
		events++;

		/* Trim \n */
		line[len - 1] = '\0';
		if (PQsendEvent(stream, line, len, conn) != 0)
		{
			printf("error sending %s\n", line);
		}
	}
	elapsed = (clock() - start) / (float)CLOCKS_PER_SEC;
	res = PQexec(conn, "COMMIT");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		printf("error committing transaction\n");
		status = 1;
	}
	eps = events / elapsed;
	bps = bytes / elapsed;

	printf("%d events, %d bytes in %.2f s\n", events, bytes, elapsed);
	printf("%.2f events/s\n", eps);
	printf("%.2f bytes/s\n", bps);

	PQfinish(conn);
	free(line);

	return status;
}
