#include "postgres_fe.h"
#include "libpq-fe.h"
#include "libpq-int.h"


static void usage(void);

static void
usage(void)
{
	printf("Usage:\n\
  pipeline_send <host> <database name> <user> <stream>\n");
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
	char connectstr[64];
	char *line = NULL;
  int status = 0;
	PGconn *conn;
	PGresult *res;
	size_t size;

	if (argc < 5)
	{
		usage();
		exit(1);
	}

	host = argv[1];
	dbname = argv[2];
	user = argv[3];
	stream = argv[4];
	sprintf(connectstr, "host='%s' dbname='%s' user='%s'", host, dbname, user);
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
	while (getline(&line, &size, stdin) > 0)
	{
		/* Trim \n */
		line[strlen(line) - 1] = '\0';
		if (PQsendEvent(stream, line, (int)strlen(line), conn) != 0)
		{
			printf("error sending %s\n", line);
		}
	}
	res = PQexec(conn, "COMMIT");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		printf("error committing transaction\n");
		status = 1;
	}

	PQfinish(conn);
	free(line);

	return status;
}
