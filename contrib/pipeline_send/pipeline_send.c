#include "postgres_fe.h"
#include "libpq-fe.h"


static void usage(void);

static void
usage(void)
{
	printf("Usage:\n\
  pipeline_send <host> <database name> <user>\n");
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
	char connectstr[64];
	char *line = NULL;
	PGconn *conn;
	size_t size;

	if (argc < 4)
	{
		usage();
		exit(1);
	}

	host = argv[1];
	dbname = argv[2];
	user = argv[3];
	sprintf(connectstr, "host='%s' dbname='%s' user='%s'", host, dbname, user);
	conn = PQconnectdb(connectstr);
	if (conn == NULL)
	{
		printf("Could not connect\n");
		exit(1);
	}
	switch (PQstatus(conn))
	{
		case CONNECTION_OK:
			break;
		default:
			printf("Error connecting to database\n");
			PQfinish(conn);
			exit(1);
	}

	/* Read in event data from stdin. Each line is a separate event */
	while (getline(&line, &size, stdin) > 0)
	{
		/* Trim \n */
		line[strlen(line) - 1] = '\0';
		if (PQsendEvent(line, (int)strlen(line), conn) != 0)
		{
			printf("Error sending %s\n", line);
		}

	}
	free(line);

	PQfinish(conn);

	return 0;
}
