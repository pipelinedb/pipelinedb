#include "postgres_fe.h"
#include "libpq-fe.h"
//#include "libpq-int.h"


/*
 * Sends raw events to the backend. Message format is:
 *
 * Event (F)
 *   Byte1('E')
 *       Identifies the message as an event.
 *   Int32(8)
 *       Length of message contents in bytes, including self.
 *   String
 *       Specifies the decoder to decode the payload with
 *   Int32
 *       Specifies the number of bytes in the raw payload
 *   ByteN
 *       Raw payload
 */



int main(int argc, char* argv[])
{
	PGconn *conn = PQconnectdb("host='localhost' dbname='derek' user='derek'");
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

	PQsendEvent("DATA!!!", conn);
	PQfinish(conn);
	printf("Done\n");

	return 0;
}
