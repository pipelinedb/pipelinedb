/*-------------------------------------------------------------------------
 *
 * combinerReceiver.c
 *	  An implementation of DestReceiver that that allows combiners to receive
 *	  tuples from worker processes.
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/combinerReceiver.c
 *
 */
#include <sys/socket.h>
#include <sys/un.h>

#include "postgres.h"

#include "access/printtup.h"
#include "pipeline/combinerReceiver.h"

int CombinerSock = -1;

typedef struct
{
	DestReceiver pub;
	CombinerDesc *desc;
} CombinerState;


static void
combiner_printtup_handler(StringInfo buf)
{
	int len = 1;

	elog(LOG, "HERE WITH THE BUF=%d", buf->len);
//  if (send(c->desc->sock, NULL, len, 0) == -1)
//  {
//
//  }
}

static void
combiner_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
  struct sockaddr_un remote;
  int len;
  int attempts = 0;

  if ((CombinerSock = socket(AF_UNIX, SOCK_STREAM, 0)) == -1)
  	elog(ERROR, "worker could not create combiner socket \"%s\"", "combiner_test_view");

  remote.sun_family = AF_UNIX;
  strcpy(remote.sun_path, "combiner_test_view");

  len = strlen(remote.sun_path) + sizeof(remote.sun_family);

  /* this will retry for up to 10 seconds */
  while (attempts++ < 100)
  {
		if (connect(CombinerSock, (struct sockaddr *) &remote, len) == 0)
			break;
		else
			pg_usleep(100*1000); /* 0.1s */
  }
}

static void
combiner_shutdown(DestReceiver *self)
{
	pfree(self);
}

static void
combiner_destroy(DestReceiver *self)
{

}

extern DestReceiver *
CreateCombinerDestReceiver(void)
{
	return (DestReceiver *) printtup_create_combiner_DR(DestCombiner,
			&combiner_printtup_handler,
			&combiner_startup);
}

/*
 * Set parameters for a CombinerDestReceiver
 */
extern void
SetCombinerDestReceiverParams(DestReceiver *self, CombinerDesc *combiner)
{
//	CombinerState *c = (CombinerState *) self;
//  c->desc = combiner;

}
