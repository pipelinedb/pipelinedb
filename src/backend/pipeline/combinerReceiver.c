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
#include "miscadmin.h"

int CombinerSock = -1;

typedef struct
{
	DestReceiver pub;
	CombinerDesc *desc;
} CombinerState;


static void
combiner_receive(TupleTableSlot *slot, DestReceiver *self)
{
	HeapTuple tup = ExecMaterializeSlot(slot);
	uint32 nlen = htonl(tup->t_len);
	char *buf = palloc0(4 + tup->t_len);

	memcpy(buf, &nlen, 4);
	memcpy(buf + 4, tup->t_data, tup->t_len);

  if (send(CombinerSock, buf, 4 + tup->t_len, 0) == -1)
  	elog(LOG, "could not send tuple to combiner");
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
	CombinerState *self = (CombinerState *) palloc0(sizeof(CombinerState));

	self->pub.receiveSlot = combiner_receive;	/* might get changed later */
	self->pub.rStartup = combiner_startup;
	self->pub.rShutdown = combiner_shutdown;
	self->pub.rDestroy = combiner_destroy;
	self->pub.mydest = DestCombiner;

	return (DestReceiver *) self;
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
