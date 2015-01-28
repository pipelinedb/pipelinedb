/* Copyright (c) 2013-2015 PipelineDB */
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
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "postgres.h"

#include "access/printtup.h"
#include "pipeline/combinerReceiver.h"
#include "miscadmin.h"

typedef struct
{
	DestReceiver pub;
	CombinerDesc *desc;
} CombinerState;

static void combiner_receive(TupleTableSlot *slot, DestReceiver *self)
{
	CombinerState *combiner = (CombinerState *) self;
	HeapTuple tup = ExecMaterializeSlot(slot);
	uint32 nlen = htonl(tup->t_len);
	char *buf = palloc0(4 + tup->t_len);

	memcpy(buf, &nlen, 4);
	memcpy(buf + 4, tup->t_data, tup->t_len);

	if (send(combiner->desc->sock, buf, 4 + tup->t_len, 0) == -1)
		elog(ERROR, "could not send tuple to combiner: %m");
}

static void combiner_startup(DestReceiver *self, int operation,
		TupleDesc typeinfo)
{
	CombinerState *combiner = (CombinerState *) self;
	struct sockaddr_un remote;
	int len;
	int attempts = 0;
	bool connected = false;

	if ((combiner->desc->sock = socket(AF_UNIX, SOCK_STREAM, 0)) == -1)
		elog(ERROR, "worker could not create combiner socket \"%s\": %m",
				combiner->desc->name);

	remote.sun_family = AF_UNIX;
	strcpy(remote.sun_path, combiner->desc->name);

	len = strlen(remote.sun_path) + sizeof(remote.sun_family);

	/* this will retry for up to 10 seconds */
	while (attempts++ < 100)
	{
		if (connect(combiner->desc->sock, (struct sockaddr *) &remote, len)
				== 0)
			connected = true;
		else
			pg_usleep(100 * 1000); /* 0.1s */
		if (connected)
			break;
	}

	if (!connected)
		elog(ERROR, "could not connect to combiner");
}

static void combiner_shutdown(DestReceiver *self)
{
	CombinerState *c = (CombinerState *) self;

	close(c->desc->sock);
	pfree(self);
}

static void combiner_destroy(DestReceiver *self)
{

}

extern DestReceiver *
CreateCombinerDestReceiver(void)
{
	CombinerState *self = (CombinerState *) palloc0(sizeof(CombinerState));

	self->pub.receiveSlot = combiner_receive; /* might get changed later */
	self->pub.rStartup = combiner_startup;
	self->pub.rShutdown = combiner_shutdown;
	self->pub.rDestroy = combiner_destroy;
	self->pub.mydest = DestCombiner;

	return (DestReceiver *) self;
}

/*
 * Set parameters for a CombinerDestReceiver
 */
extern void SetCombinerDestReceiverParams(DestReceiver *self,
		CombinerDesc *combiner)
{
	CombinerState *c = (CombinerState *) self;
	c->desc = combiner;
}
