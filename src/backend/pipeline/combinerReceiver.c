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

#define RETRY_TIMEOUT (100 * 1000)

typedef struct
{
	DestReceiver pub;
	char *sock_name;
	int sock;
	struct timeval timeout;
	pid_t pid;
	int failed_handshakes;
} CombinerState;

static bool
attempt_handshake(CombinerState *combiner)
{
	if (recv(combiner->sock, &combiner->pid, sizeof(pid_t), MSG_WAITALL) == -1)
		return false;

	if (send(combiner->sock, &MyProcPid, sizeof(pid_t), 0) == -1)
		return false;

	elog(LOG, "worker %d connected to combiner", MyProcPid);

	return true;
}

static void
combiner_shutdown(DestReceiver *self)
{
	CombinerState *c = (CombinerState *) self;
	if (c->sock != -1)
		close(c->sock);
	c->sock = -1;
}

static void
combiner_startup(DestReceiver *self, int operation,
		TupleDesc typeinfo)
{
	CombinerState *combiner = (CombinerState *) self;
	struct sockaddr_un remote;
	int len;
	int attempts = 0;
	bool connected = false;
	struct timeval timeout;

	/*
	 * In case we're recovering from an error in the worker
	 * PG_TRY/PG_CATCH block, we don't need to reconnect to
	 * the combiner.
	 */
	if (combiner->sock != -1)
		return;

	if (combiner->failed_handshakes >= 100)
		elog(ERROR, "worker handshakes failed more than 100 times");

	/*
	 * Handshake receive timeout should be a little large, just in
	 * case the combiner takes its sweet time.
	 */
	timeout.tv_sec = 1;
	timeout.tv_usec = 0;

	if ((combiner->sock = socket(AF_UNIX, SOCK_STREAM, 0)) == -1)
		elog(ERROR, "worker could not create combiner socket \"%s\": %m",
				combiner->sock_name);

	remote.sun_family = AF_UNIX;
	strcpy(remote.sun_path, combiner->sock_name);

	len = strlen(remote.sun_path) + sizeof(remote.sun_family);

	/* this will retry for up to 10 seconds */
	while (attempts++ < 100)
	{
		if (connect(combiner->sock, (struct sockaddr *) &remote, len) == 0)
		{
			if (setsockopt(combiner->sock, SOL_SOCKET, SO_RCVTIMEO, (char *) &timeout,
					sizeof(struct timeval)) == -1)
				close(combiner->sock);
			else
				connected = true;
		}
		if (connected)
			break;
		else
			pg_usleep(RETRY_TIMEOUT);
	}

	if (!connected)
		elog(ERROR, "failed to connect to combiner");

	/*
	 * Ensure handshake was successful.
	 */
	if (!attempt_handshake(combiner))
	{
		combiner->failed_handshakes++;
		elog(LOG, "worker handshake failed: %m");
		pg_usleep(RETRY_TIMEOUT);

		combiner_shutdown(self);
		combiner_startup(self, operation, typeinfo);
	}
	else
		combiner->failed_handshakes = 0;
}

static void
combiner_receive(TupleTableSlot *slot, DestReceiver *self)
{
	CombinerState *combiner = (CombinerState *) self;
	HeapTuple tup = ExecMaterializeSlot(slot);
	uint32 nlen = htonl(tup->t_len);
	char *buf = palloc0(4 + tup->t_len);

	memcpy(buf, &nlen, 4);
	memcpy(buf + 4, tup->t_data, tup->t_len);

	while (send(combiner->sock, buf, 4 + tup->t_len, 0) == -1)
	{
		pid_t combiner_pid;

		elog(LOG, "worker sending tuple failed: %m");

		/*
		 * Ensure that the combiner has a valid pid. This won't be true if
		 * the combiner is in the process of starting up.
		 */
		while ((combiner_pid = GetCombinerPid(MyCQId)) == 0)
			pg_usleep(RETRY_TIMEOUT);

		if (combiner->pid == combiner_pid)
			elog(ERROR, "could not send tuple to combiner: %m");

		combiner_shutdown(self);
		combiner_startup(self, -1, NULL);
	}
}

static void combiner_destroy(DestReceiver *self)
{
	pfree(self);
}

DestReceiver *
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
 * SetCombinerDestReceiverParams
 *
 * Set parameters for a CombinerDestReceiver
 */
void
SetCombinerDestReceiverParams(DestReceiver *self, char *sock_name)
{
	CombinerState *c = (CombinerState *) self;
	c->sock_name = sock_name;
	c->sock = -1;
	c->pid = -1;
}
