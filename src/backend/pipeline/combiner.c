/*-------------------------------------------------------------------------
 *
 * combiner.c
 *
 *	  Combiner process functionality
 *
 * src/backend/pipeline/combiner.c
 *
 *-------------------------------------------------------------------------
 */
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/unistd.h>

#include "postgres.h"

#include "pipeline/combiner.h"
#include "pipeline/merge.h"
#include "miscadmin.h"


#define NAME_PREFIX "combiner_"
#define WORKER_BACKLOG 32

static int
accept_worker(CombinerDesc *desc)
{
  int len;
  int sock;
  struct sockaddr_un local;
  struct sockaddr_un remote;
  socklen_t addrlen;

  local.sun_family = AF_UNIX;
  strcpy(local.sun_path, desc->name);
  unlink(local.sun_path);

  len = strlen(local.sun_path) + sizeof(local.sun_family);
  if (bind(desc->sock, (struct sockaddr *) &local, len) == -1)
  	elog(ERROR, "could not bind to combiner \"%s\": %m", desc->name);

  if (listen(desc->sock, WORKER_BACKLOG) == -1)
  	elog(ERROR, "could not listen on socket %d: %m", desc->sock);

	if ((sock = accept(desc->sock, (struct sockaddr *) &remote, &addrlen)) == -1)
		elog(LOG, "could not accept connections on socket %d: %m", desc->sock);

  return sock;
}

static void
receive_tuple(int sock, TupleTableSlot *slot)
{
	HeapTuple tup;
	uint32 len;
	int read;

	read = recv(sock, &len, 4, 0);
	if (read < 0)
		elog(ERROR, "combiner failed to receive tuple length");

	len = ntohl(len);

	tup = (HeapTuple) palloc0(HEAPTUPLESIZE + len);
	tup->t_len = len;
	tup->t_data = (HeapTupleHeader) ((char *) tup + HEAPTUPLESIZE);

	read = recv(sock, tup->t_data, tup->t_len, 0);
	if (read < 0)
		elog(ERROR, "combiner failed to receive tuple data");

	ExecStoreTuple(tup, slot, InvalidBuffer, false);
}

extern CombinerDesc *
CreateCombinerDesc(const char *name)
{
	CombinerDesc *desc = palloc(sizeof(CombinerDesc));

	desc->name = palloc(strlen(NAME_PREFIX) + strlen(name) + 1);

	memcpy(desc->name, NAME_PREFIX, strlen(NAME_PREFIX));
	memcpy(desc->name + strlen(NAME_PREFIX), name, strlen(name) + 1);

  if ((desc->sock = socket(AF_UNIX, SOCK_STREAM, 0)) == -1)
  	elog(ERROR, "could not create combiner socket for \"%s\"", name);

  return desc;
}

extern void
ContinuousQueryCombinerRun(CombinerDesc *combiner, QueryDesc *queryDesc, ResourceOwner owner)
{
	ResourceOwner save = CurrentResourceOwner;
	TupleTableSlot *slot;
	Tuplestorestate *store;
	long count = 0;
  int sock;
  int batchsize = 2;
  char *cvname = queryDesc->plannedstmt->cq_target->relname;

  elog(LOG, "\"%s\" combiner %d running", cvname, MyProcPid);

  CurrentResourceOwner = owner;

  InitMerge();

  store = tuplestore_begin_heap(true, true, work_mem);

  slot = MakeSingleTupleTableSlot(queryDesc->tupDesc);
  sock = accept_worker(combiner);

  for (;;)
  {
  	receive_tuple(sock, slot);
  	tuplestore_puttupleslot(store, slot);
  	if (count++ == batchsize)
  	{
  		Merge(cvname, store);
  		tuplestore_clear(store);
  	}
  }

  CurrentResourceOwner = save;
}
