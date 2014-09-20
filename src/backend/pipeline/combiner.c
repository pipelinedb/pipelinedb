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
  	elog(ERROR, "could not bind to combiner \"%s\": %d", desc->name, errno);

  if (listen(desc->sock, WORKER_BACKLOG) == -1)
  	elog(ERROR, "could not listen on socket %d: %d", desc->sock, errno);

	if ((sock = accept(desc->sock, (struct sockaddr *) &remote, &addrlen)) == -1)
		elog(LOG, "could not accept connections on socket %d: %d", desc->sock, errno);

  return sock;
}

static TupleTableSlot *
receive_slot(int sock)
{
//	n = recv(sock, buf, 5, 0);
//	elog(LOG, "n=%s", buf);

	return NULL;
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
	TupleTableSlot *slot;
  int sock;
  char *cvname = queryDesc->plannedstmt->cq_target->relname;

  elog(LOG, "\"%s\" combiner %d running", cvname, MyProcPid);

  sock = accept_worker(combiner);

  for (;;)
  {
  	slot = receive_slot(sock);
  }
}
