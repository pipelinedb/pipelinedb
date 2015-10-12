#include "postgres.h"

#include "pipeline/cont_adhoc_mgr.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "miscadmin.h"
#include "pipeline/tuplebuf.h"

// assume that the number of adhoc procs is incredibly low.
// one global array across all dbs will do it.

#define MAX_ADHOC 16

typedef struct AdhocShmemStruct
{
	ContQueryProcGroup groups[MAX_ADHOC]; // one proc per group
	int index;

} AdhocShmemStruct;

static AdhocShmemStruct *AdhocShmem;

void
AdhocShmemInit(void)
{
	bool found = false;
	AdhocShmem = ShmemInitStruct("AdhocShmem",
		sizeof(AdhocShmemStruct), &found);

	if (found)
		return;

	MemSet(AdhocShmem, 0, sizeof(AdhocShmemStruct));
}

ContQueryProc*
AdhocMgrGetProc()
{
	ContQueryProcGroup* grp = 0;
	ContQueryProc* proc = 0;
	int i = 0;
	int ind = 0;

	LWLockAcquire(AdhocMgrLock, LW_EXCLUSIVE);

	for (i = 0; i < MAX_ADHOC; ++i)
	{
		int mi = (AdhocShmem->index + i) % MAX_ADHOC;

		if (AdhocShmem->groups[mi].db_oid == 0)
		{
			grp = &AdhocShmem->groups[mi];
			ind = AdhocShmem->index++;
			break;
		}
	}

	LWLockRelease(AdhocMgrLock);

	if (grp)
	{
		MemSet(grp, 0, sizeof(ContQueryProcGroup));
		grp->db_oid = MyDatabaseId;
		grp->active = true;

		proc = grp->procs;

		proc->type = Scheduler;
		proc->id = ind;
		proc->group_id = 0;
		proc->latch = &MyProc->procLatch;
		proc->active = true;
		proc->group = grp;
	}

	return proc;
}

void
AdhocMgrReleaseProc(ContQueryProc* proc)
{
	ContQueryProcGroup* grp = proc->group;

	LWLockAcquire(AdhocMgrLock, LW_EXCLUSIVE);

	MemSet(grp, 0, sizeof(ContQueryProcGroup));

	LWLockRelease(AdhocMgrLock);
}

static ContQueryProc* get_cq(int cq_id)
{
	int i = 0;

	for (i = 0; i < MAX_ADHOC; ++i)
	{
		if (AdhocShmem->groups[i].procs[0].group_id == cq_id)
		{
			return AdhocShmem->groups[i].procs;
		}
	}

	return NULL;
}

static bool is_active(int cq_id)
{
	return get_cq(cq_id) != NULL;
}

static bool should_drain_fn(TupleBufferSlot *slot, void *ctx)
{
	return !is_active(slot->tuple->group_hash);
}

void
AdhocMgrPeriodicCleanup()
{
	LWLockAcquire(AdhocMgrLock, LW_EXCLUSIVE);

	TupleBufferDrainGeneric(AdhocTupleBuffer, should_drain_fn, 0);

	LWLockRelease(AdhocMgrLock);
}

int* AdhocMgrGetActiveFlag(int cq_id)
{
	ContQueryProc *cq = get_cq(cq_id);

	return cq ? &cq->group_id : 0;
}
