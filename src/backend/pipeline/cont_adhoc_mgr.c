#include "postgres.h"

#include "pipeline/cont_adhoc_mgr.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "miscadmin.h"
#include "pipeline/tuplebuf.h"
#include "access/xact.h"
#include "catalog/pipeline_stream_fn.h"
#include "catalog/pipeline_query_fn.h"

/*
 * The Adhoc Manager takes care of managing a small number of adhoc backends.
 *
 * It is responsible for initializing and configuring the shared memory for 
 * backend -> backend comms wrt adhoc queries.
 *
 * It contains some functions necessary for synchronizing adhoc inserts and 
 * removals from the AdhocTupleBuffer.
 */

/*
 * The underlying data structure is one array of size 
 * (max_worker_processes).
 *
 * Holes are allowed to form in the array. An empty slot is marked by
 * group.db_oid being zero.
 *
 * Assumption is that max number of adhocs is small (32), and scanning it is
 * fast.
 */
typedef struct AdhocShmemStruct
{
	int counter; /* always incrementing */
	ContQueryProcGroup groups[1]; /* dynamically allocated */
} AdhocShmemStruct;

static AdhocShmemStruct *AdhocShmem;

static size_t
adhoc_size_required()
{
	return MAXALIGN(add_size(sizeof(int), 
					mul_size(max_worker_processes,
								sizeof(ContQueryProcGroup))));
}

void
AdhocShmemInit(void)
{
	bool found = false;
	AdhocShmem = ShmemInitStruct("AdhocShmem", adhoc_size_required(), &found);

	if (found)
		return;

	MemSet(AdhocShmem, 0, adhoc_size_required());
}

/*
 * Find a slot in the AdhocShmemStruct and initialize it if found.
 * NULL is returned if there are no free slots.
 */
ContQueryProc *
AdhocMgrGetProc()
{
	ContQueryProcGroup *grp = 0;
	ContQueryProc *proc = 0;

	int i = 0;
	int ind = 0;

	LWLockAcquire(AdhocMgrLock, LW_EXCLUSIVE);

	for (i = 0; i < max_worker_processes; ++i)
	{
		ind = (AdhocShmem->counter + i) % max_worker_processes;

		if (AdhocShmem->groups[ind].db_oid == 0)
		{
			grp = &AdhocShmem->groups[ind];
			AdhocShmem->counter++;

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

		proc->type = Adhoc;
		proc->id = ind;
		proc->group_id = 0;
		proc->latch = &MyProc->procLatch;
		proc->active = true;
		proc->group = grp;
	}

	return proc;
}

/*
 * Release the slot by setting it to zero. This will allow the slot 
 * to be reused, and will also trip any pollers using the ptr obtained from
 * AdhocMgrGetActiveFlag
 */
void
AdhocMgrReleaseProc(ContQueryProc *proc)
{
	ContQueryProcGroup *grp = proc->group;

	LWLockAcquire(AdhocMgrLock, LW_EXCLUSIVE);
	MemSet(grp, 0, sizeof(ContQueryProcGroup));
	LWLockRelease(AdhocMgrLock);
}

/*
 * Find a cq in the array with matching db_oid and cq_id.
 * Returns NULL if not found.
 */
static ContQueryProc *
find_cq(Oid db_oid, int cq_id)
{
	int i = 0;

	for (i = 0; i < max_worker_processes; ++i)
	{
		if (AdhocShmem->groups[i].db_oid == db_oid &&
		    AdhocShmem->groups[i].procs[0].group_id == cq_id)
		{
			return AdhocShmem->groups[i].procs;
		}
	}

	return NULL;
}

static bool
is_active(Oid db_oid, int cq_id)
{
	return find_cq(db_oid, cq_id) != NULL;
}

static bool
should_drain_fn(TupleBufferSlot *slot, void *ctx)
{
	return !is_active(slot->tuple->db_oid, slot->tuple->group_hash);
}

/* 
 * This function needs to be called periodically to ensure the adhoc tuple
 * buffer does not deadlock. This can happen when data in inserted into 
 * the adhoc tuple buffer, but not read out by an adhoc backend.
 *
 * TODO - a better sync method between readers and writers may be required.
 */
void
AdhocMgrPeriodicDrain()
{
	if (!continuous_queries_adhoc_enabled)
		return;

	LWLockAcquire(AdhocMgrLock, LW_EXCLUSIVE);
	TupleBufferDrainGeneric(AdhocTupleBuffer, should_drain_fn, 0);
	LWLockRelease(AdhocMgrLock);
}

/* 
 * Returns a pointer to the group_id of the matching cq_id if found, 
 * or NULL otherwise.
 *
 * The underlying idea is that other processes can synchronize by
 * checking the pointed to value matches cq_id.
 *
 * TODO - a more robust sync mechanism might be required.
 * */
int *
AdhocMgrGetActiveFlag(int cq_id)
{
	ContQueryProc *cq = find_cq(MyDatabaseId, cq_id);
	return cq ? &cq->group_id : 0;
}

/* needs to be called per db */
void AdhocMgrDeleteAdhocs(void)
{
	int id = 0;
	Bitmapset *view_ids;

	StartTransactionCommand();
	view_ids = GetAdhocContinuousViewIds();

	while ((id = bms_first_member(view_ids)) >= 0)
	{
		ContinuousView *cv = GetContinuousView(id);
		AdhocMgrCleanupContinuousView(cv);
	}

	CommitTransactionCommand();
}
