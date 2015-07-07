#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <check.h>

#include "c.h"
#include "miscadmin.h"
#include "postgres.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/pg_sema.h"
#include "storage/pg_shmem.h"
#include "storage/shm_alloc.h"
#include "storage/shmem.h"
#include "unistd.h"
#include "utils/guc.h"
#include "utils/palloc.h"
#include "utils/memutils.h"

#include "suites.h"

#define SHMEM_SIZE (512 * 1024 * 1024) /* 512mb */

static void
base_init(void)
{
	PGShmemHeader *seghdr;
	PGShmemHeader *shim = NULL;
	char *cwd = (char *) palloc(1024);

	getcwd(cwd, 1024);
	DataDir = cwd;

	InitializeGUCOptions();
	seghdr = PGSharedMemoryCreate(SHMEM_SIZE, true, 0, &shim);
	PGReserveSemaphores(128, 0);
	InitShmemAccess(seghdr);
	InitShmemAllocation();
	CreateLWLocks();
	InitShmemIndex();

	ShmemDynAllocShmemInit();
}

int main(void)
{
	int number_failed;
	SRunner *sr;

	MemoryContext context = AllocSetContextCreate(NULL,
			"UnitTestContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	/* TopMemoryContext must be initialized for shmem and LW locks to work */
	TopMemoryContext = AllocSetContextCreate(NULL,
			"TopMemoryContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	/* ErrorContext must be initialized for logging to work */
	ErrorContext = AllocSetContextCreate(NULL,
			"ErrorContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	MemoryContextSwitchTo(context);

	base_init();

	srand(time(NULL));

	sr = srunner_create(suite_create ("main"));
	srunner_add_suite(sr, test_tdigest_suite());
	srunner_add_suite(sr, test_hll_suite());
	srunner_add_suite(sr, test_bloom_suite());
	srunner_add_suite(sr, test_cmsketch_suite());

	srunner_run_all(sr, CK_VERBOSE);
	number_failed = srunner_ntests_failed(sr);
	srunner_free(sr);

	MemoryContextReset(context);

	return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
