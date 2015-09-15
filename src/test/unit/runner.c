#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <check.h>

#include "c.h"
#include "miscadmin.h"
#include "postgres.h"
#include "unistd.h"
#include "utils/guc.h"
#include "utils/palloc.h"
#include "utils/memutils.h"

#include "suites.h"

const char *progname;

int main(void)
{
	int number_failed;
	SRunner *sr;

	MemoryContext context = AllocSetContextCreate(NULL,
			"UnitTestContext",
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

	srand(time(NULL));

	sr = srunner_create(suite_create ("main"));
//	srunner_add_suite(sr, test_tdigest_suite());
	srunner_add_suite(sr, test_hll_suite());
//	srunner_add_suite(sr, test_bloom_suite());
//	srunner_add_suite(sr, test_cmsketch_suite());
//	srunner_add_suite(sr, test_fss_suite());

	srunner_run_all(sr, CK_VERBOSE);
	number_failed = srunner_ntests_failed(sr);
	srunner_free(sr);

	MemoryContextReset(context);

	return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
