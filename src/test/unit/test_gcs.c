#include <check.h>
#include <math.h>
#include <time.h>

#include "suites.h"
#include "pipeline/gcs.h"
#include "pipeline/miscutils.h"
#include "utils/elog.h"
#include "utils/palloc.h"

static int
int_cmp(const void * a, const void * b)
{
	return *(int *) a - *(int *) b;
}

START_TEST(test_coding)
{
	GolombCodedSet *gcs = GolombCodedSetCreate();
	int num_keys = 1;
	int *keys = palloc(sizeof(int) * num_keys);
	int i;
	GCSReader *reader;
	ListCell *lc;

//	for (i = 0; i < num_keys; i++)
//	{
//		int key = rand();
//		GolombCodedSetAdd(gcs, &key, sizeof(int));
//	}

	gcs->vals = lappend_int(gcs->vals, 457);
	gcs->nvals = 1;

	i = 0;
	foreach(lc, gcs->vals)
	{
		keys[i] = lfirst_int(lc);
		i++;
	}
	qsort(keys, num_keys, sizeof(int), int_cmp);

	gcs = GolombCodedSetCompress(gcs);
	reader = GCSReaderCreate(gcs);

	elog(LOG, "GCS %d", gcs->blen);

	for (i = 0; i < gcs->blen; i++)
		elog(LOG, "bytes %d", gcs->b[i]);

	ck_assert_int_eq(gcs->nvals, num_keys);
	ck_assert_ptr_eq(gcs->vals, NIL);

	for (i = 0; i < num_keys; i++)
	{
		elog(LOG, "yo %d %d", GCSReaderNext(reader), keys[i]);
		//ck_assert_int_eq(GCSReaderNext(reader), keys[i]);
	}
}
END_TEST

Suite *test_gcs_suite(void)
{
	Suite *s;
	TCase *tc;

	s = suite_create("test_gcs");
	tc = tcase_create("test_gcs");
	tcase_add_test(tc, test_coding);
	suite_add_tcase(s, tc);

	return s;
}
