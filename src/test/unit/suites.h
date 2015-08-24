#include <check.h>
#include "c.h"

#define DIST_MAX 100000

/* Test suites */
extern Suite *test_tdigest_suite(void);
extern Suite *test_hll_suite(void);
extern Suite *test_bloom_suite(void);
extern Suite *test_cmsketch_suite(void);
extern Suite *test_fss_suite(void);

/* Distribution sample functions */
extern float8 uniform(void);
extern float8 gaussian(void);
