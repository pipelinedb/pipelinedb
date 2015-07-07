/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#define MAX_CUNIT_ERROR_LENGTH 512

#define PG_ADD_TEST(suite, testfunc) CU_add_test(suite, #testfunc, testfunc)

/* Contains the most recent error message generated by lwerror. */
char cu_error_msg[MAX_CUNIT_ERROR_LENGTH+1];

/* Resets cu_error_msg back to blank. */
void cu_error_msg_reset(void);

/* Our internal callback to register Suites with the main tester */
typedef void (*PG_SuiteSetup)(void);
