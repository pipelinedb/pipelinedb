/*-------------------------------------------------------------------------
 *
 * proxy_utils.h
 *
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/gtm/proxy/proxy_utils.c
 *
 *-------------------------------------------------------------------------
 */

#include "gtm/proxy_utils.h"

#include "gtm/elog.h"
#include "gtm/gtm.h"
#include "gtm/gtm_c.h"
#include "gtm/gtm_client.h"
#include "gtm/gtm_seq.h"
#include "gtm/register.h"

/*
 * This function is a dummy function of gtm_proxy module to avoid
 * object link problem.
 *
 * Most of command processing functions are existing only in GTM main
 * module, but a few are both in GTM main and GTM proxy modules, which
 * consist of same binary objects. And all the command processing
 * functions require calling gtm_standby_check_communication_error()
 * for GTM main.
 *
 * Two options should be considered here:
 * (1) Moving all command processing functions into the common modules, or
 * (2) Creating a dummy function in GTM proxy module.
 *
 * (1) may cause another hard thing because of object and variable
 * referencing issue. For example GetMyThreadInfo is specified in both
 * gtm.h and gtm_proxy.h and depends on the context.
 *
 * This is the reason why this dummy function is needed here.
 *
 * The object and module structure of GTM/GTM Proxy needs review, and
 * fix to remove this kind of tricks.
 */
bool
gtm_standby_check_communication_error(int *retry_count, GTM_Conn *oldconn)
{
	return false;
}
