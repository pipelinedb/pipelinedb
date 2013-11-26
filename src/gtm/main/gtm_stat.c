/*-------------------------------------------------------------------------
 *
 * gtm_stat.c
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#include "gtm/gtm_c.h"
#include "gtm/gtm.h"

uint32	GTM_Message_Stats[MSG_MAX_MESSAGE_TYPE];
uint32	GTM_Result_Stats[GTM_MAX_RESULT_TYPE];

void
gtm_msgstat_increment(int type)
{
	GTM_Message_Stats[type]++;
}

void
gtm_resultstat_increment(int type)
{
	GTM_Result_Stats[type]++;
}

void
gtm_print_stats(void)
{

}
