/*-------------------------------------------------------------------------
 *
 * update.h
 *	  Interface for anonymous update checks
 *
 * src/include/pipeline/update.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_UPDATE_H
#define PIPELINE_UPDATE_H

/* guc */
extern bool anonymous_update_checks;

extern void UpdateCheck(HTAB *all_dbs, bool startup);

#endif
