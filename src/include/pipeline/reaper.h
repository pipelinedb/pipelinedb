/*-------------------------------------------------------------------------
 *
 * reaper.h
 *
 * Copyright (c) 2017, PipelineDB
 *
 * IDENTIFICATION
 *    src/include/pipeline/reaper.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef REAPER_H
#define REAPER_H

extern int continuous_query_ttl_expiration_batch_size;
extern int continuous_query_ttl_expiration_threshold;

int DeleteTTLExpiredRows(RangeVar *cvname, RangeVar *matrel);

#endif   /* REAPER_H */
