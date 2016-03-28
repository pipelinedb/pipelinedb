/*-------------------------------------------------------------------------
 *
 * sliding_window.h
 *	  Interface for sliding window
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#ifndef TRIGGER_SLIDING_WINDOW_H
#define TRIGGER_SLIDING_WINDOW_H

#include "postgres.h"
#include "trigger.h"

extern void init_sw(TriggerCacheEntry *entry, Relation rel);
extern void sw_vacuum(TriggerProcessState *state);

#endif
