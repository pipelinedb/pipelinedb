#ifndef CONT_WORKER_UTIL_H
#define CONT_WORKER_UTIL_H

#include "nodes/execnodes.h"
#include "utils/resowner.h"
#include "executor/execdesc.h"

/* Utility funcs required by both cont_worker.c and cont_adhoc.c */
extern EState *create_estate(QueryDesc *query_desc);
extern void set_snapshot(EState *estate, ResourceOwner owner);
extern void unset_snapshot(EState *estate, ResourceOwner owner);

#endif
