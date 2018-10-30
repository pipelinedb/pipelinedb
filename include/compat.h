/*-------------------------------------------------------------------------
 *
 * compat.h
 *  Interface for functionality that breaks between major PG versions
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */

#define PIPELINE_COMPAT_INDEX_ATTR_BITMAP_ALL INDEX_ATTR_BITMAP_ALL

extern bool ProcOidIsAgg(Oid oid);
