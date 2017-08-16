/*-------------------------------------------------------------------------
 *
 * syscache.c
 *	  PipelineDB system cache management routines
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2017, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_namespace.h"
#include "catalog/pipeline_combine.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_stream.h"
#include "pipeline/executor.h"
#include "pipeline/syscache.h"
#include "utils/catcache.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/*
 * struct cachedesc: information defining a single syscache
 */
struct cachedesc
{
	char *relname;
	char *indname;
	int			nkeys; /* # of keys needed for cache lookup */
	int			key[4]; /* attribute numbers of key attrs */
	int			nbuckets; /* number of hash buckets for this cache */
};

static const struct cachedesc cacheinfo[] = {
	{"pipeline_combine", "pipeline_combine_oid_index",
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		2048
	},
	{"pipeline_combine", "pipeline_combine_transfn_index",
		2,
		{
			Anum_pipeline_combine_aggfinalfn,
			Anum_pipeline_combine_transfn,
			0,
			0
		},
		2048
	},
	{"pipeline_query", "pipeline_query_id_index",
		1,
		{
			Anum_pipeline_query_id,
			0,
			0,
			0
		},
		2048
	},
	{"pipeline_query", "pipeline_query_relid_index",
		1,
		{
			Anum_pipeline_query_relid,
			0,
			0,
			0
		},
		2048
	},
	{"pipeline_query", "pipeline_query_matrelid_index",
		1,
		{
				Anum_pipeline_query_matrelid,
			0,
			0,
			0
		},
		2048
	},
	{"pipeline_query", "pipeline_query_oid_index",
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		2048
	},
	{"pipeline_stream", "pipeline_stream_relid_index",
		1,
		{
			Anum_pipeline_stream_relid,
			0,
			0,
			0
		},
		2048
	},
	{"pipeline_stream", "pipeline_stream_oid_index",
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		2048
	}
};

/*
 * We extend the PostgreSQL syscache by appending our cache entries after them.
 * We need to make sure we don't clobber any of the PostgreSQL syscache IDs,
 * which are 0-based entries in an array, one element for each catalog table.
 * Currently there are about ~70 catalogs, so we need to make sure our own
 * IDs start safely beyond that.
 *
 * The other constraint here is that the cacheId must be < 128, so we don't
 * want too large of an offset! This is obviously a tight squeeze, but catalogs
 * are rarely added and we can revisit when they are, and/or if we devise a
 * superior design.
 *
 * Currently we can accept ~30 PostgreSQL catalog additions and ~20 of our own
 * without changing anything.
 */
#define PipelineSysCacheOffset 100

#define SysCacheSize	((int) lengthof(cacheinfo))

static CatCache *SysCache[SysCacheSize];

static bool CacheInitialized = false;

/* Sorted array of OIDs of tables that have caches on them */
static Oid	SysCacheRelationOid[SysCacheSize];
static int	SysCacheRelationOidSize;

/* Sorted array of OIDs of tables and indexes used by caches */
static Oid	SysCacheSupportingRelOid[SysCacheSize * 2];
static int	SysCacheSupportingRelOidSize;

/*
 * OID comparator for pg_qsort
 */
static int
oid_compare(const void *a, const void *b)
{
	Oid			oa = *((const Oid *) a);
	Oid			ob = *((const Oid *) b);

	if (oa == ob)
		return 0;
	return (oa > ob) ? 1 : -1;
}

/*
 * InitPipelineSysCache - initialize the caches
 *
 * Note that no database access is done here; we only allocate memory
 * and initialize the cache structure.  Interrogation of the database
 * to complete initialization of a cache happens upon first use
 * of that cache.
 */
void
InitPipelineSysCache(void)
{
	int cacheId;
	int i;
	int j;

	Assert(!CacheInitialized);

	SysCacheRelationOidSize = SysCacheSupportingRelOidSize = 0;

	for (cacheId = PipelineSysCacheOffset; cacheId < SysCacheSize + PipelineSysCacheOffset; cacheId++)
	{
		int index = cacheId - PipelineSysCacheOffset;
		struct cachedesc entry = cacheinfo[index];

		Oid reloid = get_relname_relid(entry.relname, PG_CATALOG_NAMESPACE);
		Oid indoid = get_relname_relid(entry.indname, PG_CATALOG_NAMESPACE);

		SysCache[index] = InitCatCache(cacheId,
				reloid,
				indoid,
				entry.nkeys,
				entry.key,
				entry.nbuckets);

		if (!PointerIsValid(SysCache[index]))
			elog(ERROR, "could not initialize cache %u (%d)", reloid, cacheId);
		/* Accumulate data for OID lists, too */
		SysCacheRelationOid[SysCacheRelationOidSize++] = reloid;
		SysCacheSupportingRelOid[SysCacheSupportingRelOidSize++] = reloid;
		SysCacheSupportingRelOid[SysCacheSupportingRelOidSize++] = indoid;
		/* see comments for RelationInvalidatesSnapshotsOnly */
		Assert(!RelationInvalidatesSnapshotsOnly(reloid));
	}

	Assert(SysCacheRelationOidSize <= lengthof(SysCacheRelationOid));
	Assert(SysCacheSupportingRelOidSize <= lengthof(SysCacheSupportingRelOid));

	/* Sort and de-dup OID arrays, so we can use binary search. */
	pg_qsort(SysCacheRelationOid, SysCacheRelationOidSize,
			 sizeof(Oid), oid_compare);
	for (i = 1, j = 0; i < SysCacheRelationOidSize; i++)
	{
		if (SysCacheRelationOid[i] != SysCacheRelationOid[j])
			SysCacheRelationOid[++j] = SysCacheRelationOid[i];
	}
	SysCacheRelationOidSize = j + 1;

	pg_qsort(SysCacheSupportingRelOid, SysCacheSupportingRelOidSize,
			 sizeof(Oid), oid_compare);
	for (i = 1, j = 0; i < SysCacheSupportingRelOidSize; i++)
	{
		if (SysCacheSupportingRelOid[i] != SysCacheSupportingRelOid[j])
			SysCacheSupportingRelOid[++j] = SysCacheSupportingRelOid[i];
	}
	SysCacheSupportingRelOidSize = j + 1;

	CacheInitialized = true;

	/*
	 * Now assign OIDs to our catalog tables for when we explicitly open them
	 *
	 * TODO(derekjn): this needs to be moved into the extension load path once we're
	 * an extension. OIDs will obviously change between extension DROP/CREATE,
	 * so executing this only at system startup won't be enough.
	 */
	PipelineCombineRelationOid = get_relname_relid("pipeline_combine", PG_CATALOG_NAMESPACE);
	PipelineQueryRelationOid = get_relname_relid("pipeline_query", PG_CATALOG_NAMESPACE);
	PipelineStreamRelationOid = get_relname_relid("pipeline_stream", PG_CATALOG_NAMESPACE);
	PipelineExecLockRelationOid = get_relname_relid("_pipeline_exec_lock", PG_CATALOG_NAMESPACE);
}

/*
 * SearchPipelineSysCache
 *
 *	A layer on top of SearchCatCache that does the initialization and
 *	key-setting for you.
 *
 *	Returns the cache copy of the tuple if one is found, NULL if not.
 *	The tuple is the 'cache' copy and must NOT be modified!
 *
 *	When the caller is done using the tuple, call ReleaseSysCache()
 *	to release the reference count grabbed by SearchSysCache().  If this
 *	is not done, the tuple will remain locked in cache until end of
 *	transaction, which is tolerable but not desirable.
 *
 *	CAUTION: The tuple that is returned must NOT be freed by the caller!
 */
HeapTuple
SearchPipelineSysCache(int cacheId,
			   Datum key1,
			   Datum key2,
			   Datum key3,
			   Datum key4)
{
	if (cacheId < 0 || cacheId >= SysCacheSize ||
		!PointerIsValid(SysCache[cacheId]))
		elog(ERROR, "invalid cache ID: %d", cacheId);

	return SearchCatCache(SysCache[cacheId], key1, key2, key3, key4);
}

Datum
PipelineSysCacheGetAttr(int cacheId, HeapTuple tup,
				AttrNumber attributeNumber,
				bool *isNull)
{
	/*
	 * We just need to get the TupleDesc out of the cache entry, and then we
	 * can apply heap_getattr().  Normally the cache control data is already
	 * valid (because the caller recently fetched the tuple via this same
	 * cache), but there are cases where we have to initialize the cache here.
	 */
	if (cacheId < 0 || cacheId >= SysCacheSize ||
		!PointerIsValid(SysCache[cacheId]))
		elog(ERROR, "invalid cache ID: %d", cacheId);
	if (!PointerIsValid(SysCache[cacheId]->cc_tupdesc))
	{
		InitCatCachePhase2(SysCache[cacheId], false);
		Assert(PointerIsValid(SysCache[cacheId]->cc_tupdesc));
	}

	return heap_getattr(tup, attributeNumber,
						SysCache[cacheId]->cc_tupdesc,
						isNull);
}
