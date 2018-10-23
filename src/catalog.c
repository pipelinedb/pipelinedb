/*-------------------------------------------------------------------------
 *
 * catalog.c
 *	  PipelineDB catalog cache management routines
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "config.h"
#include "miscadmin.h"
#include "pipeline_combine.h"
#include "pipeline_query.h"
#include "pipeline_stream.h"
#include "executor.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/inval.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#define PIPELINE_COMBINE "combine"
#define PIPELINE_QUERY "cont_query"
#define PIPELINE_STREAM "stream"
#define PIPELINE_EXEC_LOCK "_exec_lock"

#define PIPELINE_CATALOG_NAMESPACE PG_CATALOG_NAMESPACE
#define PIPELINEDB_NAMESPACE PIPELINEDB_EXTENSION_NAME

struct CatalogDesc
{
	char *nspname;
	char *relname;
	char *indname;
	Oid relid;
	Oid indrelid;
	TupleDesc desc;
	int	nkeys; /* # of keys needed for cache lookup */
	int	key[4]; /* attribute numbers of key attrs */
	int	nbuckets; /* number of hash buckets for this cache */
};

static struct CatalogDesc catalogdesc[] = {
	{PIPELINEDB_NAMESPACE, PIPELINE_QUERY, "pipeline_cont_query_id_index", InvalidOid, InvalidOid, NULL,
		1,
		{
			Anum_pipeline_query_id,
			0,
			0,
			0
		},
		2048
	},
	{PIPELINEDB_NAMESPACE, PIPELINE_QUERY, "pipeline_cont_query_relid_index", InvalidOid, InvalidOid, NULL,
		1,
		{
			Anum_pipeline_query_relid,
			0,
			0,
			0
		},
		2048
	},
	{PIPELINEDB_NAMESPACE, PIPELINE_QUERY, "pipeline_cont_query_defrelid_index", InvalidOid, InvalidOid, NULL,
		1,
		{
				Anum_pipeline_query_defrelid,
			0,
			0,
			0
		},
		2048
	},
	{PIPELINEDB_NAMESPACE, PIPELINE_QUERY, "pipeline_cont_query_matrelid_index", InvalidOid, InvalidOid, NULL,
		1,
		{
				Anum_pipeline_query_matrelid,
			0,
			0,
			0
		},
		2048
	},
	{PIPELINEDB_NAMESPACE, PIPELINE_QUERY, "pipeline_cont_query_osrelid_index", InvalidOid, InvalidOid, NULL,
		1,
		{
				Anum_pipeline_query_osrelid,
			0,
			0,
			0
		},
		2048
	},
	{PIPELINEDB_NAMESPACE, PIPELINE_QUERY, "pipeline_cont_query_seqrelid_index", InvalidOid, InvalidOid, NULL,
		1,
		{
				Anum_pipeline_query_seqrelid,
			0,
			0,
			0
		},
		2048
	},
	{PIPELINEDB_NAMESPACE, PIPELINE_QUERY, "pipeline_cont_query_pkidxid_index", InvalidOid, InvalidOid, NULL,
		1,
		{
				Anum_pipeline_query_pkidxid,
			0,
			0,
			0
		},
		2048
	},
	{PIPELINEDB_NAMESPACE, PIPELINE_QUERY, "pipeline_cont_query_lookupidxid_index", InvalidOid, InvalidOid, NULL,
		1,
		{
				Anum_pipeline_query_lookupidxid,
			0,
			0,
			0
		},
		2048
	},
	{PIPELINEDB_NAMESPACE, PIPELINE_STREAM, "pipeline_stream_relid_index", InvalidOid, InvalidOid, NULL,
		1,
		{
			Anum_pipeline_stream_relid,
			0,
			0,
			0
		},
		2048
	},
	{PIPELINEDB_NAMESPACE, PIPELINE_STREAM, "pipeline_stream_oid_index", InvalidOid, InvalidOid, NULL,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		2048
	},
	{"pg_catalog", "pg_aggregate", NULL, InvalidOid, InvalidOid, NULL,
		4,
		{
			Anum_pg_aggregate_aggtransfn,
			Anum_pg_aggregate_aggfinalfn,
			Anum_pg_aggregate_aggserialfn,
			Anum_pg_aggregate_aggdeserialfn
		},
		2048
	},
	{"pg_catalog", "pg_aggregate", NULL, InvalidOid, InvalidOid, NULL,
		4,
		{
			Anum_pg_aggregate_aggcombinefn,
			Anum_pg_aggregate_aggtransfn,
			Anum_pg_aggregate_aggfinalfn,
			Anum_pg_aggregate_aggserialfn
		},
		2048
	},
	{PIPELINEDB_NAMESPACE, PIPELINE_COMBINE, NULL, InvalidOid, InvalidOid, NULL,
		1,
		{
			Anum_pipeline_combine_aggfn,
			0,
			0,
			0
		},
		16
	}
};

#define PipelineSysCacheSize	((int) lengthof(catalogdesc))

typedef struct PipelineCatalog
{
	HTAB *tables[PipelineSysCacheSize];
	MemoryContext context;
} PipelineCatalog;

typedef struct PipelineCatalogEntry
{
	uint32 id;
	HeapTuple tuple;
	bool valid;
} PipelineCatalogEntry;

static PipelineCatalog *Catalog = NULL;
static bool CatalogInitialized = false;

static void
invalidate_catalog(int id)
{
	HASH_SEQ_STATUS iter;
	PipelineCatalogEntry *entry;

	if (!CatalogInitialized)
		InitPipelineCatalog();

	Assert(CatalogInitialized);
	Assert(id >= 0 && id < PipelineSysCacheSize);

	hash_seq_init(&iter, Catalog->tables[id]);
	while ((entry = (PipelineCatalogEntry *) hash_seq_search(&iter)) != NULL)
	{
		entry->valid = false;
	}
}

static void
invalidate_relid(Datum v, Oid relid)
{
	if (!CatalogInitialized)
		InitPipelineCatalog();

	/*
	 * NB: this may be called before the transaction is fully prepared, so
	 * we can't do anything here that requires being in a transaction.
	 */
	if (relid == PipelineQueryRelationOid)
	{
		invalidate_catalog(PIPELINEQUERYID);
		invalidate_catalog(PIPELINEQUERYDEFRELID);
		invalidate_catalog(PIPELINEQUERYRELID);
		invalidate_catalog(PIPELINEQUERYMATRELID);
		invalidate_catalog(PIPELINEQUERYOSRELID);
		invalidate_catalog(PIPELINEQUERYSEQRELID);
		invalidate_catalog(PIPELINEQUERYPKIDXID);
		invalidate_catalog(PIPELINEQUERYLOOKUPIDXID);

	}
	else if (relid == PipelineStreamRelationOid)
	{
		invalidate_catalog(PIPELINESTREAMRELID);
		invalidate_catalog(PIPELINESTREAMOID);
	}
	else if (relid == AggregateRelationId)
	{
		invalidate_catalog(PGAGGCOMBINEFN);
		invalidate_catalog(PGAGGPARTIALCOMBINEFN);
	}

	/* TODO(derekjn) Handle InvalidOid here, which will be passed during DROP EXTENSION */
}

static void
init_catalog_xact(void)
{
	int i;
	Oid catalog_nsp;

	if (!CatalogInitialized)
		InitPipelineCatalog();

	Assert(CatalogInitialized);

	catalog_nsp = get_namespace_oid(PIPELINEDB_NAMESPACE, false);
	PipelineQueryRelationOid = get_relname_relid(PIPELINE_QUERY, catalog_nsp);
	PipelineStreamRelationOid = get_relname_relid(PIPELINE_STREAM, catalog_nsp);
	PipelineExecLockRelationOid = get_relname_relid(PIPELINE_EXEC_LOCK, catalog_nsp);
	PipelineQueryCombineOid = get_relname_relid(PIPELINE_COMBINE, catalog_nsp);

	for (i = 0; i < PipelineSysCacheSize; i++)
	{
		Oid nsp = get_namespace_oid(catalogdesc[i].nspname, false);
		Oid relid = get_relname_relid(catalogdesc[i].relname, nsp);
		Relation rel;
		MemoryContext old;

		/*
		 * During a binary upgrade, we may get here before our catalog tables are present. In this
		 * case we tolerate it because they may not be needed anyways. If they are in fact needed,
		 * an error will be thrown later on since these catalogs will simply be uninitialized.
		 */
		if (IsBinaryUpgrade && (!OidIsValid(nsp) || !OidIsValid(relid)))
			continue;

		rel = heap_open(relid, NoLock);

		/* Cache OIDs while we're here so we don't have to do extraneous lookups after this point */
		catalogdesc[i].relid = relid;
		if (catalogdesc[i].indname)
			catalogdesc[i].indrelid = get_relname_relid(catalogdesc[i].indname, nsp);

		old = MemoryContextSwitchTo(Catalog->context);
		catalogdesc[i].desc = CreateTupleDescCopyConstr(RelationGetDescr(rel));
		MemoryContextSwitchTo(old);

		heap_close(rel, NoLock);
	}
}

static void
init_catalog_internal(void)
{
	int i;
	MemoryContext old;

	if (CatalogInitialized)
		return;

	Assert(IsTransactionState());
	old = MemoryContextSwitchTo(CacheMemoryContext);

	Catalog = palloc0(sizeof(PipelineCatalog));
	Catalog->context = CurrentMemoryContext;

	for (i = 0; i < PipelineSysCacheSize; i++)
	{
		HASHCTL ctl;
		char *name;

		MemSet(&ctl, 0, sizeof(HASHCTL));
		ctl.keysize = sizeof(uint32);
		ctl.entrysize = sizeof(PipelineCatalogEntry);
		ctl.hcxt = Catalog->context;
		ctl.hash = uint32_hash;

		name = catalogdesc[i].indname ? catalogdesc[i].indname : catalogdesc[i].relname;

		Catalog->tables[i] = hash_create(name, 32, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	MemoryContextSwitchTo(old);

	/*
	 * We use the relcache invalidation infrastructure to broadcast our catalog writes to
	 * all processes. Usually this is used when modifying a relation itself and not for writes
	 * performed on that relation. But by manually firing invalidation events on our catalog
	 * tables for any writes performed on them, we get a simple and reliable way to communicate
	 * catalog changes to all processes using them.
	 */
	CacheRegisterRelcacheCallback(invalidate_relid, (Datum) 0);

	CatalogInitialized = true;

	Assert(IsTransactionState());
	init_catalog_xact();
}

/*
 * PipelineCatalogInvalidate
 */
void
PipelineCatalogInvalidate(int id)
{
	Assert(id >= 0 && id < PipelineSysCacheSize);
	init_catalog_internal();
	Assert(CatalogInitialized);

	invalidate_catalog(id);
}

/*
 * PipelineCatalogInvalidateAll
 */
void
PipelineCatalogInvalidateAll(void)
{
	PipelineCatalogInvalidate(PIPELINEQUERYID);
	PipelineCatalogInvalidate(PIPELINEQUERYRELID);
	PipelineCatalogInvalidate(PIPELINEQUERYDEFRELID);
	PipelineCatalogInvalidate(PIPELINEQUERYMATRELID);
	PipelineCatalogInvalidate(PIPELINEQUERYOSRELID);
	PipelineCatalogInvalidate(PIPELINEQUERYSEQRELID);
	PipelineCatalogInvalidate(PIPELINEQUERYPKIDXID);
	PipelineCatalogInvalidate(PIPELINEQUERYLOOKUPIDXID);
	PipelineCatalogInvalidate(PIPELINESTREAMRELID);
	PipelineCatalogInvalidate(PIPELINESTREAMOID);
	PipelineCatalogInvalidate(PGAGGCOMBINEFN);
	PipelineCatalogInvalidate(PGAGGPARTIALCOMBINEFN);
	PipelineCatalogInvalidate(PIPELINECOMBINEAGGFN);
	CatalogInitialized = false;
}

/*
 * CatalogTupleInsert
 */
Oid
PipelineCatalogTupleInsert(Relation rel, HeapTuple tup)
{
	Oid result;

	Assert(CatalogInitialized);

	result = CatalogTupleInsert(rel, tup);
	CacheInvalidateRelcacheByRelid(RelationGetRelid(rel));

	return result;
}

/*
 * CatalogTupleUpdate
 */
void
PipelineCatalogTupleUpdate(Relation rel, ItemPointer otid, HeapTuple tup)
{
	Assert(CatalogInitialized);

	CatalogTupleUpdate(rel, otid, tup);
	CacheInvalidateRelcacheByRelid(RelationGetRelid(rel));
}

/*
 * CatalogTupleDelete
 */
void
PipelineCatalogTupleDelete(Relation rel, ItemPointer tid)
{
	Assert(CatalogInitialized);

	simple_heap_delete(rel, tid);
	CacheInvalidateRelcacheByRelid(RelationGetRelid(rel));
}

static HeapTuple
lookup_tuple(Relation rel, int id, Datum keys[])
{
	struct CatalogDesc desc;
	HeapTuple	tuple;
	ScanKeyData scankey[4];
	SysScanDesc scan;
	int i;

	desc = catalogdesc[id];

	for (i = 0; i < desc.nkeys; i++)
	{
		ScanKeyInit(&scankey[i], desc.key[i], BTEqualStrategyNumber, F_OIDEQ, keys[i]);
	}

	scan = systable_beginscan(rel, desc.indrelid, true, NULL, desc.nkeys, scankey);
	tuple = systable_getnext(scan);

	/* Copy it so our result is valid after we close the scanned relation */
	if (HeapTupleIsValid(tuple))
		tuple = heap_copytuple(tuple);

	systable_endscan(scan);

	return tuple;
}

static uint32
get_hash_value_from_keys(int nkeys, Datum key[])
{
	uint32 hashValue = 0;
	uint32 oneHash;

	/*
	 * This is taken from catcache.c:CatalogCacheComputeHashValue.
	 *
	 * Our hash scheme here doesn't necessarily need to be the same as syscache's,
	 * but it works well already so we just re-use it, with the main difference being
	 * we always use hashoid instead of looking up hash functions based on cache key type,
	 * since we currently only use OIDs.
	 */
	switch (nkeys)
	{
		case 4:
			oneHash = DatumGetUInt32(DirectFunctionCall1(hashoid, key[3]));
			hashValue ^= oneHash << 24;
			hashValue ^= oneHash >> 8;
			/* FALLTHROUGH */
		case 3:
			oneHash = DatumGetUInt32(DirectFunctionCall1(hashoid, key[2]));
			hashValue ^= oneHash << 16;
			hashValue ^= oneHash >> 16;
			/* FALLTHROUGH */
		case 2:
			oneHash = DatumGetUInt32(DirectFunctionCall1(hashoid, key[1]));
			hashValue ^= oneHash << 8;
			hashValue ^= oneHash >> 24;
			/* FALLTHROUGH */
		case 1:
			oneHash = DatumGetUInt32(DirectFunctionCall1(hashoid, key[0]));
			hashValue ^= oneHash;
			break;
		default:
			elog(FATAL, "wrong number of hash keys: %d", nkeys);
			break;
	}

	return hashValue;
}

/*
 * PipelineCatalogLookup
 */
HeapTuple
PipelineCatalogLookup(int id, int nkeys, ...)
{
	va_list valist;
	int i;
	Datum keys[nkeys];
	PipelineCatalogEntry *entry;
	bool found;
	HeapTuple tup;
	MemoryContext old;
	uint32 hash;
	Relation rel;

	Assert(id >= 0 && id < PipelineSysCacheSize);
	init_catalog_internal();
	Assert(CatalogInitialized);

	va_start(valist, nkeys);
	for (i = 0; i < nkeys; i++)
	{
		keys[i] = va_arg(valist, Datum);
	}
	va_end(valist);

	hash = get_hash_value_from_keys(nkeys, keys);
	entry = hash_search(Catalog->tables[id], &hash, HASH_ENTER, &found);

	if (found)
	{
		/*
		 * The invalidate_relid callback will be called by this if there have been any invalidation events
		 * since we initially created this entry, and the entry's valid flag will be unset if the invalidation
		 * event affected it.
		 */
		AcceptInvalidationMessages();

		if (entry->valid)
			return entry->tuple;

		/*
		 * We cache NULL tuples for lookups for keys that don't correspond to
		 * anything in our catalogs
		 */
		if (HeapTupleIsValid(entry->tuple))
			heap_freetuple(entry->tuple);
	}

	/* Cache miss */
	rel = heap_open(catalogdesc[id].relid, AccessShareLock);
	tup = lookup_tuple(rel, id, keys);
	heap_close(rel, AccessShareLock);

	/*
	 * Note: our lookup may have returned NULL, meaning that no entry was found for the given key.
	 * We still want to cache that because we'll often do catalog lookups for a relation to check if
	 * it's a PipelineDB object. If it's not, we'll get NULL here and the next lookup will see the
	 * cached result rather than leading to extraneous cache misses.
	 */
	old = MemoryContextSwitchTo(Catalog->context);
	entry->tuple = heap_copytuple(tup);
	MemoryContextSwitchTo(old);

	entry->valid = true;

	return entry->tuple;
}

/*
 * PipelineCatalogLookupForUpdate
 *
 * Returns a catalog tuple that may be updated
 */
HeapTuple
PipelineCatalogLookupForUpdate(Relation rel, int id, Datum key)
{
	Datum keys[1];

	Assert(id >= 0 && id < PipelineSysCacheSize);
	init_catalog_internal();
	Assert(CatalogInitialized);

	keys[0] = key;

	return lookup_tuple(rel, id, keys);
}

/*
 * InitPipelineCatalog
 */
void
InitPipelineCatalog(void)
{
	if (!IsTransactionState())
		return;
	if (CatalogInitialized)
		return;
	init_catalog_internal();
	Assert(CatalogInitialized);
}

Datum
PipelineCatalogGetAttr(int id, HeapTuple tup, AttrNumber attr, bool *isnull)
{
	Assert(id >= 0 && id < PipelineSysCacheSize);
	init_catalog_internal();
	Assert(CatalogInitialized);

	return heap_getattr(tup, attr, catalogdesc[id].desc, isnull);
}

/*
 * lookup_func_oid
 */
static Oid
lookup_func_oid(char *name, Oid *args, int nargs)
{
	oidvector *vec;
	HeapTuple tup;
	Oid result = InvalidOid;

	vec = buildoidvector(args, nargs);

	tup = SearchSysCache3(PROCNAMEARGSNSP,
			CStringGetDatum(name), PointerGetDatum(vec), ObjectIdGetDatum(get_namespace_oid(PIPELINEDB_EXTENSION_NAME, false)));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "function \"%s\" not found", name);

	result = HeapTupleGetOid(tup);

	ReleaseSysCache(tup);

	return result;
}

/*
 * GetHashGroupOid
 */
Oid
GetHashGroupOid(void)
{
	Oid args[1];

	args[0] = ANYOID;

	return lookup_func_oid("hash_group", args, 1);
}

/*
 * GetLSHashGroupOid
 */
Oid
GetLSHashGroupOid(void)
{
	Oid args[1];

	args[0] = ANYOID;

	return lookup_func_oid("ls_hash_group", args, 1);
}

/*
 * GetInsertIntoStreamOid
 */
Oid
GetInsertIntoStreamOid(void)
{
	return lookup_func_oid("insert_into_stream", NULL, 0);
}

/*
 * GetPipelineQueryOid
 */
Oid
GetPipelineQueryOid(void)
{
	if (!CatalogInitialized)
		InitPipelineCatalog();

	return PipelineQueryRelationOid;
}

/*
 * GetPipelineCombineOid
 */
Oid
GetPipelineCombineOid(void)
{
	if (!CatalogInitialized)
		InitPipelineCatalog();

	return PipelineQueryCombineOid;
}

/*
 * GetPipelineStreamOid
 */
Oid
GetPipelineStreamOid(void)
{
	if (!CatalogInitialized)
		InitPipelineCatalog();

	return PipelineStreamRelationOid;
}

/*
 * GetPipelineExecLockOid
 */
Oid
GetPipelineExecLockOid(void)
{
	if (!CatalogInitialized)
		InitPipelineCatalog();

	return PipelineExecLockRelationOid;
}

/*
 * GetDeserializeOid
 */
Oid
GetDeserializeOid(void)
{
	static Oid args[3] = {REGPROCOID, BYTEAOID};
	return lookup_func_oid("deserialize", args, 2);
}
