/*-------------------------------------------------------------------------
 *
 * lockfuncs.c
 *		Functions for SQL access to various lock-manager capabilities.
 *
 * Copyright (c) 2002-2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/backend/utils/adt/lockfuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#ifdef PGXC
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/nodemgr.h"
#include "executor/spi.h"
#include "tcop/utility.h"
#endif
#include "storage/predicate_internals.h"
#include "utils/builtins.h"


/* This must match enum LockTagType! */
static const char *const LockTagTypeNames[] = {
	"relation",
	"extend",
	"page",
	"tuple",
	"transactionid",
	"virtualxid",
	"object",
	"userlock",
	"advisory"
};

/* This must match enum PredicateLockTargetType (predicate_internals.h) */
static const char *const PredicateLockTagTypeNames[] = {
	"relation",
	"page",
	"tuple"
};

/* Working status for pg_lock_status */
typedef struct
{
	LockData   *lockData;		/* state data from lmgr */
	int			currIdx;		/* current PROCLOCK index */
	PredicateLockData *predLockData;	/* state data for pred locks */
	int			predLockIdx;	/* current index for pred lock */
} PG_Lock_Status;

#ifdef PGXC
/*
 * These enums are defined to make calls to pgxc_advisory_lock more readable.
 */
typedef enum
{
	SESSION_LOCK,
	TRANSACTION_LOCK
} LockLevel;

typedef enum
{
	WAIT,
	DONT_WAIT
} TryType;

static bool
pgxc_advisory_lock(int64 key64, int32 key1, int32 key2, bool iskeybig,
			LOCKMODE lockmode, LockLevel locklevel, TryType try);
#endif

/* Number of columns in pg_locks output */
#define NUM_LOCK_STATUS_COLUMNS		15

/*
 * VXIDGetDatum - Construct a text representation of a VXID
 *
 * This is currently only used in pg_lock_status, so we put it here.
 */
static Datum
VXIDGetDatum(BackendId bid, LocalTransactionId lxid)
{
	/*
	 * The representation is "<bid>/<lxid>", decimal and unsigned decimal
	 * respectively.  Note that elog.c also knows how to format a vxid.
	 */
	char		vxidstr[32];

	snprintf(vxidstr, sizeof(vxidstr), "%d/%u", bid, lxid);

	return CStringGetTextDatum(vxidstr);
}


/*
 * pg_lock_status - produce a view with one row per held or awaited lock mode
 */
Datum
pg_lock_status(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	PG_Lock_Status *mystatus;
	LockData   *lockData;
	PredicateLockData *predLockData;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;
		MemoryContext oldcontext;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/*
		 * switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tupdesc for result tuples */
		/* this had better match pg_locks view in system_views.sql */
		tupdesc = CreateTemplateTupleDesc(NUM_LOCK_STATUS_COLUMNS, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "locktype",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "database",
						   OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "relation",
						   OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "page",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "tuple",
						   INT2OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "virtualxid",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "transactionid",
						   XIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "classid",
						   OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 9, "objid",
						   OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 10, "objsubid",
						   INT2OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 11, "virtualtransaction",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 12, "pid",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 13, "mode",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 14, "granted",
						   BOOLOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 15, "fastpath",
						   BOOLOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		/*
		 * Collect all the locking information that we will format and send
		 * out as a result set.
		 */
		mystatus = (PG_Lock_Status *) palloc(sizeof(PG_Lock_Status));
		funcctx->user_fctx = (void *) mystatus;

		mystatus->lockData = GetLockStatusData();
		mystatus->currIdx = 0;
		mystatus->predLockData = GetPredicateLockStatusData();
		mystatus->predLockIdx = 0;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	mystatus = (PG_Lock_Status *) funcctx->user_fctx;
	lockData = mystatus->lockData;

	while (mystatus->currIdx < lockData->nelements)
	{
		bool		granted;
		LOCKMODE	mode = 0;
		const char *locktypename;
		char		tnbuf[32];
		Datum		values[NUM_LOCK_STATUS_COLUMNS];
		bool		nulls[NUM_LOCK_STATUS_COLUMNS];
		HeapTuple	tuple;
		Datum		result;
		LockInstanceData *instance;

		instance = &(lockData->locks[mystatus->currIdx]);

		/*
		 * Look to see if there are any held lock modes in this PROCLOCK. If
		 * so, report, and destructively modify lockData so we don't report
		 * again.
		 */
		granted = false;
		if (instance->holdMask)
		{
			for (mode = 0; mode < MAX_LOCKMODES; mode++)
			{
				if (instance->holdMask & LOCKBIT_ON(mode))
				{
					granted = true;
					instance->holdMask &= LOCKBIT_OFF(mode);
					break;
				}
			}
		}

		/*
		 * If no (more) held modes to report, see if PROC is waiting for a
		 * lock on this lock.
		 */
		if (!granted)
		{
			if (instance->waitLockMode != NoLock)
			{
				/* Yes, so report it with proper mode */
				mode = instance->waitLockMode;

				/*
				 * We are now done with this PROCLOCK, so advance pointer to
				 * continue with next one on next call.
				 */
				mystatus->currIdx++;
			}
			else
			{
				/*
				 * Okay, we've displayed all the locks associated with this
				 * PROCLOCK, proceed to the next one.
				 */
				mystatus->currIdx++;
				continue;
			}
		}

		/*
		 * Form tuple with appropriate data.
		 */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));

		if (instance->locktag.locktag_type <= LOCKTAG_LAST_TYPE)
			locktypename = LockTagTypeNames[instance->locktag.locktag_type];
		else
		{
			snprintf(tnbuf, sizeof(tnbuf), "unknown %d",
					 (int) instance->locktag.locktag_type);
			locktypename = tnbuf;
		}
		values[0] = CStringGetTextDatum(locktypename);

		switch ((LockTagType) instance->locktag.locktag_type)
		{
			case LOCKTAG_RELATION:
			case LOCKTAG_RELATION_EXTEND:
				values[1] = ObjectIdGetDatum(instance->locktag.locktag_field1);
				values[2] = ObjectIdGetDatum(instance->locktag.locktag_field2);
				nulls[3] = true;
				nulls[4] = true;
				nulls[5] = true;
				nulls[6] = true;
				nulls[7] = true;
				nulls[8] = true;
				nulls[9] = true;
				break;
			case LOCKTAG_PAGE:
				values[1] = ObjectIdGetDatum(instance->locktag.locktag_field1);
				values[2] = ObjectIdGetDatum(instance->locktag.locktag_field2);
				values[3] = UInt32GetDatum(instance->locktag.locktag_field3);
				nulls[4] = true;
				nulls[5] = true;
				nulls[6] = true;
				nulls[7] = true;
				nulls[8] = true;
				nulls[9] = true;
				break;
			case LOCKTAG_TUPLE:
				values[1] = ObjectIdGetDatum(instance->locktag.locktag_field1);
				values[2] = ObjectIdGetDatum(instance->locktag.locktag_field2);
				values[3] = UInt32GetDatum(instance->locktag.locktag_field3);
				values[4] = UInt16GetDatum(instance->locktag.locktag_field4);
				nulls[5] = true;
				nulls[6] = true;
				nulls[7] = true;
				nulls[8] = true;
				nulls[9] = true;
				break;
			case LOCKTAG_TRANSACTION:
				values[6] =
					TransactionIdGetDatum(instance->locktag.locktag_field1);
				nulls[1] = true;
				nulls[2] = true;
				nulls[3] = true;
				nulls[4] = true;
				nulls[5] = true;
				nulls[7] = true;
				nulls[8] = true;
				nulls[9] = true;
				break;
			case LOCKTAG_VIRTUALTRANSACTION:
				values[5] = VXIDGetDatum(instance->locktag.locktag_field1,
										 instance->locktag.locktag_field2);
				nulls[1] = true;
				nulls[2] = true;
				nulls[3] = true;
				nulls[4] = true;
				nulls[6] = true;
				nulls[7] = true;
				nulls[8] = true;
				nulls[9] = true;
				break;
			case LOCKTAG_OBJECT:
			case LOCKTAG_USERLOCK:
			case LOCKTAG_ADVISORY:
			default:			/* treat unknown locktags like OBJECT */
				values[1] = ObjectIdGetDatum(instance->locktag.locktag_field1);
				values[7] = ObjectIdGetDatum(instance->locktag.locktag_field2);
				values[8] = ObjectIdGetDatum(instance->locktag.locktag_field3);
				values[9] = Int16GetDatum(instance->locktag.locktag_field4);
				nulls[2] = true;
				nulls[3] = true;
				nulls[4] = true;
				nulls[5] = true;
				nulls[6] = true;
				break;
		}

		values[10] = VXIDGetDatum(instance->backend, instance->lxid);
		if (instance->pid != 0)
			values[11] = Int32GetDatum(instance->pid);
		else
			nulls[11] = true;
		values[12] = CStringGetTextDatum(GetLockmodeName(instance->locktag.locktag_lockmethodid, mode));
		values[13] = BoolGetDatum(granted);
		values[14] = BoolGetDatum(instance->fastpath);

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);
		SRF_RETURN_NEXT(funcctx, result);
	}

	/*
	 * Have returned all regular locks. Now start on the SIREAD predicate
	 * locks.
	 */
	predLockData = mystatus->predLockData;
	if (mystatus->predLockIdx < predLockData->nelements)
	{
		PredicateLockTargetType lockType;

		PREDICATELOCKTARGETTAG *predTag = &(predLockData->locktags[mystatus->predLockIdx]);
		SERIALIZABLEXACT *xact = &(predLockData->xacts[mystatus->predLockIdx]);
		Datum		values[NUM_LOCK_STATUS_COLUMNS];
		bool		nulls[NUM_LOCK_STATUS_COLUMNS];
		HeapTuple	tuple;
		Datum		result;

		mystatus->predLockIdx++;

		/*
		 * Form tuple with appropriate data.
		 */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));

		/* lock type */
		lockType = GET_PREDICATELOCKTARGETTAG_TYPE(*predTag);

		values[0] = CStringGetTextDatum(PredicateLockTagTypeNames[lockType]);

		/* lock target */
		values[1] = GET_PREDICATELOCKTARGETTAG_DB(*predTag);
		values[2] = GET_PREDICATELOCKTARGETTAG_RELATION(*predTag);
		if (lockType == PREDLOCKTAG_TUPLE)
			values[4] = GET_PREDICATELOCKTARGETTAG_OFFSET(*predTag);
		else
			nulls[4] = true;
		if ((lockType == PREDLOCKTAG_TUPLE) ||
			(lockType == PREDLOCKTAG_PAGE))
			values[3] = GET_PREDICATELOCKTARGETTAG_PAGE(*predTag);
		else
			nulls[3] = true;

		/* these fields are targets for other types of locks */
		nulls[5] = true;		/* virtualxid */
		nulls[6] = true;		/* transactionid */
		nulls[7] = true;		/* classid */
		nulls[8] = true;		/* objid */
		nulls[9] = true;		/* objsubid */

		/* lock holder */
		values[10] = VXIDGetDatum(xact->vxid.backendId,
								  xact->vxid.localTransactionId);
		if (xact->pid != 0)
			values[11] = Int32GetDatum(xact->pid);
		else
			nulls[11] = true;

		/*
		 * Lock mode. Currently all predicate locks are SIReadLocks, which are
		 * always held (never waiting) and have no fast path
		 */
		values[12] = CStringGetTextDatum("SIReadLock");
		values[13] = BoolGetDatum(true);
		values[14] = BoolGetDatum(false);

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);
		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);
}


/*
 * Functions for manipulating advisory locks
 *
 * We make use of the locktag fields as follows:
 *
 *	field1: MyDatabaseId ... ensures locks are local to each database
 *	field2: first of 2 int4 keys, or high-order half of an int8 key
 *	field3: second of 2 int4 keys, or low-order half of an int8 key
 *	field4: 1 if using an int8 key, 2 if using 2 int4 keys
 */
#define SET_LOCKTAG_INT64(tag, key64) \
	SET_LOCKTAG_ADVISORY(tag, \
						 MyDatabaseId, \
						 (uint32) ((key64) >> 32), \
						 (uint32) (key64), \
						 1)
#define SET_LOCKTAG_INT32(tag, key1, key2) \
	SET_LOCKTAG_ADVISORY(tag, MyDatabaseId, key1, key2, 2)

#ifdef PGXC

#define MAXINT8LEN 25

/*
 * pgxc_advisory_lock - Core function that implements the algorithm needed to
 * propogate the advisory lock function calls to all Coordinators.
 * The idea is to make the advisory locks cluster-aware, so that a user having
 * a lock from Coordinator 1 will make the user from Coordinator 2 to wait for
 * the same lock.
 *
 * Return true if all locks are returned successfully. False otherwise.
 * Effectively this function returns false only if dontWait is true. Otherwise
 * it either returns true, or waits on a resource, or throws an exception
 * returned by the lock function calls in case of unexpected or fatal errors.
 *
 * Currently used only for session level locks; not used for transaction level
 * locks.
 */
static bool
pgxc_advisory_lock(int64 key64, int32 key1, int32 key2, bool iskeybig,
			LOCKMODE lockmode,
			LockLevel locklevel,
			TryType try)
{
	LOCKTAG		locktag;
	Oid				*coOids, *dnOids;
	int numdnodes, numcoords;
	StringInfoData  lock_cmd, unlock_cmd, lock_funcname, unlock_funcname, args;
	char		str_key[MAXINT8LEN + 1];
	int i, prev;
	bool abort_locking = false;
	Datum lock_status;
	bool sessionLock = (locklevel == SESSION_LOCK);
	bool dontWait = (try == DONT_WAIT);

	if (iskeybig)
		SET_LOCKTAG_INT64(locktag, key64);
	else
		SET_LOCKTAG_INT32(locktag, key1, key2);

	PgxcNodeGetOids(&coOids, &dnOids, &numcoords, &numdnodes, false);

	/* Skip everything XC specific if there's only one Coordinator running */
	if (numcoords <= 1)
	{
		LockAcquireResult res;

		res = LockAcquire(&locktag, lockmode, sessionLock, dontWait);
		return (res == LOCKACQUIRE_OK || res == LOCKACQUIRE_ALREADY_HELD);
	}

	/*
	 * If there is already a lock held by us, just increment and return; we
	 * already did all necessary steps when we locked for the first time.
	 */
	if (LockIncrementIfExists(&locktag, lockmode, sessionLock) == true)
		return true;

	initStringInfo(&lock_funcname);
	appendStringInfo(&lock_funcname, "pg_%sadvisory_%slock%s",
	                                 (dontWait ? "try_" : ""),
									 (sessionLock ? "" : "xact_"),
									 (lockmode == ShareLock ? "_shared": ""));

	initStringInfo(&unlock_funcname);
	appendStringInfo(&unlock_funcname, "pg_advisory_unlock%s",
									 (lockmode == ShareLock ? "_shared": ""));

	initStringInfo(&args);

	if (iskeybig)
	{
		pg_lltoa(key64, str_key);
		appendStringInfo(&args, "%s", str_key);
	}
	else
	{
		pg_ltoa(key1, str_key);
		appendStringInfo(&args, "%s, ", str_key);
		pg_ltoa(key2, str_key);
		appendStringInfo(&args, "%s", str_key);
	}

	initStringInfo(&lock_cmd);
	appendStringInfo(&lock_cmd, "SELECT pg_catalog.%s(%s)", lock_funcname.data, args.data);
	initStringInfo(&unlock_cmd);
	appendStringInfo(&unlock_cmd, "SELECT pg_catalog.%s(%s)", unlock_funcname.data, args.data);

	/*
	 * Go on locking on each Coordinator. Keep on unlocking the previous one
	 * after a lock is held on next Coordinator. Don't unlock the local
	 * Coordinator. After finishing all Coordinators, ultimately only the local
	 * Coordinator would be locked, but still we will have scanned all
	 * Coordinators to make sure no one else has already grabbed the lock. The
	 * reason for unlocking all remote locks is because the session level locks
	 * don't get unlocked until explicitly unlocked or the session quits. After
	 * the user session quits without explicitly unlocking, the coord-to-coord
	 * pooler connection stays and so does the remote Coordinator lock.
	 */
	prev = -1;
	for (i = 0; i <= numcoords && !abort_locking; i++, prev++)
	{
		if (i < numcoords)
		{
			/* If this Coordinator is myself, execute native lock calls */
			if (i == PGXCNodeId - 1)
				lock_status = LockAcquire(&locktag, lockmode, sessionLock, dontWait);
			else
				lock_status = pgxc_execute_on_nodes(1, &coOids[i], lock_cmd.data);

			if (dontWait == true && DatumGetBool(lock_status) == false)
			{
				abort_locking = true;
				/*
				 * If we have gone past the local Coordinator node, it implies
				 * that we have obtained a local lock. But now that we are
				 * aborting, we need to release the local lock first.
				 */
				if (i > PGXCNodeId - 1)
					(void) LockRelease(&locktag, lockmode, sessionLock);
			}
		}

		/*
		 * If we are dealing with session locks, unlock the previous lock, but
		 * only if it is a remote Coordinator. If it is a local one, we want to
		 * keep that lock. Remember, the final status should be that there is
		 * only *one* lock held, and that is the local lock.
		 */
		if (sessionLock && prev >= 0 && prev != PGXCNodeId - 1)
			pgxc_execute_on_nodes(1, &coOids[prev], unlock_cmd.data);
	}

	return (!abort_locking);
}

#endif /* PGXC */

/*
 * pg_advisory_lock(int8) - acquire exclusive lock on an int8 key
 */
Datum
pg_advisory_lock_int8(PG_FUNCTION_ARGS)
{
	int64		key = PG_GETARG_INT64(0);
	LOCKTAG		tag;

#ifdef PGXC
	if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
	{
		pgxc_advisory_lock(key, 0, 0, true, ExclusiveLock, SESSION_LOCK, WAIT);
		PG_RETURN_VOID();
	}
#endif

	SET_LOCKTAG_INT64(tag, key);

	(void) LockAcquire(&tag, ExclusiveLock, true, false);

	PG_RETURN_VOID();
}

/*
 * pg_advisory_xact_lock(int8) - acquire xact scoped
 * exclusive lock on an int8 key
 */
Datum
pg_advisory_xact_lock_int8(PG_FUNCTION_ARGS)
{
	int64		key = PG_GETARG_INT64(0);
	LOCKTAG		tag;

#ifdef PGXC
	if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
	{
		pgxc_advisory_lock(key, 0, 0, true, ExclusiveLock, TRANSACTION_LOCK, WAIT);
		PG_RETURN_VOID();
	}
#endif

	SET_LOCKTAG_INT64(tag, key);

	(void) LockAcquire(&tag, ExclusiveLock, false, false);

	PG_RETURN_VOID();
}

/*
 * pg_advisory_lock_shared(int8) - acquire share lock on an int8 key
 */
Datum
pg_advisory_lock_shared_int8(PG_FUNCTION_ARGS)
{
	int64		key = PG_GETARG_INT64(0);
	LOCKTAG		tag;

#ifdef PGXC
	if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
	{
		pgxc_advisory_lock(key, 0, 0, true, ShareLock, SESSION_LOCK, WAIT);
		PG_RETURN_VOID();
	}
#endif

	SET_LOCKTAG_INT64(tag, key);

	(void) LockAcquire(&tag, ShareLock, true, false);

	PG_RETURN_VOID();
}

/*
 * pg_advisory_xact_lock_shared(int8) - acquire xact scoped
 * share lock on an int8 key
 */
Datum
pg_advisory_xact_lock_shared_int8(PG_FUNCTION_ARGS)
{
	int64		key = PG_GETARG_INT64(0);
	LOCKTAG		tag;

#ifdef PGXC
	if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
	{
		pgxc_advisory_lock(key, 0, 0, true, ShareLock, TRANSACTION_LOCK, WAIT);
		PG_RETURN_VOID();
	}
#endif

	SET_LOCKTAG_INT64(tag, key);

	(void) LockAcquire(&tag, ShareLock, false, false);

	PG_RETURN_VOID();
}

/*
 * pg_try_advisory_lock(int8) - acquire exclusive lock on an int8 key, no wait
 *
 * Returns true if successful, false if lock not available
 */
Datum
pg_try_advisory_lock_int8(PG_FUNCTION_ARGS)
{
	int64		key = PG_GETARG_INT64(0);
	LOCKTAG		tag;
	LockAcquireResult res;

#ifdef PGXC
	if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
		PG_RETURN_BOOL(pgxc_advisory_lock(key, 0, 0, true, ExclusiveLock, SESSION_LOCK, DONT_WAIT));
#endif

	SET_LOCKTAG_INT64(tag, key);

	res = LockAcquire(&tag, ExclusiveLock, true, true);

	PG_RETURN_BOOL(res != LOCKACQUIRE_NOT_AVAIL);
}

/*
 * pg_try_advisory_xact_lock(int8) - acquire xact scoped
 * exclusive lock on an int8 key, no wait
 *
 * Returns true if successful, false if lock not available
 */
Datum
pg_try_advisory_xact_lock_int8(PG_FUNCTION_ARGS)
{
	int64		key = PG_GETARG_INT64(0);
	LOCKTAG		tag;
	LockAcquireResult res;

#ifdef PGXC
	if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
		PG_RETURN_BOOL(pgxc_advisory_lock(key, 0, 0, true, ExclusiveLock, TRANSACTION_LOCK, DONT_WAIT));
#endif

	SET_LOCKTAG_INT64(tag, key);

	res = LockAcquire(&tag, ExclusiveLock, false, true);

	PG_RETURN_BOOL(res != LOCKACQUIRE_NOT_AVAIL);
}

/*
 * pg_try_advisory_lock_shared(int8) - acquire share lock on an int8 key, no wait
 *
 * Returns true if successful, false if lock not available
 */
Datum
pg_try_advisory_lock_shared_int8(PG_FUNCTION_ARGS)
{
	int64		key = PG_GETARG_INT64(0);
	LOCKTAG		tag;
	LockAcquireResult res;

#ifdef PGXC
	if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
		PG_RETURN_BOOL(pgxc_advisory_lock(key, 0, 0, true, ShareLock, SESSION_LOCK, DONT_WAIT));
#endif

	SET_LOCKTAG_INT64(tag, key);

	res = LockAcquire(&tag, ShareLock, true, true);

	PG_RETURN_BOOL(res != LOCKACQUIRE_NOT_AVAIL);
}

/*
 * pg_try_advisory_xact_lock_shared(int8) - acquire xact scoped
 * share lock on an int8 key, no wait
 *
 * Returns true if successful, false if lock not available
 */
Datum
pg_try_advisory_xact_lock_shared_int8(PG_FUNCTION_ARGS)
{
	int64		key = PG_GETARG_INT64(0);
	LOCKTAG		tag;
	LockAcquireResult res;

#ifdef PGXC
	if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
		PG_RETURN_BOOL(pgxc_advisory_lock(key, 0, 0, true, ShareLock, TRANSACTION_LOCK, DONT_WAIT));
#endif

	SET_LOCKTAG_INT64(tag, key);

	res = LockAcquire(&tag, ShareLock, false, true);

	PG_RETURN_BOOL(res != LOCKACQUIRE_NOT_AVAIL);
}

/*
 * pg_advisory_unlock(int8) - release exclusive lock on an int8 key
 *
 * Returns true if successful, false if lock was not held
*/
Datum
pg_advisory_unlock_int8(PG_FUNCTION_ARGS)
{
	int64		key = PG_GETARG_INT64(0);
	LOCKTAG		tag;
	bool		res;

	SET_LOCKTAG_INT64(tag, key);

	res = LockRelease(&tag, ExclusiveLock, true);

	PG_RETURN_BOOL(res);
}

/*
 * pg_advisory_unlock_shared(int8) - release share lock on an int8 key
 *
 * Returns true if successful, false if lock was not held
 */
Datum
pg_advisory_unlock_shared_int8(PG_FUNCTION_ARGS)
{
	int64		key = PG_GETARG_INT64(0);
	LOCKTAG		tag;
	bool		res;

	SET_LOCKTAG_INT64(tag, key);

	res = LockRelease(&tag, ShareLock, true);

	PG_RETURN_BOOL(res);
}

/*
 * pg_advisory_lock(int4, int4) - acquire exclusive lock on 2 int4 keys
 */
Datum
pg_advisory_lock_int4(PG_FUNCTION_ARGS)
{
	int32		key1 = PG_GETARG_INT32(0);
	int32		key2 = PG_GETARG_INT32(1);
	LOCKTAG		tag;

#ifdef PGXC
	if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
	{
		pgxc_advisory_lock(0, key1, key2, false, ExclusiveLock, SESSION_LOCK, WAIT);
		PG_RETURN_VOID();
	}
#endif

	SET_LOCKTAG_INT32(tag, key1, key2);

	(void) LockAcquire(&tag, ExclusiveLock, true, false);

	PG_RETURN_VOID();
}

/*
 * pg_advisory_xact_lock(int4, int4) - acquire xact scoped
 * exclusive lock on 2 int4 keys
 */
Datum
pg_advisory_xact_lock_int4(PG_FUNCTION_ARGS)
{
	int32		key1 = PG_GETARG_INT32(0);
	int32		key2 = PG_GETARG_INT32(1);
	LOCKTAG		tag;

#ifdef PGXC
	if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
	{
		pgxc_advisory_lock(0, key1, key2, false, ExclusiveLock, TRANSACTION_LOCK, WAIT);
		PG_RETURN_VOID();
	}
#endif

	SET_LOCKTAG_INT32(tag, key1, key2);

	(void) LockAcquire(&tag, ExclusiveLock, false, false);

	PG_RETURN_VOID();
}

/*
 * pg_advisory_lock_shared(int4, int4) - acquire share lock on 2 int4 keys
 */
Datum
pg_advisory_lock_shared_int4(PG_FUNCTION_ARGS)
{
	int32		key1 = PG_GETARG_INT32(0);
	int32		key2 = PG_GETARG_INT32(1);
	LOCKTAG		tag;

#ifdef PGXC
	if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
	{
		pgxc_advisory_lock(0, key1, key2, false, ShareLock, SESSION_LOCK, WAIT);
		PG_RETURN_VOID();
	}
#endif

	SET_LOCKTAG_INT32(tag, key1, key2);

	(void) LockAcquire(&tag, ShareLock, true, false);

	PG_RETURN_VOID();
}

/*
 * pg_advisory_xact_lock_shared(int4, int4) - acquire xact scoped
 * share lock on 2 int4 keys
 */
Datum
pg_advisory_xact_lock_shared_int4(PG_FUNCTION_ARGS)
{
	int32		key1 = PG_GETARG_INT32(0);
	int32		key2 = PG_GETARG_INT32(1);
	LOCKTAG		tag;

#ifdef PGXC
	if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
	{
		pgxc_advisory_lock(0, key1, key2, false, ShareLock, TRANSACTION_LOCK, WAIT);
		PG_RETURN_VOID();
	}
#endif

	SET_LOCKTAG_INT32(tag, key1, key2);

	(void) LockAcquire(&tag, ShareLock, false, false);

	PG_RETURN_VOID();
}

/*
 * pg_try_advisory_lock(int4, int4) - acquire exclusive lock on 2 int4 keys, no wait
 *
 * Returns true if successful, false if lock not available
 */
Datum
pg_try_advisory_lock_int4(PG_FUNCTION_ARGS)
{
	int32		key1 = PG_GETARG_INT32(0);
	int32		key2 = PG_GETARG_INT32(1);
	LOCKTAG		tag;
	LockAcquireResult res;

#ifdef PGXC
	if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
		PG_RETURN_BOOL(pgxc_advisory_lock(0, key1, key2, false, ExclusiveLock, SESSION_LOCK, DONT_WAIT));
#endif

	SET_LOCKTAG_INT32(tag, key1, key2);

	res = LockAcquire(&tag, ExclusiveLock, true, true);

	PG_RETURN_BOOL(res != LOCKACQUIRE_NOT_AVAIL);
}

/*
 * pg_try_advisory_xact_lock(int4, int4) - acquire xact scoped
 * exclusive lock on 2 int4 keys, no wait
 *
 * Returns true if successful, false if lock not available
 */
Datum
pg_try_advisory_xact_lock_int4(PG_FUNCTION_ARGS)
{
	int32		key1 = PG_GETARG_INT32(0);
	int32		key2 = PG_GETARG_INT32(1);
	LOCKTAG		tag;
	LockAcquireResult res;

#ifdef PGXC
	if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
		PG_RETURN_BOOL(pgxc_advisory_lock(0, key1, key2, false, ExclusiveLock, TRANSACTION_LOCK, DONT_WAIT));
#endif

	SET_LOCKTAG_INT32(tag, key1, key2);

	res = LockAcquire(&tag, ExclusiveLock, false, true);

	PG_RETURN_BOOL(res != LOCKACQUIRE_NOT_AVAIL);
}

/*
 * pg_try_advisory_lock_shared(int4, int4) - acquire share lock on 2 int4 keys, no wait
 *
 * Returns true if successful, false if lock not available
 */
Datum
pg_try_advisory_lock_shared_int4(PG_FUNCTION_ARGS)
{
	int32		key1 = PG_GETARG_INT32(0);
	int32		key2 = PG_GETARG_INT32(1);
	LOCKTAG		tag;
	LockAcquireResult res;

#ifdef PGXC
	if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
		PG_RETURN_BOOL(pgxc_advisory_lock(0, key1, key2, false, ShareLock, SESSION_LOCK, DONT_WAIT));
#endif

	SET_LOCKTAG_INT32(tag, key1, key2);

	res = LockAcquire(&tag, ShareLock, true, true);

	PG_RETURN_BOOL(res != LOCKACQUIRE_NOT_AVAIL);
}

/*
 * pg_try_advisory_xact_lock_shared(int4, int4) - acquire xact scoped
 * share lock on 2 int4 keys, no wait
 *
 * Returns true if successful, false if lock not available
 */
Datum
pg_try_advisory_xact_lock_shared_int4(PG_FUNCTION_ARGS)
{
	int32		key1 = PG_GETARG_INT32(0);
	int32		key2 = PG_GETARG_INT32(1);
	LOCKTAG		tag;
	LockAcquireResult res;

#ifdef PGXC
	if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
		PG_RETURN_BOOL(pgxc_advisory_lock(0, key1, key2, false, ShareLock, TRANSACTION_LOCK, DONT_WAIT));
#endif

	SET_LOCKTAG_INT32(tag, key1, key2);

	res = LockAcquire(&tag, ShareLock, false, true);

	PG_RETURN_BOOL(res != LOCKACQUIRE_NOT_AVAIL);
}

/*
 * pg_advisory_unlock(int4, int4) - release exclusive lock on 2 int4 keys
 *
 * Returns true if successful, false if lock was not held
*/
Datum
pg_advisory_unlock_int4(PG_FUNCTION_ARGS)
{
	int32		key1 = PG_GETARG_INT32(0);
	int32		key2 = PG_GETARG_INT32(1);
	LOCKTAG		tag;
	bool		res;

	SET_LOCKTAG_INT32(tag, key1, key2);

	res = LockRelease(&tag, ExclusiveLock, true);

	PG_RETURN_BOOL(res);
}

/*
 * pg_advisory_unlock_shared(int4, int4) - release share lock on 2 int4 keys
 *
 * Returns true if successful, false if lock was not held
 */
Datum
pg_advisory_unlock_shared_int4(PG_FUNCTION_ARGS)
{
	int32		key1 = PG_GETARG_INT32(0);
	int32		key2 = PG_GETARG_INT32(1);
	LOCKTAG		tag;
	bool		res;

	SET_LOCKTAG_INT32(tag, key1, key2);

	res = LockRelease(&tag, ShareLock, true);

	PG_RETURN_BOOL(res);
}

/*
 * pg_advisory_unlock_all() - release all advisory locks
 */
Datum
pg_advisory_unlock_all(PG_FUNCTION_ARGS)
{
	LockReleaseSession(USER_LOCKMETHOD);

	PG_RETURN_VOID();
}

#ifdef PGXC
/*
 * pgxc_lock_for_backup
 *
 * Lock the cluster for taking backup
 * To lock the cluster, try to acquire a session level advisory lock exclusivly
 * By lock we mean to disallow any statements that change
 * the portions of the catalog which are backed up by pg_dump/pg_dumpall
 * Returns true or fails with an error message.
 */
Datum
pgxc_lock_for_backup(PG_FUNCTION_ARGS)
{
	bool lockAcquired = false;
	int prepared_xact_count = 0;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					errmsg("only superuser can lock the cluster for backup")));

	/*
	 * The system cannot be locked for backup if there is an uncommitted
	 * prepared transaction, the reason is as follows:
	 * Utility statements are divided into two groups, one is allowed group
	 * and the other is disallowed group. A statement is put in allowed group
	 * if it does not make changes to the catalog or makes such changes which
	 * are not backed up by pg_dump or pg_dumpall, otherwise it is put in
	 * disallowed group. Every time a disallowed statement is issued we try to
	 * hold an advisory lock in shared mode and if the lock can be acquired
	 * only then the statement is allowed.
	 * In case of prepared transactions suppose the lock is not released at
	 * prepare transaction 'txn_id'
	 * Consider the following scenario:
	 *
	 *	begin;
	 *	create table abc_def(a int, b int);
	 *	insert into abc_def values(1,2),(3,4);
	 *	prepare transaction 'abc';
	 *
	 * Now assume that the server is restarted for any reason.
	 * When prepared transactions are saved on disk, session level locks are
	 * ignored and hence when the prepared transactions are reterieved and all
	 * the other locks are reclaimed, but session level advisory locks are
	 * not reclaimed.
	 * Hence we made the following decisions
	 * a) Transaction level advisory locks should be used for DDLs which are
	 *    automatically released at prepare transaction 'txn_id'
	 * b) If there is any uncomitted prepared transaction, it is assumed
	 *    that it must be issuing a statement that belongs to disallowed
	 *    group and hence the request to hold the advisory lock exclusively
	 *    is denied.
	 */

	/* Connect to SPI manager to check any prepared transactions */
	if (SPI_connect() < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
					errmsg("internal error while locking the cluster for backup")));
	}

	/* Are there any prepared transactions that have not yet been committed? */
	SPI_execute("select gid from pg_catalog.pg_prepared_xacts limit 1", true, 0);
	prepared_xact_count = SPI_processed;
	SPI_finish();

	if (prepared_xact_count > 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("cannot lock cluster for backup in presence of %d uncommitted prepared transactions",
						prepared_xact_count)));
	}

	/* try to acquire the advisory lock in exclusive mode */
	lockAcquired = DatumGetBool(DirectFunctionCall2(
										pg_try_advisory_lock_int4,
										xc_lockForBackupKey1,
										xc_lockForBackupKey2));

	if (!lockAcquired)
		ereport(ERROR,
				(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
				errmsg("cannot lock cluster for backup, lock is already held")));

	/*
	 * sessin level advisory locks stay for only as long as the session
	 * that issues them does
	 */
	elog(INFO, "please do not close this session until you are done adding the new node");

	/* will be true always */
	PG_RETURN_BOOL(lockAcquired);
}

/*
 * pgxc_lock_for_backup
 *
 * Lock the cluster for taking backup
 * To lock the cluster, try to acquire a session level advisory lock exclusivly
 * By lock we mean to disallow any statements that change
 * the portions of the catalog which are backed up by pg_dump/pg_dumpall
 * Returns true or fails with an error message.
 */
bool
pgxc_lock_for_utility_stmt(Node *parsetree)
{
	bool lockAcquired;

	lockAcquired = DatumGetBool(DirectFunctionCall2(
								pg_try_advisory_xact_lock_shared_int4,
								xc_lockForBackupKey1,
								xc_lockForBackupKey2));

	if (!lockAcquired)
		ereport(ERROR,
				(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
				errmsg("cannot execute %s in a locked cluster",
						CreateCommandTag(parsetree))));

	return lockAcquired;
}
#endif
