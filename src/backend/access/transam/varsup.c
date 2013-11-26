/*-------------------------------------------------------------------------
 *
 * varsup.c
 *	  postgres OID & XID variables support routines
 *
 * Copyright (c) 2000-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/transam/varsup.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/clog.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/xact.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "postmaster/autovacuum.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "utils/syscache.h"
#ifdef PGXC
#include "pgxc/pgxc.h"
#include "access/gtm.h"
#include "storage/procarray.h"
#endif


/* Number of OIDs to prefetch (preallocate) per XLOG write */
#define VAR_OID_PREFETCH		8192

/* pointer to "variable cache" in shared memory (set up by shmem.c) */
VariableCache ShmemVariableCache = NULL;

#ifdef PGXC  /* PGXC_DATANODE */
static TransactionId next_xid = InvalidTransactionId;
static bool force_get_xid_from_gtm = false;

/*
 * Set next transaction id to use
 */
void
SetNextTransactionId(TransactionId xid)
{
	elog (DEBUG1, "[re]setting xid = %d, old_value = %d", xid, next_xid);
	next_xid = xid;
}

/*
 * Allow force of getting XID from GTM
 * Useful for explicit VACUUM (autovacuum already handled)
 */
void
SetForceXidFromGTM(bool value)
{
	force_get_xid_from_gtm = value;
}

/*
 * See if we should force using GTM
 * Useful for explicit VACUUM (autovacuum already handled)
 */
bool
GetForceXidFromGTM(void)
{
	return force_get_xid_from_gtm;
}
#endif /* PGXC */

/*
 * Allocate the next XID for a new transaction or subtransaction.
 *
 * The new XID is also stored into MyPgXact before returning.
 *
 * Note: when this is called, we are actually already inside a valid
 * transaction, since XIDs are now not allocated until the transaction
 * does something.	So it is safe to do a database lookup if we want to
 * issue a warning about XID wrap.
 */
TransactionId
#ifdef PGXC
GetNewTransactionId(bool isSubXact, bool *timestamp_received, GTM_Timestamp *timestamp)
#else
GetNewTransactionId(bool isSubXact)
#endif
{
	TransactionId xid;
#ifdef PGXC
	bool increment_xid = true;

	*timestamp_received = false;
#endif

	/*
	 * During bootstrap initialization, we return the special bootstrap
	 * transaction id.
	 */
	if (IsBootstrapProcessingMode())
	{
		Assert(!isSubXact);
		MyPgXact->xid = BootstrapTransactionId;
		return BootstrapTransactionId;
	}

	/* safety check, we should never get this far in a HS slave */
	if (RecoveryInProgress())
		elog(ERROR, "cannot assign TransactionIds during recovery");

#ifdef PGXC
	/* Initialize transaction ID */
	xid = InvalidTransactionId;

	if ((IS_PGXC_COORDINATOR && !IsConnFromCoord()) || IsPGXCNodeXactDatanodeDirect())
	{
		/*
		 * Get XID from GTM before acquiring the lock as concurrent connections are
		 * being handled on GTM side even if the lock is acquired in a different
		 * order.
		 */
		if (IsAutoVacuumWorkerProcess() && (MyPgXact->vacuumFlags & PROC_IN_VACUUM))
			xid = (TransactionId) BeginTranAutovacuumGTM();
		else
			xid = (TransactionId) BeginTranGTM(timestamp);
		*timestamp_received = true;
	}
#endif

	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);

#ifdef PGXC
	/* Only remote Coordinator or a Datanode accessed directly by an application can get a GXID */
	if ((IS_PGXC_COORDINATOR && !IsConnFromCoord()) || IsPGXCNodeXactDatanodeDirect())
	{
		if (TransactionIdIsValid(xid))
		{
			/* Log some information about the new transaction ID obtained */
			if (IsAutoVacuumWorkerProcess() && (MyPgXact->vacuumFlags & PROC_IN_VACUUM))
				elog(DEBUG1, "Assigned new transaction ID from GTM for autovacuum = %d", xid);
			else
				elog(DEBUG1, "Assigned new transaction ID from GTM = %d", xid);

			if (!TransactionIdFollowsOrEquals(xid, ShmemVariableCache->nextXid))
			{
				increment_xid = false;
				ereport(DEBUG1,
				   (errmsg("xid (%d) was less than ShmemVariableCache->nextXid (%d)",
					   xid, ShmemVariableCache->nextXid)));
			}
			else
				ShmemVariableCache->nextXid = xid;
		}
		else
		{
			ereport(WARNING,
			   (errmsg("Xid is invalid.")));

			/* Problem is already reported, so just remove lock and return */
			LWLockRelease(XidGenLock);
			return xid;
		}
	}
	else if(IS_PGXC_DATANODE || IsConnFromCoord())
	{
		if (IsAutoVacuumWorkerProcess())
		{
			/*
			 * For an autovacuum worker process, get transaction ID directly from GTM.
			 * If this vacuum process is a vacuum analyze, its GXID has to be excluded
			 * from snapshots so use a special function for this purpose.
			 * For a simple worker get transaction ID like a normal transaction would do.
			 */
			if (MyPgXact->vacuumFlags & PROC_IN_VACUUM)
				next_xid = (TransactionId) BeginTranAutovacuumGTM();
			else
				next_xid = (TransactionId) BeginTranGTM(timestamp);
		}
		else if (GetForceXidFromGTM())
		{
			elog (DEBUG1, "Force get XID from GTM");
			/* try and get gxid directly from GTM */
			next_xid = (TransactionId) BeginTranGTM(NULL);
		}

		if (TransactionIdIsValid(next_xid))
		{
			xid = next_xid;
			elog(DEBUG1, "TransactionId = %d", next_xid);
			next_xid = InvalidTransactionId; /* reset */
			if (!TransactionIdFollowsOrEquals(xid, ShmemVariableCache->nextXid))
			{
				/* This should be ok, due to concurrency from multiple coords
				 * passing down the xids.
				 * We later do not want to bother incrementing the value
				 * in shared memory though.
				 */
				increment_xid = false;
				elog(DEBUG1, "xid (%d) does not follow ShmemVariableCache->nextXid (%d)",
					xid, ShmemVariableCache->nextXid);
			}
			else
				ShmemVariableCache->nextXid = xid;
		}
		else
		{
			/* Fallback to default */
			elog(LOG, "Falling back to local Xid. Was = %d, now is = %d",
					next_xid, ShmemVariableCache->nextXid);
			xid = ShmemVariableCache->nextXid;
		}
	}
#else
	xid = ShmemVariableCache->nextXid;
#endif /* PGXC */


	/*----------
	 * Check to see if it's safe to assign another XID.  This protects against
	 * catastrophic data loss due to XID wraparound.  The basic rules are:
	 *
	 * If we're past xidVacLimit, start trying to force autovacuum cycles.
	 * If we're past xidWarnLimit, start issuing warnings.
	 * If we're past xidStopLimit, refuse to execute transactions, unless
	 * we are running in a standalone backend (which gives an escape hatch
	 * to the DBA who somehow got past the earlier defenses).
	 *----------
	 */
#ifdef PGXC
	/*
	 * In PG, the xid will never cross the wrap-around limit thanks to the
	 * above checks. But in PGXC, if a new node is initialized and brought up,
	 * it's own xid may not be in sync with the GTM gxid. The wrap-around limits
	 * are initially set w.r.t. the last xid used. So if the Gxid-xid difference
	 * is already more than 2^31, then the gxid is deemed to have already
	 * crossed the wrap-around limit. So again in such cases as well, we should
	 * allow only a standalone backend to run vacuum.
	 */
	if (TransactionIdFollowsOrEquals(xid, ShmemVariableCache->xidWrapLimit))
	{
		if (IsPostmasterEnvironment)
		    ereport(ERROR,
		       (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
		       errmsg("Xid wraparound might have already happened. database is not accepting commands on database with OID %u",
		       ShmemVariableCache->oldestXidDB),
		       errhint("Stop the postmaster and use a standalone backend to vacuum that database.\n"
		               "You might also need to commit or roll back old prepared transactions.")));
	}
	else
#endif
	if (TransactionIdFollowsOrEquals(xid, ShmemVariableCache->xidVacLimit))
	{
		/*
		 * For safety's sake, we release XidGenLock while sending signals,
		 * warnings, etc.  This is not so much because we care about
		 * preserving concurrency in this situation, as to avoid any
		 * possibility of deadlock while doing get_database_name(). First,
		 * copy all the shared values we'll need in this path.
		 */
		TransactionId xidWarnLimit = ShmemVariableCache->xidWarnLimit;
		TransactionId xidStopLimit = ShmemVariableCache->xidStopLimit;
		TransactionId xidWrapLimit = ShmemVariableCache->xidWrapLimit;
		Oid			oldest_datoid = ShmemVariableCache->oldestXidDB;

		LWLockRelease(XidGenLock);

		/*
		 * To avoid swamping the postmaster with signals, we issue the autovac
		 * request only once per 64K transaction starts.  This still gives
		 * plenty of chances before we get into real trouble.
		 */
		if (IsUnderPostmaster && (xid % 65536) == 0)
			SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER);

		if (IsUnderPostmaster &&
			TransactionIdFollowsOrEquals(xid, xidStopLimit))
		{
#ifdef PGXC
			/*
			 * Allow auto-vacuum to carry-on, so that it gets a chance to correct
			 * the xid-wrap-limits w.r.t to gxid fetched from GTM.
			 */
			if (!IsAutoVacuumLauncherProcess() && !IsAutoVacuumWorkerProcess())
			{
				char  *oldest_datname = (OidIsValid(MyDatabaseId) ?
			           get_database_name(oldest_datoid): NULL);
#else
			char	   *oldest_datname = get_database_name(oldest_datoid);
#endif
				/* complain even if that DB has disappeared */
				if (oldest_datname)
					ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("database is not accepting commands to avoid wraparound data loss in database \"%s\"",
								oldest_datname),
						 errhint("Stop the postmaster and use a standalone backend to vacuum that database.\n"
								 "You might also need to commit or roll back old prepared transactions.")));
				else
					ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("database is not accepting commands to avoid wraparound data loss in database with OID %u",
								oldest_datoid),
						 errhint("Stop the postmaster and use a standalone backend to vacuum that database.\n"
								 "You might also need to commit or roll back old prepared transactions.")));
#ifdef PGXC
			}
#endif
		}
		else if (TransactionIdFollowsOrEquals(xid, xidWarnLimit))
		{
			char  *oldest_datname = (OidIsValid(MyDatabaseId) ?
			           get_database_name(oldest_datoid): NULL);

			/* complain even if that DB has disappeared */
			if (oldest_datname)
				ereport(WARNING,
						(errmsg("database \"%s\" must be vacuumed within %u transactions",
								oldest_datname,
								xidWrapLimit - xid),
						 errhint("To avoid a database shutdown, execute a database-wide VACUUM in that database.\n"
								 "You might also need to commit or roll back old prepared transactions.")));
			else
				ereport(WARNING,
						(errmsg("database with OID %u must be vacuumed within %u transactions",
								oldest_datoid,
								xidWrapLimit - xid),
						 errhint("To avoid a database shutdown, execute a database-wide VACUUM in that database.\n"
								 "You might also need to commit or roll back old prepared transactions.")));
		}

		/* Re-acquire lock and start over */
		LWLockAcquire(XidGenLock, LW_EXCLUSIVE);

#ifndef PGXC
		/*
		 * In the case of Postgres-XC, transaction ID is managed globally at GTM level,
		 * so updating the GXID here based on the cache that might have been changed
		 * by another session when checking for wraparound errors at this local node
		 * level breaks transaction ID consistency of cluster.
		 */
		xid = ShmemVariableCache->nextXid;
#endif
	}
	/*
	 * If we are allocating the first XID of a new page of the commit log,
	 * zero out that commit-log page before returning. We must do this while
	 * holding XidGenLock, else another xact could acquire and commit a later
	 * XID before we zero the page.  Fortunately, a page of the commit log
	 * holds 32K or more transactions, so we don't have to do this very often.
	 *
	 * Extend pg_subtrans too.
	 */
	ExtendCLOG(xid);
	ExtendSUBTRANS(xid);

	/*
	 * Now advance the nextXid counter.  This must not happen until after we
	 * have successfully completed ExtendCLOG() --- if that routine fails, we
	 * want the next incoming transaction to try it again.	We cannot assign
	 * more XIDs until there is CLOG space for them.
	 */
#ifdef PGXC
	/*
	 * But first bring nextXid in sync with global xid. Actually we get xid
	 * externally anyway, so it should not be needed to update nextXid in
	 * theory, but it is required to keep nextXid close to the gxid
	 * especially when vacuumfreeze is run using a standalone backend.
	 */
	if (increment_xid || !IsPostmasterEnvironment)
	{
		ShmemVariableCache->nextXid = xid;
		TransactionIdAdvance(ShmemVariableCache->nextXid);
	}
#else
	TransactionIdAdvance(ShmemVariableCache->nextXid);
#endif

	/*
	 * We must store the new XID into the shared ProcArray before releasing
	 * XidGenLock.	This ensures that every active XID older than
	 * latestCompletedXid is present in the ProcArray, which is essential for
	 * correct OldestXmin tracking; see src/backend/access/transam/README.
	 *
	 * XXX by storing xid into MyPgXact without acquiring ProcArrayLock, we
	 * are relying on fetch/store of an xid to be atomic, else other backends
	 * might see a partially-set xid here.	But holding both locks at once
	 * would be a nasty concurrency hit.  So for now, assume atomicity.
	 *
	 * Note that readers of PGXACT xid fields should be careful to fetch the
	 * value only once, rather than assume they can read a value multiple
	 * times and get the same answer each time.
	 *
	 * The same comments apply to the subxact xid count and overflow fields.
	 *
	 * A solution to the atomic-store problem would be to give each PGXACT its
	 * own spinlock used only for fetching/storing that PGXACT's xid and
	 * related fields.
	 *
	 * If there's no room to fit a subtransaction XID into PGPROC, set the
	 * cache-overflowed flag instead.  This forces readers to look in
	 * pg_subtrans to map subtransaction XIDs up to top-level XIDs. There is a
	 * race-condition window, in that the new XID will not appear as running
	 * until its parent link has been placed into pg_subtrans. However, that
	 * will happen before anyone could possibly have a reason to inquire about
	 * the status of the XID, so it seems OK.  (Snapshots taken during this
	 * window *will* include the parent XID, so they will deliver the correct
	 * answer later on when someone does have a reason to inquire.)
	 */
	{
		/*
		 * Use volatile pointer to prevent code rearrangement; other backends
		 * could be examining my subxids info concurrently, and we don't want
		 * them to see an invalid intermediate state, such as incrementing
		 * nxids before filling the array entry.  Note we are assuming that
		 * TransactionId and int fetch/store are atomic.
		 */
		volatile PGPROC *myproc = MyProc;
		volatile PGXACT *mypgxact = MyPgXact;

		if (!isSubXact)
			mypgxact->xid = xid;
		else
		{
			int			nxids = mypgxact->nxids;

			if (nxids < PGPROC_MAX_CACHED_SUBXIDS)
			{
				myproc->subxids.xids[nxids] = xid;
				mypgxact->nxids = nxids + 1;
			}
			else
				mypgxact->overflowed = true;
		}
	}

	LWLockRelease(XidGenLock);
	return xid;
}

/*
 * Read nextXid but don't allocate it.
 */
TransactionId
ReadNewTransactionId(void)
{
	TransactionId xid;

	LWLockAcquire(XidGenLock, LW_SHARED);
	xid = ShmemVariableCache->nextXid;
	LWLockRelease(XidGenLock);

	return xid;
}

/*
 * Determine the last safe XID to allocate given the currently oldest
 * datfrozenxid (ie, the oldest XID that might exist in any database
 * of our cluster), and the OID of the (or a) database with that value.
 */
void
SetTransactionIdLimit(TransactionId oldest_datfrozenxid, Oid oldest_datoid)
{
	TransactionId xidVacLimit;
	TransactionId xidWarnLimit;
	TransactionId xidStopLimit;
	TransactionId xidWrapLimit;
	TransactionId curXid;

	Assert(TransactionIdIsNormal(oldest_datfrozenxid));

	/*
	 * The place where we actually get into deep trouble is halfway around
	 * from the oldest potentially-existing XID.  (This calculation is
	 * probably off by one or two counts, because the special XIDs reduce the
	 * size of the loop a little bit.  But we throw in plenty of slop below,
	 * so it doesn't matter.)
	 */
	xidWrapLimit = oldest_datfrozenxid + (MaxTransactionId >> 1);
	if (xidWrapLimit < FirstNormalTransactionId)
		xidWrapLimit += FirstNormalTransactionId;

	/*
	 * We'll refuse to continue assigning XIDs in interactive mode once we get
	 * within 1M transactions of data loss.  This leaves lots of room for the
	 * DBA to fool around fixing things in a standalone backend, while not
	 * being significant compared to total XID space. (Note that since
	 * vacuuming requires one transaction per table cleaned, we had better be
	 * sure there's lots of XIDs left...)
	 */
	xidStopLimit = xidWrapLimit - 1000000;
	if (xidStopLimit < FirstNormalTransactionId)
		xidStopLimit -= FirstNormalTransactionId;

	/*
	 * We'll start complaining loudly when we get within 10M transactions of
	 * the stop point.	This is kind of arbitrary, but if you let your gas
	 * gauge get down to 1% of full, would you be looking for the next gas
	 * station?  We need to be fairly liberal about this number because there
	 * are lots of scenarios where most transactions are done by automatic
	 * clients that won't pay attention to warnings. (No, we're not gonna make
	 * this configurable.  If you know enough to configure it, you know enough
	 * to not get in this kind of trouble in the first place.)
	 */
	xidWarnLimit = xidStopLimit - 10000000;
	if (xidWarnLimit < FirstNormalTransactionId)
		xidWarnLimit -= FirstNormalTransactionId;

	/*
	 * We'll start trying to force autovacuums when oldest_datfrozenxid gets
	 * to be more than autovacuum_freeze_max_age transactions old.
	 *
	 * Note: guc.c ensures that autovacuum_freeze_max_age is in a sane range,
	 * so that xidVacLimit will be well before xidWarnLimit.
	 *
	 * Note: autovacuum_freeze_max_age is a PGC_POSTMASTER parameter so that
	 * we don't have to worry about dealing with on-the-fly changes in its
	 * value.  It doesn't look practical to update shared state from a GUC
	 * assign hook (too many processes would try to execute the hook,
	 * resulting in race conditions as well as crashes of those not connected
	 * to shared memory).  Perhaps this can be improved someday.
	 */
	xidVacLimit = oldest_datfrozenxid + autovacuum_freeze_max_age;
	if (xidVacLimit < FirstNormalTransactionId)
		xidVacLimit += FirstNormalTransactionId;

	/* Grab lock for just long enough to set the new limit values */
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	ShmemVariableCache->oldestXid = oldest_datfrozenxid;
	ShmemVariableCache->xidVacLimit = xidVacLimit;
	ShmemVariableCache->xidWarnLimit = xidWarnLimit;
	ShmemVariableCache->xidStopLimit = xidStopLimit;
	ShmemVariableCache->xidWrapLimit = xidWrapLimit;
	ShmemVariableCache->oldestXidDB = oldest_datoid;
	curXid = ShmemVariableCache->nextXid;
	LWLockRelease(XidGenLock);

	/* Log the info */
	ereport(DEBUG1,
			(errmsg("transaction ID wrap limit is %u, limited by database with OID %u",
					xidWrapLimit, oldest_datoid)));

	/*
	 * If past the autovacuum force point, immediately signal an autovac
	 * request.  The reason for this is that autovac only processes one
	 * database per invocation.  Once it's finished cleaning up the oldest
	 * database, it'll call here, and we'll signal the postmaster to start
	 * another iteration immediately if there are still any old databases.
	 */
	if (TransactionIdFollowsOrEquals(curXid, xidVacLimit) &&
		IsUnderPostmaster && !InRecovery)
		SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER);

	/* Give an immediate warning if past the wrap warn point */
	if (TransactionIdFollowsOrEquals(curXid, xidWarnLimit) && !InRecovery)
	{
		char	   *oldest_datname;

		/*
		 * We can be called when not inside a transaction, for example during
		 * StartupXLOG().  In such a case we cannot do database access, so we
		 * must just report the oldest DB's OID.
		 *
		 * Note: it's also possible that get_database_name fails and returns
		 * NULL, for example because the database just got dropped.  We'll
		 * still warn, even though the warning might now be unnecessary.
		 */
		if (IsTransactionState())
			oldest_datname = get_database_name(oldest_datoid);
		else
			oldest_datname = NULL;

		if (oldest_datname)
			ereport(WARNING,
			(errmsg("database \"%s\" must be vacuumed within %u transactions",
					oldest_datname,
					xidWrapLimit - curXid),
			 errhint("To avoid a database shutdown, execute a database-wide VACUUM in that database.\n"
					 "You might also need to commit or roll back old prepared transactions.")));
		else
			ereport(WARNING,
					(errmsg("database with OID %u must be vacuumed within %u transactions",
							oldest_datoid,
							xidWrapLimit - curXid),
					 errhint("To avoid a database shutdown, execute a database-wide VACUUM in that database.\n"
							 "You might also need to commit or roll back old prepared transactions.")));
	}
}


/*
 * ForceTransactionIdLimitUpdate -- does the XID wrap-limit data need updating?
 *
 * We primarily check whether oldestXidDB is valid.  The cases we have in
 * mind are that that database was dropped, or the field was reset to zero
 * by pg_resetxlog.  In either case we should force recalculation of the
 * wrap limit.	Also do it if oldestXid is old enough to be forcing
 * autovacuums or other actions; this ensures we update our state as soon
 * as possible once extra overhead is being incurred.
 */
bool
ForceTransactionIdLimitUpdate(void)
{
	TransactionId nextXid;
	TransactionId xidVacLimit;
	TransactionId oldestXid;
	Oid			oldestXidDB;

	/* Locking is probably not really necessary, but let's be careful */
	LWLockAcquire(XidGenLock, LW_SHARED);
	nextXid = ShmemVariableCache->nextXid;
	xidVacLimit = ShmemVariableCache->xidVacLimit;
	oldestXid = ShmemVariableCache->oldestXid;
	oldestXidDB = ShmemVariableCache->oldestXidDB;
	LWLockRelease(XidGenLock);

	if (!TransactionIdIsNormal(oldestXid))
		return true;			/* shouldn't happen, but just in case */
	if (!TransactionIdIsValid(xidVacLimit))
		return true;			/* this shouldn't happen anymore either */
	if (TransactionIdFollowsOrEquals(nextXid, xidVacLimit))
		return true;			/* past VacLimit, don't delay updating */
	if (!SearchSysCacheExists1(DATABASEOID, ObjectIdGetDatum(oldestXidDB)))
		return true;			/* could happen, per comments above */
	return false;
}


/*
 * GetNewObjectId -- allocate a new OID
 *
 * OIDs are generated by a cluster-wide counter.  Since they are only 32 bits
 * wide, counter wraparound will occur eventually, and therefore it is unwise
 * to assume they are unique unless precautions are taken to make them so.
 * Hence, this routine should generally not be used directly.  The only
 * direct callers should be GetNewOid() and GetNewRelFileNode() in
 * catalog/catalog.c.
 */
Oid
GetNewObjectId(void)
{
	Oid			result;

	/* safety check, we should never get this far in a HS slave */
	if (RecoveryInProgress())
		elog(ERROR, "cannot assign OIDs during recovery");

	LWLockAcquire(OidGenLock, LW_EXCLUSIVE);

	/*
	 * Check for wraparound of the OID counter.  We *must* not return 0
	 * (InvalidOid); and as long as we have to check that, it seems a good
	 * idea to skip over everything below FirstNormalObjectId too. (This
	 * basically just avoids lots of collisions with bootstrap-assigned OIDs
	 * right after a wrap occurs, so as to avoid a possibly large number of
	 * iterations in GetNewOid.)  Note we are relying on unsigned comparison.
	 *
	 * During initdb, we start the OID generator at FirstBootstrapObjectId, so
	 * we only enforce wrapping to that point when in bootstrap or standalone
	 * mode.  The first time through this routine after normal postmaster
	 * start, the counter will be forced up to FirstNormalObjectId. This
	 * mechanism leaves the OIDs between FirstBootstrapObjectId and
	 * FirstNormalObjectId available for automatic assignment during initdb,
	 * while ensuring they will never conflict with user-assigned OIDs.
	 */
	if (ShmemVariableCache->nextOid < ((Oid) FirstNormalObjectId))
	{
		if (IsPostmasterEnvironment)
		{
			/* wraparound in normal environment */
			ShmemVariableCache->nextOid = FirstNormalObjectId;
			ShmemVariableCache->oidCount = 0;
		}
		else
		{
			/* we may be bootstrapping, so don't enforce the full range */
			if (ShmemVariableCache->nextOid < ((Oid) FirstBootstrapObjectId))
			{
				/* wraparound in standalone environment? */
				ShmemVariableCache->nextOid = FirstBootstrapObjectId;
				ShmemVariableCache->oidCount = 0;
			}
		}
	}

	/* If we run out of logged for use oids then we must log more */
	if (ShmemVariableCache->oidCount == 0)
	{
		XLogPutNextOid(ShmemVariableCache->nextOid + VAR_OID_PREFETCH);
		ShmemVariableCache->oidCount = VAR_OID_PREFETCH;
	}

	result = ShmemVariableCache->nextOid;

	(ShmemVariableCache->nextOid)++;
	(ShmemVariableCache->oidCount)--;

	LWLockRelease(OidGenLock);

	return result;
}
