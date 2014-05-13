/*-------------------------------------------------------------------------
 *
 * sequence.c
 *	  PostgreSQL sequences support code.
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/sequence.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/transam.h"
#include "access/xlogutils.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/smgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "commands/dbcommands.h"

#ifdef PGXC
#include "pgxc/pgxc.h"
/* PGXC_COORD */
#include "access/gtm.h"
#include "utils/memutils.h"
#endif

/*
 * We don't want to log each fetching of a value from a sequence,
 * so we pre-log a few fetches in advance. In the event of
 * crash we can lose (skip over) as many values as we pre-logged.
 */
#define SEQ_LOG_VALS	32

/*
 * The "special area" of a sequence's buffer page looks like this.
 */
#define SEQ_MAGIC	  0x1717

typedef struct sequence_magic
{
	uint32		magic;
} sequence_magic;

/*
 * We store a SeqTable item for every sequence we have touched in the current
 * session.  This is needed to hold onto nextval/currval state.  (We can't
 * rely on the relcache, since it's only, well, a cache, and may decide to
 * discard entries.)
 *
 * XXX We use linear search to find pre-existing SeqTable entries.	This is
 * good when only a small number of sequences are touched in a session, but
 * would suck with many different sequences.  Perhaps use a hashtable someday.
 */
typedef struct SeqTableData
{
	struct SeqTableData *next;	/* link to next SeqTable object */
	Oid			relid;			/* pg_class OID of this sequence */
	Oid			filenode;		/* last seen relfilenode of this sequence */
	LocalTransactionId lxid;	/* xact in which we last did a seq op */
	bool		last_valid;		/* do we have a valid "last" value? */
	int64		last;			/* value last returned by nextval */
	int64		cached;			/* last value already cached for nextval */
	/* if last != cached, we have not used up all the cached values */
	int64		increment;		/* copy of sequence's increment field */
	/* note that increment is zero until we first do read_seq_tuple() */
} SeqTableData;

typedef SeqTableData *SeqTable;

static SeqTable seqtab = NULL;	/* Head of list of SeqTable items */

#ifdef PGXC
/*
 * Arguments for callback of sequence drop on GTM
 */
typedef struct drop_sequence_callback_arg
{
	char *seqname;
	GTM_SequenceDropType type;
	GTM_SequenceKeyType key;
} drop_sequence_callback_arg;

/*
 * Arguments for callback of sequence rename on GTM
 */
typedef struct rename_sequence_callback_arg
{
	char *newseqname;
	char *oldseqname;
} rename_sequence_callback_arg;
#endif

/*
 * last_used_seq is updated by nextval() to point to the last used
 * sequence.
 */
static SeqTableData *last_used_seq = NULL;

static void fill_seq_with_data(Relation rel, HeapTuple tuple);
static int64 nextval_internal(Oid relid);
static Relation open_share_lock(SeqTable seq);
static void init_sequence(Oid relid, SeqTable *p_elm, Relation *p_rel);
static Form_pg_sequence read_seq_tuple(SeqTable elm, Relation rel,
			   Buffer *buf, HeapTuple seqtuple);
#ifdef PGXC
static void init_params(List *options, bool isInit,
						Form_pg_sequence new, List **owned_by, bool *is_restart);
#else
static void init_params(List *options, bool isInit,
						Form_pg_sequence new, List **owned_by);
#endif
static void do_setval(Oid relid, int64 next, bool iscalled);
static void process_owned_by(Relation seqrel, List *owned_by);


/*
 * DefineSequence
 *				Creates a new sequence relation
 */
Oid
DefineSequence(CreateSeqStmt *seq)
{
	FormData_pg_sequence new;
	List	   *owned_by;
	CreateStmt *stmt = makeNode(CreateStmt);
	Oid			seqoid;
	Relation	rel;
	HeapTuple	tuple;
	TupleDesc	tupDesc;
	Datum		value[SEQ_COL_LASTCOL];
	bool		null[SEQ_COL_LASTCOL];
	int			i;
	NameData	name;
#ifdef PGXC /* PGXC_COORD */
	GTM_Sequence	start_value = 1;
	GTM_Sequence	min_value = 1;
	GTM_Sequence	max_value = InvalidSequenceValue;
	GTM_Sequence	increment = 1;
	bool		cycle = false;
	bool		is_restart;
#endif

	/* Unlogged sequences are not implemented -- not clear if useful. */
	if (seq->sequence->relpersistence == RELPERSISTENCE_UNLOGGED)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("unlogged sequences are not supported")));

	/* Check and set all option values */
#ifdef PGXC
	init_params(seq->options, true, &new, &owned_by, &is_restart);
#else
	init_params(seq->options, true, &new, &owned_by);
#endif

	/*
	 * Create relation (and fill value[] and null[] for the tuple)
	 */
	stmt->tableElts = NIL;
	for (i = SEQ_COL_FIRSTCOL; i <= SEQ_COL_LASTCOL; i++)
	{
		ColumnDef  *coldef = makeNode(ColumnDef);

		coldef->inhcount = 0;
		coldef->is_local = true;
		coldef->is_not_null = true;
		coldef->is_from_type = false;
		coldef->storage = 0;
		coldef->raw_default = NULL;
		coldef->cooked_default = NULL;
		coldef->collClause = NULL;
		coldef->collOid = InvalidOid;
		coldef->constraints = NIL;

		null[i - 1] = false;

		switch (i)
		{
			case SEQ_COL_NAME:
				coldef->typeName = makeTypeNameFromOid(NAMEOID, -1);
				coldef->colname = "sequence_name";
				namestrcpy(&name, seq->sequence->relname);
				value[i - 1] = NameGetDatum(&name);
				break;
			case SEQ_COL_LASTVAL:
				coldef->typeName = makeTypeNameFromOid(INT8OID, -1);
				coldef->colname = "last_value";
				value[i - 1] = Int64GetDatumFast(new.last_value);
				break;
			case SEQ_COL_STARTVAL:
				coldef->typeName = makeTypeNameFromOid(INT8OID, -1);
				coldef->colname = "start_value";
				value[i - 1] = Int64GetDatumFast(new.start_value);
#ifdef PGXC /* PGXC_COORD */
				start_value = new.start_value;
#endif
				break;
			case SEQ_COL_INCBY:
				coldef->typeName = makeTypeNameFromOid(INT8OID, -1);
				coldef->colname = "increment_by";
				value[i - 1] = Int64GetDatumFast(new.increment_by);
#ifdef PGXC /* PGXC_COORD */
				increment = new.increment_by;
#endif
				break;
			case SEQ_COL_MAXVALUE:
				coldef->typeName = makeTypeNameFromOid(INT8OID, -1);
				coldef->colname = "max_value";
				value[i - 1] = Int64GetDatumFast(new.max_value);
#ifdef PGXC /* PGXC_COORD */
				max_value = new.max_value;
#endif
				break;
			case SEQ_COL_MINVALUE:
				coldef->typeName = makeTypeNameFromOid(INT8OID, -1);
				coldef->colname = "min_value";
				value[i - 1] = Int64GetDatumFast(new.min_value);
#ifdef PGXC /* PGXC_COORD */
				min_value = new.min_value;
#endif
				break;
			case SEQ_COL_CACHE:
				coldef->typeName = makeTypeNameFromOid(INT8OID, -1);
				coldef->colname = "cache_value";
				value[i - 1] = Int64GetDatumFast(new.cache_value);
				break;
			case SEQ_COL_LOG:
				coldef->typeName = makeTypeNameFromOid(INT8OID, -1);
				coldef->colname = "log_cnt";
				value[i - 1] = Int64GetDatum((int64) 0);
				break;
			case SEQ_COL_CYCLE:
				coldef->typeName = makeTypeNameFromOid(BOOLOID, -1);
				coldef->colname = "is_cycled";
				value[i - 1] = BoolGetDatum(new.is_cycled);
#ifdef PGXC  /* PGXC_COORD */
				cycle = new.is_cycled;
#endif
				break;
			case SEQ_COL_CALLED:
				coldef->typeName = makeTypeNameFromOid(BOOLOID, -1);
				coldef->colname = "is_called";
				value[i - 1] = BoolGetDatum(false);
				break;
		}
		stmt->tableElts = lappend(stmt->tableElts, coldef);
	}

	stmt->relation = seq->sequence;
	stmt->inhRelations = NIL;
	stmt->constraints = NIL;
	stmt->options = NIL;
	stmt->oncommit = ONCOMMIT_NOOP;
	stmt->tablespacename = NULL;
	stmt->if_not_exists = false;

	seqoid = DefineRelation(stmt, RELKIND_SEQUENCE, seq->ownerId);
	Assert(seqoid != InvalidOid);

	rel = heap_open(seqoid, AccessExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	/* now initialize the sequence's data */
	tuple = heap_form_tuple(tupDesc, value, null);
	fill_seq_with_data(rel, tuple);

	/* process OWNED BY if given */
	if (owned_by)
		process_owned_by(rel, owned_by);

	heap_close(rel, NoLock);

#ifdef PGXC  /* PGXC_COORD */
	/*
	 * Remote Coordinator is in charge of creating sequence in GTM.
	 * If sequence is temporary, it is not necessary to create it on GTM.
	 */
	if (IS_PGXC_COORDINATOR &&
		!IsConnFromCoord() &&
		(seq->sequence->relpersistence == RELPERSISTENCE_PERMANENT ||
		 seq->sequence->relpersistence == RELPERSISTENCE_UNLOGGED))
	{
		char *seqname = GetGlobalSeqName(rel, NULL, NULL);

		/* We also need to create it on the GTM */
		if (CreateSequenceGTM(seqname,
							  increment,
							  min_value,
							  max_value,
				start_value, cycle) < 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("GTM error, could not create sequence")));
		}

		/* Define a callback to drop sequence on GTM in case transaction fails  */
		register_sequence_cb(seqname, GTM_SEQ_FULL_NAME, GTM_CREATE_SEQ);

		pfree(seqname);
	}
#endif
	return seqoid;
}

/*
 * Reset a sequence to its initial value.
 *
 * The change is made transactionally, so that on failure of the current
 * transaction, the sequence will be restored to its previous state.
 * We do that by creating a whole new relfilenode for the sequence; so this
 * works much like the rewriting forms of ALTER TABLE.
 *
 * Caller is assumed to have acquired AccessExclusiveLock on the sequence,
 * which must not be released until end of transaction.  Caller is also
 * responsible for permissions checking.
 */
void
ResetSequence(Oid seq_relid)
{
	Relation	seq_rel;
	SeqTable	elm;
	Form_pg_sequence seq;
	Buffer		buf;
	HeapTupleData seqtuple;
	HeapTuple	tuple;

	/*
	 * Read the old sequence.  This does a bit more work than really
	 * necessary, but it's simple, and we do want to double-check that it's
	 * indeed a sequence.
	 */
	init_sequence(seq_relid, &elm, &seq_rel);
	(void) read_seq_tuple(elm, seq_rel, &buf, &seqtuple);

	/*
	 * Copy the existing sequence tuple.
	 */
	tuple = heap_copytuple(&seqtuple);

	/* Now we're done with the old page */
	UnlockReleaseBuffer(buf);

	/*
	 * Modify the copied tuple to execute the restart (compare the RESTART
	 * action in AlterSequence)
	 */
	seq = (Form_pg_sequence) GETSTRUCT(tuple);
	seq->last_value = seq->start_value;
	seq->is_called = false;
	seq->log_cnt = 0;

	/*
	 * Create a new storage file for the sequence.	We want to keep the
	 * sequence's relfrozenxid at 0, since it won't contain any unfrozen XIDs.
	 * Same with relminmxid, since a sequence will never contain multixacts.
	 */
	RelationSetNewRelfilenode(seq_rel, InvalidTransactionId,
							  InvalidMultiXactId);

	/*
	 * Insert the modified tuple into the new storage file.
	 */
	fill_seq_with_data(seq_rel, tuple);

	/* Clear local cache so that we don't think we have cached numbers */
	/* Note that we do not change the currval() state */
	elm->cached = elm->last;

	relation_close(seq_rel, NoLock);
}

/*
 * Initialize a sequence's relation with the specified tuple as content
 */
static void
fill_seq_with_data(Relation rel, HeapTuple tuple)
{
	Buffer		buf;
	Page		page;
	sequence_magic *sm;

	/* Initialize first page of relation with special magic number */

	buf = ReadBuffer(rel, P_NEW);
	Assert(BufferGetBlockNumber(buf) == 0);

	page = BufferGetPage(buf);

	PageInit(page, BufferGetPageSize(buf), sizeof(sequence_magic));
	sm = (sequence_magic *) PageGetSpecialPointer(page);
	sm->magic = SEQ_MAGIC;

	/* hack: ensure heap_insert will insert on the just-created page */
	RelationSetTargetBlock(rel, 0);

	/* Now insert sequence tuple */
	simple_heap_insert(rel, tuple);

	Assert(ItemPointerGetOffsetNumber(&(tuple->t_self)) == FirstOffsetNumber);

	/*
	 * Two special hacks here:
	 *
	 * 1. Since VACUUM does not process sequences, we have to force the tuple
	 * to have xmin = FrozenTransactionId now.	Otherwise it would become
	 * invisible to SELECTs after 2G transactions.	It is okay to do this
	 * because if the current transaction aborts, no other xact will ever
	 * examine the sequence tuple anyway.
	 *
	 * 2. Even though heap_insert emitted a WAL log record, we have to emit an
	 * XLOG_SEQ_LOG record too, since (a) the heap_insert record will not have
	 * the right xmin, and (b) REDO of the heap_insert record would re-init
	 * page and sequence magic number would be lost.  This means two log
	 * records instead of one :-(
	 */
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	START_CRIT_SECTION();

	{
		/*
		 * Note that the "tuple" structure is still just a local tuple record
		 * created by heap_form_tuple; its t_data pointer doesn't point at the
		 * disk buffer.  To scribble on the disk buffer we need to fetch the
		 * item pointer.  But do the same to the local tuple, since that will
		 * be the source for the WAL log record, below.
		 */
		ItemId		itemId;
		Item		item;

		itemId = PageGetItemId((Page) page, FirstOffsetNumber);
		item = PageGetItem((Page) page, itemId);

		HeapTupleHeaderSetXmin((HeapTupleHeader) item, FrozenTransactionId);
		((HeapTupleHeader) item)->t_infomask |= HEAP_XMIN_COMMITTED;

		HeapTupleHeaderSetXmin(tuple->t_data, FrozenTransactionId);
		tuple->t_data->t_infomask |= HEAP_XMIN_COMMITTED;
	}

	MarkBufferDirty(buf);

	/* XLOG stuff */
	if (RelationNeedsWAL(rel))
	{
		xl_seq_rec	xlrec;
		XLogRecPtr	recptr;
		XLogRecData rdata[2];

		xlrec.node = rel->rd_node;
		rdata[0].data = (char *) &xlrec;
		rdata[0].len = sizeof(xl_seq_rec);
		rdata[0].buffer = InvalidBuffer;
		rdata[0].next = &(rdata[1]);

		rdata[1].data = (char *) tuple->t_data;
		rdata[1].len = tuple->t_len;
		rdata[1].buffer = InvalidBuffer;
		rdata[1].next = NULL;

		recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG, rdata);

		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buf);
}

/*
 * AlterSequence
 *
 * Modify the definition of a sequence relation
 */
Oid
AlterSequence(AlterSeqStmt *stmt)
{
	Oid			relid;
	SeqTable	elm;
	Relation	seqrel;
	Buffer		buf;
	HeapTupleData seqtuple;
	Form_pg_sequence seq;
	FormData_pg_sequence new;
	List	   *owned_by;
#ifdef PGXC
	GTM_Sequence	start_value;
	GTM_Sequence	last_value;
	GTM_Sequence	min_value;
	GTM_Sequence	max_value;
	GTM_Sequence	increment;
	bool			cycle;
	bool			is_restart;
#endif

	/* Open and lock sequence. */
	relid = RangeVarGetRelid(stmt->sequence, AccessShareLock, stmt->missing_ok);
	if (relid == InvalidOid)
	{
		ereport(NOTICE,
				(errmsg("relation \"%s\" does not exist, skipping",
						stmt->sequence->relname)));
		return InvalidOid;
	}

	init_sequence(relid, &elm, &seqrel);

	/* allow ALTER to sequence owner only */
	if (!pg_class_ownercheck(relid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS,
					   stmt->sequence->relname);

	/* lock page' buffer and read tuple into new sequence structure */
	seq = read_seq_tuple(elm, seqrel, &buf, &seqtuple);

	/* Copy old values of options into workspace */
	memcpy(&new, seq, sizeof(FormData_pg_sequence));

	/* Check and set new values */
#ifdef PGXC
	init_params(stmt->options, false, &new, &owned_by, &is_restart);
#else
	init_params(stmt->options, false, &new, &owned_by);
#endif

	/* Clear local cache so that we don't think we have cached numbers */
	/* Note that we do not change the currval() state */
	elm->cached = elm->last;

	/* Now okay to update the on-disk tuple */
	START_CRIT_SECTION();

	memcpy(seq, &new, sizeof(FormData_pg_sequence));

#ifdef PGXC
	increment = new.increment_by;
	min_value = new.min_value;
	max_value = new.max_value;
	start_value = new.start_value;
	last_value = new.last_value;
	cycle = new.is_cycled;
#endif

	MarkBufferDirty(buf);

	/* XLOG stuff */
	if (RelationNeedsWAL(seqrel))
	{
		xl_seq_rec	xlrec;
		XLogRecPtr	recptr;
		XLogRecData rdata[2];
		Page		page = BufferGetPage(buf);

		xlrec.node = seqrel->rd_node;
		rdata[0].data = (char *) &xlrec;
		rdata[0].len = sizeof(xl_seq_rec);
		rdata[0].buffer = InvalidBuffer;
		rdata[0].next = &(rdata[1]);

		rdata[1].data = (char *) seqtuple.t_data;
		rdata[1].len = seqtuple.t_len;
		rdata[1].buffer = InvalidBuffer;
		rdata[1].next = NULL;

		recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG, rdata);

		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buf);

	/* process OWNED BY if given */
	if (owned_by)
		process_owned_by(seqrel, owned_by);

	InvokeObjectPostAlterHook(RelationRelationId, relid, 0);

	relation_close(seqrel, NoLock);

#ifdef PGXC
	/*
	 * Remote Coordinator is in charge of create sequence in GTM
	 * If sequence is temporary, no need to go through GTM.
	 */
	if (IS_PGXC_COORDINATOR &&
		!IsConnFromCoord() &&
		seqrel->rd_backend != MyBackendId)
	{
		char *seqname = GetGlobalSeqName(seqrel, NULL, NULL);

		/* We also need to create it on the GTM */
		if (AlterSequenceGTM(seqname,
							 increment,
							 min_value,
							 max_value,
							 start_value,
							 last_value,
							 cycle,
							 is_restart) < 0)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("GTM error, could not alter sequence")));
		pfree(seqname);
	}
#endif
	return relid;
}


/*
 * Note: nextval with a text argument is no longer exported as a pg_proc
 * entry, but we keep it around to ease porting of C code that may have
 * called the function directly.
 */
Datum
nextval(PG_FUNCTION_ARGS)
{
	text	   *seqin = PG_GETARG_TEXT_P(0);
	RangeVar   *sequence;
	Oid			relid;

	sequence = makeRangeVarFromNameList(textToQualifiedNameList(seqin));

	/*
	 * XXX: This is not safe in the presence of concurrent DDL, but acquiring
	 * a lock here is more expensive than letting nextval_internal do it,
	 * since the latter maintains a cache that keeps us from hitting the lock
	 * manager more than once per transaction.	It's not clear whether the
	 * performance penalty is material in practice, but for now, we do it this
	 * way.
	 */
	relid = RangeVarGetRelid(sequence, NoLock, false);

	PG_RETURN_INT64(nextval_internal(relid));
}

Datum
nextval_oid(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);

	PG_RETURN_INT64(nextval_internal(relid));
}

static int64
nextval_internal(Oid relid)
{
	SeqTable	elm;
	Relation	seqrel;
	Buffer		buf;
	Page		page;
	HeapTupleData seqtuple;
	Form_pg_sequence seq;
	int64		incby,
				maxv,
				minv,
				cache,
				log,
				fetch,
				last;
	int64		result,
				next,
				rescnt = 0;
	bool		logit = false;
#ifdef PGXC
	bool		is_temp;
#endif

	/* open and AccessShareLock sequence */
	init_sequence(relid, &elm, &seqrel);

	if (pg_class_aclcheck(elm->relid, GetUserId(), ACL_USAGE) != ACLCHECK_OK &&
		pg_class_aclcheck(elm->relid, GetUserId(), ACL_UPDATE) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for sequence %s",
						RelationGetRelationName(seqrel))));

	/* read-only transactions may only modify temp sequences */
	if (!seqrel->rd_islocaltemp)
		PreventCommandIfReadOnly("nextval()");

#ifdef PGXC
	is_temp = seqrel->rd_backend == MyBackendId;
#endif

	if (elm->last != elm->cached)		/* some numbers were cached */
	{
		Assert(elm->last_valid);
		Assert(elm->increment != 0);
		elm->last += elm->increment;
		relation_close(seqrel, NoLock);
		last_used_seq = elm;
		return elm->last;
	}

	/* lock page' buffer and read tuple */
	seq = read_seq_tuple(elm, seqrel, &buf, &seqtuple);
	page = BufferGetPage(buf);

#ifdef PGXC  /* PGXC_COORD */
	if (IS_PGXC_COORDINATOR && !is_temp)
	{
		char *seqname = GetGlobalSeqName(seqrel, NULL, NULL);

		/*
		 * Above, we still use the page as a locking mechanism to handle
		 * concurrency
		 */
		result = (int64) GetNextValGTM(seqname);
		pfree(seqname);

		/* Update the on-disk data */
		seq->last_value = result; /* last fetched number */
		seq->is_called = true;

		/* save info in local cache */
		elm->last = result;			/* last returned number */
		elm->cached = result;		/* last fetched number */
		elm->last_valid = true;

		last_used_seq = elm;
	}
	else
	{
#endif
	last = next = result = seq->last_value;
	incby = seq->increment_by;
	maxv = seq->max_value;
	minv = seq->min_value;
#ifdef PGXC
	}
#endif
	fetch = cache = seq->cache_value;
	log = seq->log_cnt;

	if (!seq->is_called)
	{
		rescnt++;				/* return last_value if not is_called */
		fetch--;
	}

	/*
	 * Decide whether we should emit a WAL log record.	If so, force up the
	 * fetch count to grab SEQ_LOG_VALS more values than we actually need to
	 * cache.  (These will then be usable without logging.)
	 *
	 * If this is the first nextval after a checkpoint, we must force a new
	 * WAL record to be written anyway, else replay starting from the
	 * checkpoint would fail to advance the sequence past the logged values.
	 * In this case we may as well fetch extra values.
	 */
	if (log < fetch || !seq->is_called)
	{
		/* forced log to satisfy local demand for values */
		fetch = log = fetch + SEQ_LOG_VALS;
		logit = true;
	}
	else
	{
		XLogRecPtr	redoptr = GetRedoRecPtr();

		if (PageGetLSN(page) <= redoptr)
		{
			/* last update of seq was before checkpoint */
			fetch = log = fetch + SEQ_LOG_VALS;
			logit = true;
		}
	}

	while (fetch)				/* try to fetch cache [+ log ] numbers */
	{
		/*
		 * Check MAXVALUE for ascending sequences and MINVALUE for descending
		 * sequences
		 */
#ifdef PGXC
		/* Temporary sequences go through normal process */
		if (is_temp)
		{
#endif
		/* Result has been checked and received from GTM */
		if (incby > 0)
		{
			/* ascending sequence */
			if ((maxv >= 0 && next > maxv - incby) ||
				(maxv < 0 && next + incby > maxv))
			{
				if (rescnt > 0)
					break;		/* stop fetching */
				if (!seq->is_cycled)
				{
					char		buf[100];

					snprintf(buf, sizeof(buf), INT64_FORMAT, maxv);
					ereport(ERROR,
						  (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						   errmsg("nextval: reached maximum value of sequence \"%s\" (%s)",
								  RelationGetRelationName(seqrel), buf)));
				}
				next = minv;
			}
			else
				next += incby;
		}
		else
		{
			/* descending sequence */
			if ((minv < 0 && next < minv - incby) ||
				(minv >= 0 && next + incby < minv))
			{
				if (rescnt > 0)
					break;		/* stop fetching */
				if (!seq->is_cycled)
				{
					char		buf[100];

					snprintf(buf, sizeof(buf), INT64_FORMAT, minv);
					ereport(ERROR,
						  (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						   errmsg("nextval: reached minimum value of sequence \"%s\" (%s)",
								  RelationGetRelationName(seqrel), buf)));
				}
				next = maxv;
			}
			else
				next += incby;
		}
#ifdef PGXC
		}
#endif
		fetch--;
		if (rescnt < cache)
		{
			log--;
			rescnt++;
#ifdef PGXC
			/* Temporary sequences can go through normal process */
			if (is_temp)
			{
#endif
			/*
			 * This part is not taken into account,
			 * result has been received from GTM
			 */
			last = next;
			if (rescnt == 1)	/* if it's first result - */
				result = next;	/* it's what to return */
#ifdef PGXC
			}
#endif
		}
	}

	log -= fetch;				/* adjust for any unfetched numbers */
	Assert(log >= 0);

#ifdef PGXC
	/* Temporary sequences go through normal process */
	if (is_temp)
	{
#endif
	/* Result has been received from GTM */
	/* save info in local cache */
	elm->last = result;			/* last returned number */
	elm->cached = last;			/* last fetched number */
	elm->last_valid = true;

	last_used_seq = elm;

	/* ready to change the on-disk (or really, in-buffer) tuple */
	START_CRIT_SECTION();

	/*
	 * We must mark the buffer dirty before doing XLogInsert(); see notes in
	 * SyncOneBuffer().  However, we don't apply the desired changes just yet.
	 * This looks like a violation of the buffer update protocol, but it is in
	 * fact safe because we hold exclusive lock on the buffer.	Any other
	 * process, including a checkpoint, that tries to examine the buffer
	 * contents will block until we release the lock, and then will see the
	 * final state that we install below.
	 */
	MarkBufferDirty(buf);

	/* XLOG stuff */
	if (logit && RelationNeedsWAL(seqrel))
	{
		xl_seq_rec	xlrec;
		XLogRecPtr	recptr;
		XLogRecData rdata[2];

		/*
		 * We don't log the current state of the tuple, but rather the state
		 * as it would appear after "log" more fetches.  This lets us skip
		 * that many future WAL records, at the cost that we lose those
		 * sequence values if we crash.
		 */

		/* set values that will be saved in xlog */
		seq->last_value = next;
		seq->is_called = true;
		seq->log_cnt = 0;

		xlrec.node = seqrel->rd_node;
		rdata[0].data = (char *) &xlrec;
		rdata[0].len = sizeof(xl_seq_rec);
		rdata[0].buffer = InvalidBuffer;
		rdata[0].next = &(rdata[1]);

		rdata[1].data = (char *) seqtuple.t_data;
		rdata[1].len = seqtuple.t_len;
		rdata[1].buffer = InvalidBuffer;
		rdata[1].next = NULL;

		recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG, rdata);

		PageSetLSN(page, recptr);
	}

	/* Now update sequence tuple to the intended final state */
	seq->last_value = last;		/* last fetched number */
	seq->is_called = true;
	seq->log_cnt = log;			/* how much is logged */

	END_CRIT_SECTION();

#ifdef PGXC
	}
	else
	{
		seq->log_cnt = log;
	}
#endif
	UnlockReleaseBuffer(buf);

	relation_close(seqrel, NoLock);

	return result;
}

Datum
currval_oid(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int64		result;
	SeqTable	elm;
	Relation	seqrel;

	/* open and AccessShareLock sequence */
	init_sequence(relid, &elm, &seqrel);

	if (pg_class_aclcheck(elm->relid, GetUserId(), ACL_SELECT) != ACLCHECK_OK &&
		pg_class_aclcheck(elm->relid, GetUserId(), ACL_USAGE) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for sequence %s",
						RelationGetRelationName(seqrel))));

	if (!elm->last_valid)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("currval of sequence \"%s\" is not yet defined in this session",
						RelationGetRelationName(seqrel))));

	result = elm->last;
	relation_close(seqrel, NoLock);

	PG_RETURN_INT64(result);
}

Datum
lastval(PG_FUNCTION_ARGS)
{
	Relation	seqrel;
	int64		result;

	if (last_used_seq == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("lastval is not yet defined in this session")));

	/* Someone may have dropped the sequence since the last nextval() */
	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(last_used_seq->relid)))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("lastval is not yet defined in this session")));

	seqrel = open_share_lock(last_used_seq);

	/* nextval() must have already been called for this sequence */
	Assert(last_used_seq->last_valid);

	if (pg_class_aclcheck(last_used_seq->relid, GetUserId(), ACL_SELECT) != ACLCHECK_OK &&
		pg_class_aclcheck(last_used_seq->relid, GetUserId(), ACL_USAGE) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for sequence %s",
						RelationGetRelationName(seqrel))));

	result = last_used_seq->last;
	relation_close(seqrel, NoLock);

	PG_RETURN_INT64(result);
}

/*
 * Main internal procedure that handles 2 & 3 arg forms of SETVAL.
 *
 * Note that the 3 arg version (which sets the is_called flag) is
 * only for use in pg_dump, and setting the is_called flag may not
 * work if multiple users are attached to the database and referencing
 * the sequence (unlikely if pg_dump is restoring it).
 *
 * It is necessary to have the 3 arg version so that pg_dump can
 * restore the state of a sequence exactly during data-only restores -
 * it is the only way to clear the is_called flag in an existing
 * sequence.
 */
static void
do_setval(Oid relid, int64 next, bool iscalled)
{
	SeqTable	elm;
	Relation	seqrel;
	Buffer		buf;
	HeapTupleData seqtuple;
	Form_pg_sequence seq;
#ifdef PGXC
	bool		is_temp;
#endif

	/* open and AccessShareLock sequence */
	init_sequence(relid, &elm, &seqrel);

	if (pg_class_aclcheck(elm->relid, GetUserId(), ACL_UPDATE) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for sequence %s",
						RelationGetRelationName(seqrel))));

	/* read-only transactions may only modify temp sequences */
	if (!seqrel->rd_islocaltemp)
		PreventCommandIfReadOnly("setval()");

#ifdef PGXC
	is_temp = seqrel->rd_backend == MyBackendId;
#endif

	/* lock page' buffer and read tuple */
	seq = read_seq_tuple(elm, seqrel, &buf, &seqtuple);

	if ((next < seq->min_value) || (next > seq->max_value))
	{
		char		bufv[100],
					bufm[100],
					bufx[100];

		snprintf(bufv, sizeof(bufv), INT64_FORMAT, next);
		snprintf(bufm, sizeof(bufm), INT64_FORMAT, seq->min_value);
		snprintf(bufx, sizeof(bufx), INT64_FORMAT, seq->max_value);
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("setval: value %s is out of bounds for sequence \"%s\" (%s..%s)",
						bufv, RelationGetRelationName(seqrel),
						bufm, bufx)));
	}

#ifdef PGXC
	if (IS_PGXC_COORDINATOR && !is_temp)
	{
		char *seqname = GetGlobalSeqName(seqrel, NULL, NULL);

		if (SetValGTM(seqname, next, iscalled) < 0)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("GTM error, could not obtain sequence value")));
		pfree(seqname);
		/* Update the on-disk data */
		seq->last_value = next; /* last fetched number */
		seq->is_called = iscalled;
		seq->log_cnt = (iscalled) ? 0 : 1;

		if (iscalled)
		{
			elm->last = next;		/* last returned number */
			elm->last_valid = true;
		}

		elm->cached = elm->last;
	}
	else
	{
#endif

	/* Set the currval() state only if iscalled = true */
	if (iscalled)
	{
		elm->last = next;		/* last returned number */
		elm->last_valid = true;
	}

	/* In any case, forget any future cached numbers */
	elm->cached = elm->last;

	/* ready to change the on-disk (or really, in-buffer) tuple */
	START_CRIT_SECTION();

	seq->last_value = next;		/* last fetched number */
	seq->is_called = iscalled;
	seq->log_cnt = 0;

	MarkBufferDirty(buf);

	/* XLOG stuff */
	if (RelationNeedsWAL(seqrel))
	{
		xl_seq_rec	xlrec;
		XLogRecPtr	recptr;
		XLogRecData rdata[2];
		Page		page = BufferGetPage(buf);

		xlrec.node = seqrel->rd_node;
		rdata[0].data = (char *) &xlrec;
		rdata[0].len = sizeof(xl_seq_rec);
		rdata[0].buffer = InvalidBuffer;
		rdata[0].next = &(rdata[1]);

		rdata[1].data = (char *) seqtuple.t_data;
		rdata[1].len = seqtuple.t_len;
		rdata[1].buffer = InvalidBuffer;
		rdata[1].next = NULL;

		recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG, rdata);

		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

#ifdef PGXC
	}
#endif

	UnlockReleaseBuffer(buf);

	relation_close(seqrel, NoLock);
}

/*
 * Implement the 2 arg setval procedure.
 * See do_setval for discussion.
 */
Datum
setval_oid(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int64		next = PG_GETARG_INT64(1);

	do_setval(relid, next, true);

	PG_RETURN_INT64(next);
}

/*
 * Implement the 3 arg setval procedure.
 * See do_setval for discussion.
 */
Datum
setval3_oid(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int64		next = PG_GETARG_INT64(1);
	bool		iscalled = PG_GETARG_BOOL(2);

	do_setval(relid, next, iscalled);

	PG_RETURN_INT64(next);
}


/*
 * Open the sequence and acquire AccessShareLock if needed
 *
 * If we haven't touched the sequence already in this transaction,
 * we need to acquire AccessShareLock.	We arrange for the lock to
 * be owned by the top transaction, so that we don't need to do it
 * more than once per xact.
 */
static Relation
open_share_lock(SeqTable seq)
{
	LocalTransactionId thislxid = MyProc->lxid;

	/* Get the lock if not already held in this xact */
	if (seq->lxid != thislxid)
	{
		ResourceOwner currentOwner;

		currentOwner = CurrentResourceOwner;
		PG_TRY();
		{
			CurrentResourceOwner = TopTransactionResourceOwner;
			LockRelationOid(seq->relid, AccessShareLock);
		}
		PG_CATCH();
		{
			/* Ensure CurrentResourceOwner is restored on error */
			CurrentResourceOwner = currentOwner;
			PG_RE_THROW();
		}
		PG_END_TRY();
		CurrentResourceOwner = currentOwner;

		/* Flag that we have a lock in the current xact */
		seq->lxid = thislxid;
	}

	/* We now know we have AccessShareLock, and can safely open the rel */
	return relation_open(seq->relid, NoLock);
}

/*
 * Given a relation OID, open and lock the sequence.  p_elm and p_rel are
 * output parameters.
 */
static void
init_sequence(Oid relid, SeqTable *p_elm, Relation *p_rel)
{
	SeqTable	elm;
	Relation	seqrel;

	/* Look to see if we already have a seqtable entry for relation */
	for (elm = seqtab; elm != NULL; elm = elm->next)
	{
		if (elm->relid == relid)
			break;
	}

	/*
	 * Allocate new seqtable entry if we didn't find one.
	 *
	 * NOTE: seqtable entries remain in the list for the life of a backend. If
	 * the sequence itself is deleted then the entry becomes wasted memory,
	 * but it's small enough that this should not matter.
	 */
	if (elm == NULL)
	{
		/*
		 * Time to make a new seqtable entry.  These entries live as long as
		 * the backend does, so we use plain malloc for them.
		 */
		elm = (SeqTable) malloc(sizeof(SeqTableData));
		if (elm == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
		elm->relid = relid;
		elm->filenode = InvalidOid;
		elm->lxid = InvalidLocalTransactionId;
		elm->last_valid = false;
		elm->last = elm->cached = elm->increment = 0;
		elm->next = seqtab;
		seqtab = elm;
	}

	/*
	 * Open the sequence relation.
	 */
	seqrel = open_share_lock(elm);

	if (seqrel->rd_rel->relkind != RELKIND_SEQUENCE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a sequence",
						RelationGetRelationName(seqrel))));

	/*
	 * If the sequence has been transactionally replaced since we last saw it,
	 * discard any cached-but-unissued values.	We do not touch the currval()
	 * state, however.
	 */
	if (seqrel->rd_rel->relfilenode != elm->filenode)
	{
		elm->filenode = seqrel->rd_rel->relfilenode;
		elm->cached = elm->last;
	}

	/* Return results */
	*p_elm = elm;
	*p_rel = seqrel;
}


/*
 * Given an opened sequence relation, lock the page buffer and find the tuple
 *
 * *buf receives the reference to the pinned-and-ex-locked buffer
 * *seqtuple receives the reference to the sequence tuple proper
 *		(this arg should point to a local variable of type HeapTupleData)
 *
 * Function's return value points to the data payload of the tuple
 */
static Form_pg_sequence
read_seq_tuple(SeqTable elm, Relation rel, Buffer *buf, HeapTuple seqtuple)
{
	Page		page;
	ItemId		lp;
	sequence_magic *sm;
	Form_pg_sequence seq;

	*buf = ReadBuffer(rel, 0);
	LockBuffer(*buf, BUFFER_LOCK_EXCLUSIVE);

	page = BufferGetPage(*buf);
	sm = (sequence_magic *) PageGetSpecialPointer(page);

	if (sm->magic != SEQ_MAGIC)
		elog(ERROR, "bad magic number in sequence \"%s\": %08X",
			 RelationGetRelationName(rel), sm->magic);

	lp = PageGetItemId(page, FirstOffsetNumber);
	Assert(ItemIdIsNormal(lp));

	/* Note we currently only bother to set these two fields of *seqtuple */
	seqtuple->t_data = (HeapTupleHeader) PageGetItem(page, lp);
	seqtuple->t_len = ItemIdGetLength(lp);

	/*
	 * Previous releases of Postgres neglected to prevent SELECT FOR UPDATE on
	 * a sequence, which would leave a non-frozen XID in the sequence tuple's
	 * xmax, which eventually leads to clog access failures or worse. If we
	 * see this has happened, clean up after it.  We treat this like a hint
	 * bit update, ie, don't bother to WAL-log it, since we can certainly do
	 * this again if the update gets lost.
	 */
	Assert(!(seqtuple->t_data->t_infomask & HEAP_XMAX_IS_MULTI));
	if (HeapTupleHeaderGetRawXmax(seqtuple->t_data) != InvalidTransactionId)
	{
		HeapTupleHeaderSetXmax(seqtuple->t_data, InvalidTransactionId);
		seqtuple->t_data->t_infomask &= ~HEAP_XMAX_COMMITTED;
		seqtuple->t_data->t_infomask |= HEAP_XMAX_INVALID;
		MarkBufferDirtyHint(*buf, true);
	}

	seq = (Form_pg_sequence) GETSTRUCT(seqtuple);

	/* this is a handy place to update our copy of the increment */
	elm->increment = seq->increment_by;

	return seq;
}

/*
 * init_params: process the options list of CREATE or ALTER SEQUENCE,
 * and store the values into appropriate fields of *new.  Also set
 * *owned_by to any OWNED BY option, or to NIL if there is none.
 *
 * If isInit is true, fill any unspecified options with default values;
 * otherwise, do not change existing options that aren't explicitly overridden.
 */
static void
#ifdef PGXC
init_params(List *options, bool isInit,
			Form_pg_sequence new, List **owned_by, bool *is_restart)
#else
init_params(List *options, bool isInit,
			Form_pg_sequence new, List **owned_by)
#endif
{
	DefElem    *start_value = NULL;
	DefElem    *restart_value = NULL;
	DefElem    *increment_by = NULL;
	DefElem    *max_value = NULL;
	DefElem    *min_value = NULL;
	DefElem    *cache_value = NULL;
	DefElem    *is_cycled = NULL;
	ListCell   *option;

#ifdef PGXC
	*is_restart = false;
#endif

	*owned_by = NIL;

	foreach(option, options)
	{
		DefElem    *defel = (DefElem *) lfirst(option);

		if (strcmp(defel->defname, "increment") == 0)
		{
			if (increment_by)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			increment_by = defel;
		}
		else if (strcmp(defel->defname, "start") == 0)
		{
			if (start_value)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			start_value = defel;
		}
		else if (strcmp(defel->defname, "restart") == 0)
		{
			if (restart_value)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			restart_value = defel;
		}
		else if (strcmp(defel->defname, "maxvalue") == 0)
		{
			if (max_value)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			max_value = defel;
		}
		else if (strcmp(defel->defname, "minvalue") == 0)
		{
			if (min_value)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			min_value = defel;
		}
		else if (strcmp(defel->defname, "cache") == 0)
		{
			if (cache_value)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			cache_value = defel;
		}
		else if (strcmp(defel->defname, "cycle") == 0)
		{
			if (is_cycled)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			is_cycled = defel;
		}
		else if (strcmp(defel->defname, "owned_by") == 0)
		{
			if (*owned_by)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			*owned_by = defGetQualifiedName(defel);
		}
		else
			elog(ERROR, "option \"%s\" not recognized",
				 defel->defname);
	}

	/*
	 * We must reset log_cnt when isInit or when changing any parameters that
	 * would affect future nextval allocations.
	 */
	if (isInit)
		new->log_cnt = 0;

	/* INCREMENT BY */
	if (increment_by != NULL)
	{
		new->increment_by = defGetInt64(increment_by);
		if (new->increment_by == 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("INCREMENT must not be zero")));
		new->log_cnt = 0;
	}
	else if (isInit)
		new->increment_by = 1;

	/* CYCLE */
	if (is_cycled != NULL)
	{
		new->is_cycled = intVal(is_cycled->arg);
		Assert(BoolIsValid(new->is_cycled));
		new->log_cnt = 0;
	}
	else if (isInit)
		new->is_cycled = false;

	/* MAXVALUE (null arg means NO MAXVALUE) */
	if (max_value != NULL && max_value->arg)
	{
		new->max_value = defGetInt64(max_value);
		new->log_cnt = 0;
	}
	else if (isInit || max_value != NULL)
	{
		if (new->increment_by > 0)
			new->max_value = SEQ_MAXVALUE;		/* ascending seq */
		else
			new->max_value = -1;	/* descending seq */
		new->log_cnt = 0;
	}

	/* MINVALUE (null arg means NO MINVALUE) */
	if (min_value != NULL && min_value->arg)
	{
		new->min_value = defGetInt64(min_value);
		new->log_cnt = 0;
	}
	else if (isInit || min_value != NULL)
	{
		if (new->increment_by > 0)
			new->min_value = 1; /* ascending seq */
		else
			new->min_value = SEQ_MINVALUE;		/* descending seq */
		new->log_cnt = 0;
	}

	/* crosscheck min/max */
	if (new->min_value >= new->max_value)
	{
		char		bufm[100],
					bufx[100];

		snprintf(bufm, sizeof(bufm), INT64_FORMAT, new->min_value);
		snprintf(bufx, sizeof(bufx), INT64_FORMAT, new->max_value);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("MINVALUE (%s) must be less than MAXVALUE (%s)",
						bufm, bufx)));
	}

	/* START WITH */
	if (start_value != NULL)
		new->start_value = defGetInt64(start_value);
	else if (isInit)
	{
		if (new->increment_by > 0)
			new->start_value = new->min_value;	/* ascending seq */
		else
			new->start_value = new->max_value;	/* descending seq */
	}

	/* crosscheck START */
	if (new->start_value < new->min_value)
	{
		char		bufs[100],
					bufm[100];

		snprintf(bufs, sizeof(bufs), INT64_FORMAT, new->start_value);
		snprintf(bufm, sizeof(bufm), INT64_FORMAT, new->min_value);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("START value (%s) cannot be less than MINVALUE (%s)",
						bufs, bufm)));
	}
	if (new->start_value > new->max_value)
	{
		char		bufs[100],
					bufm[100];

		snprintf(bufs, sizeof(bufs), INT64_FORMAT, new->start_value);
		snprintf(bufm, sizeof(bufm), INT64_FORMAT, new->max_value);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("START value (%s) cannot be greater than MAXVALUE (%s)",
						bufs, bufm)));
	}

	/* RESTART [WITH] */
	if (restart_value != NULL)
	{
		if (restart_value->arg != NULL)
			new->last_value = defGetInt64(restart_value);
		else
			new->last_value = new->start_value;
#ifdef PGXC
		*is_restart = true;
#endif
		new->is_called = false;
		new->log_cnt = 0;
	}
	else if (isInit)
	{
		new->last_value = new->start_value;
		new->is_called = false;
	}

	/* crosscheck RESTART (or current value, if changing MIN/MAX) */
	if (new->last_value < new->min_value)
	{
		char		bufs[100],
					bufm[100];

		snprintf(bufs, sizeof(bufs), INT64_FORMAT, new->last_value);
		snprintf(bufm, sizeof(bufm), INT64_FORMAT, new->min_value);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("RESTART value (%s) cannot be less than MINVALUE (%s)",
						bufs, bufm)));
	}
	if (new->last_value > new->max_value)
	{
		char		bufs[100],
					bufm[100];

		snprintf(bufs, sizeof(bufs), INT64_FORMAT, new->last_value);
		snprintf(bufm, sizeof(bufm), INT64_FORMAT, new->max_value);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("RESTART value (%s) cannot be greater than MAXVALUE (%s)",
								bufs, bufm)));
	}

	/* CACHE */
	if (cache_value != NULL)
	{
		new->cache_value = defGetInt64(cache_value);
		if (new->cache_value <= 0)
		{
			char		buf[100];

			snprintf(buf, sizeof(buf), INT64_FORMAT, new->cache_value);
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("CACHE (%s) must be greater than zero",
							buf)));
		}
		new->log_cnt = 0;
	}
	else if (isInit)
		new->cache_value = 1;
}

#ifdef PGXC
/*
 * GetGlobalSeqName
 *
 * Returns a global sequence name adapted to GTM
 * Name format is dbname.schemaname.seqname
 * so as to identify in a unique way in the whole cluster each sequence
 */
char *
GetGlobalSeqName(Relation seqrel, const char *new_seqname, const char *new_schemaname)
{
	char *seqname, *dbname, *schemaname, *relname;
	int charlen;

	/* Get all the necessary relation names */
	dbname = get_database_name(seqrel->rd_node.dbNode);

	if (new_seqname)
		relname = (char *) new_seqname;
	else
		relname = RelationGetRelationName(seqrel);

	if (new_schemaname)
		schemaname = (char *) new_schemaname;
	else
		schemaname = get_namespace_name(RelationGetNamespace(seqrel));

	/* Calculate the global name size including the dots and \0 */
	charlen = strlen(dbname) + strlen(schemaname) + strlen(relname) + 3;
	seqname = (char *) palloc(charlen);

	/* Form a unique sequence name with schema and database name for GTM */
	snprintf(seqname,
			 charlen,
			 "%s.%s.%s",
			 dbname,
			 schemaname,
			 relname);

	if (dbname)
		pfree(dbname);
	if (schemaname)
		pfree(schemaname);

	return seqname;
}

/*
 * IsTempSequence
 *
 * Determine if given sequence is temporary or not.
 */
bool
IsTempSequence(Oid relid)
{
	Relation seqrel;
	bool res;
	SeqTable	elm;

	/* open and AccessShareLock sequence */
	init_sequence(relid, &elm, &seqrel);

	res = seqrel->rd_backend == MyBackendId;
	relation_close(seqrel, NoLock);
	return res;
}
#endif

/*
 * Process an OWNED BY option for CREATE/ALTER SEQUENCE
 *
 * Ownership permissions on the sequence are already checked,
 * but if we are establishing a new owned-by dependency, we must
 * enforce that the referenced table has the same owner and namespace
 * as the sequence.
 */
static void
process_owned_by(Relation seqrel, List *owned_by)
{
	int			nnames;
	Relation	tablerel;
	AttrNumber	attnum;

	nnames = list_length(owned_by);
	Assert(nnames > 0);
	if (nnames == 1)
	{
		/* Must be OWNED BY NONE */
		if (strcmp(strVal(linitial(owned_by)), "none") != 0)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("invalid OWNED BY option"),
				errhint("Specify OWNED BY table.column or OWNED BY NONE.")));
		tablerel = NULL;
		attnum = 0;
	}
	else
	{
		List	   *relname;
		char	   *attrname;
		RangeVar   *rel;

		/* Separate relname and attr name */
		relname = list_truncate(list_copy(owned_by), nnames - 1);
		attrname = strVal(lfirst(list_tail(owned_by)));

		/* Open and lock rel to ensure it won't go away meanwhile */
		rel = makeRangeVarFromNameList(relname);
		tablerel = relation_openrv(rel, AccessShareLock);

		/* Must be a regular or foreign table */
		if (!(tablerel->rd_rel->relkind == RELKIND_RELATION ||
			  tablerel->rd_rel->relkind == RELKIND_FOREIGN_TABLE))
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("referenced relation \"%s\" is not a table or foreign table",
							RelationGetRelationName(tablerel))));

		/* We insist on same owner and schema */
		if (seqrel->rd_rel->relowner != tablerel->rd_rel->relowner)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("sequence must have same owner as table it is linked to")));
		if (RelationGetNamespace(seqrel) != RelationGetNamespace(tablerel))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("sequence must be in same schema as table it is linked to")));

		/* Now, fetch the attribute number from the system cache */
		attnum = get_attnum(RelationGetRelid(tablerel), attrname);
		if (attnum == InvalidAttrNumber)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" of relation \"%s\" does not exist",
							attrname, RelationGetRelationName(tablerel))));
	}

	/*
	 * OK, we are ready to update pg_depend.  First remove any existing AUTO
	 * dependencies for the sequence, then optionally add a new one.
	 */
	markSequenceUnowned(RelationGetRelid(seqrel));

	if (tablerel)
	{
		ObjectAddress refobject,
					depobject;

		refobject.classId = RelationRelationId;
		refobject.objectId = RelationGetRelid(tablerel);
		refobject.objectSubId = attnum;
		depobject.classId = RelationRelationId;
		depobject.objectId = RelationGetRelid(seqrel);
		depobject.objectSubId = 0;
		recordDependencyOn(&depobject, &refobject, DEPENDENCY_AUTO);
	}

	/* Done, but hold lock until commit */
	if (tablerel)
		relation_close(tablerel, NoLock);
}


/*
 * Return sequence parameters, for use by information schema
 */
Datum
pg_sequence_parameters(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	TupleDesc	tupdesc;
	Datum		values[5];
	bool		isnull[5];
	SeqTable	elm;
	Relation	seqrel;
	Buffer		buf;
	HeapTupleData seqtuple;
	Form_pg_sequence seq;

	/* open and AccessShareLock sequence */
	init_sequence(relid, &elm, &seqrel);

	if (pg_class_aclcheck(relid, GetUserId(), ACL_SELECT | ACL_UPDATE | ACL_USAGE) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for sequence %s",
						RelationGetRelationName(seqrel))));

	tupdesc = CreateTemplateTupleDesc(5, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "start_value",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "minimum_value",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "maximum_value",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "increment",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 5, "cycle_option",
					   BOOLOID, -1, 0);

	BlessTupleDesc(tupdesc);

	memset(isnull, 0, sizeof(isnull));

	seq = read_seq_tuple(elm, seqrel, &buf, &seqtuple);

	values[0] = Int64GetDatum(seq->start_value);
	values[1] = Int64GetDatum(seq->min_value);
	values[2] = Int64GetDatum(seq->max_value);
	values[3] = Int64GetDatum(seq->increment_by);
	values[4] = BoolGetDatum(seq->is_cycled);

	UnlockReleaseBuffer(buf);
	relation_close(seqrel, NoLock);

	return HeapTupleGetDatum(heap_form_tuple(tupdesc, values, isnull));
}


void
seq_redo(XLogRecPtr lsn, XLogRecord *record)
{
	uint8		info = record->xl_info & ~XLR_INFO_MASK;
	Buffer		buffer;
	Page		page;
	Page		localpage;
	char	   *item;
	Size		itemsz;
	xl_seq_rec *xlrec = (xl_seq_rec *) XLogRecGetData(record);
	sequence_magic *sm;

	/* Backup blocks are not used in seq records */
	Assert(!(record->xl_info & XLR_BKP_BLOCK_MASK));

	if (info != XLOG_SEQ_LOG)
		elog(PANIC, "seq_redo: unknown op code %u", info);

	buffer = XLogReadBuffer(xlrec->node, 0, true);
	Assert(BufferIsValid(buffer));
	page = (Page) BufferGetPage(buffer);

	/*
	 * We must always reinit the page and reinstall the magic number (see
	 * comments in fill_seq_with_data).  However, since this WAL record type
	 * is also used for updating sequences, it's possible that a hot-standby
	 * backend is examining the page concurrently; so we mustn't transiently
	 * trash the buffer.  The solution is to build the correct new page
	 * contents in local workspace and then memcpy into the buffer.  Then only
	 * bytes that are supposed to change will change, even transiently. We
	 * must palloc the local page for alignment reasons.
	 */
	localpage = (Page) palloc(BufferGetPageSize(buffer));

	PageInit(localpage, BufferGetPageSize(buffer), sizeof(sequence_magic));
	sm = (sequence_magic *) PageGetSpecialPointer(localpage);
	sm->magic = SEQ_MAGIC;

	item = (char *) xlrec + sizeof(xl_seq_rec);
	itemsz = record->xl_len - sizeof(xl_seq_rec);

	if (PageAddItem(localpage, (Item) item, itemsz,
					FirstOffsetNumber, false, false) == InvalidOffsetNumber)
		elog(PANIC, "seq_redo: failed to add item to page");

	PageSetLSN(localpage, lsn);

	memcpy(page, localpage, BufferGetPageSize(buffer));
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);

	pfree(localpage);
}
#ifdef PGXC
/*
 * Register a callback for a sequence rename drop on GTM
 */
void
register_sequence_rename_cb(char *oldseqname, char *newseqname)
{
	rename_sequence_callback_arg *args;
	char *oldseqnamearg = NULL;
	char *newseqnamearg = NULL;

	/* All the arguments are transaction-dependent, so save them in TopTransactionContext */
	args = (rename_sequence_callback_arg *)
		MemoryContextAlloc(TopTransactionContext, sizeof(rename_sequence_callback_arg));

	oldseqnamearg = MemoryContextAlloc(TopTransactionContext, strlen(oldseqname) + 1);
	newseqnamearg = MemoryContextAlloc(TopTransactionContext, strlen(newseqname) + 1);
	sprintf(oldseqnamearg, "%s", oldseqname);
	sprintf(newseqnamearg, "%s", newseqname);

	args->oldseqname = oldseqnamearg;
	args->newseqname = newseqnamearg;

	RegisterGTMCallback(rename_sequence_cb, (void *) args);
}

/*
 * Callback a sequence rename
 */
void
rename_sequence_cb(GTMEvent event, void *args)
{
	rename_sequence_callback_arg *cbargs = (rename_sequence_callback_arg *) args;
	char *newseqname = cbargs->newseqname;
	char *oldseqname = cbargs->oldseqname;
	int err = 0;

	/*
	 * A sequence is here renamed to its former name only when a transaction
	 * that involved a sequence rename was dropped.
	 */
	switch (event)
	{
		case GTM_EVENT_ABORT:
			/*
			 * Here sequence is renamed to its former name
			 * so what was new becomes old.
			 */
			err = RenameSequenceGTM(newseqname, oldseqname);
			break;
		case GTM_EVENT_COMMIT:
		case GTM_EVENT_PREPARE:
			/* Nothing to do */
			break;
		default:
			Assert(0);
	}

	/* Report error if necessary */
	if (err < 0 && event != GTM_EVENT_ABORT)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("GTM error, could not rename sequence")));
}


/*
 * Register a callback for a sequence drop on GTM
 */
void
register_sequence_cb(char *seqname, GTM_SequenceKeyType key, GTM_SequenceDropType type)
{
	drop_sequence_callback_arg *args;
	char *seqnamearg = NULL;

	/* All the arguments are transaction-dependent, so save them in TopTransactionContext */
	args = (drop_sequence_callback_arg *)
		MemoryContextAlloc(TopTransactionContext, sizeof(drop_sequence_callback_arg));

	seqnamearg = MemoryContextAlloc(TopTransactionContext, strlen(seqname) + 1);
	sprintf(seqnamearg, "%s", seqname);
	args->seqname = seqnamearg;
	args->key = key;
	args->type = type;

	RegisterGTMCallback(drop_sequence_cb, (void *) args);
}

/*
 * Callback of sequence drop
 */
void
drop_sequence_cb(GTMEvent event, void *args)
{
	drop_sequence_callback_arg *cbargs = (drop_sequence_callback_arg *) args;
	char *seqname = cbargs->seqname;
	GTM_SequenceKeyType key = cbargs->key;
	GTM_SequenceDropType type = cbargs->type;
	int err = 0;

	/*
	 * A sequence is dropped on GTM if the transaction that created sequence
	 * aborts or if the transaction that dropped the sequence commits. This mechanism
	 * insures that sequence information is consistent on all the cluster nodes including
	 * GTM. This callback is done before transaction really commits so it can still fail
	 * if an error occurs.
	 */
	switch (event)
	{
		case GTM_EVENT_COMMIT:
		case GTM_EVENT_PREPARE:
			if (type == GTM_DROP_SEQ)
				err = DropSequenceGTM(seqname, key);
			break;
		case GTM_EVENT_ABORT:
			if (type == GTM_CREATE_SEQ)
				err = DropSequenceGTM(seqname, key);
			break;
		default:
			/* Should not come here */
			Assert(0);
	}

	/* Report error if necessary */
	if (err < 0 && event != GTM_EVENT_ABORT)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("GTM error, could not drop sequence")));
}
#endif
