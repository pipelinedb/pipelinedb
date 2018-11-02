/*-------------------------------------------------------------------------
 *
 * commands.c
 * 		Functionality for processing utility/DDL commands
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "analyzer.h"
#include "catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "commands.h"
#include "commands/dbcommands.h"
#include "compat.h"
#include "config.h"
#include "copy.h"
#include "executor.h"
#include "miscadmin.h"
#include "pipeline_query.h"
#include "pipeline_stream.h"
#include "scheduler.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"

static ProcessUtility_hook_type save_utility_hook = NULL;

/*
 * InstallCommandHooks
 */
void
InstallCommandHooks(void)
{
	save_utility_hook = ProcessUtility_hook;
	ProcessUtility_hook = PipelineProcessUtility;
}

/*
 * sync_pipeline_objects
 */
static void
sync_pipeline_objects(void)
{
	PushActiveSnapshot(GetTransactionSnapshot());

	InitPipelineCatalog();

	SyncPipelineQuery();

	SyncPipelineStream();

	CommandCounterIncrement();

	SyncPipelineStreamReaders();

	CommandCounterIncrement();

	/*
	 * Since our pg_aggregate lookup cache doesn't use our own write functions, we need
	 * to manually invalidate our cached aggregates whenever aggregates are potentially dropped.
	 */
	PipelineCatalogInvalidate(PGAGGCOMBINEFN);
	PipelineCatalogInvalidate(PGAGGPARTIALCOMBINEFN);

	PopActiveSnapshot();
}

/*
 * get_worker_select_stmt
 */
static SelectStmt *
get_worker_select_stmt(ContQuery* view, SelectStmt** viewptr)
{
	List		*parsetree_list;
	SelectStmt	*selectstmt;
	RawStmt *raw;

	parsetree_list = pg_parse_query(view->sql);
	Assert(list_length(parsetree_list) == 1);

	raw = (RawStmt *) linitial(parsetree_list);
	selectstmt = (SelectStmt *) raw->stmt;
	selectstmt = TransformSelectStmtForContProcess(view->matrel, selectstmt, viewptr, view->sw_step_factor, Worker);

	return selectstmt;
}

/*
 * get_res_target
 */
static ResTarget *
get_res_target(char *name, List *tlist)
{
	ListCell *lc;

	Assert(name);

	foreach(lc, tlist)
	{
		ResTarget *rt = (ResTarget *) lfirst(lc);
		if (pg_strcasecmp(rt->name, name) == 0)
			return rt;
	}

	elog(ERROR, "column \"%s\" does not exist", name);

	return NULL;
}

/*
 * create_index_on_matrel
 *
 * Given a CREATE INDEX query on a continuous view, modify it to
 * create the index on the continuous view's matrel instead.
 */
static void
create_index_on_matrel(IndexStmt *stmt)
{
	ContQuery *cv;
	RangeVar *cv_name = stmt->relation;
	SelectStmt	*viewstmt;
	List *tlist;
	ListCell *lc;
	bool is_sw = RangeVarIsSWContView(cv_name);

	stmt->relation = RangeVarGetMatRelName(cv_name);
	cv = RangeVarGetContView(cv_name);
	get_worker_select_stmt(cv, &viewstmt);

	Assert(viewstmt);
	tlist = viewstmt->targetList;

	foreach(lc, stmt->indexParams)
	{
		IndexElem *elem = (IndexElem *) lfirst(lc);
		ResTarget *res;

		if (!elem->name)
			continue;

		/*
		 * If the column isn't a plain column reference, then it's wrapped in
		 * a finalize call so we need to index on that with an expression index
		 */
		res = get_res_target(elem->name, tlist);
		if (!IsA(res->val, ColumnRef))
		{
			if (is_sw)
				elog(ERROR, "sliding-window aggregate columns cannot be indexed");
			elem->expr = copyObject(res->val);
			elem->name = NULL;
		}
	}
}

/*
 * PipelineProcessUtility
 *
 * Hook for PipelineDB to intercept relevant utility queries
 */
void
PipelineProcessUtility(PlannedStmt *pstmt, const char *sql, ProcessUtilityContext context,
		ParamListInfo params, QueryEnvironment *env, DestReceiver *dest, char *tag)
{
	ContExecutionLock exec_lock = NULL;
	PipelineDDLLock ddl_lock = NULL;
	Oid dbid = InvalidOid;
	bool isstream = false;
	Node *parsetree = NULL;
	volatile PlannedStmt *vpstmt = pstmt;
	bool exists = PipelineDBExists();
	RangeVar *sync_cv = NULL;
	Oid sync_renamed_cq = InvalidOid;
	Oid sync_renamed_stream = InvalidOid;
	bool renamed_schema = false;

	/*
	 * NB: Don't goto anywhere outside of the try/catch block here
	 */
	PG_TRY();
	{
		/*
		 * If we're in CREATE EXTENSION pipelinedb, no need to do anything yet
		 */
		if (CreatingPipelineDB() || !exists)
			goto run;

		if (!pstmt->utilityStmt)
			goto run;

		parsetree = pstmt->utilityStmt;

		if (IsA(parsetree, CopyStmt))
		{
			CopyStmt *copy = (CopyStmt *) parsetree;
			if (copy->is_from && RangeVarIsStream(copy->relation, true))
			{
				ParseState *pstate = make_parsestate(NULL);
				uint64 processed;

				pstate->p_sourcetext = sql;

				DoStreamCopy(pstate, (CopyStmt *) parsetree,
						 pstmt->stmt_location, pstmt->stmt_len,
						 &processed);

				sprintf(tag, "COPY %ld", processed);
				goto epilogue;
			}
		}
		else if (IsA(parsetree, ViewStmt) && IsA(((ViewStmt *) parsetree)->query, SelectStmt))
		{
			ViewStmt *stmt = (ViewStmt *) parsetree;
			ContQueryAction action;

			action = GetContQueryAction(stmt);
			if (action == DUMPED)
			{
				AnalyzeDumped(stmt);
				goto run;
			}
			if (action != NONE)
			{
				/*
				 * We currently don't allow CREATE OR REPLACE semantics on continuous queries, because it would essentially
				 * equate to modifying CQ definitions, which is not supported.
				 */
				if (stmt->replace)
					elog(ERROR, "continuous queries cannot be replaced");

				PipelineContextSetIsDDL();
				ddl_lock = AcquirePipelineDDLLock();
			}

			if (action == TRANSFORM)
			{
				AnalyzeCreateViewForTransform(stmt);
				ExecCreateContTransformStmt(stmt->view, stmt->query, stmt->options, sql);
				goto epilogue;
			}
			else if (action == MATERIALIZE)
			{
				ExecCreateContViewStmt(stmt->view, stmt->query, stmt->options, sql);
				goto epilogue;
			}

			/*
			 * During a restore, each component of the CV will be created individually, with the overlay
			 * view being last. At that point we must sync all metadata for the CV based on each component
			 * that has been created.
			 *
			 * If there is no action attached to this VIEW and its name corresponds to a CV,
			 * then we're restoring that CV and must sync.
			 */
			if (!IsBinaryUpgrade && RangeVarIsContView(stmt->view))
			{
				sync_cv = stmt->view;
				goto run;
			}
		}
		else if (IsA(parsetree, DropStmt))
		{
			DropStmt *stmt = (DropStmt *) parsetree;

			/*
			 * If we're dropping the extension, we want to shut down all workers first
			 * so that they don't attempt to continue accessing extension objects after
			 * we've dropped them in this transaction.
			 *
			 * If the DROP EXTENSION command fails here, the workers will be terminated anyways
			 * but we'll attempt to restart them below during PG_CATCH.
			 */
			if (IsDropPipelineDBCommand(stmt))
				SignalContQuerySchedulerDropPipelineDB(MyDatabaseId);

			/*
			 * If we're dropping a CQ, we must acquire the continuous execution lock,
			 * which when held exclusively prevents all CQs (they acquire it as a shared lock)
			 * from being executed by workers/combiners. This prevents deadlocks between
			 * this process and workers/combiners, which can all acquire contended locks in
			 * basically any order they want.
			 *
			 * So we simply ensure mutual exclusion between droppers and workers/combiners execution.
			 */
			if (list_length(stmt->objects) == 1 && IsA(linitial(stmt->objects), List))
			{
				RangeVar *rv = makeRangeVarFromNameList(linitial(stmt->objects));

				if (RangeVarIsPipelineObject(rv)||
						stmt->removeType == OBJECT_SCHEMA ||
						stmt->removeType == OBJECT_TYPE ||
						stmt->removeType == OBJECT_FUNCTION)
				{
					exec_lock = AcquireContExecutionLock(AccessExclusiveLock);
					ddl_lock = AcquirePipelineDDLLock();
				}
			}
		}
		else if (IsA(parsetree, CreateForeignTableStmt))
		{
			/*
			 * If we're creating a stream, we perform a constraints check since constraints are not
			 * supported by streams although they are supported by the grammar.
			 */
			if (pg_strcasecmp(((CreateForeignTableStmt *) parsetree)->servername, PIPELINEDB_SERVER) == 0)
			{
				CreateStmt *stmt = (CreateStmt *) parsetree;

				isstream = true;
				transformCreateStreamStmt((CreateForeignTableStmt *) stmt);
			}
		}
		else if (IsA(pstmt->utilityStmt, DropdbStmt))
		{
			DropdbStmt *stmt = (DropdbStmt *) parsetree;

			/*
			 * We need to stop the workers for this database before the DROP DATABASE command
			 * is executing, as it will just fail because of the open connections held by workers.
			 */
			dbid = get_database_oid(stmt->dbname, stmt->missing_ok);
			SignalContQuerySchedulerDropDB(dbid);
		}
		else if (IsA(parsetree, IndexStmt))
		{
			/*
			 * Don't do anything here if we're currently in CREATE EXTENSION pipelinedb, since we create indices
			 * in the extension creation SQL but obviously we don't care about doing anything special until
			 * the extension actually exists.
			 */
			IndexStmt *stmt = (IndexStmt *) parsetree;
			if (!CreatingPipelineDB() && RangeVarIsContView(stmt->relation))
				create_index_on_matrel(stmt);
		}
		else if (IsA(parsetree, VacuumStmt))
		{
			VacuumStmt *vstmt = (VacuumStmt *) parsetree;
			/*
			 * If the user is trying to vacuum a CV, what they're really
			 * trying to do vacuum the CV's materialization table, so rewrite
			 * the name of the target relation if we need to.
			 */
			CompatAnalyzeVacuumStmt(vstmt);
		}
		else if (IsA(parsetree, CreateTrigStmt))
		{
			CreateTrigStmt *stmt = (CreateTrigStmt *) parsetree;

			if (RangeVarIsContView(stmt->relation))
				elog(ERROR, "continuous views do not support triggers");
		}
		else if (IsA(parsetree, AlterTableStmt))
		{
			/*
			 * Streams only support adding columns,
			 * so if we're altering a stream ensure that's all we're doing.
			 */
			AlterTableStmt *stmt = (AlterTableStmt *) parsetree;

			if (RangeVarIsStream(stmt->relation, true))
			{
				ListCell *lc;
				foreach(lc, stmt->cmds)
				{
					AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);
					if (cmd->subtype != AT_AddColumn && cmd->subtype != AT_ChangeOwner)
					{
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
										(errmsg("streams only support ADD COLUMN or OWNER TO actions"))));
					}
				}
			}
			else if (stmt->relkind == OBJECT_VIEW && RangeVarIsContView(stmt->relation))
			{
				/*
				 * Note that continuous views can be renamed and their schemas may be altered, but neither
				 * of those operations is encoded as an AlterTableStmt so we need not handle them here.
				 */
				elog(ERROR, "continuous views cannot be modified");
			}
		}
		else if (IsA(parsetree, RenameStmt))
		{
			RenameStmt *rename = (RenameStmt *) parsetree;
			RangeVar 	*cv;

			if (rename->renameType == OBJECT_SCHEMA)
			{
				/*
				 * If we've renamed a schema, we need to sync all CQs to reflect the new schema name. Technically
				 * a schema rename may very well not effect any CQs, but we just blindly re-sync in this case because
				 * it's not expensive anyways.
				 */
				renamed_schema = true;
			}
			else if (rename->relation)
			{
				Oid relid = RangeVarGetRelid(rename->relation, NoLock, true);
				/*
				 * We disallow the renaming of a continuous view/transform's internal relations
				 */
				if (OidIsValid(relid) && RelidIsContQueryInternalRelation(relid))
				{
					if (rename->relation->schemaname)
						elog(ERROR, "relation \"%s.%s\" is an internal relation and cannot be renamed",
								rename->relation->schemaname, rename->relation->relname);
					else
						elog(ERROR, "relation \"%s\" is an internal relation and cannot be renamed",
								rename->relation->relname);
				}

				/*
				 * If a continuous query or stream is being renamed, we must update all relevant metadata
				 * to reference the new name.
				 */
				if (RelidIsContQuery(relid))
					sync_renamed_cq = relid;
				else if (RelidIsStream(relid))
					sync_renamed_stream = relid;

				if (rename->renameType == OBJECT_TABLE && RangeVarIsMatRel(rename->relation, &cv))
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							 errmsg("cannot rename materialization table \"%s\" for continuous view \"%s\"",
									 rename->relation->relname, cv->relname)));
			}
		}
		else if (IsA(parsetree, AlterObjectSchemaStmt))
		{
			AlterObjectSchemaStmt *stmt = (AlterObjectSchemaStmt *) parsetree;

			if (stmt->objectType == OBJECT_FOREIGN_TABLE && RangeVarIsStream(stmt->relation, false))
			{
				/*
				 * We're changing a streams's schema, so we must sync all metadata that references this stream
				 * to reflect the new schema.
				 */
				sync_renamed_stream = RangeVarGetRelid(stmt->relation, NoLock, false);
			}
			else if (stmt->objectType == OBJECT_VIEW && RangeVarIsContQuery(stmt->relation))
			{
				/*
				 * We're changing the schema of a CQ, so we must put all dependent relations in the new schema.
				 * It isn't imperative that we do this for CQs to work, but it is a design decision we've made
				 * to keep things organized and intuitive. It would be confusing to have a CQ in one schema
				 * and all of its dependent relations in the old one.
				 */
				sync_renamed_cq = RangeVarGetRelid(stmt->relation, NoLock, false);
				SyncContQuerySchema(sync_renamed_cq, stmt->newschema);
			}
		}

run:

		if (save_utility_hook != NULL)
			(*save_utility_hook) (pstmt, sql, context, params, env, dest, tag);
		else
			standard_ProcessUtility(pstmt, sql, context, params, env, dest, tag);

		if (!exists && pstmt->utilityStmt &&
				IsA(pstmt->utilityStmt, CreateExtensionStmt) && IsCreatePipelineDBCommand((CreateExtensionStmt *) pstmt->utilityStmt))
		{
			/*
			 * If we just ran CREATE EXTENSION pipelinedb, we need to tell the scheduler to start up new bgworkers and we'll
			 * want to invalidate any previously existing PipelineDB catalog cache entries
			 */
			SignalContQuerySchedulerCreatePipelineDB(MyDatabaseId);
		}

		if (!exists)
			goto done;

epilogue:

		/*
		 * We may have dropped a PipelineDB object, so we reconcile our own catalog tables
		 * to ensure any orphaned entries are removed in this transaction.
		 */
		if (pstmt->utilityStmt && IsA(pstmt->utilityStmt, DropStmt))
		{
			DropStmt *ds = (DropStmt *) pstmt->utilityStmt;
			if (!IsDropPipelineDBCommand(ds))
				sync_pipeline_objects();
		}

		if (exec_lock)
			ReleaseContExecutionLock(exec_lock);

		if (ddl_lock)
			ReleasePipelineDDLLock(ddl_lock);

		/*
		 * If a CREATE DATABASE was successful, signal the scheduler to start workers for the new DB
		 */
		if (pstmt->utilityStmt && IsA(pstmt->utilityStmt, CreatedbStmt))
			SignalContQuerySchedulerRefreshDBList();

		if (sync_cv)
			SyncContView(sync_cv);

		/*
		 * If we created a stream, we need to create a pipeline_stream entry in addition
		 * to the underlying foreign table.
		 */
		if (isstream)
		{
			CreateForeignTableStmt *stmt = (CreateForeignTableStmt *) parsetree;
			Oid relid = RangeVarGetRelid(stmt->base.relation, NoLock, false);
			CreatePipelineStreamEntry(stmt, relid);
		}

		CommandCounterIncrement();
		/*
		 * A stream or CQ rename was successful, so now we must sync the metadata of relevant objects
		 * that reference the renamed object. We use relation names instead of OIDs in various places for
		 * dump/restore/upgrade support, so we must do a sync in these cases rather than continue
		 * referencing the object's OID, even though the OID hasn't been changed by the rename.
		 */
		if (OidIsValid(sync_renamed_cq))
			SyncContQueryDefRel(sync_renamed_cq);
		else if (OidIsValid(sync_renamed_stream))
			SyncStreamReaderDefRels(sync_renamed_stream);
		else if (renamed_schema)
			SyncAllContQueryDefRels();

done:
		/*
		 * Clear analyzer/planner context flags
		 */
		ClearPipelineContext();
	}
	PG_CATCH();
	{
		/*
		 * Clear analyzer/planner context flags
		 */
		ClearPipelineContext();

		if (vpstmt->utilityStmt)
		{
			/*
			 * If a DROP DATBASE or DROP EXTENSION pipelinedb fails, we've already terminated background
			 * workers so tell the scheduler to restart them so we stay in whatever state we were in
			 * before the failed transaction.
			 */
			if (IsA(vpstmt->utilityStmt, DropdbStmt))
			{
				DropdbStmt *stmt = (DropdbStmt *) vpstmt->utilityStmt;
				SignalContQuerySchedulerRefreshDBList();
				dbid = get_database_oid(stmt->dbname, stmt->missing_ok);

				SignalContQuerySchedulerCreatePipelineDB(dbid);
			}
			else if (IsA(vpstmt->utilityStmt, DropStmt))
			{
				DropStmt *ds = (DropStmt *) vpstmt->utilityStmt;
				if (IsDropPipelineDBCommand(ds))
					SignalContQuerySchedulerCreatePipelineDB(MyDatabaseId);
			}
		}

		PG_RE_THROW();
	}
	PG_END_TRY();
}
