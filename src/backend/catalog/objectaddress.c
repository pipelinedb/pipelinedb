/*-------------------------------------------------------------------------
 *
 * objectaddress.c
 *	  functions for working with ObjectAddresses
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2013-2015, PipelineDB
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/objectaddress.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_default_acl.h"
#include "catalog/pg_event_trigger.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_database.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_language.h"
#include "catalog/pg_largeobject.h"
#include "catalog/pg_largeobject_metadata.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_policy.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_transform.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_ts_parser.h"
#include "catalog/pg_ts_template.h"
#include "catalog/pg_type.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_stream_fn.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/event_trigger.h"
#include "commands/extension.h"
#include "commands/policy.h"
#include "commands/proclang.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "libpq/be-fsstubs.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "parser/parse_type.h"
#include "pipeline/syscache.h"
#include "rewrite/rewriteSupport.h"
#include "storage/lmgr.h"
#include "storage/sinval.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/tqual.h"

/*
 * ObjectProperty
 *
 * This array provides a common part of system object structure; to help
 * consolidate routines to handle various kind of object classes.
 */
typedef struct
{
	Oid			class_oid;		/* oid of catalog */
	Oid			oid_index_oid;	/* oid of index on system oid column */
	int			oid_catcache_id;	/* id of catcache on system oid column	*/
	int			name_catcache_id;		/* id of catcache on (name,namespace),
										 * or (name) if the object does not
										 * live in a namespace */
	AttrNumber	attnum_name;	/* attnum of name field */
	AttrNumber	attnum_namespace;		/* attnum of namespace field */
	AttrNumber	attnum_owner;	/* attnum of owner field */
	AttrNumber	attnum_acl;		/* attnum of acl field */
	AclObjectKind acl_kind;		/* ACL_KIND_* of this object type */
	bool		is_nsp_name_unique;		/* can the nsp/name combination (or
										 * name alone, if there's no
										 * namespace) be considered a unique
										 * identifier for an object of this
										 * class? */
} ObjectPropertyType;

static const ObjectPropertyType ObjectProperty[] =
{
	{
		CastRelationId,
		CastOidIndexId,
		-1,
		-1,
		InvalidAttrNumber,
		InvalidAttrNumber,
		InvalidAttrNumber,
		InvalidAttrNumber,
		-1,
		false
	},
	{
		CollationRelationId,
		CollationOidIndexId,
		COLLOID,
		-1,						/* COLLNAMEENCNSP also takes encoding */
		Anum_pg_collation_collname,
		Anum_pg_collation_collnamespace,
		Anum_pg_collation_collowner,
		InvalidAttrNumber,
		ACL_KIND_COLLATION,
		true
	},
	{
		ConstraintRelationId,
		ConstraintOidIndexId,
		CONSTROID,
		-1,
		Anum_pg_constraint_conname,
		Anum_pg_constraint_connamespace,
		InvalidAttrNumber,
		InvalidAttrNumber,
		-1,
		false
	},
	{
		ConversionRelationId,
		ConversionOidIndexId,
		CONVOID,
		CONNAMENSP,
		Anum_pg_conversion_conname,
		Anum_pg_conversion_connamespace,
		Anum_pg_conversion_conowner,
		InvalidAttrNumber,
		ACL_KIND_CONVERSION,
		true
	},
	{
		DatabaseRelationId,
		DatabaseOidIndexId,
		DATABASEOID,
		-1,
		Anum_pg_database_datname,
		InvalidAttrNumber,
		Anum_pg_database_datdba,
		Anum_pg_database_datacl,
		ACL_KIND_DATABASE,
		true
	},
	{
		ExtensionRelationId,
		ExtensionOidIndexId,
		-1,
		-1,
		Anum_pg_extension_extname,
		InvalidAttrNumber,		/* extension doesn't belong to extnamespace */
		Anum_pg_extension_extowner,
		InvalidAttrNumber,
		ACL_KIND_EXTENSION,
		true
	},
	{
		ForeignDataWrapperRelationId,
		ForeignDataWrapperOidIndexId,
		FOREIGNDATAWRAPPEROID,
		FOREIGNDATAWRAPPERNAME,
		Anum_pg_foreign_data_wrapper_fdwname,
		InvalidAttrNumber,
		Anum_pg_foreign_data_wrapper_fdwowner,
		Anum_pg_foreign_data_wrapper_fdwacl,
		ACL_KIND_FDW,
		true
	},
	{
		ForeignServerRelationId,
		ForeignServerOidIndexId,
		FOREIGNSERVEROID,
		FOREIGNSERVERNAME,
		Anum_pg_foreign_server_srvname,
		InvalidAttrNumber,
		Anum_pg_foreign_server_srvowner,
		Anum_pg_foreign_server_srvacl,
		ACL_KIND_FOREIGN_SERVER,
		true
	},
	{
		ProcedureRelationId,
		ProcedureOidIndexId,
		PROCOID,
		-1,						/* PROCNAMEARGSNSP also takes argument types */
		Anum_pg_proc_proname,
		Anum_pg_proc_pronamespace,
		Anum_pg_proc_proowner,
		Anum_pg_proc_proacl,
		ACL_KIND_PROC,
		false
	},
	{
		LanguageRelationId,
		LanguageOidIndexId,
		LANGOID,
		LANGNAME,
		Anum_pg_language_lanname,
		InvalidAttrNumber,
		Anum_pg_language_lanowner,
		Anum_pg_language_lanacl,
		ACL_KIND_LANGUAGE,
		true
	},
	{
		LargeObjectMetadataRelationId,
		LargeObjectMetadataOidIndexId,
		-1,
		-1,
		InvalidAttrNumber,
		InvalidAttrNumber,
		Anum_pg_largeobject_metadata_lomowner,
		Anum_pg_largeobject_metadata_lomacl,
		ACL_KIND_LARGEOBJECT,
		false
	},
	{
		OperatorClassRelationId,
		OpclassOidIndexId,
		CLAOID,
		-1,						/* CLAAMNAMENSP also takes opcmethod */
		Anum_pg_opclass_opcname,
		Anum_pg_opclass_opcnamespace,
		Anum_pg_opclass_opcowner,
		InvalidAttrNumber,
		ACL_KIND_OPCLASS,
		true
	},
	{
		OperatorRelationId,
		OperatorOidIndexId,
		OPEROID,
		-1,						/* OPERNAMENSP also takes left and right type */
		Anum_pg_operator_oprname,
		Anum_pg_operator_oprnamespace,
		Anum_pg_operator_oprowner,
		InvalidAttrNumber,
		ACL_KIND_OPER,
		false
	},
	{
		OperatorFamilyRelationId,
		OpfamilyOidIndexId,
		OPFAMILYOID,
		-1,						/* OPFAMILYAMNAMENSP also takes opfmethod */
		Anum_pg_opfamily_opfname,
		Anum_pg_opfamily_opfnamespace,
		Anum_pg_opfamily_opfowner,
		InvalidAttrNumber,
		ACL_KIND_OPFAMILY,
		true
	},
	{
		AuthIdRelationId,
		AuthIdOidIndexId,
		AUTHOID,
		AUTHNAME,
		Anum_pg_authid_rolname,
		InvalidAttrNumber,
		InvalidAttrNumber,
		InvalidAttrNumber,
		-1,
		true
	},
	{
		RewriteRelationId,
		RewriteOidIndexId,
		-1,
		-1,
		Anum_pg_rewrite_rulename,
		InvalidAttrNumber,
		InvalidAttrNumber,
		InvalidAttrNumber,
		-1,
		false
	},
	{
		NamespaceRelationId,
		NamespaceOidIndexId,
		NAMESPACEOID,
		NAMESPACENAME,
		Anum_pg_namespace_nspname,
		InvalidAttrNumber,
		Anum_pg_namespace_nspowner,
		Anum_pg_namespace_nspacl,
		ACL_KIND_NAMESPACE,
		true
	},
	{
		RelationRelationId,
		ClassOidIndexId,
		RELOID,
		RELNAMENSP,
		Anum_pg_class_relname,
		Anum_pg_class_relnamespace,
		Anum_pg_class_relowner,
		Anum_pg_class_relacl,
		ACL_KIND_CLASS,
		true
	},
	{
		TableSpaceRelationId,
		TablespaceOidIndexId,
		TABLESPACEOID,
		-1,
		Anum_pg_tablespace_spcname,
		InvalidAttrNumber,
		Anum_pg_tablespace_spcowner,
		Anum_pg_tablespace_spcacl,
		ACL_KIND_TABLESPACE,
		true
	},
	{
		TransformRelationId,
		TransformOidIndexId,
		TRFOID,
		InvalidAttrNumber
	},
	{
		TriggerRelationId,
		TriggerOidIndexId,
		-1,
		-1,
		Anum_pg_trigger_tgname,
		InvalidAttrNumber,
		InvalidAttrNumber,
		InvalidAttrNumber,
		-1,
		false
	},
	{
		PolicyRelationId,
		PolicyOidIndexId,
		-1,
		-1,
		Anum_pg_policy_polname,
		InvalidAttrNumber,
		InvalidAttrNumber,
		InvalidAttrNumber,
		-1,
		false
	},
	{
		EventTriggerRelationId,
		EventTriggerOidIndexId,
		EVENTTRIGGEROID,
		EVENTTRIGGERNAME,
		Anum_pg_event_trigger_evtname,
		InvalidAttrNumber,
		Anum_pg_event_trigger_evtowner,
		InvalidAttrNumber,
		ACL_KIND_EVENT_TRIGGER,
		true
	},
	{
		TSConfigRelationId,
		TSConfigOidIndexId,
		TSCONFIGOID,
		TSCONFIGNAMENSP,
		Anum_pg_ts_config_cfgname,
		Anum_pg_ts_config_cfgnamespace,
		Anum_pg_ts_config_cfgowner,
		InvalidAttrNumber,
		ACL_KIND_TSCONFIGURATION,
		true
	},
	{
		TSDictionaryRelationId,
		TSDictionaryOidIndexId,
		TSDICTOID,
		TSDICTNAMENSP,
		Anum_pg_ts_dict_dictname,
		Anum_pg_ts_dict_dictnamespace,
		Anum_pg_ts_dict_dictowner,
		InvalidAttrNumber,
		ACL_KIND_TSDICTIONARY,
		true
	},
	{
		TSParserRelationId,
		TSParserOidIndexId,
		TSPARSEROID,
		TSPARSERNAMENSP,
		Anum_pg_ts_parser_prsname,
		Anum_pg_ts_parser_prsnamespace,
		InvalidAttrNumber,
		InvalidAttrNumber,
		-1,
		true
	},
	{
		TSTemplateRelationId,
		TSTemplateOidIndexId,
		TSTEMPLATEOID,
		TSTEMPLATENAMENSP,
		Anum_pg_ts_template_tmplname,
		Anum_pg_ts_template_tmplnamespace,
		InvalidAttrNumber,
		InvalidAttrNumber,
		-1,
		true,
	},
	{
		TypeRelationId,
		TypeOidIndexId,
		TYPEOID,
		TYPENAMENSP,
		Anum_pg_type_typname,
		Anum_pg_type_typnamespace,
		Anum_pg_type_typowner,
		Anum_pg_type_typacl,
		ACL_KIND_TYPE,
		true
	}
};

/*
 * This struct maps the string object types as returned by
 * getObjectTypeDescription into ObjType enum values.  Note that some enum
 * values can be obtained by different names, and that some string object types
 * do not have corresponding values in the output enum.  The user of this map
 * must be careful to test for invalid values being returned.
 *
 * To ease maintenance, this follows the order of getObjectTypeDescription.
 */
static const struct object_type_map
{
	const char *tm_name;
	ObjectType	tm_type;
}

			ObjectTypeMap[] =
{
	/* OCLASS_CLASS, all kinds of relations */
	{
		"table", OBJECT_TABLE
	},
	{
		"index", OBJECT_INDEX
	},
	{
		"sequence", OBJECT_SEQUENCE
	},
	{
		"toast table", -1
	},							/* unmapped */
	{
		"view", OBJECT_VIEW
	},
	{
		"materialized view", OBJECT_MATVIEW
	},
	{
		"composite type", -1
	},							/* unmapped */
	{
		"foreign table", OBJECT_FOREIGN_TABLE
	},
	{
		"table column", OBJECT_COLUMN
	},
	{
		"index column", -1
	},							/* unmapped */
	{
		"sequence column", -1
	},							/* unmapped */
	{
		"toast table column", -1
	},							/* unmapped */
	{
		"view column", -1
	},							/* unmapped */
	{
		"materialized view column", -1
	},							/* unmapped */
	{
		"composite type column", -1
	},							/* unmapped */
	{
		"foreign table column", OBJECT_COLUMN
	},
	/* OCLASS_PROC */
	{
		"aggregate", OBJECT_AGGREGATE
	},
	{
		"function", OBJECT_FUNCTION
	},
	/* OCLASS_TYPE */
	{
		"type", OBJECT_TYPE
	},
	/* OCLASS_CAST */
	{
		"cast", OBJECT_CAST
	},
	/* OCLASS_COLLATION */
	{
		"collation", OBJECT_COLLATION
	},
	/* OCLASS_CONSTRAINT */
	{
		"table constraint", OBJECT_TABCONSTRAINT
	},
	{
		"domain constraint", OBJECT_DOMCONSTRAINT
	},
	/* OCLASS_CONVERSION */
	{
		"conversion", OBJECT_CONVERSION
	},
	/* OCLASS_DEFAULT */
	{
		"default value", OBJECT_DEFAULT
	},
	/* OCLASS_LANGUAGE */
	{
		"language", OBJECT_LANGUAGE
	},
	/* OCLASS_LARGEOBJECT */
	{
		"large object", OBJECT_LARGEOBJECT
	},
	/* OCLASS_OPERATOR */
	{
		"operator", OBJECT_OPERATOR
	},
	/* OCLASS_OPCLASS */
	{
		"operator class", OBJECT_OPCLASS
	},
	/* OCLASS_OPFAMILY */
	{
		"operator family", OBJECT_OPFAMILY
	},
	/* OCLASS_AMOP */
	{
		"operator of access method", OBJECT_AMOP
	},
	/* OCLASS_AMPROC */
	{
		"function of access method", OBJECT_AMPROC
	},
	/* OCLASS_REWRITE */
	{
		"rule", OBJECT_RULE
	},
	/* OCLASS_TRIGGER */
	{
		"trigger", OBJECT_TRIGGER
	},
	/* OCLASS_SCHEMA */
	{
		"schema", OBJECT_SCHEMA
	},
	/* OCLASS_TSPARSER */
	{
		"text search parser", OBJECT_TSPARSER
	},
	/* OCLASS_TSDICT */
	{
		"text search dictionary", OBJECT_TSDICTIONARY
	},
	/* OCLASS_TSTEMPLATE */
	{
		"text search template", OBJECT_TSTEMPLATE
	},
	/* OCLASS_TSCONFIG */
	{
		"text search configuration", OBJECT_TSCONFIGURATION
	},
	/* OCLASS_ROLE */
	{
		"role", OBJECT_ROLE
	},
	/* OCLASS_DATABASE */
	{
		"database", OBJECT_DATABASE
	},
	/* OCLASS_TBLSPACE */
	{
		"tablespace", OBJECT_TABLESPACE
	},
	/* OCLASS_FDW */
	{
		"foreign-data wrapper", OBJECT_FDW
	},
	/* OCLASS_FOREIGN_SERVER */
	{
		"server", OBJECT_FOREIGN_SERVER
	},
	/* OCLASS_USER_MAPPING */
	{
		"user mapping", OBJECT_USER_MAPPING
	},
	/* OCLASS_DEFACL */
	{
		"default acl", OBJECT_DEFACL
	},
	/* OCLASS_EXTENSION */
	{
		"extension", OBJECT_EXTENSION
	},
	/* OCLASS_EVENT_TRIGGER */
	{
		"event trigger", OBJECT_EVENT_TRIGGER
	},
	/* OCLASS_POLICY */
	{
		"policy", OBJECT_POLICY
	},
	/* OCLASS_TRANSFORM */
	{
		"transform", OBJECT_TRANSFORM
	}
};

const ObjectAddress InvalidObjectAddress =
{
	InvalidOid,
	InvalidOid,
	0
};

static ObjectAddress get_object_address_unqualified(ObjectType objtype,
							   List *qualname, bool missing_ok);
static ObjectAddress get_relation_by_qualified_name(ObjectType objtype,
							   List *objname, Relation *relp,
							   LOCKMODE lockmode, bool missing_ok);
static ObjectAddress get_object_address_relobject(ObjectType objtype,
							 List *objname, Relation *relp, bool missing_ok);
static ObjectAddress get_object_address_attribute(ObjectType objtype,
							 List *objname, Relation *relp,
							 LOCKMODE lockmode, bool missing_ok);
static ObjectAddress get_object_address_attrdef(ObjectType objtype,
						   List *objname, Relation *relp, LOCKMODE lockmode,
						   bool missing_ok);
static ObjectAddress get_object_address_type(ObjectType objtype,
						ListCell *typecell, bool missing_ok);
static ObjectAddress get_object_address_opcf(ObjectType objtype, List *objname,
						bool missing_ok);
static ObjectAddress get_object_address_opf_member(ObjectType objtype,
							  List *objname, List *objargs, bool missing_ok);

static ObjectAddress get_object_address_usermapping(List *objname,
							   List *objargs, bool missing_ok);
static ObjectAddress get_object_address_defacl(List *objname, List *objargs,
						  bool missing_ok);
static const ObjectPropertyType *get_object_property_data(Oid class_id);

static void getRelationDescription(StringInfo buffer, Oid relid);
static void getOpFamilyDescription(StringInfo buffer, Oid opfid);
static void getRelationTypeDescription(StringInfo buffer, Oid relid,
						   int32 objectSubId);
static void getProcedureTypeDescription(StringInfo buffer, Oid procid);
static void getConstraintTypeDescription(StringInfo buffer, Oid constroid);
static void getOpFamilyIdentity(StringInfo buffer, Oid opfid, List **objname);
static void getRelationIdentity(StringInfo buffer, Oid relid, List **objname);

/*
 * Translate an object name and arguments (as passed by the parser) to an
 * ObjectAddress.
 *
 * The returned object will be locked using the specified lockmode.  If a
 * sub-object is looked up, the parent object will be locked instead.
 *
 * If the object is a relation or a child object of a relation (e.g. an
 * attribute or constraint), the relation is also opened and *relp receives
 * the open relcache entry pointer; otherwise, *relp is set to NULL.  This
 * is a bit grotty but it makes life simpler, since the caller will
 * typically need the relcache entry too.  Caller must close the relcache
 * entry when done with it.  The relation is locked with the specified lockmode
 * if the target object is the relation itself or an attribute, but for other
 * child objects, only AccessShareLock is acquired on the relation.
 *
 * If the object is not found, an error is thrown, unless missing_ok is
 * true.  In this case, no lock is acquired, relp is set to NULL, and the
 * returned address has objectId set to InvalidOid.
 *
 * We don't currently provide a function to release the locks acquired here;
 * typically, the lock must be held until commit to guard against a concurrent
 * drop operation.
 *
 * Note: If the object is not found, we don't give any indication of the
 * reason.  (It might have been a missing schema if the name was qualified, or
 * an inexistant type name in case of a cast, function or operator; etc).
 * Currently there is only one caller that might be interested in such info, so
 * we don't spend much effort here.  If more callers start to care, it might be
 * better to add some support for that in this function.
 */
ObjectAddress
get_object_address(ObjectType objtype, List *objname, List *objargs,
				   Relation *relp, LOCKMODE lockmode, bool missing_ok)
{
	ObjectAddress address;
	ObjectAddress old_address = {InvalidOid, InvalidOid, 0};
	Relation	relation = NULL;
	uint64		inval_count;

	/* Some kind of lock must be taken. */
	Assert(lockmode != NoLock);

	for (;;)
	{
		/*
		 * Remember this value, so that, after looking up the object name and
		 * locking it, we can check whether any invalidation messages have
		 * been processed that might require a do-over.
		 */
		inval_count = SharedInvalidMessageCounter;

		/* Look up object address. */
		switch (objtype)
		{
			case OBJECT_INDEX:
			case OBJECT_SEQUENCE:
			case OBJECT_TABLE:
			case OBJECT_VIEW:
			case OBJECT_MATVIEW:
			case OBJECT_FOREIGN_TABLE:
			case OBJECT_CONTVIEW:
			case OBJECT_CONTTRANSFORM:
				address =
					get_relation_by_qualified_name(objtype, objname,
												   &relation, lockmode,
												   missing_ok);
				break;
			case OBJECT_COLUMN:
				address =
					get_object_address_attribute(objtype, objname,
												 &relation, lockmode,
												 missing_ok);
				break;
			case OBJECT_DEFAULT:
				address =
					get_object_address_attrdef(objtype, objname,
											   &relation, lockmode,
											   missing_ok);
				break;
			case OBJECT_RULE:
			case OBJECT_TRIGGER:
			case OBJECT_TABCONSTRAINT:
			case OBJECT_POLICY:
				address = get_object_address_relobject(objtype, objname,
													   &relation, missing_ok);
				break;
			case OBJECT_DOMCONSTRAINT:
				{
					ObjectAddress domaddr;
					char	   *constrname;

					domaddr = get_object_address_type(OBJECT_DOMAIN,
											 list_head(objname), missing_ok);
					constrname = strVal(linitial(objargs));

					address.classId = ConstraintRelationId;
					address.objectId = get_domain_constraint_oid(domaddr.objectId,
													 constrname, missing_ok);
					address.objectSubId = 0;

				}
				break;
			case OBJECT_DATABASE:
			case OBJECT_EXTENSION:
			case OBJECT_TABLESPACE:
			case OBJECT_ROLE:
			case OBJECT_SCHEMA:
			case OBJECT_LANGUAGE:
			case OBJECT_FDW:
			case OBJECT_FOREIGN_SERVER:
			case OBJECT_EVENT_TRIGGER:
				address = get_object_address_unqualified(objtype,
														 objname, missing_ok);
				break;
			case OBJECT_TYPE:
			case OBJECT_DOMAIN:
				address = get_object_address_type(objtype, list_head(objname), missing_ok);
				break;
			case OBJECT_AGGREGATE:
				address.classId = ProcedureRelationId;
				address.objectId =
					LookupAggNameTypeNames(objname, objargs, missing_ok);
				address.objectSubId = 0;
				break;
			case OBJECT_FUNCTION:
				address.classId = ProcedureRelationId;
				address.objectId =
					LookupFuncNameTypeNames(objname, objargs, missing_ok);
				address.objectSubId = 0;
				break;
			case OBJECT_OPERATOR:
				Assert(list_length(objargs) == 2);
				address.classId = OperatorRelationId;
				address.objectId =
					LookupOperNameTypeNames(NULL, objname,
											(TypeName *) linitial(objargs),
											(TypeName *) lsecond(objargs),
											missing_ok, -1);
				address.objectSubId = 0;
				break;
			case OBJECT_COLLATION:
				address.classId = CollationRelationId;
				address.objectId = get_collation_oid(objname, missing_ok);
				address.objectSubId = 0;
				break;
			case OBJECT_CONVERSION:
				address.classId = ConversionRelationId;
				address.objectId = get_conversion_oid(objname, missing_ok);
				address.objectSubId = 0;
				break;
			case OBJECT_OPCLASS:
			case OBJECT_OPFAMILY:
				address = get_object_address_opcf(objtype, objname, missing_ok);
				break;
			case OBJECT_AMOP:
			case OBJECT_AMPROC:
				address = get_object_address_opf_member(objtype, objname,
														objargs, missing_ok);
				break;
			case OBJECT_LARGEOBJECT:
				Assert(list_length(objname) == 1);
				address.classId = LargeObjectRelationId;
				address.objectId = oidparse(linitial(objname));
				address.objectSubId = 0;
				if (!LargeObjectExists(address.objectId))
				{
					if (!missing_ok)
						ereport(ERROR,
								(errcode(ERRCODE_UNDEFINED_OBJECT),
								 errmsg("large object %u does not exist",
										address.objectId)));
				}
				break;
			case OBJECT_CAST:
				{
					TypeName   *sourcetype = (TypeName *) linitial(objname);
					TypeName   *targettype = (TypeName *) linitial(objargs);
					Oid			sourcetypeid;
					Oid			targettypeid;

					sourcetypeid = LookupTypeNameOid(NULL, sourcetype, missing_ok);
					targettypeid = LookupTypeNameOid(NULL, targettype, missing_ok);
					address.classId = CastRelationId;
					address.objectId =
						get_cast_oid(sourcetypeid, targettypeid, missing_ok);
					address.objectSubId = 0;
				}
				break;
			case OBJECT_TRANSFORM:
				{
					TypeName   *typename = (TypeName *) linitial(objname);
					char	   *langname = strVal(linitial(objargs));
					Oid			type_id = LookupTypeNameOid(NULL, typename, missing_ok);
					Oid			lang_id = get_language_oid(langname, missing_ok);

					address.classId = TransformRelationId;
					address.objectId =
						get_transform_oid(type_id, lang_id, missing_ok);
					address.objectSubId = 0;
				}
				break;
			case OBJECT_TSPARSER:
				address.classId = TSParserRelationId;
				address.objectId = get_ts_parser_oid(objname, missing_ok);
				address.objectSubId = 0;
				break;
			case OBJECT_TSDICTIONARY:
				address.classId = TSDictionaryRelationId;
				address.objectId = get_ts_dict_oid(objname, missing_ok);
				address.objectSubId = 0;
				break;
			case OBJECT_TSTEMPLATE:
				address.classId = TSTemplateRelationId;
				address.objectId = get_ts_template_oid(objname, missing_ok);
				address.objectSubId = 0;
				break;
			case OBJECT_TSCONFIGURATION:
				address.classId = TSConfigRelationId;
				address.objectId = get_ts_config_oid(objname, missing_ok);
				address.objectSubId = 0;
				break;
			case OBJECT_USER_MAPPING:
				address = get_object_address_usermapping(objname, objargs,
														 missing_ok);
				break;
			case OBJECT_DEFACL:
				address = get_object_address_defacl(objname, objargs,
													missing_ok);
				break;
			default:
				elog(ERROR, "unrecognized objtype: %d", (int) objtype);
				/* placate compiler, in case it thinks elog might return */
				address.classId = InvalidOid;
				address.objectId = InvalidOid;
				address.objectSubId = 0;
		}

		/*
		 * If we could not find the supplied object, return without locking.
		 */
		if (!OidIsValid(address.objectId))
		{
			Assert(missing_ok);
			return address;
		}

		/*
		 * If we're retrying, see if we got the same answer as last time.  If
		 * so, we're done; if not, we locked the wrong thing, so give up our
		 * lock.
		 */
		if (OidIsValid(old_address.classId))
		{
			if (old_address.classId == address.classId
				&& old_address.objectId == address.objectId
				&& old_address.objectSubId == address.objectSubId)
				break;
			if (old_address.classId != RelationRelationId)
			{
				if (IsSharedRelation(old_address.classId))
					UnlockSharedObject(old_address.classId,
									   old_address.objectId,
									   0, lockmode);
				else
					UnlockDatabaseObject(old_address.classId,
										 old_address.objectId,
										 0, lockmode);
			}
		}

		/*
		 * If we're dealing with a relation or attribute, then the relation is
		 * already locked.  Otherwise, we lock it now.
		 */
		if (address.classId != RelationRelationId)
		{
			if (IsSharedRelation(address.classId))
				LockSharedObject(address.classId, address.objectId, 0,
								 lockmode);
			else
				LockDatabaseObject(address.classId, address.objectId, 0,
								   lockmode);
		}

		/*
		 * At this point, we've resolved the name to an OID and locked the
		 * corresponding database object.  However, it's possible that by the
		 * time we acquire the lock on the object, concurrent DDL has modified
		 * the database in such a way that the name we originally looked up no
		 * longer resolves to that OID.
		 *
		 * We can be certain that this isn't an issue if (a) no shared
		 * invalidation messages have been processed or (b) we've locked a
		 * relation somewhere along the line.  All the relation name lookups
		 * in this module ultimately use RangeVarGetRelid() to acquire a
		 * relation lock, and that function protects against the same kinds of
		 * races we're worried about here.  Even when operating on a
		 * constraint, rule, or trigger, we still acquire AccessShareLock on
		 * the relation, which is enough to freeze out any concurrent DDL.
		 *
		 * In all other cases, however, it's possible that the name we looked
		 * up no longer refers to the object we locked, so we retry the lookup
		 * and see whether we get the same answer.
		 */
		if (inval_count == SharedInvalidMessageCounter || relation != NULL)
			break;
		old_address = address;
	}

	/* Return the object address and the relation. */
	*relp = relation;
	return address;
}

/*
 * Find an ObjectAddress for a type of object that is identified by an
 * unqualified name.
 */
static ObjectAddress
get_object_address_unqualified(ObjectType objtype,
							   List *qualname, bool missing_ok)
{
	const char *name;
	ObjectAddress address;

	/*
	 * The types of names handled by this function are not permitted to be
	 * schema-qualified or catalog-qualified.
	 */
	if (list_length(qualname) != 1)
	{
		const char *msg;

		switch (objtype)
		{
			case OBJECT_DATABASE:
				msg = gettext_noop("database name cannot be qualified");
				break;
			case OBJECT_EXTENSION:
				msg = gettext_noop("extension name cannot be qualified");
				break;
			case OBJECT_TABLESPACE:
				msg = gettext_noop("tablespace name cannot be qualified");
				break;
			case OBJECT_ROLE:
				msg = gettext_noop("role name cannot be qualified");
				break;
			case OBJECT_SCHEMA:
				msg = gettext_noop("schema name cannot be qualified");
				break;
			case OBJECT_LANGUAGE:
				msg = gettext_noop("language name cannot be qualified");
				break;
			case OBJECT_FDW:
				msg = gettext_noop("foreign-data wrapper name cannot be qualified");
				break;
			case OBJECT_FOREIGN_SERVER:
				msg = gettext_noop("server name cannot be qualified");
				break;
			case OBJECT_EVENT_TRIGGER:
				msg = gettext_noop("event trigger name cannot be qualified");
				break;
			default:
				elog(ERROR, "unrecognized objtype: %d", (int) objtype);
				msg = NULL;		/* placate compiler */
		}
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("%s", _(msg))));
	}

	/* Format is valid, extract the actual name. */
	name = strVal(linitial(qualname));

	/* Translate name to OID. */
	switch (objtype)
	{
		case OBJECT_DATABASE:
			address.classId = DatabaseRelationId;
			address.objectId = get_database_oid(name, missing_ok);
			address.objectSubId = 0;
			break;
		case OBJECT_EXTENSION:
			address.classId = ExtensionRelationId;
			address.objectId = get_extension_oid(name, missing_ok);
			address.objectSubId = 0;
			break;
		case OBJECT_TABLESPACE:
			address.classId = TableSpaceRelationId;
			address.objectId = get_tablespace_oid(name, missing_ok);
			address.objectSubId = 0;
			break;
		case OBJECT_ROLE:
			address.classId = AuthIdRelationId;
			address.objectId = get_role_oid(name, missing_ok);
			address.objectSubId = 0;
			break;
		case OBJECT_SCHEMA:
			address.classId = NamespaceRelationId;
			address.objectId = get_namespace_oid(name, missing_ok);
			address.objectSubId = 0;
			break;
		case OBJECT_LANGUAGE:
			address.classId = LanguageRelationId;
			address.objectId = get_language_oid(name, missing_ok);
			address.objectSubId = 0;
			break;
		case OBJECT_FDW:
			address.classId = ForeignDataWrapperRelationId;
			address.objectId = get_foreign_data_wrapper_oid(name, missing_ok);
			address.objectSubId = 0;
			break;
		case OBJECT_FOREIGN_SERVER:
			address.classId = ForeignServerRelationId;
			address.objectId = get_foreign_server_oid(name, missing_ok);
			address.objectSubId = 0;
			break;
		case OBJECT_EVENT_TRIGGER:
			address.classId = EventTriggerRelationId;
			address.objectId = get_event_trigger_oid(name, missing_ok);
			address.objectSubId = 0;
			break;
		default:
			elog(ERROR, "unrecognized objtype: %d", (int) objtype);
			/* placate compiler, which doesn't know elog won't return */
			address.classId = InvalidOid;
			address.objectId = InvalidOid;
			address.objectSubId = 0;
	}

	return address;
}

/*
 * Locate a relation by qualified name.
 */
static ObjectAddress
get_relation_by_qualified_name(ObjectType objtype, List *objname,
							   Relation *relp, LOCKMODE lockmode,
							   bool missing_ok)
{
	Relation	relation;
	ObjectAddress address;

	address.classId = RelationRelationId;
	address.objectId = InvalidOid;
	address.objectSubId = 0;

	relation = relation_openrv_extended(makeRangeVarFromNameList(objname),
										lockmode, missing_ok);
	if (!relation)
		return address;

	switch (objtype)
	{
		case OBJECT_INDEX:
			if (relation->rd_rel->relkind != RELKIND_INDEX)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("\"%s\" is not an index",
								RelationGetRelationName(relation))));
			break;
		case OBJECT_SEQUENCE:
			if (relation->rd_rel->relkind != RELKIND_SEQUENCE)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("\"%s\" is not a sequence",
								RelationGetRelationName(relation))));
			break;
		case OBJECT_TABLE:
			if (relation->rd_rel->relkind != RELKIND_RELATION)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("\"%s\" is not a table",
								RelationGetRelationName(relation))));
			break;
		case OBJECT_CONTVIEW:
			if (relation->rd_rel->relkind != RELKIND_CONTVIEW)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("\"%s\" is not a continuous view",
								RelationGetRelationName(relation))));
			break;
		case OBJECT_VIEW:
			if (relation->rd_rel->relkind != RELKIND_VIEW)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("\"%s\" is not a view",
								RelationGetRelationName(relation))));
			break;
		case OBJECT_MATVIEW:
			if (relation->rd_rel->relkind != RELKIND_MATVIEW)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("\"%s\" is not a materialized view",
								RelationGetRelationName(relation))));
			break;
		case OBJECT_FOREIGN_TABLE:
			if (relation->rd_rel->relkind != RELKIND_FOREIGN_TABLE)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("\"%s\" is not a foreign table",
								RelationGetRelationName(relation))));
			break;
		case OBJECT_CONTTRANSFORM:
			if (relation->rd_rel->relkind != RELKIND_CONTTRANSFORM)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("\"%s\" is not a continuous transform",
								RelationGetRelationName(relation))));
			break;
		default:
			elog(ERROR, "unrecognized objtype: %d", (int) objtype);
			break;
	}

	/* Done. */
	address.objectId = RelationGetRelid(relation);
	*relp = relation;

	return address;
}

/*
 * Find object address for an object that is attached to a relation.
 *
 * Note that we take only an AccessShareLock on the relation.  We need not
 * pass down the LOCKMODE from get_object_address(), because that is the lock
 * mode for the object itself, not the relation to which it is attached.
 */
static ObjectAddress
get_object_address_relobject(ObjectType objtype, List *objname,
							 Relation *relp, bool missing_ok)
{
	ObjectAddress address;
	Relation	relation = NULL;
	int			nnames;
	const char *depname;

	/* Extract name of dependent object. */
	depname = strVal(llast(objname));

	/* Separate relation name from dependent object name. */
	nnames = list_length(objname);
	if (nnames < 2)
	{
		Oid			reloid;

		/*
		 * For compatibility with very old releases, we sometimes allow users
		 * to attempt to specify a rule without mentioning the relation name.
		 * If there's only rule by that name in the entire database, this will
		 * work.  But objects other than rules don't get this special
		 * treatment.
		 */
		if (objtype != OBJECT_RULE)
			elog(ERROR, "must specify relation and object name");
		address.classId = RewriteRelationId;
		address.objectId =
			get_rewrite_oid_without_relid(depname, &reloid, missing_ok);
		address.objectSubId = 0;

		/*
		 * Caller is expecting to get back the relation, even though we didn't
		 * end up using it to find the rule.
		 */
		if (OidIsValid(address.objectId))
			relation = heap_open(reloid, AccessShareLock);
	}
	else
	{
		List	   *relname;
		Oid			reloid;

		/* Extract relation name and open relation. */
		relname = list_truncate(list_copy(objname), nnames - 1);
		relation = heap_openrv_extended(makeRangeVarFromNameList(relname),
										AccessShareLock,
										missing_ok);

		reloid = relation ? RelationGetRelid(relation) : InvalidOid;

		switch (objtype)
		{
			case OBJECT_RULE:
				address.classId = RewriteRelationId;
				address.objectId = relation ?
					get_rewrite_oid(reloid, depname, missing_ok) : InvalidOid;
				address.objectSubId = 0;
				break;
			case OBJECT_TRIGGER:
				address.classId = TriggerRelationId;
				address.objectId = relation ?
					get_trigger_oid(reloid, depname, missing_ok) : InvalidOid;
				address.objectSubId = 0;
				break;
			case OBJECT_TABCONSTRAINT:
				address.classId = ConstraintRelationId;
				address.objectId = relation ?
					get_relation_constraint_oid(reloid, depname, missing_ok) :
					InvalidOid;
				address.objectSubId = 0;
				break;
			case OBJECT_POLICY:
				address.classId = PolicyRelationId;
				address.objectId = relation ?
					get_relation_policy_oid(reloid, depname, missing_ok) :
					InvalidOid;
				address.objectSubId = 0;
				break;
			default:
				elog(ERROR, "unrecognized objtype: %d", (int) objtype);
				/* placate compiler, which doesn't know elog won't return */
				address.classId = InvalidOid;
				address.objectId = InvalidOid;
				address.objectSubId = 0;
		}

		/* Avoid relcache leak when object not found. */
		if (!OidIsValid(address.objectId))
		{
			if (relation != NULL)
				heap_close(relation, AccessShareLock);

			relation = NULL;	/* department of accident prevention */
			return address;
		}
	}

	/* Done. */
	*relp = relation;
	return address;
}

/*
 * Find the ObjectAddress for an attribute.
 */
static ObjectAddress
get_object_address_attribute(ObjectType objtype, List *objname,
							 Relation *relp, LOCKMODE lockmode,
							 bool missing_ok)
{
	ObjectAddress address;
	List	   *relname;
	Oid			reloid;
	Relation	relation;
	const char *attname;
	AttrNumber	attnum;

	/* Extract relation name and open relation. */
	if (list_length(objname) < 2)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("column name must be qualified")));
	attname = strVal(lfirst(list_tail(objname)));
	relname = list_truncate(list_copy(objname), list_length(objname) - 1);
	/* XXX no missing_ok support here */
	relation = relation_openrv(makeRangeVarFromNameList(relname), lockmode);
	reloid = RelationGetRelid(relation);

	/* Look up attribute and construct return value. */
	attnum = get_attnum(reloid, attname);
	if (attnum == InvalidAttrNumber)
	{
		if (!missing_ok)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" of relation \"%s\" does not exist",
							attname, NameListToString(relname))));

		address.classId = RelationRelationId;
		address.objectId = InvalidOid;
		address.objectSubId = InvalidAttrNumber;
		relation_close(relation, lockmode);
		return address;
	}

	address.classId = RelationRelationId;
	address.objectId = reloid;
	address.objectSubId = attnum;

	*relp = relation;
	return address;
}

/*
 * Find the ObjectAddress for an attribute's default value.
 */
static ObjectAddress
get_object_address_attrdef(ObjectType objtype, List *objname,
						   Relation *relp, LOCKMODE lockmode,
						   bool missing_ok)
{
	ObjectAddress address;
	List	   *relname;
	Oid			reloid;
	Relation	relation;
	const char *attname;
	AttrNumber	attnum;
	TupleDesc	tupdesc;
	Oid			defoid;

	/* Extract relation name and open relation. */
	if (list_length(objname) < 2)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("column name must be qualified")));
	attname = strVal(llast(objname));
	relname = list_truncate(list_copy(objname), list_length(objname) - 1);
	/* XXX no missing_ok support here */
	relation = relation_openrv(makeRangeVarFromNameList(relname), lockmode);
	reloid = RelationGetRelid(relation);

	tupdesc = RelationGetDescr(relation);

	/* Look up attribute number and scan pg_attrdef to find its tuple */
	attnum = get_attnum(reloid, attname);
	defoid = InvalidOid;
	if (attnum != InvalidAttrNumber && tupdesc->constr != NULL)
	{
		Relation	attrdef;
		ScanKeyData keys[2];
		SysScanDesc scan;
		HeapTuple	tup;

		attrdef = relation_open(AttrDefaultRelationId, AccessShareLock);
		ScanKeyInit(&keys[0],
					Anum_pg_attrdef_adrelid,
					BTEqualStrategyNumber,
					F_OIDEQ,
					ObjectIdGetDatum(reloid));
		ScanKeyInit(&keys[1],
					Anum_pg_attrdef_adnum,
					BTEqualStrategyNumber,
					F_INT2EQ,
					Int16GetDatum(attnum));
		scan = systable_beginscan(attrdef, AttrDefaultIndexId, true,
								  NULL, 2, keys);
		if (HeapTupleIsValid(tup = systable_getnext(scan)))
			defoid = HeapTupleGetOid(tup);

		systable_endscan(scan);
		relation_close(attrdef, AccessShareLock);
	}
	if (!OidIsValid(defoid))
	{
		if (!missing_ok)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("default value for column \"%s\" of relation \"%s\" does not exist",
							attname, NameListToString(relname))));

		address.classId = AttrDefaultRelationId;
		address.objectId = InvalidOid;
		address.objectSubId = InvalidAttrNumber;
		relation_close(relation, lockmode);
		return address;
	}

	address.classId = AttrDefaultRelationId;
	address.objectId = defoid;
	address.objectSubId = 0;

	*relp = relation;
	return address;
}

/*
 * Find the ObjectAddress for a type or domain
 */
static ObjectAddress
get_object_address_type(ObjectType objtype, ListCell *typecell, bool missing_ok)
{
	ObjectAddress address;
	TypeName   *typename;
	Type		tup;

	typename = (TypeName *) lfirst(typecell);

	address.classId = TypeRelationId;
	address.objectId = InvalidOid;
	address.objectSubId = 0;

	tup = LookupTypeName(NULL, typename, NULL, missing_ok);
	if (!HeapTupleIsValid(tup))
	{
		if (!missing_ok)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("type \"%s\" does not exist",
							TypeNameToString(typename))));
		return address;
	}
	address.objectId = typeTypeId(tup);

	if (objtype == OBJECT_DOMAIN)
	{
		if (((Form_pg_type) GETSTRUCT(tup))->typtype != TYPTYPE_DOMAIN)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is not a domain",
							TypeNameToString(typename))));
	}

	ReleaseSysCache(tup);

	return address;
}

/*
 * Find the ObjectAddress for an opclass or opfamily.
 */
static ObjectAddress
get_object_address_opcf(ObjectType objtype, List *objname, bool missing_ok)
{
	Oid			amoid;
	ObjectAddress address;

	/* XXX no missing_ok support here */
	amoid = get_am_oid(strVal(linitial(objname)), false);
	objname = list_copy_tail(objname, 1);

	switch (objtype)
	{
		case OBJECT_OPCLASS:
			address.classId = OperatorClassRelationId;
			address.objectId = get_opclass_oid(amoid, objname, missing_ok);
			address.objectSubId = 0;
			break;
		case OBJECT_OPFAMILY:
			address.classId = OperatorFamilyRelationId;
			address.objectId = get_opfamily_oid(amoid, objname, missing_ok);
			address.objectSubId = 0;
			break;
		default:
			elog(ERROR, "unrecognized objtype: %d", (int) objtype);
			/* placate compiler, which doesn't know elog won't return */
			address.classId = InvalidOid;
			address.objectId = InvalidOid;
			address.objectSubId = 0;
	}

	return address;
}

/*
 * Find the ObjectAddress for an opclass/opfamily member.
 *
 * (The returned address corresponds to a pg_amop/pg_amproc object).
 */
static ObjectAddress
get_object_address_opf_member(ObjectType objtype,
							  List *objname, List *objargs, bool missing_ok)
{
	ObjectAddress famaddr;
	ObjectAddress address;
	ListCell   *cell;
	List	   *copy;
	char	   *typenames[2];
	Oid			typeoids[2];
	int			membernum;
	int			i;

	/*
	 * The last element of the objname list contains the strategy or procedure
	 * number.  We need to strip that out before getting the opclass/family
	 * address.  The rest can be used directly by get_object_address_opcf().
	 */
	membernum = atoi(strVal(llast(objname)));
	copy = list_truncate(list_copy(objname), list_length(objname) - 1);

	/* no missing_ok support here */
	famaddr = get_object_address_opcf(OBJECT_OPFAMILY, copy, false);

	/* find out left/right type names and OIDs */
	i = 0;
	foreach(cell, objargs)
	{
		ObjectAddress typaddr;

		typenames[i] = strVal(lfirst(cell));
		typaddr = get_object_address_type(OBJECT_TYPE, cell, missing_ok);
		typeoids[i] = typaddr.objectId;
		if (++i >= 2)
			break;
	}

	switch (objtype)
	{
		case OBJECT_AMOP:
			{
				HeapTuple	tp;

				ObjectAddressSet(address, AccessMethodOperatorRelationId,
								 InvalidOid);

				tp = SearchSysCache4(AMOPSTRATEGY,
									 ObjectIdGetDatum(famaddr.objectId),
									 ObjectIdGetDatum(typeoids[0]),
									 ObjectIdGetDatum(typeoids[1]),
									 Int16GetDatum(membernum));
				if (!HeapTupleIsValid(tp))
				{
					if (!missing_ok)
						ereport(ERROR,
								(errcode(ERRCODE_UNDEFINED_OBJECT),
						  errmsg("operator %d (%s, %s) of %s does not exist",
								 membernum, typenames[0], typenames[1],
								 getObjectDescription(&famaddr))));
				}
				else
				{
					address.objectId = HeapTupleGetOid(tp);
					ReleaseSysCache(tp);
				}
			}
			break;

		case OBJECT_AMPROC:
			{
				HeapTuple	tp;

				ObjectAddressSet(address, AccessMethodProcedureRelationId,
								 InvalidOid);

				tp = SearchSysCache4(AMPROCNUM,
									 ObjectIdGetDatum(famaddr.objectId),
									 ObjectIdGetDatum(typeoids[0]),
									 ObjectIdGetDatum(typeoids[1]),
									 Int16GetDatum(membernum));
				if (!HeapTupleIsValid(tp))
				{
					if (!missing_ok)
						ereport(ERROR,
								(errcode(ERRCODE_UNDEFINED_OBJECT),
						  errmsg("function %d (%s, %s) of %s does not exist",
								 membernum, typenames[0], typenames[1],
								 getObjectDescription(&famaddr))));
				}
				else
				{
					address.objectId = HeapTupleGetOid(tp);
					ReleaseSysCache(tp);
				}
			}
			break;
		default:
			elog(ERROR, "unrecognized objtype: %d", (int) objtype);
	}

	return address;
}

/*
 * Find the ObjectAddress for a user mapping.
 */
static ObjectAddress
get_object_address_usermapping(List *objname, List *objargs, bool missing_ok)
{
	ObjectAddress address;
	Oid			userid;
	char	   *username;
	char	   *servername;
	ForeignServer *server;
	HeapTuple	tp;

	ObjectAddressSet(address, UserMappingRelationId, InvalidOid);

	/* fetch string names from input lists, for error messages */
	username = strVal(linitial(objname));
	servername = strVal(linitial(objargs));

	/* look up pg_authid OID of mapped user; InvalidOid if PUBLIC */
	if (strcmp(username, "public") == 0)
		userid = InvalidOid;
	else
	{
		tp = SearchSysCache1(AUTHNAME,
							 CStringGetDatum(username));
		if (!HeapTupleIsValid(tp))
		{
			if (!missing_ok)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("user mapping for user \"%s\" on server \"%s\" does not exist",
								username, servername)));
			return address;
		}
		userid = HeapTupleGetOid(tp);
		ReleaseSysCache(tp);
	}

	/* Now look up the pg_user_mapping tuple */
	server = GetForeignServerByName(servername, true);
	if (!server)
	{
		if (!missing_ok)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("server \"%s\" does not exist", servername)));
		return address;
	}
	tp = SearchSysCache2(USERMAPPINGUSERSERVER,
						 ObjectIdGetDatum(userid),
						 ObjectIdGetDatum(server->serverid));
	if (!HeapTupleIsValid(tp))
	{
		if (!missing_ok)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("user mapping for user \"%s\" on server \"%s\" does not exist",
							username, servername)));
		return address;
	}

	address.objectId = HeapTupleGetOid(tp);

	ReleaseSysCache(tp);

	return address;
}

/*
 * Find the ObjectAddress for a default ACL.
 */
static ObjectAddress
get_object_address_defacl(List *objname, List *objargs, bool missing_ok)
{
	HeapTuple	tp;
	Oid			userid;
	Oid			schemaid;
	char	   *username;
	char	   *schema;
	char		objtype;
	char	   *objtype_str;
	ObjectAddress address;

	ObjectAddressSet(address, DefaultAclRelationId, InvalidOid);

	/*
	 * First figure out the textual attributes so that they can be used for
	 * error reporting.
	 */
	username = strVal(linitial(objname));
	if (list_length(objname) >= 2)
		schema = (char *) strVal(lsecond(objname));
	else
		schema = NULL;

	/*
	 * Decode defaclobjtype.  Only first char is considered; the rest of the
	 * string, if any, is blissfully ignored.
	 */
	objtype = ((char *) strVal(linitial(objargs)))[0];
	switch (objtype)
	{
		case DEFACLOBJ_RELATION:
			objtype_str = "tables";
			break;
		case DEFACLOBJ_SEQUENCE:
			objtype_str = "sequences";
			break;
		case DEFACLOBJ_FUNCTION:
			objtype_str = "functions";
			break;
		case DEFACLOBJ_TYPE:
			objtype_str = "types";
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				  errmsg("unrecognized default ACL object type %c", objtype),
				 errhint("Valid object types are \"r\", \"S\", \"f\", and \"T\".")));
	}

	/*
	 * Look up user ID.  Behave as "default ACL not found" if the user doesn't
	 * exist.
	 */
	tp = SearchSysCache1(AUTHNAME,
						 CStringGetDatum(username));
	if (!HeapTupleIsValid(tp))
		goto not_found;
	userid = HeapTupleGetOid(tp);
	ReleaseSysCache(tp);

	/*
	 * If a schema name was given, look up its OID.  If it doesn't exist,
	 * behave as "default ACL not found".
	 */
	if (schema)
	{
		schemaid = get_namespace_oid(schema, true);
		if (schemaid == InvalidOid)
			goto not_found;
	}
	else
		schemaid = InvalidOid;

	/* Finally, look up the pg_default_acl object */
	tp = SearchSysCache3(DEFACLROLENSPOBJ,
						 ObjectIdGetDatum(userid),
						 ObjectIdGetDatum(schemaid),
						 CharGetDatum(objtype));
	if (!HeapTupleIsValid(tp))
		goto not_found;

	address.objectId = HeapTupleGetOid(tp);
	ReleaseSysCache(tp);

	return address;

not_found:
	if (!missing_ok)
	{
		if (schema)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("default ACL for user \"%s\" in schema \"%s\" on %s does not exist",
							username, schema, objtype_str)));
		else
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
				   errmsg("default ACL for user \"%s\" on %s does not exist",
						  username, objtype_str)));
	}
	return address;
}

/*
 * Convert an array of TEXT into a List of string Values, as emitted by the
 * parser, which is what get_object_address uses as input.
 */
static List *
textarray_to_strvaluelist(ArrayType *arr)
{
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;
	List	   *list = NIL;
	int			i;

	deconstruct_array(arr, TEXTOID, -1, false, 'i',
					  &elems, &nulls, &nelems);

	for (i = 0; i < nelems; i++)
	{
		if (nulls[i])
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("name or argument lists may not contain nulls")));
		list = lappend(list, makeString(TextDatumGetCString(elems[i])));
	}

	return list;
}


/*
 * SQL-callable version of get_object_address
 */
Datum
pg_get_object_address(PG_FUNCTION_ARGS)
{
	char	   *ttype = TextDatumGetCString(PG_GETARG_DATUM(0));
	ArrayType  *namearr = PG_GETARG_ARRAYTYPE_P(1);
	ArrayType  *argsarr = PG_GETARG_ARRAYTYPE_P(2);
	int			itype;
	ObjectType	type;
	List	   *name;
	List	   *args;
	ObjectAddress addr;
	TupleDesc	tupdesc;
	Datum		values[3];
	bool		nulls[3];
	HeapTuple	htup;
	Relation	relation;

	/* Decode object type, raise error if unknown */
	itype = read_objtype_from_string(ttype);
	if (itype < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unsupported object type \"%s\"", ttype)));
	type = (ObjectType) itype;

	/*
	 * Convert the text array to the representation appropriate for the given
	 * object type.  Most use a simple string Values list, but there are some
	 * exceptions.
	 */
	if (type == OBJECT_TYPE || type == OBJECT_DOMAIN || type == OBJECT_CAST ||
		type == OBJECT_TRANSFORM || type == OBJECT_DOMCONSTRAINT)
	{
		Datum	   *elems;
		bool	   *nulls;
		int			nelems;

		deconstruct_array(namearr, TEXTOID, -1, false, 'i',
						  &elems, &nulls, &nelems);
		if (nelems != 1)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("name list length must be exactly %d", 1)));
		if (nulls[0])
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("name or argument lists may not contain nulls")));
		name = list_make1(typeStringToTypeName(TextDatumGetCString(elems[0])));
	}
	else if (type == OBJECT_LARGEOBJECT)
	{
		Datum	   *elems;
		bool	   *nulls;
		int			nelems;

		deconstruct_array(namearr, TEXTOID, -1, false, 'i',
						  &elems, &nulls, &nelems);
		if (nelems != 1)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("name list length must be exactly %d", 1)));
		if (nulls[0])
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("large object OID may not be null")));
		name = list_make1(makeFloat(TextDatumGetCString(elems[0])));
	}
	else
	{
		name = textarray_to_strvaluelist(namearr);
		if (list_length(name) < 1)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("name list length must be at least %d", 1)));
	}

	/*
	 * If args are given, decode them according to the object type.
	 */
	if (type == OBJECT_AGGREGATE ||
		type == OBJECT_FUNCTION ||
		type == OBJECT_OPERATOR ||
		type == OBJECT_CAST ||
		type == OBJECT_AMOP ||
		type == OBJECT_AMPROC)
	{
		/* in these cases, the args list must be of TypeName */
		Datum	   *elems;
		bool	   *nulls;
		int			nelems;
		int			i;

		deconstruct_array(argsarr, TEXTOID, -1, false, 'i',
						  &elems, &nulls, &nelems);

		args = NIL;
		for (i = 0; i < nelems; i++)
		{
			if (nulls[i])
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("name or argument lists may not contain nulls")));
			args = lappend(args,
						typeStringToTypeName(TextDatumGetCString(elems[i])));
		}
	}
	else
	{
		/* For all other object types, use string Values */
		args = textarray_to_strvaluelist(argsarr);
	}

	/*
	 * get_object_name is pretty sensitive to the length its input lists;
	 * check that they're what it wants.
	 */
	switch (type)
	{
		case OBJECT_DOMCONSTRAINT:
		case OBJECT_CAST:
		case OBJECT_USER_MAPPING:
		case OBJECT_DEFACL:
		case OBJECT_TRANSFORM:
			if (list_length(args) != 1)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					  errmsg("argument list length must be exactly %d", 1)));
			break;
		case OBJECT_OPFAMILY:
		case OBJECT_OPCLASS:
			if (list_length(name) < 2)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("name list length must be at least %d", 2)));
			break;
		case OBJECT_AMOP:
		case OBJECT_AMPROC:
			if (list_length(name) < 3)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("name list length must be at least %d", 3)));
			/* fall through to check args length */
		case OBJECT_OPERATOR:
			if (list_length(args) != 2)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					  errmsg("argument list length must be exactly %d", 2)));
			break;
		default:
			break;
	}

	addr = get_object_address(type, name, args,
							  &relation, AccessShareLock, false);

	/* We don't need the relcache entry, thank you very much */
	if (relation)
		relation_close(relation, AccessShareLock);

	tupdesc = CreateTemplateTupleDesc(3, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "classid",
					   OIDOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "objid",
					   OIDOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "objsubid",
					   INT4OID, -1, 0);
	tupdesc = BlessTupleDesc(tupdesc);

	values[0] = ObjectIdGetDatum(addr.classId);
	values[1] = ObjectIdGetDatum(addr.objectId);
	values[2] = Int32GetDatum(addr.objectSubId);
	nulls[0] = false;
	nulls[1] = false;
	nulls[2] = false;

	htup = heap_form_tuple(tupdesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}

/*
 * Check ownership of an object previously identified by get_object_address.
 */
void
check_object_ownership(Oid roleid, ObjectType objtype, ObjectAddress address,
					   List *objname, List *objargs, Relation relation)
{
	switch (objtype)
	{
		case OBJECT_INDEX:
		case OBJECT_SEQUENCE:
		case OBJECT_TABLE:
		case OBJECT_VIEW:
		case OBJECT_MATVIEW:
		case OBJECT_FOREIGN_TABLE:
		case OBJECT_COLUMN:
		case OBJECT_RULE:
		case OBJECT_TRIGGER:
		case OBJECT_POLICY:
		case OBJECT_TABCONSTRAINT:
			if (!pg_class_ownercheck(RelationGetRelid(relation), roleid))
				aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS,
							   RelationGetRelationName(relation));
			break;
		case OBJECT_DATABASE:
			if (!pg_database_ownercheck(address.objectId, roleid))
				aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_DATABASE,
							   NameListToString(objname));
			break;
		case OBJECT_TYPE:
		case OBJECT_DOMAIN:
		case OBJECT_ATTRIBUTE:
		case OBJECT_DOMCONSTRAINT:
			if (!pg_type_ownercheck(address.objectId, roleid))
				aclcheck_error_type(ACLCHECK_NOT_OWNER, address.objectId);
			break;
		case OBJECT_AGGREGATE:
		case OBJECT_FUNCTION:
			if (!pg_proc_ownercheck(address.objectId, roleid))
				aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PROC,
							   NameListToString(objname));
			break;
		case OBJECT_OPERATOR:
			if (!pg_oper_ownercheck(address.objectId, roleid))
				aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_OPER,
							   NameListToString(objname));
			break;
		case OBJECT_SCHEMA:
			if (!pg_namespace_ownercheck(address.objectId, roleid))
				aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_NAMESPACE,
							   NameListToString(objname));
			break;
		case OBJECT_COLLATION:
			if (!pg_collation_ownercheck(address.objectId, roleid))
				aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_COLLATION,
							   NameListToString(objname));
			break;
		case OBJECT_CONVERSION:
			if (!pg_conversion_ownercheck(address.objectId, roleid))
				aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CONVERSION,
							   NameListToString(objname));
			break;
		case OBJECT_EXTENSION:
			if (!pg_extension_ownercheck(address.objectId, roleid))
				aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_EXTENSION,
							   NameListToString(objname));
			break;
		case OBJECT_FDW:
			if (!pg_foreign_data_wrapper_ownercheck(address.objectId, roleid))
				aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_FDW,
							   NameListToString(objname));
			break;
		case OBJECT_FOREIGN_SERVER:
			if (!pg_foreign_server_ownercheck(address.objectId, roleid))
				aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_FOREIGN_SERVER,
							   NameListToString(objname));
			break;
		case OBJECT_EVENT_TRIGGER:
			if (!pg_event_trigger_ownercheck(address.objectId, roleid))
				aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_EVENT_TRIGGER,
							   NameListToString(objname));
			break;
		case OBJECT_LANGUAGE:
			if (!pg_language_ownercheck(address.objectId, roleid))
				aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_LANGUAGE,
							   NameListToString(objname));
			break;
		case OBJECT_OPCLASS:
			if (!pg_opclass_ownercheck(address.objectId, roleid))
				aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_OPCLASS,
							   NameListToString(objname));
			break;
		case OBJECT_OPFAMILY:
			if (!pg_opfamily_ownercheck(address.objectId, roleid))
				aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_OPFAMILY,
							   NameListToString(objname));
			break;
		case OBJECT_LARGEOBJECT:
			if (!lo_compat_privileges &&
				!pg_largeobject_ownercheck(address.objectId, roleid))
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("must be owner of large object %u",
								address.objectId)));
			break;
		case OBJECT_CAST:
			{
				/* We can only check permissions on the source/target types */
				TypeName   *sourcetype = (TypeName *) linitial(objname);
				TypeName   *targettype = (TypeName *) linitial(objargs);
				Oid			sourcetypeid = typenameTypeId(NULL, sourcetype);
				Oid			targettypeid = typenameTypeId(NULL, targettype);

				if (!pg_type_ownercheck(sourcetypeid, roleid)
					&& !pg_type_ownercheck(targettypeid, roleid))
					ereport(ERROR,
							(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
							 errmsg("must be owner of type %s or type %s",
									format_type_be(sourcetypeid),
									format_type_be(targettypeid))));
			}
			break;
		case OBJECT_TRANSFORM:
			{
				TypeName   *typename = (TypeName *) linitial(objname);
				Oid			typeid = typenameTypeId(NULL, typename);

				if (!pg_type_ownercheck(typeid, roleid))
					aclcheck_error_type(ACLCHECK_NOT_OWNER, typeid);
			}
			break;
		case OBJECT_TABLESPACE:
			if (!pg_tablespace_ownercheck(address.objectId, roleid))
				aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_TABLESPACE,
							   NameListToString(objname));
			break;
		case OBJECT_TSDICTIONARY:
			if (!pg_ts_dict_ownercheck(address.objectId, roleid))
				aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_TSDICTIONARY,
							   NameListToString(objname));
			break;
		case OBJECT_TSCONFIGURATION:
			if (!pg_ts_config_ownercheck(address.objectId, roleid))
				aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_TSCONFIGURATION,
							   NameListToString(objname));
			break;
		case OBJECT_ROLE:

			/*
			 * We treat roles as being "owned" by those with CREATEROLE priv,
			 * except that superusers are only owned by superusers.
			 */
			if (superuser_arg(address.objectId))
			{
				if (!superuser_arg(roleid))
					ereport(ERROR,
							(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
							 errmsg("must be superuser")));
			}
			else
			{
				if (!has_createrole_privilege(roleid))
					ereport(ERROR,
							(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
							 errmsg("must have CREATEROLE privilege")));
			}
			break;
		case OBJECT_TSPARSER:
		case OBJECT_TSTEMPLATE:
			/* We treat these object types as being owned by superusers */
			if (!superuser_arg(roleid))
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("must be superuser")));
			break;
		default:
			elog(ERROR, "unrecognized object type: %d",
				 (int) objtype);
	}
}

/*
 * get_object_namespace
 *
 * Find the schema containing the specified object.  For non-schema objects,
 * this function returns InvalidOid.
 */
Oid
get_object_namespace(const ObjectAddress *address)
{
	int			cache;
	HeapTuple	tuple;
	bool		isnull;
	Oid			oid;
	const ObjectPropertyType *property;

	/* If not owned by a namespace, just return InvalidOid. */
	property = get_object_property_data(address->classId);
	if (property->attnum_namespace == InvalidAttrNumber)
		return InvalidOid;

	/* Currently, we can only handle object types with system caches. */
	cache = property->oid_catcache_id;
	Assert(cache != -1);

	/* Fetch tuple from syscache and extract namespace attribute. */
	tuple = SearchSysCache1(cache, ObjectIdGetDatum(address->objectId));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for cache %d oid %u",
			 cache, address->objectId);
	oid = DatumGetObjectId(SysCacheGetAttr(cache,
										   tuple,
										   property->attnum_namespace,
										   &isnull));
	Assert(!isnull);
	ReleaseSysCache(tuple);

	return oid;
}

/*
 * Return ObjectType for the given object type as given by
 * getObjectTypeDescription; if no valid ObjectType code exists, but it's a
 * possible output type from getObjectTypeDescription, return -1.
 * Otherwise, an error is thrown.
 */
int
read_objtype_from_string(const char *objtype)
{
	ObjectType	type;
	int			i;

	for (i = 0; i < lengthof(ObjectTypeMap); i++)
	{
		if (strcmp(ObjectTypeMap[i].tm_name, objtype) == 0)
		{
			type = ObjectTypeMap[i].tm_type;
			break;
		}
	}
	if (i >= lengthof(ObjectTypeMap))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unrecognized object type \"%s\"", objtype)));

	return type;
}

/*
 * Interfaces to reference fields of ObjectPropertyType
 */
Oid
get_object_oid_index(Oid class_id)
{
	const ObjectPropertyType *prop = get_object_property_data(class_id);

	return prop->oid_index_oid;
}

int
get_object_catcache_oid(Oid class_id)
{
	const ObjectPropertyType *prop = get_object_property_data(class_id);

	return prop->oid_catcache_id;
}

int
get_object_catcache_name(Oid class_id)
{
	const ObjectPropertyType *prop = get_object_property_data(class_id);

	return prop->name_catcache_id;
}

AttrNumber
get_object_attnum_name(Oid class_id)
{
	const ObjectPropertyType *prop = get_object_property_data(class_id);

	return prop->attnum_name;
}

AttrNumber
get_object_attnum_namespace(Oid class_id)
{
	const ObjectPropertyType *prop = get_object_property_data(class_id);

	return prop->attnum_namespace;
}

AttrNumber
get_object_attnum_owner(Oid class_id)
{
	const ObjectPropertyType *prop = get_object_property_data(class_id);

	return prop->attnum_owner;
}

AttrNumber
get_object_attnum_acl(Oid class_id)
{
	const ObjectPropertyType *prop = get_object_property_data(class_id);

	return prop->attnum_acl;
}

AclObjectKind
get_object_aclkind(Oid class_id)
{
	const ObjectPropertyType *prop = get_object_property_data(class_id);

	return prop->acl_kind;
}

bool
get_object_namensp_unique(Oid class_id)
{
	const ObjectPropertyType *prop = get_object_property_data(class_id);

	return prop->is_nsp_name_unique;
}

/*
 * Return whether we have useful data for the given object class in the
 * ObjectProperty table.
 */
bool
is_objectclass_supported(Oid class_id)
{
	int			index;

	for (index = 0; index < lengthof(ObjectProperty); index++)
	{
		if (ObjectProperty[index].class_oid == class_id)
			return true;
	}

	return false;
}

/*
 * Find ObjectProperty structure by class_id.
 */
static const ObjectPropertyType *
get_object_property_data(Oid class_id)
{
	static const ObjectPropertyType *prop_last = NULL;
	int			index;

	/*
	 * A shortcut to speed up multiple consecutive lookups of a particular
	 * object class.
	 */
	if (prop_last && prop_last->class_oid == class_id)
		return prop_last;

	for (index = 0; index < lengthof(ObjectProperty); index++)
	{
		if (ObjectProperty[index].class_oid == class_id)
		{
			prop_last = &ObjectProperty[index];
			return &ObjectProperty[index];
		}
	}

	ereport(ERROR,
			(errmsg_internal("unrecognized class ID: %u", class_id)));

	return NULL;				/* keep MSC compiler happy */
}

/*
 * Return a copy of the tuple for the object with the given object OID, from
 * the given catalog (which must have been opened by the caller and suitably
 * locked).  NULL is returned if the OID is not found.
 *
 * We try a syscache first, if available.
 */
HeapTuple
get_catalog_object_by_oid(Relation catalog, Oid objectId)
{
	HeapTuple	tuple;
	Oid			classId = RelationGetRelid(catalog);
	int			oidCacheId = get_object_catcache_oid(classId);

	if (oidCacheId > 0)
	{
		tuple = SearchSysCacheCopy1(oidCacheId, ObjectIdGetDatum(objectId));
		if (!HeapTupleIsValid(tuple))	/* should not happen */
			return NULL;
	}
	else
	{
		Oid			oidIndexId = get_object_oid_index(classId);
		SysScanDesc scan;
		ScanKeyData skey;

		Assert(OidIsValid(oidIndexId));

		ScanKeyInit(&skey,
					ObjectIdAttributeNumber,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(objectId));

		scan = systable_beginscan(catalog, oidIndexId, true,
								  NULL, 1, &skey);
		tuple = systable_getnext(scan);
		if (!HeapTupleIsValid(tuple))
		{
			systable_endscan(scan);
			return NULL;
		}
		tuple = heap_copytuple(tuple);

		systable_endscan(scan);
	}

	return tuple;
}

/*
 * getObjectDescription: build an object description for messages
 *
 * The result is a palloc'd string.
 */
char *
getObjectDescription(const ObjectAddress *object)
{
	StringInfoData buffer;

	initStringInfo(&buffer);

	switch (getObjectClass(object))
	{
		case OCLASS_CLASS:
			getRelationDescription(&buffer, object->objectId);
			if (object->objectSubId != 0)
				appendStringInfo(&buffer, _(" column %s"),
								 get_relid_attribute_name(object->objectId,
													   object->objectSubId));
			break;

		case OCLASS_PROC:
			appendStringInfo(&buffer, _("function %s"),
							 format_procedure(object->objectId));
			break;

		case OCLASS_TYPE:
			appendStringInfo(&buffer, _("type %s"),
							 format_type_be(object->objectId));
			break;

		case OCLASS_CAST:
			{
				Relation	castDesc;
				ScanKeyData skey[1];
				SysScanDesc rcscan;
				HeapTuple	tup;
				Form_pg_cast castForm;

				castDesc = heap_open(CastRelationId, AccessShareLock);

				ScanKeyInit(&skey[0],
							ObjectIdAttributeNumber,
							BTEqualStrategyNumber, F_OIDEQ,
							ObjectIdGetDatum(object->objectId));

				rcscan = systable_beginscan(castDesc, CastOidIndexId, true,
											NULL, 1, skey);

				tup = systable_getnext(rcscan);

				if (!HeapTupleIsValid(tup))
					elog(ERROR, "could not find tuple for cast %u",
						 object->objectId);

				castForm = (Form_pg_cast) GETSTRUCT(tup);

				appendStringInfo(&buffer, _("cast from %s to %s"),
								 format_type_be(castForm->castsource),
								 format_type_be(castForm->casttarget));

				systable_endscan(rcscan);
				heap_close(castDesc, AccessShareLock);
				break;
			}

		case OCLASS_COLLATION:
			{
				HeapTuple	collTup;
				Form_pg_collation coll;

				collTup = SearchSysCache1(COLLOID,
										  ObjectIdGetDatum(object->objectId));
				if (!HeapTupleIsValid(collTup))
					elog(ERROR, "cache lookup failed for collation %u",
						 object->objectId);
				coll = (Form_pg_collation) GETSTRUCT(collTup);
				appendStringInfo(&buffer, _("collation %s"),
								 NameStr(coll->collname));
				ReleaseSysCache(collTup);
				break;
			}

		case OCLASS_CONSTRAINT:
			{
				HeapTuple	conTup;
				Form_pg_constraint con;

				conTup = SearchSysCache1(CONSTROID,
										 ObjectIdGetDatum(object->objectId));
				if (!HeapTupleIsValid(conTup))
					elog(ERROR, "cache lookup failed for constraint %u",
						 object->objectId);
				con = (Form_pg_constraint) GETSTRUCT(conTup);

				if (OidIsValid(con->conrelid))
				{
					StringInfoData rel;

					initStringInfo(&rel);
					getRelationDescription(&rel, con->conrelid);
					appendStringInfo(&buffer, _("constraint %s on %s"),
									 NameStr(con->conname), rel.data);
					pfree(rel.data);
				}
				else
				{
					appendStringInfo(&buffer, _("constraint %s"),
									 NameStr(con->conname));
				}

				ReleaseSysCache(conTup);
				break;
			}

		case OCLASS_CONVERSION:
			{
				HeapTuple	conTup;

				conTup = SearchSysCache1(CONVOID,
										 ObjectIdGetDatum(object->objectId));
				if (!HeapTupleIsValid(conTup))
					elog(ERROR, "cache lookup failed for conversion %u",
						 object->objectId);
				appendStringInfo(&buffer, _("conversion %s"),
				 NameStr(((Form_pg_conversion) GETSTRUCT(conTup))->conname));
				ReleaseSysCache(conTup);
				break;
			}

		case OCLASS_DEFAULT:
			{
				Relation	attrdefDesc;
				ScanKeyData skey[1];
				SysScanDesc adscan;
				HeapTuple	tup;
				Form_pg_attrdef attrdef;
				ObjectAddress colobject;

				attrdefDesc = heap_open(AttrDefaultRelationId, AccessShareLock);

				ScanKeyInit(&skey[0],
							ObjectIdAttributeNumber,
							BTEqualStrategyNumber, F_OIDEQ,
							ObjectIdGetDatum(object->objectId));

				adscan = systable_beginscan(attrdefDesc, AttrDefaultOidIndexId,
											true, NULL, 1, skey);

				tup = systable_getnext(adscan);

				if (!HeapTupleIsValid(tup))
					elog(ERROR, "could not find tuple for attrdef %u",
						 object->objectId);

				attrdef = (Form_pg_attrdef) GETSTRUCT(tup);

				colobject.classId = RelationRelationId;
				colobject.objectId = attrdef->adrelid;
				colobject.objectSubId = attrdef->adnum;

				appendStringInfo(&buffer, _("default for %s"),
								 getObjectDescription(&colobject));

				systable_endscan(adscan);
				heap_close(attrdefDesc, AccessShareLock);
				break;
			}

		case OCLASS_LANGUAGE:
			appendStringInfo(&buffer, _("language %s"),
							 get_language_name(object->objectId, false));
			break;
		case OCLASS_LARGEOBJECT:
			appendStringInfo(&buffer, _("large object %u"),
							 object->objectId);
			break;

		case OCLASS_OPERATOR:
			appendStringInfo(&buffer, _("operator %s"),
							 format_operator(object->objectId));
			break;

		case OCLASS_OPCLASS:
			{
				HeapTuple	opcTup;
				Form_pg_opclass opcForm;
				HeapTuple	amTup;
				Form_pg_am	amForm;
				char	   *nspname;

				opcTup = SearchSysCache1(CLAOID,
										 ObjectIdGetDatum(object->objectId));
				if (!HeapTupleIsValid(opcTup))
					elog(ERROR, "cache lookup failed for opclass %u",
						 object->objectId);
				opcForm = (Form_pg_opclass) GETSTRUCT(opcTup);

				amTup = SearchSysCache1(AMOID,
										ObjectIdGetDatum(opcForm->opcmethod));
				if (!HeapTupleIsValid(amTup))
					elog(ERROR, "cache lookup failed for access method %u",
						 opcForm->opcmethod);
				amForm = (Form_pg_am) GETSTRUCT(amTup);

				/* Qualify the name if not visible in search path */
				if (OpclassIsVisible(object->objectId))
					nspname = NULL;
				else
					nspname = get_namespace_name(opcForm->opcnamespace);

				appendStringInfo(&buffer, _("operator class %s for access method %s"),
								 quote_qualified_identifier(nspname,
												  NameStr(opcForm->opcname)),
								 NameStr(amForm->amname));

				ReleaseSysCache(amTup);
				ReleaseSysCache(opcTup);
				break;
			}

		case OCLASS_OPFAMILY:
			getOpFamilyDescription(&buffer, object->objectId);
			break;

		case OCLASS_AMOP:
			{
				Relation	amopDesc;
				HeapTuple	tup;
				ScanKeyData skey[1];
				SysScanDesc amscan;
				Form_pg_amop amopForm;
				StringInfoData opfam;

				amopDesc = heap_open(AccessMethodOperatorRelationId,
									 AccessShareLock);

				ScanKeyInit(&skey[0],
							ObjectIdAttributeNumber,
							BTEqualStrategyNumber, F_OIDEQ,
							ObjectIdGetDatum(object->objectId));

				amscan = systable_beginscan(amopDesc, AccessMethodOperatorOidIndexId, true,
											NULL, 1, skey);

				tup = systable_getnext(amscan);

				if (!HeapTupleIsValid(tup))
					elog(ERROR, "could not find tuple for amop entry %u",
						 object->objectId);

				amopForm = (Form_pg_amop) GETSTRUCT(tup);

				initStringInfo(&opfam);
				getOpFamilyDescription(&opfam, amopForm->amopfamily);

				/*------
				   translator: %d is the operator strategy (a number), the
				   first two %s's are data type names, the third %s is the
				   description of the operator family, and the last %s is the
				   textual form of the operator with arguments.  */
				appendStringInfo(&buffer, _("operator %d (%s, %s) of %s: %s"),
								 amopForm->amopstrategy,
								 format_type_be(amopForm->amoplefttype),
								 format_type_be(amopForm->amoprighttype),
								 opfam.data,
								 format_operator(amopForm->amopopr));

				pfree(opfam.data);

				systable_endscan(amscan);
				heap_close(amopDesc, AccessShareLock);
				break;
			}

		case OCLASS_AMPROC:
			{
				Relation	amprocDesc;
				ScanKeyData skey[1];
				SysScanDesc amscan;
				HeapTuple	tup;
				Form_pg_amproc amprocForm;
				StringInfoData opfam;

				amprocDesc = heap_open(AccessMethodProcedureRelationId,
									   AccessShareLock);

				ScanKeyInit(&skey[0],
							ObjectIdAttributeNumber,
							BTEqualStrategyNumber, F_OIDEQ,
							ObjectIdGetDatum(object->objectId));

				amscan = systable_beginscan(amprocDesc, AccessMethodProcedureOidIndexId, true,
											NULL, 1, skey);

				tup = systable_getnext(amscan);

				if (!HeapTupleIsValid(tup))
					elog(ERROR, "could not find tuple for amproc entry %u",
						 object->objectId);

				amprocForm = (Form_pg_amproc) GETSTRUCT(tup);

				initStringInfo(&opfam);
				getOpFamilyDescription(&opfam, amprocForm->amprocfamily);

				/*------
				   translator: %d is the function number, the first two %s's
				   are data type names, the third %s is the description of the
				   operator family, and the last %s is the textual form of the
				   function with arguments.  */
				appendStringInfo(&buffer, _("function %d (%s, %s) of %s: %s"),
								 amprocForm->amprocnum,
								 format_type_be(amprocForm->amproclefttype),
								 format_type_be(amprocForm->amprocrighttype),
								 opfam.data,
								 format_procedure(amprocForm->amproc));

				pfree(opfam.data);

				systable_endscan(amscan);
				heap_close(amprocDesc, AccessShareLock);
				break;
			}

		case OCLASS_REWRITE:
			{
				Relation	ruleDesc;
				ScanKeyData skey[1];
				SysScanDesc rcscan;
				HeapTuple	tup;
				Form_pg_rewrite rule;

				ruleDesc = heap_open(RewriteRelationId, AccessShareLock);

				ScanKeyInit(&skey[0],
							ObjectIdAttributeNumber,
							BTEqualStrategyNumber, F_OIDEQ,
							ObjectIdGetDatum(object->objectId));

				rcscan = systable_beginscan(ruleDesc, RewriteOidIndexId, true,
											NULL, 1, skey);

				tup = systable_getnext(rcscan);

				if (!HeapTupleIsValid(tup))
					elog(ERROR, "could not find tuple for rule %u",
						 object->objectId);

				rule = (Form_pg_rewrite) GETSTRUCT(tup);

				appendStringInfo(&buffer, _("rule %s on "),
								 NameStr(rule->rulename));
				getRelationDescription(&buffer, rule->ev_class);

				systable_endscan(rcscan);
				heap_close(ruleDesc, AccessShareLock);
				break;
			}

		case OCLASS_TRANSFORM:
			{
				HeapTuple	trfTup;
				Form_pg_transform trfForm;

				trfTup = SearchSysCache1(TRFOID,
										 ObjectIdGetDatum(object->objectId));
				if (!HeapTupleIsValid(trfTup))
					elog(ERROR, "could not find tuple for transform %u",
						 object->objectId);

				trfForm = (Form_pg_transform) GETSTRUCT(trfTup);

				appendStringInfo(&buffer, _("transform for %s language %s"),
								 format_type_be(trfForm->trftype),
								 get_language_name(trfForm->trflang, false));

				ReleaseSysCache(trfTup);
				break;
			}

		case OCLASS_TRIGGER:
			{
				Relation	trigDesc;
				ScanKeyData skey[1];
				SysScanDesc tgscan;
				HeapTuple	tup;
				Form_pg_trigger trig;

				trigDesc = heap_open(TriggerRelationId, AccessShareLock);

				ScanKeyInit(&skey[0],
							ObjectIdAttributeNumber,
							BTEqualStrategyNumber, F_OIDEQ,
							ObjectIdGetDatum(object->objectId));

				tgscan = systable_beginscan(trigDesc, TriggerOidIndexId, true,
											NULL, 1, skey);

				tup = systable_getnext(tgscan);

				if (!HeapTupleIsValid(tup))
					elog(ERROR, "could not find tuple for trigger %u",
						 object->objectId);

				trig = (Form_pg_trigger) GETSTRUCT(tup);

				appendStringInfo(&buffer, _("trigger %s on "),
								 NameStr(trig->tgname));
				getRelationDescription(&buffer, trig->tgrelid);

				systable_endscan(tgscan);
				heap_close(trigDesc, AccessShareLock);
				break;
			}

		case OCLASS_SCHEMA:
			{
				char	   *nspname;

				nspname = get_namespace_name(object->objectId);
				if (!nspname)
					elog(ERROR, "cache lookup failed for namespace %u",
						 object->objectId);
				appendStringInfo(&buffer, _("schema %s"), nspname);
				break;
			}

		case OCLASS_TSPARSER:
			{
				HeapTuple	tup;

				tup = SearchSysCache1(TSPARSEROID,
									  ObjectIdGetDatum(object->objectId));
				if (!HeapTupleIsValid(tup))
					elog(ERROR, "cache lookup failed for text search parser %u",
						 object->objectId);
				appendStringInfo(&buffer, _("text search parser %s"),
					 NameStr(((Form_pg_ts_parser) GETSTRUCT(tup))->prsname));
				ReleaseSysCache(tup);
				break;
			}

		case OCLASS_TSDICT:
			{
				HeapTuple	tup;

				tup = SearchSysCache1(TSDICTOID,
									  ObjectIdGetDatum(object->objectId));
				if (!HeapTupleIsValid(tup))
					elog(ERROR, "cache lookup failed for text search dictionary %u",
						 object->objectId);
				appendStringInfo(&buffer, _("text search dictionary %s"),
					  NameStr(((Form_pg_ts_dict) GETSTRUCT(tup))->dictname));
				ReleaseSysCache(tup);
				break;
			}

		case OCLASS_TSTEMPLATE:
			{
				HeapTuple	tup;

				tup = SearchSysCache1(TSTEMPLATEOID,
									  ObjectIdGetDatum(object->objectId));
				if (!HeapTupleIsValid(tup))
					elog(ERROR, "cache lookup failed for text search template %u",
						 object->objectId);
				appendStringInfo(&buffer, _("text search template %s"),
				  NameStr(((Form_pg_ts_template) GETSTRUCT(tup))->tmplname));
				ReleaseSysCache(tup);
				break;
			}

		case OCLASS_TSCONFIG:
			{
				HeapTuple	tup;

				tup = SearchSysCache1(TSCONFIGOID,
									  ObjectIdGetDatum(object->objectId));
				if (!HeapTupleIsValid(tup))
					elog(ERROR, "cache lookup failed for text search configuration %u",
						 object->objectId);
				appendStringInfo(&buffer, _("text search configuration %s"),
					 NameStr(((Form_pg_ts_config) GETSTRUCT(tup))->cfgname));
				ReleaseSysCache(tup);
				break;
			}

		case OCLASS_ROLE:
			{
				appendStringInfo(&buffer, _("role %s"),
						 GetUserNameFromId(object->objectId, false));
				break;
			}

		case OCLASS_DATABASE:
			{
				char	   *datname;

				datname = get_database_name(object->objectId);
				if (!datname)
					elog(ERROR, "cache lookup failed for database %u",
						 object->objectId);
				appendStringInfo(&buffer, _("database %s"), datname);
				break;
			}

		case OCLASS_TBLSPACE:
			{
				char	   *tblspace;

				tblspace = get_tablespace_name(object->objectId);
				if (!tblspace)
					elog(ERROR, "cache lookup failed for tablespace %u",
						 object->objectId);
				appendStringInfo(&buffer, _("tablespace %s"), tblspace);
				break;
			}

		case OCLASS_FDW:
			{
				ForeignDataWrapper *fdw;

				fdw = GetForeignDataWrapper(object->objectId);
				appendStringInfo(&buffer, _("foreign-data wrapper %s"), fdw->fdwname);
				break;
			}

		case OCLASS_FOREIGN_SERVER:
			{
				ForeignServer *srv;

				srv = GetForeignServer(object->objectId);
				appendStringInfo(&buffer, _("server %s"), srv->servername);
				break;
			}

		case OCLASS_USER_MAPPING:
			{
				HeapTuple	tup;
				Oid			useid;
				char	   *usename;
				Form_pg_user_mapping umform;
				ForeignServer *srv;

				tup = SearchSysCache1(USERMAPPINGOID,
									  ObjectIdGetDatum(object->objectId));
				if (!HeapTupleIsValid(tup))
					elog(ERROR, "cache lookup failed for user mapping %u",
						 object->objectId);

				umform = (Form_pg_user_mapping) GETSTRUCT(tup);
				useid = umform->umuser;
				srv = GetForeignServer(umform->umserver);

				ReleaseSysCache(tup);

				if (OidIsValid(useid))
					usename = GetUserNameFromId(useid, false);
				else
					usename = "public";

				appendStringInfo(&buffer, _("user mapping for %s on server %s"), usename,
								 srv->servername);
				break;
			}

		case OCLASS_DEFACL:
			{
				Relation	defaclrel;
				ScanKeyData skey[1];
				SysScanDesc rcscan;
				HeapTuple	tup;
				Form_pg_default_acl defacl;

				defaclrel = heap_open(DefaultAclRelationId, AccessShareLock);

				ScanKeyInit(&skey[0],
							ObjectIdAttributeNumber,
							BTEqualStrategyNumber, F_OIDEQ,
							ObjectIdGetDatum(object->objectId));

				rcscan = systable_beginscan(defaclrel, DefaultAclOidIndexId,
											true, NULL, 1, skey);

				tup = systable_getnext(rcscan);

				if (!HeapTupleIsValid(tup))
					elog(ERROR, "could not find tuple for default ACL %u",
						 object->objectId);

				defacl = (Form_pg_default_acl) GETSTRUCT(tup);

				switch (defacl->defaclobjtype)
				{
					case DEFACLOBJ_RELATION:
						appendStringInfo(&buffer,
										 _("default privileges on new relations belonging to role %s"),
							   GetUserNameFromId(defacl->defaclrole, false));
						break;
					case DEFACLOBJ_SEQUENCE:
						appendStringInfo(&buffer,
										 _("default privileges on new sequences belonging to role %s"),
							   GetUserNameFromId(defacl->defaclrole, false));
						break;
					case DEFACLOBJ_FUNCTION:
						appendStringInfo(&buffer,
										 _("default privileges on new functions belonging to role %s"),
							   GetUserNameFromId(defacl->defaclrole, false));
						break;
					case DEFACLOBJ_TYPE:
						appendStringInfo(&buffer,
										 _("default privileges on new types belonging to role %s"),
							   GetUserNameFromId(defacl->defaclrole, false));
						break;
					default:
						/* shouldn't get here */
						appendStringInfo(&buffer,
								_("default privileges belonging to role %s"),
							   GetUserNameFromId(defacl->defaclrole, false));
						break;
				}

				if (OidIsValid(defacl->defaclnamespace))
				{
					appendStringInfo(&buffer,
									 _(" in schema %s"),
								get_namespace_name(defacl->defaclnamespace));
				}

				systable_endscan(rcscan);
				heap_close(defaclrel, AccessShareLock);
				break;
			}

		case OCLASS_EXTENSION:
			{
				char	   *extname;

				extname = get_extension_name(object->objectId);
				if (!extname)
					elog(ERROR, "cache lookup failed for extension %u",
						 object->objectId);
				appendStringInfo(&buffer, _("extension %s"), extname);
				break;
			}

		case OCLASS_EVENT_TRIGGER:
			{
				HeapTuple	tup;

				tup = SearchSysCache1(EVENTTRIGGEROID,
									  ObjectIdGetDatum(object->objectId));
				if (!HeapTupleIsValid(tup))
					elog(ERROR, "cache lookup failed for event trigger %u",
						 object->objectId);
				appendStringInfo(&buffer, _("event trigger %s"),
				 NameStr(((Form_pg_event_trigger) GETSTRUCT(tup))->evtname));
				ReleaseSysCache(tup);
				break;
			}

		case OCLASS_POLICY:
			{
				Relation	policy_rel;
				ScanKeyData skey[1];
				SysScanDesc sscan;
				HeapTuple	tuple;
				Form_pg_policy form_policy;

				policy_rel = heap_open(PolicyRelationId, AccessShareLock);

				ScanKeyInit(&skey[0],
							ObjectIdAttributeNumber,
							BTEqualStrategyNumber, F_OIDEQ,
							ObjectIdGetDatum(object->objectId));

				sscan = systable_beginscan(policy_rel, PolicyOidIndexId,
										   true, NULL, 1, skey);

				tuple = systable_getnext(sscan);

				if (!HeapTupleIsValid(tuple))
					elog(ERROR, "cache lookup failed for policy %u",
						 object->objectId);

				form_policy = (Form_pg_policy) GETSTRUCT(tuple);

				appendStringInfo(&buffer, _("policy %s on "),
								 NameStr(form_policy->polname));
				getRelationDescription(&buffer, form_policy->polrelid);

				systable_endscan(sscan);
				heap_close(policy_rel, AccessShareLock);
				break;
			}

		case OCLASS_CONTINUOUS_QUERY:
			{
				HeapTuple	tup;

				tup = SearchPipelineSysCache1(PIPELINEQUERYOID,
									  ObjectIdGetDatum(object->objectId));
				if (!HeapTupleIsValid(tup))
					elog(ERROR, "cache lookup failed for pipeline_query %u",
						 object->objectId);
				appendStringInfo(&buffer, _("pipeline_query %s"),
						get_rel_name(((Form_pipeline_query) GETSTRUCT(tup))->relid));
				ReleaseSysCache(tup);
				break;
			}

		default:
			appendStringInfo(&buffer, "unrecognized object %u %u %d",
							 object->classId,
							 object->objectId,
							 object->objectSubId);
			break;
	}

	return buffer.data;
}

/*
 * getObjectDescriptionOids: as above, except the object is specified by Oids
 */
char *
getObjectDescriptionOids(Oid classid, Oid objid)
{
	ObjectAddress address;

	address.classId = classid;
	address.objectId = objid;
	address.objectSubId = 0;

	return getObjectDescription(&address);
}

/*
 * subroutine for getObjectDescription: describe a relation
 */
static void
getRelationDescription(StringInfo buffer, Oid relid)
{
	HeapTuple	relTup;
	Form_pg_class relForm;
	char	   *nspname;
	char	   *relname;

	relTup = SearchSysCache1(RELOID,
							 ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(relTup))
		elog(ERROR, "cache lookup failed for relation %u", relid);
	relForm = (Form_pg_class) GETSTRUCT(relTup);

	/* Qualify the name if not visible in search path */
	if (RelationIsVisible(relid))
		nspname = NULL;
	else
		nspname = get_namespace_name(relForm->relnamespace);

	relname = quote_qualified_identifier(nspname, NameStr(relForm->relname));

	switch (relForm->relkind)
	{
		case RELKIND_RELATION:
			appendStringInfo(buffer, _("table %s"),
							 relname);
			break;
		case RELKIND_INDEX:
			appendStringInfo(buffer, _("index %s"),
							 relname);
			break;
		case RELKIND_SEQUENCE:
			appendStringInfo(buffer, _("sequence %s"),
							 relname);
			break;
		case RELKIND_TOASTVALUE:
			appendStringInfo(buffer, _("toast table %s"),
							 relname);
			break;
		case RELKIND_VIEW:
			appendStringInfo(buffer, _("view %s"),
							 relname);
			break;
		case RELKIND_MATVIEW:
			appendStringInfo(buffer, _("materialized view %s"),
							 relname);
			break;
		case RELKIND_COMPOSITE_TYPE:
			appendStringInfo(buffer, _("composite type %s"),
							 relname);
			break;
		case RELKIND_FOREIGN_TABLE:
			appendStringInfo(buffer, _("foreign table %s"),
							 relname);
			break;
		case RELKIND_CONTVIEW:
			appendStringInfo(buffer, _("continuous view %s"),
							 relname);
			break;
		case RELKIND_CONTTRANSFORM:
			appendStringInfo(buffer, _("continuous transform %s"),
							 relname);
			break;
		default:
			/* shouldn't get here */
			appendStringInfo(buffer, _("relation %s"),
							 relname);
			break;
	}

	ReleaseSysCache(relTup);
}

/*
 * subroutine for getObjectDescription: describe an operator family
 */
static void
getOpFamilyDescription(StringInfo buffer, Oid opfid)
{
	HeapTuple	opfTup;
	Form_pg_opfamily opfForm;
	HeapTuple	amTup;
	Form_pg_am	amForm;
	char	   *nspname;

	opfTup = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(opfid));
	if (!HeapTupleIsValid(opfTup))
		elog(ERROR, "cache lookup failed for opfamily %u", opfid);
	opfForm = (Form_pg_opfamily) GETSTRUCT(opfTup);

	amTup = SearchSysCache1(AMOID, ObjectIdGetDatum(opfForm->opfmethod));
	if (!HeapTupleIsValid(amTup))
		elog(ERROR, "cache lookup failed for access method %u",
			 opfForm->opfmethod);
	amForm = (Form_pg_am) GETSTRUCT(amTup);

	/* Qualify the name if not visible in search path */
	if (OpfamilyIsVisible(opfid))
		nspname = NULL;
	else
		nspname = get_namespace_name(opfForm->opfnamespace);

	appendStringInfo(buffer, _("operator family %s for access method %s"),
					 quote_qualified_identifier(nspname,
												NameStr(opfForm->opfname)),
					 NameStr(amForm->amname));

	ReleaseSysCache(amTup);
	ReleaseSysCache(opfTup);
}

/*
 * SQL-level callable version of getObjectDescription
 */
Datum
pg_describe_object(PG_FUNCTION_ARGS)
{
	Oid			classid = PG_GETARG_OID(0);
	Oid			objid = PG_GETARG_OID(1);
	int32		subobjid = PG_GETARG_INT32(2);
	char	   *description;
	ObjectAddress address;

	/* for "pinned" items in pg_depend, return null */
	if (!OidIsValid(classid) && !OidIsValid(objid))
		PG_RETURN_NULL();

	address.classId = classid;
	address.objectId = objid;
	address.objectSubId = subobjid;

	description = getObjectDescription(&address);
	PG_RETURN_TEXT_P(cstring_to_text(description));
}

/*
 * SQL-level callable function to obtain object type + identity
 */
Datum
pg_identify_object(PG_FUNCTION_ARGS)
{
	Oid			classid = PG_GETARG_OID(0);
	Oid			objid = PG_GETARG_OID(1);
	int32		subobjid = PG_GETARG_INT32(2);
	Oid			schema_oid = InvalidOid;
	const char *objname = NULL;
	ObjectAddress address;
	Datum		values[4];
	bool		nulls[4];
	TupleDesc	tupdesc;
	HeapTuple	htup;

	address.classId = classid;
	address.objectId = objid;
	address.objectSubId = subobjid;

	/*
	 * Construct a tuple descriptor for the result row.  This must match this
	 * function's pg_proc entry!
	 */
	tupdesc = CreateTemplateTupleDesc(4, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "type",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "schema",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "name",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "identity",
					   TEXTOID, -1, 0);

	tupdesc = BlessTupleDesc(tupdesc);

	if (is_objectclass_supported(address.classId))
	{
		HeapTuple	objtup;
		Relation	catalog = heap_open(address.classId, AccessShareLock);

		objtup = get_catalog_object_by_oid(catalog, address.objectId);
		if (objtup != NULL)
		{
			bool		isnull;
			AttrNumber	nspAttnum;
			AttrNumber	nameAttnum;

			nspAttnum = get_object_attnum_namespace(address.classId);
			if (nspAttnum != InvalidAttrNumber)
			{
				schema_oid = heap_getattr(objtup, nspAttnum,
										  RelationGetDescr(catalog), &isnull);
				if (isnull)
					elog(ERROR, "invalid null namespace in object %u/%u/%d",
					 address.classId, address.objectId, address.objectSubId);
			}

			/*
			 * We only return the object name if it can be used (together with
			 * the schema name, if any) as a unique identifier.
			 */
			if (get_object_namensp_unique(address.classId))
			{
				nameAttnum = get_object_attnum_name(address.classId);
				if (nameAttnum != InvalidAttrNumber)
				{
					Datum		nameDatum;

					nameDatum = heap_getattr(objtup, nameAttnum,
										 RelationGetDescr(catalog), &isnull);
					if (isnull)
						elog(ERROR, "invalid null name in object %u/%u/%d",
							 address.classId, address.objectId, address.objectSubId);
					objname = quote_identifier(NameStr(*(DatumGetName(nameDatum))));
				}
			}
		}

		heap_close(catalog, AccessShareLock);
	}

	/* object type */
	values[0] = CStringGetTextDatum(getObjectTypeDescription(&address));
	nulls[0] = false;

	/* schema name */
	if (OidIsValid(schema_oid))
	{
		const char *schema = quote_identifier(get_namespace_name(schema_oid));

		values[1] = CStringGetTextDatum(schema);
		nulls[1] = false;
	}
	else
		nulls[1] = true;

	/* object name */
	if (objname)
	{
		values[2] = CStringGetTextDatum(objname);
		nulls[2] = false;
	}
	else
		nulls[2] = true;

	/* object identity */
	values[3] = CStringGetTextDatum(getObjectIdentity(&address));
	nulls[3] = false;

	htup = heap_form_tuple(tupdesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}

/*
 * SQL-level callable function to obtain object type + identity
 */
Datum
pg_identify_object_as_address(PG_FUNCTION_ARGS)
{
	Oid			classid = PG_GETARG_OID(0);
	Oid			objid = PG_GETARG_OID(1);
	int32		subobjid = PG_GETARG_INT32(2);
	ObjectAddress address;
	char	   *identity;
	List	   *names;
	List	   *args;
	Datum		values[3];
	bool		nulls[3];
	TupleDesc	tupdesc;
	HeapTuple	htup;

	address.classId = classid;
	address.objectId = objid;
	address.objectSubId = subobjid;

	/*
	 * Construct a tuple descriptor for the result row.  This must match this
	 * function's pg_proc entry!
	 */
	tupdesc = CreateTemplateTupleDesc(3, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "type",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "object_names",
					   TEXTARRAYOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "object_args",
					   TEXTARRAYOID, -1, 0);

	tupdesc = BlessTupleDesc(tupdesc);

	/* object type */
	values[0] = CStringGetTextDatum(getObjectTypeDescription(&address));
	nulls[0] = false;

	/* object identity */
	identity = getObjectIdentityParts(&address, &names, &args);
	pfree(identity);

	/* object_names */
	values[1] = PointerGetDatum(strlist_to_textarray(names));
	nulls[1] = false;

	/* object_args */
	if (args)
		values[2] = PointerGetDatum(strlist_to_textarray(args));
	else
		values[2] = PointerGetDatum(construct_empty_array(TEXTOID));
	nulls[2] = false;

	htup = heap_form_tuple(tupdesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}


/*
 * Return a palloc'ed string that describes the type of object that the
 * passed address is for.
 *
 * Keep ObjectTypeMap in sync with this.
 */
char *
getObjectTypeDescription(const ObjectAddress *object)
{
	StringInfoData buffer;

	initStringInfo(&buffer);

	switch (getObjectClass(object))
	{
		case OCLASS_CLASS:
			getRelationTypeDescription(&buffer, object->objectId,
									   object->objectSubId);
			break;

		case OCLASS_PROC:
			getProcedureTypeDescription(&buffer, object->objectId);
			break;

		case OCLASS_TYPE:
			appendStringInfoString(&buffer, "type");
			break;

		case OCLASS_CAST:
			appendStringInfoString(&buffer, "cast");
			break;

		case OCLASS_COLLATION:
			appendStringInfoString(&buffer, "collation");
			break;

		case OCLASS_CONSTRAINT:
			getConstraintTypeDescription(&buffer, object->objectId);
			break;

		case OCLASS_CONVERSION:
			appendStringInfoString(&buffer, "conversion");
			break;

		case OCLASS_DEFAULT:
			appendStringInfoString(&buffer, "default value");
			break;

		case OCLASS_LANGUAGE:
			appendStringInfoString(&buffer, "language");
			break;

		case OCLASS_LARGEOBJECT:
			appendStringInfoString(&buffer, "large object");
			break;

		case OCLASS_OPERATOR:
			appendStringInfoString(&buffer, "operator");
			break;

		case OCLASS_OPCLASS:
			appendStringInfoString(&buffer, "operator class");
			break;

		case OCLASS_OPFAMILY:
			appendStringInfoString(&buffer, "operator family");
			break;

		case OCLASS_AMOP:
			appendStringInfoString(&buffer, "operator of access method");
			break;

		case OCLASS_AMPROC:
			appendStringInfoString(&buffer, "function of access method");
			break;

		case OCLASS_REWRITE:
			appendStringInfoString(&buffer, "rule");
			break;

		case OCLASS_TRIGGER:
			appendStringInfoString(&buffer, "trigger");
			break;

		case OCLASS_SCHEMA:
			appendStringInfoString(&buffer, "schema");
			break;

		case OCLASS_TSPARSER:
			appendStringInfoString(&buffer, "text search parser");
			break;

		case OCLASS_TSDICT:
			appendStringInfoString(&buffer, "text search dictionary");
			break;

		case OCLASS_TSTEMPLATE:
			appendStringInfoString(&buffer, "text search template");
			break;

		case OCLASS_TSCONFIG:
			appendStringInfoString(&buffer, "text search configuration");
			break;

		case OCLASS_ROLE:
			appendStringInfoString(&buffer, "role");
			break;

		case OCLASS_DATABASE:
			appendStringInfoString(&buffer, "database");
			break;

		case OCLASS_TBLSPACE:
			appendStringInfoString(&buffer, "tablespace");
			break;

		case OCLASS_FDW:
			appendStringInfoString(&buffer, "foreign-data wrapper");
			break;

		case OCLASS_FOREIGN_SERVER:
			appendStringInfoString(&buffer, "server");
			break;

		case OCLASS_USER_MAPPING:
			appendStringInfoString(&buffer, "user mapping");
			break;

		case OCLASS_DEFACL:
			appendStringInfoString(&buffer, "default acl");
			break;

		case OCLASS_EXTENSION:
			appendStringInfoString(&buffer, "extension");
			break;

		case OCLASS_EVENT_TRIGGER:
			appendStringInfoString(&buffer, "event trigger");
			break;

		case OCLASS_POLICY:
			appendStringInfoString(&buffer, "policy");
			break;

		case OCLASS_TRANSFORM:
			appendStringInfoString(&buffer, "transform");
			break;

		case OCLASS_CONTINUOUS_QUERY:
			appendStringInfoString(&buffer, "continuous query");
			break;

		default:
			appendStringInfo(&buffer, "unrecognized %u", object->classId);
			break;
	}

	return buffer.data;
}

/*
 * subroutine for getObjectTypeDescription: describe a relation type
 */
static void
getRelationTypeDescription(StringInfo buffer, Oid relid, int32 objectSubId)
{
	HeapTuple	relTup;
	Form_pg_class relForm;

	relTup = SearchSysCache1(RELOID,
							 ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(relTup))
		elog(ERROR, "cache lookup failed for relation %u", relid);
	relForm = (Form_pg_class) GETSTRUCT(relTup);

	switch (relForm->relkind)
	{
		case RELKIND_RELATION:
			appendStringInfoString(buffer, "table");
			break;
		case RELKIND_INDEX:
			appendStringInfoString(buffer, "index");
			break;
		case RELKIND_SEQUENCE:
			appendStringInfoString(buffer, "sequence");
			break;
		case RELKIND_TOASTVALUE:
			appendStringInfoString(buffer, "toast table");
			break;
		case RELKIND_VIEW:
			appendStringInfoString(buffer, "view");
			break;
		case RELKIND_MATVIEW:
			appendStringInfoString(buffer, "materialized view");
			break;
		case RELKIND_COMPOSITE_TYPE:
			appendStringInfoString(buffer, "composite type");
			break;
		case RELKIND_FOREIGN_TABLE:
			appendStringInfoString(buffer, "foreign table");
			break;
		case RELKIND_CONTVIEW:
			appendStringInfoString(buffer, "continuous view");
			break;
		case RELKIND_CONTTRANSFORM:
			appendStringInfoString(buffer, "continuous transform");
			break;
		default:
			/* shouldn't get here */
			appendStringInfoString(buffer, "relation");
			break;
	}

	if (objectSubId != 0)
		appendStringInfoString(buffer, " column");

	ReleaseSysCache(relTup);
}

/*
 * subroutine for getObjectTypeDescription: describe a constraint type
 */
static void
getConstraintTypeDescription(StringInfo buffer, Oid constroid)
{
	Relation	constrRel;
	HeapTuple	constrTup;
	Form_pg_constraint constrForm;

	constrRel = heap_open(ConstraintRelationId, AccessShareLock);
	constrTup = get_catalog_object_by_oid(constrRel, constroid);
	if (!HeapTupleIsValid(constrTup))
		elog(ERROR, "cache lookup failed for constraint %u", constroid);

	constrForm = (Form_pg_constraint) GETSTRUCT(constrTup);

	if (OidIsValid(constrForm->conrelid))
		appendStringInfoString(buffer, "table constraint");
	else if (OidIsValid(constrForm->contypid))
		appendStringInfoString(buffer, "domain constraint");
	else
		elog(ERROR, "invalid constraint %u", HeapTupleGetOid(constrTup));

	heap_close(constrRel, AccessShareLock);
}

/*
 * subroutine for getObjectTypeDescription: describe a procedure type
 */
static void
getProcedureTypeDescription(StringInfo buffer, Oid procid)
{
	HeapTuple	procTup;
	Form_pg_proc procForm;

	procTup = SearchSysCache1(PROCOID,
							  ObjectIdGetDatum(procid));
	if (!HeapTupleIsValid(procTup))
		elog(ERROR, "cache lookup failed for procedure %u", procid);
	procForm = (Form_pg_proc) GETSTRUCT(procTup);

	if (procForm->proisagg)
		appendStringInfoString(buffer, "aggregate");
	else
		appendStringInfoString(buffer, "function");

	ReleaseSysCache(procTup);
}

/*
 * Obtain a given object's identity, as a palloc'ed string.
 *
 * This is for machine consumption, so it's not translated.  All elements are
 * schema-qualified when appropriate.
 */
char *
getObjectIdentity(const ObjectAddress *object)
{
	return getObjectIdentityParts(object, NULL, NULL);
}

/*
 * As above, but more detailed.
 *
 * There are two sets of return values: the identity itself as a palloc'd
 * string is returned.  objname and objargs, if not NULL, are output parameters
 * that receive lists of C-strings that are useful to give back to
 * get_object_address() to reconstruct the ObjectAddress.
 */
char *
getObjectIdentityParts(const ObjectAddress *object,
					   List **objname, List **objargs)
{
	StringInfoData buffer;

	initStringInfo(&buffer);

	/*
	 * Make sure that both objname and objargs were passed, or none was; and
	 * initialize them to empty lists.  For objname this is useless because it
	 * will be initialized in all cases inside the switch; but we do it anyway
	 * so that we can test below that no branch leaves it unset.
	 */
	Assert(PointerIsValid(objname) == PointerIsValid(objargs));
	if (objname)
	{
		*objname = NIL;
		*objargs = NIL;
	}

	switch (getObjectClass(object))
	{
		case OCLASS_CLASS:
			getRelationIdentity(&buffer, object->objectId, objname);
			if (object->objectSubId != 0)
			{
				char	   *attr;

				attr = get_relid_attribute_name(object->objectId,
												object->objectSubId);
				appendStringInfo(&buffer, ".%s", quote_identifier(attr));
				if (objname)
					*objname = lappend(*objname, attr);
			}
			break;

		case OCLASS_PROC:
			appendStringInfoString(&buffer,
							   format_procedure_qualified(object->objectId));
			if (objname)
				format_procedure_parts(object->objectId, objname, objargs);
			break;

		case OCLASS_TYPE:
			{
				char	   *typeout;

				typeout = format_type_be_qualified(object->objectId);
				appendStringInfoString(&buffer, typeout);
				if (objname)
					*objname = list_make1(typeout);
			}
			break;

		case OCLASS_CAST:
			{
				Relation	castRel;
				HeapTuple	tup;
				Form_pg_cast castForm;

				castRel = heap_open(CastRelationId, AccessShareLock);

				tup = get_catalog_object_by_oid(castRel, object->objectId);

				if (!HeapTupleIsValid(tup))
					elog(ERROR, "could not find tuple for cast %u",
						 object->objectId);

				castForm = (Form_pg_cast) GETSTRUCT(tup);

				appendStringInfo(&buffer, "(%s AS %s)",
							  format_type_be_qualified(castForm->castsource),
							 format_type_be_qualified(castForm->casttarget));

				if (objname)
				{
					*objname = list_make1(format_type_be_qualified(castForm->castsource));
					*objargs = list_make1(format_type_be_qualified(castForm->casttarget));
				}

				heap_close(castRel, AccessShareLock);
				break;
			}

		case OCLASS_COLLATION:
			{
				HeapTuple	collTup;
				Form_pg_collation coll;
				char	   *schema;

				collTup = SearchSysCache1(COLLOID,
										  ObjectIdGetDatum(object->objectId));
				if (!HeapTupleIsValid(collTup))
					elog(ERROR, "cache lookup failed for collation %u",
						 object->objectId);
				coll = (Form_pg_collation) GETSTRUCT(collTup);
				schema = get_namespace_name_or_temp(coll->collnamespace);
				appendStringInfoString(&buffer,
									   quote_qualified_identifier(schema,
												   NameStr(coll->collname)));
				if (objname)
					*objname = list_make2(schema,
										  pstrdup(NameStr(coll->collname)));
				ReleaseSysCache(collTup);
				break;
			}

		case OCLASS_CONSTRAINT:
			{
				HeapTuple	conTup;
				Form_pg_constraint con;

				conTup = SearchSysCache1(CONSTROID,
										 ObjectIdGetDatum(object->objectId));
				if (!HeapTupleIsValid(conTup))
					elog(ERROR, "cache lookup failed for constraint %u",
						 object->objectId);
				con = (Form_pg_constraint) GETSTRUCT(conTup);

				if (OidIsValid(con->conrelid))
				{
					appendStringInfo(&buffer, "%s on ",
									 quote_identifier(NameStr(con->conname)));
					getRelationIdentity(&buffer, con->conrelid, objname);
					if (objname)
						*objname = lappend(*objname, pstrdup(NameStr(con->conname)));
				}
				else
				{
					ObjectAddress domain;

					Assert(OidIsValid(con->contypid));
					domain.classId = TypeRelationId;
					domain.objectId = con->contypid;
					domain.objectSubId = 0;

					appendStringInfo(&buffer, "%s on %s",
									 quote_identifier(NameStr(con->conname)),
						  getObjectIdentityParts(&domain, objname, objargs));

					if (objname)
						*objargs = lappend(*objargs, pstrdup(NameStr(con->conname)));
				}

				ReleaseSysCache(conTup);
				break;
			}

		case OCLASS_CONVERSION:
			{
				HeapTuple	conTup;
				Form_pg_conversion conForm;
				char	   *schema;

				conTup = SearchSysCache1(CONVOID,
										 ObjectIdGetDatum(object->objectId));
				if (!HeapTupleIsValid(conTup))
					elog(ERROR, "cache lookup failed for conversion %u",
						 object->objectId);
				conForm = (Form_pg_conversion) GETSTRUCT(conTup);
				schema = get_namespace_name_or_temp(conForm->connamespace);
				appendStringInfoString(&buffer,
									   quote_qualified_identifier(schema,
												 NameStr(conForm->conname)));
				if (objname)
					*objname = list_make2(schema,
										  pstrdup(NameStr(conForm->conname)));
				ReleaseSysCache(conTup);
				break;
			}

		case OCLASS_DEFAULT:
			{
				Relation	attrdefDesc;
				ScanKeyData skey[1];
				SysScanDesc adscan;

				HeapTuple	tup;
				Form_pg_attrdef attrdef;
				ObjectAddress colobject;

				attrdefDesc = heap_open(AttrDefaultRelationId, AccessShareLock);

				ScanKeyInit(&skey[0],
							ObjectIdAttributeNumber,
							BTEqualStrategyNumber, F_OIDEQ,
							ObjectIdGetDatum(object->objectId));

				adscan = systable_beginscan(attrdefDesc, AttrDefaultOidIndexId,
											true, NULL, 1, skey);

				tup = systable_getnext(adscan);

				if (!HeapTupleIsValid(tup))
					elog(ERROR, "could not find tuple for attrdef %u",
						 object->objectId);

				attrdef = (Form_pg_attrdef) GETSTRUCT(tup);

				colobject.classId = RelationRelationId;
				colobject.objectId = attrdef->adrelid;
				colobject.objectSubId = attrdef->adnum;

				appendStringInfo(&buffer, "for %s",
								 getObjectIdentityParts(&colobject,
														objname, objargs));

				systable_endscan(adscan);
				heap_close(attrdefDesc, AccessShareLock);
				break;
			}

		case OCLASS_LANGUAGE:
			{
				HeapTuple	langTup;
				Form_pg_language langForm;

				langTup = SearchSysCache1(LANGOID,
										  ObjectIdGetDatum(object->objectId));
				if (!HeapTupleIsValid(langTup))
					elog(ERROR, "cache lookup failed for language %u",
						 object->objectId);
				langForm = (Form_pg_language) GETSTRUCT(langTup);
				appendStringInfoString(&buffer,
							   quote_identifier(NameStr(langForm->lanname)));
				if (objname)
					*objname = list_make1(pstrdup(NameStr(langForm->lanname)));
				ReleaseSysCache(langTup);
				break;
			}
		case OCLASS_LARGEOBJECT:
			appendStringInfo(&buffer, "%u",
							 object->objectId);
			if (objname)
				*objname = list_make1(psprintf("%u", object->objectId));
			break;

		case OCLASS_OPERATOR:
			appendStringInfoString(&buffer,
								format_operator_qualified(object->objectId));
			if (objname)
				format_operator_parts(object->objectId, objname, objargs);
			break;

		case OCLASS_OPCLASS:
			{
				HeapTuple	opcTup;
				Form_pg_opclass opcForm;
				HeapTuple	amTup;
				Form_pg_am	amForm;
				char	   *schema;

				opcTup = SearchSysCache1(CLAOID,
										 ObjectIdGetDatum(object->objectId));
				if (!HeapTupleIsValid(opcTup))
					elog(ERROR, "cache lookup failed for opclass %u",
						 object->objectId);
				opcForm = (Form_pg_opclass) GETSTRUCT(opcTup);
				schema = get_namespace_name_or_temp(opcForm->opcnamespace);

				amTup = SearchSysCache1(AMOID,
										ObjectIdGetDatum(opcForm->opcmethod));
				if (!HeapTupleIsValid(amTup))
					elog(ERROR, "cache lookup failed for access method %u",
						 opcForm->opcmethod);
				amForm = (Form_pg_am) GETSTRUCT(amTup);

				appendStringInfo(&buffer, "%s USING %s",
								 quote_qualified_identifier(schema,
												  NameStr(opcForm->opcname)),
								 quote_identifier(NameStr(amForm->amname)));
				if (objname)
					*objname = list_make3(pstrdup(NameStr(amForm->amname)),
										  schema,
										  pstrdup(NameStr(opcForm->opcname)));

				ReleaseSysCache(amTup);
				ReleaseSysCache(opcTup);
				break;
			}

		case OCLASS_OPFAMILY:
			getOpFamilyIdentity(&buffer, object->objectId, objname);
			break;

		case OCLASS_AMOP:
			{
				Relation	amopDesc;
				HeapTuple	tup;
				ScanKeyData skey[1];
				SysScanDesc amscan;
				Form_pg_amop amopForm;
				StringInfoData opfam;
				char	   *ltype;
				char	   *rtype;

				amopDesc = heap_open(AccessMethodOperatorRelationId,
									 AccessShareLock);

				ScanKeyInit(&skey[0],
							ObjectIdAttributeNumber,
							BTEqualStrategyNumber, F_OIDEQ,
							ObjectIdGetDatum(object->objectId));

				amscan = systable_beginscan(amopDesc, AccessMethodOperatorOidIndexId, true,
											NULL, 1, skey);

				tup = systable_getnext(amscan);

				if (!HeapTupleIsValid(tup))
					elog(ERROR, "could not find tuple for amop entry %u",
						 object->objectId);

				amopForm = (Form_pg_amop) GETSTRUCT(tup);

				initStringInfo(&opfam);
				getOpFamilyIdentity(&opfam, amopForm->amopfamily, objname);

				ltype = format_type_be_qualified(amopForm->amoplefttype);
				rtype = format_type_be_qualified(amopForm->amoprighttype);

				if (objname)
				{
					*objname = lappend(*objname,
									 psprintf("%d", amopForm->amopstrategy));
					*objargs = list_make2(ltype, rtype);
				}

				appendStringInfo(&buffer, "operator %d (%s, %s) of %s",
								 amopForm->amopstrategy,
								 ltype, rtype, opfam.data);

				pfree(opfam.data);

				systable_endscan(amscan);
				heap_close(amopDesc, AccessShareLock);
				break;
			}

		case OCLASS_AMPROC:
			{
				Relation	amprocDesc;
				ScanKeyData skey[1];
				SysScanDesc amscan;
				HeapTuple	tup;
				Form_pg_amproc amprocForm;
				StringInfoData opfam;
				char	   *ltype;
				char	   *rtype;

				amprocDesc = heap_open(AccessMethodProcedureRelationId,
									   AccessShareLock);

				ScanKeyInit(&skey[0],
							ObjectIdAttributeNumber,
							BTEqualStrategyNumber, F_OIDEQ,
							ObjectIdGetDatum(object->objectId));

				amscan = systable_beginscan(amprocDesc, AccessMethodProcedureOidIndexId, true,
											NULL, 1, skey);

				tup = systable_getnext(amscan);

				if (!HeapTupleIsValid(tup))
					elog(ERROR, "could not find tuple for amproc entry %u",
						 object->objectId);

				amprocForm = (Form_pg_amproc) GETSTRUCT(tup);

				initStringInfo(&opfam);
				getOpFamilyIdentity(&opfam, amprocForm->amprocfamily, objname);

				ltype = format_type_be_qualified(amprocForm->amproclefttype);
				rtype = format_type_be_qualified(amprocForm->amprocrighttype);

				if (objname)
				{
					*objname = lappend(*objname,
									   psprintf("%d", amprocForm->amprocnum));
					*objargs = list_make2(ltype, rtype);
				}

				appendStringInfo(&buffer, "function %d (%s, %s) of %s",
								 amprocForm->amprocnum,
								 ltype, rtype, opfam.data);

				pfree(opfam.data);

				systable_endscan(amscan);
				heap_close(amprocDesc, AccessShareLock);
				break;
			}

		case OCLASS_REWRITE:
			{
				Relation	ruleDesc;
				HeapTuple	tup;
				Form_pg_rewrite rule;

				ruleDesc = heap_open(RewriteRelationId, AccessShareLock);

				tup = get_catalog_object_by_oid(ruleDesc, object->objectId);

				if (!HeapTupleIsValid(tup))
					elog(ERROR, "could not find tuple for rule %u",
						 object->objectId);

				rule = (Form_pg_rewrite) GETSTRUCT(tup);

				appendStringInfo(&buffer, "%s on ",
								 quote_identifier(NameStr(rule->rulename)));
				getRelationIdentity(&buffer, rule->ev_class, objname);
				if (objname)
					*objname = lappend(*objname, pstrdup(NameStr(rule->rulename)));

				heap_close(ruleDesc, AccessShareLock);
				break;
			}

		case OCLASS_TRIGGER:
			{
				Relation	trigDesc;
				HeapTuple	tup;
				Form_pg_trigger trig;

				trigDesc = heap_open(TriggerRelationId, AccessShareLock);

				tup = get_catalog_object_by_oid(trigDesc, object->objectId);

				if (!HeapTupleIsValid(tup))
					elog(ERROR, "could not find tuple for trigger %u",
						 object->objectId);

				trig = (Form_pg_trigger) GETSTRUCT(tup);

				appendStringInfo(&buffer, "%s on ",
								 quote_identifier(NameStr(trig->tgname)));
				getRelationIdentity(&buffer, trig->tgrelid, objname);
				if (objname)
					*objname = lappend(*objname, pstrdup(NameStr(trig->tgname)));

				heap_close(trigDesc, AccessShareLock);
				break;
			}

		case OCLASS_POLICY:
			{
				Relation	polDesc;
				HeapTuple	tup;
				Form_pg_policy policy;

				polDesc = heap_open(PolicyRelationId, AccessShareLock);

				tup = get_catalog_object_by_oid(polDesc, object->objectId);

				if (!HeapTupleIsValid(tup))
					elog(ERROR, "could not find tuple for policy %u",
						 object->objectId);

				policy = (Form_pg_policy) GETSTRUCT(tup);

				appendStringInfo(&buffer, "%s on ",
								 quote_identifier(NameStr(policy->polname)));
				getRelationIdentity(&buffer, policy->polrelid, objname);
				if (objname)
					*objname = lappend(*objname, pstrdup(NameStr(policy->polname)));

				heap_close(polDesc, AccessShareLock);
				break;
			}

		case OCLASS_SCHEMA:
			{
				char	   *nspname;

				nspname = get_namespace_name_or_temp(object->objectId);
				if (!nspname)
					elog(ERROR, "cache lookup failed for namespace %u",
						 object->objectId);
				appendStringInfoString(&buffer,
									   quote_identifier(nspname));
				if (objname)
					*objname = list_make1(nspname);
				break;
			}

		case OCLASS_TSPARSER:
			{
				HeapTuple	tup;
				Form_pg_ts_parser formParser;
				char	   *schema;

				tup = SearchSysCache1(TSPARSEROID,
									  ObjectIdGetDatum(object->objectId));
				if (!HeapTupleIsValid(tup))
					elog(ERROR, "cache lookup failed for text search parser %u",
						 object->objectId);
				formParser = (Form_pg_ts_parser) GETSTRUCT(tup);
				schema = get_namespace_name_or_temp(formParser->prsnamespace);
				appendStringInfoString(&buffer,
									   quote_qualified_identifier(schema,
											  NameStr(formParser->prsname)));
				if (objname)
					*objname = list_make2(schema,
									  pstrdup(NameStr(formParser->prsname)));
				ReleaseSysCache(tup);
				break;
			}

		case OCLASS_TSDICT:
			{
				HeapTuple	tup;
				Form_pg_ts_dict formDict;
				char	   *schema;

				tup = SearchSysCache1(TSDICTOID,
									  ObjectIdGetDatum(object->objectId));
				if (!HeapTupleIsValid(tup))
					elog(ERROR, "cache lookup failed for text search dictionary %u",
						 object->objectId);
				formDict = (Form_pg_ts_dict) GETSTRUCT(tup);
				schema = get_namespace_name_or_temp(formDict->dictnamespace);
				appendStringInfoString(&buffer,
									   quote_qualified_identifier(schema,
											   NameStr(formDict->dictname)));
				if (objname)
					*objname = list_make2(schema,
									   pstrdup(NameStr(formDict->dictname)));
				ReleaseSysCache(tup);
				break;
			}

		case OCLASS_TSTEMPLATE:
			{
				HeapTuple	tup;
				Form_pg_ts_template formTmpl;
				char	   *schema;

				tup = SearchSysCache1(TSTEMPLATEOID,
									  ObjectIdGetDatum(object->objectId));
				if (!HeapTupleIsValid(tup))
					elog(ERROR, "cache lookup failed for text search template %u",
						 object->objectId);
				formTmpl = (Form_pg_ts_template) GETSTRUCT(tup);
				schema = get_namespace_name_or_temp(formTmpl->tmplnamespace);
				appendStringInfoString(&buffer,
									   quote_qualified_identifier(schema,
											   NameStr(formTmpl->tmplname)));
				if (objname)
					*objname = list_make2(schema,
									   pstrdup(NameStr(formTmpl->tmplname)));
				ReleaseSysCache(tup);
				break;
			}

		case OCLASS_TSCONFIG:
			{
				HeapTuple	tup;
				Form_pg_ts_config formCfg;
				char	   *schema;

				tup = SearchSysCache1(TSCONFIGOID,
									  ObjectIdGetDatum(object->objectId));
				if (!HeapTupleIsValid(tup))
					elog(ERROR, "cache lookup failed for text search configuration %u",
						 object->objectId);
				formCfg = (Form_pg_ts_config) GETSTRUCT(tup);
				schema = get_namespace_name_or_temp(formCfg->cfgnamespace);
				appendStringInfoString(&buffer,
									   quote_qualified_identifier(schema,
												 NameStr(formCfg->cfgname)));
				if (objname)
					*objname = list_make2(schema,
										  pstrdup(NameStr(formCfg->cfgname)));
				ReleaseSysCache(tup);
				break;
			}

		case OCLASS_ROLE:
			{
				char	   *username;

				username = GetUserNameFromId(object->objectId, false);
				if (objname)
					*objname = list_make1(username);
				appendStringInfoString(&buffer,
									   quote_identifier(username));
				break;
			}

		case OCLASS_DATABASE:
			{
				char	   *datname;

				datname = get_database_name(object->objectId);
				if (!datname)
					elog(ERROR, "cache lookup failed for database %u",
						 object->objectId);
				if (objname)
					*objname = list_make1(datname);
				appendStringInfoString(&buffer,
									   quote_identifier(datname));
				break;
			}

		case OCLASS_TBLSPACE:
			{
				char	   *tblspace;

				tblspace = get_tablespace_name(object->objectId);
				if (!tblspace)
					elog(ERROR, "cache lookup failed for tablespace %u",
						 object->objectId);
				if (objname)
					*objname = list_make1(tblspace);
				appendStringInfoString(&buffer,
									   quote_identifier(tblspace));
				break;
			}

		case OCLASS_FDW:
			{
				ForeignDataWrapper *fdw;

				fdw = GetForeignDataWrapper(object->objectId);
				appendStringInfoString(&buffer, quote_identifier(fdw->fdwname));
				if (objname)
					*objname = list_make1(pstrdup(fdw->fdwname));
				break;
			}

		case OCLASS_FOREIGN_SERVER:
			{
				ForeignServer *srv;

				srv = GetForeignServer(object->objectId);
				appendStringInfoString(&buffer,
									   quote_identifier(srv->servername));
				if (objname)
					*objname = list_make1(pstrdup(srv->servername));
				break;
			}

		case OCLASS_USER_MAPPING:
			{
				HeapTuple	tup;
				Oid			useid;
				Form_pg_user_mapping umform;
				ForeignServer *srv;
				const char *usename;

				tup = SearchSysCache1(USERMAPPINGOID,
									  ObjectIdGetDatum(object->objectId));
				if (!HeapTupleIsValid(tup))
					elog(ERROR, "cache lookup failed for user mapping %u",
						 object->objectId);
				umform = (Form_pg_user_mapping) GETSTRUCT(tup);
				useid = umform->umuser;
				srv = GetForeignServer(umform->umserver);

				ReleaseSysCache(tup);

				if (OidIsValid(useid))
					usename = GetUserNameFromId(useid, false);
				else
					usename = "public";

				if (objname)
				{
					*objname = list_make1(pstrdup(usename));
					*objargs = list_make1(pstrdup(srv->servername));
				}

				appendStringInfo(&buffer, "%s on server %s",
								 quote_identifier(usename),
								 srv->servername);
				break;
			}

		case OCLASS_DEFACL:
			{
				Relation	defaclrel;
				ScanKeyData skey[1];
				SysScanDesc rcscan;
				HeapTuple	tup;
				Form_pg_default_acl defacl;
				char	   *schema;
				char	   *username;

				defaclrel = heap_open(DefaultAclRelationId, AccessShareLock);

				ScanKeyInit(&skey[0],
							ObjectIdAttributeNumber,
							BTEqualStrategyNumber, F_OIDEQ,
							ObjectIdGetDatum(object->objectId));

				rcscan = systable_beginscan(defaclrel, DefaultAclOidIndexId,
											true, NULL, 1, skey);

				tup = systable_getnext(rcscan);

				if (!HeapTupleIsValid(tup))
					elog(ERROR, "could not find tuple for default ACL %u",
						 object->objectId);

				defacl = (Form_pg_default_acl) GETSTRUCT(tup);

				username = GetUserNameFromId(defacl->defaclrole, false);
				appendStringInfo(&buffer,
								 "for role %s",
								 quote_identifier(username));

				if (OidIsValid(defacl->defaclnamespace))
				{
					schema = get_namespace_name_or_temp(defacl->defaclnamespace);
					appendStringInfo(&buffer,
									 " in schema %s",
									 quote_identifier(schema));
				}
				else
					schema = NULL;

				switch (defacl->defaclobjtype)
				{
					case DEFACLOBJ_RELATION:
						appendStringInfoString(&buffer,
											   " on tables");
						break;
					case DEFACLOBJ_SEQUENCE:
						appendStringInfoString(&buffer,
											   " on sequences");
						break;
					case DEFACLOBJ_FUNCTION:
						appendStringInfoString(&buffer,
											   " on functions");
						break;
					case DEFACLOBJ_TYPE:
						appendStringInfoString(&buffer,
											   " on types");
						break;
				}

				if (objname)
				{
					*objname = list_make1(username);
					if (schema)
						*objname = lappend(*objname, schema);
					*objargs = list_make1(psprintf("%c", defacl->defaclobjtype));
				}

				systable_endscan(rcscan);
				heap_close(defaclrel, AccessShareLock);
				break;
			}

		case OCLASS_EXTENSION:
			{
				char	   *extname;

				extname = get_extension_name(object->objectId);
				if (!extname)
					elog(ERROR, "cache lookup failed for extension %u",
						 object->objectId);
				appendStringInfoString(&buffer, quote_identifier(extname));
				if (objname)
					*objname = list_make1(extname);
				break;
			}

		case OCLASS_EVENT_TRIGGER:
			{
				HeapTuple	tup;
				Form_pg_event_trigger trigForm;

				/* no objname support here */
				if (objname)
					*objname = NIL;

				tup = SearchSysCache1(EVENTTRIGGEROID,
									  ObjectIdGetDatum(object->objectId));
				if (!HeapTupleIsValid(tup))
					elog(ERROR, "cache lookup failed for event trigger %u",
						 object->objectId);
				trigForm = (Form_pg_event_trigger) GETSTRUCT(tup);
				appendStringInfoString(&buffer,
							   quote_identifier(NameStr(trigForm->evtname)));
				ReleaseSysCache(tup);
				break;
			}

		case OCLASS_TRANSFORM:
			{
				Relation	transformDesc;
				HeapTuple	tup;
				Form_pg_transform transform;
				char	   *transformLang;
				char	   *transformType;

				transformDesc = heap_open(TransformRelationId, AccessShareLock);

				tup = get_catalog_object_by_oid(transformDesc, object->objectId);

				if (!HeapTupleIsValid(tup))
					elog(ERROR, "could not find tuple for transform %u",
						 object->objectId);

				transform = (Form_pg_transform) GETSTRUCT(tup);

				transformType = format_type_be_qualified(transform->trftype);
				transformLang = get_language_name(transform->trflang, false);

				appendStringInfo(&buffer, "for %s on language %s",
								 transformType,
								 transformLang);
				if (objname)
				{
					*objname = list_make1(transformType);
					*objargs = list_make1(pstrdup(transformLang));
				}

				heap_close(transformDesc, AccessShareLock);
			}
			break;

		default:
			appendStringInfo(&buffer, "unrecognized object %u %u %d",
							 object->classId,
							 object->objectId,
							 object->objectSubId);
			break;
	}

	/*
	 * If a get_object_address representation was requested, make sure we are
	 * providing one.  We don't check objargs, because many of the cases above
	 * leave it as NIL.
	 */
	if (objname && *objname == NIL)
		elog(ERROR, "requested object address for unsupported object class %d: text result \"%s\"",
			 (int) getObjectClass(object), buffer.data);

	return buffer.data;
}

static void
getOpFamilyIdentity(StringInfo buffer, Oid opfid, List **objname)
{
	HeapTuple	opfTup;
	Form_pg_opfamily opfForm;
	HeapTuple	amTup;
	Form_pg_am	amForm;
	char	   *schema;

	opfTup = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(opfid));
	if (!HeapTupleIsValid(opfTup))
		elog(ERROR, "cache lookup failed for opfamily %u", opfid);
	opfForm = (Form_pg_opfamily) GETSTRUCT(opfTup);

	amTup = SearchSysCache1(AMOID, ObjectIdGetDatum(opfForm->opfmethod));
	if (!HeapTupleIsValid(amTup))
		elog(ERROR, "cache lookup failed for access method %u",
			 opfForm->opfmethod);
	amForm = (Form_pg_am) GETSTRUCT(amTup);

	schema = get_namespace_name_or_temp(opfForm->opfnamespace);
	appendStringInfo(buffer, "%s USING %s",
					 quote_qualified_identifier(schema,
												NameStr(opfForm->opfname)),
					 NameStr(amForm->amname));

	if (objname)
		*objname = list_make3(pstrdup(NameStr(amForm->amname)),
							  pstrdup(schema),
							  pstrdup(NameStr(opfForm->opfname)));

	ReleaseSysCache(amTup);
	ReleaseSysCache(opfTup);
}

/*
 * Append the relation identity (quoted qualified name) to the given
 * StringInfo.
 */
static void
getRelationIdentity(StringInfo buffer, Oid relid, List **objname)
{
	HeapTuple	relTup;
	Form_pg_class relForm;
	char	   *schema;

	relTup = SearchSysCache1(RELOID,
							 ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(relTup))
		elog(ERROR, "cache lookup failed for relation %u", relid);
	relForm = (Form_pg_class) GETSTRUCT(relTup);

	schema = get_namespace_name_or_temp(relForm->relnamespace);
	appendStringInfoString(buffer,
						   quote_qualified_identifier(schema,
												 NameStr(relForm->relname)));
	if (objname)
		*objname = list_make2(schema, pstrdup(NameStr(relForm->relname)));

	ReleaseSysCache(relTup);
}


/*
 * Auxiliary function to return a TEXT array out of a list of C-strings.
 */
ArrayType *
strlist_to_textarray(List *list)
{
	ArrayType  *arr;
	Datum	   *datums;
	int			j = 0;
	ListCell   *cell;
	MemoryContext memcxt;
	MemoryContext oldcxt;

	memcxt = AllocSetContextCreate(CurrentMemoryContext,
								   "strlist to array",
								   ALLOCSET_DEFAULT_MINSIZE,
								   ALLOCSET_DEFAULT_INITSIZE,
								   ALLOCSET_DEFAULT_MAXSIZE);
	oldcxt = MemoryContextSwitchTo(memcxt);

	datums = palloc(sizeof(text *) * list_length(list));
	foreach(cell, list)
	{
		char	   *name = lfirst(cell);

		datums[j++] = CStringGetTextDatum(name);
	}

	MemoryContextSwitchTo(oldcxt);

	arr = construct_array(datums, list_length(list),
						  TEXTOID, -1, false, 'i');
	MemoryContextDelete(memcxt);

	return arr;
}
