/*
 * psql - the PostgreSQL interactive terminal
 *
 * Support for PipelineDB commands
 *
 * src/bin/psql/pipeline.c
 */
#include "postgres_fe.h"

#include <ctype.h>

#include "common.h"
#include "dumputils.h"
#include "settings.h"
#include "pipeline.h"

#define SELECT_CONTINUOUS_VIEWS \
"SELECT pipeline_queries.id AS Id, pg_namespace.nspname AS Schema, pipeline_queries.name AS Name, pipeline_queries.query AS Query "\
"  FROM pipeline_queries() JOIN pg_namespace ON pipeline_query.namespace = pg_namespace.oid"

/*
 * \zq
 *
 * List all REGISTERed PipelineDB queries
 */
extern bool
listPipelineQuery(void)
{

	PQExpBufferData buf;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;

	initPQExpBuffer(&buf);
	printfPQExpBuffer(&buf,	SELECT_CONTINUOUS_VIEWS);

	res = PSQLexec(buf.data, false);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	if (PQntuples(res) == 0 && !pset.quiet)
	{
		fprintf(pset.queryFout, _("No continuous views found.\n"));
	}
	else
	{
		myopt.nullPrint = NULL;
		myopt.title = _("List of continuous views");
		myopt.translate_header = true;

		printQuery(res, &myopt, pset.queryFout, pset.logfile);
	}

	PQclear(res);
	return true;
}
