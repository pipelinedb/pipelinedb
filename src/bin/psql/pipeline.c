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

/*
 * \zq
 *
 * List all REGISTERed PipelineDB queries
 */
extern bool
listPipelineQueries(void)
{

	PQExpBufferData buf;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;

	initPQExpBuffer(&buf);
	printfPQExpBuffer(&buf, "SELECT * FROM pipeline_queries");

	res = PSQLexec(buf.data, false);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	myopt.nullPrint = NULL;
	myopt.title = _("List of continuous views");
	myopt.translate_header = true;

	printQuery(res, &myopt, pset.queryFout, pset.logfile);
	PQclear(res);

	return true;
}
