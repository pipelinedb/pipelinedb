/*
 * psql - the PostgreSQL interactive terminal
 *
 * Support for PipelineDB commands
 *
 * Copyright (c) 2013-2015, PipelineDB
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
listPipelineQuery(void)
{

	PQExpBufferData buf;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;

	initPQExpBuffer(&buf);
	printfPQExpBuffer(&buf,	"SELECT * FROM pipeline_queries()");

	res = PSQLexec(buf.data);
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

		printQuery(res, &myopt, pset.queryFout, false, pset.logfile);
	}

	PQclear(res);
	return true;
}
