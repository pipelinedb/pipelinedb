/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * Copyright (C) 2004 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include "postgres.h"
#include "executor/spi.h"       /* this is what you need to work with SPI */
#include "commands/trigger.h"   /* ... and triggers */

#include "../postgis_config.h"
#include "lwgeom_pg.h"
#include "utils/rel.h"

Datum cache_bbox(PG_FUNCTION_ARGS);

/** @file
 * 	The intended use for this trigger function is making
 * 	a geometry field cache it's bbox. Use like this:
 *
 * 	CREATE TRIGGER <name> BEFORE INSERT OR UPDATE
 *		ON <table> FOR EACH ROW EXECUTE PROCEDURE
 *		cache_bbox(<field>);
 *
 */
PG_FUNCTION_INFO_V1(cache_bbox);
Datum cache_bbox(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	Trigger *trigger;
	TupleDesc   tupdesc;
	HeapTuple   rettuple;
	bool        isnull;
	Datum in, out;
	int attno, ret;

	/* make sure it's called as a trigger at all */
	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "cache_bbox: not called by trigger manager");

	/*
	 * make sure it's called with at least one argument
	 * (the geometry fields)
	 */
	if ( trigdata->tg_trigger->tgnargs != 1 )
		elog(ERROR, "trigger 'cache_bbox' must be called with one argument");

	trigger = trigdata->tg_trigger;

	/* tuple to return to executor */
	if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		rettuple = trigdata->tg_newtuple;
	else
		rettuple = trigdata->tg_trigtuple;

	/* Do nothing when fired by delete, after or for statement */
	if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
	{
		elog(NOTICE, "Useless cache_box trigger fired by DELETE");
		return PointerGetDatum(rettuple);
	}
	if (TRIGGER_FIRED_AFTER(trigdata->tg_event))
	{
		elog(NOTICE, "Useless cache_box trigger fired AFTER");
		return PointerGetDatum(rettuple);
	}
	if (TRIGGER_FIRED_FOR_STATEMENT(trigdata->tg_event))
	{
		elog(NOTICE, "Useless cache_box trigger fired for STATEMENT");
		return PointerGetDatum(rettuple);
	}

	tupdesc = trigdata->tg_relation->rd_att;

	/* Connect to SPI manager */
	if ((ret = SPI_connect()) < 0)
		elog(ERROR, "cache_bbox: SPI_connect returned %d", ret);

	/* Find number of requested argument */
	attno = SPI_fnumber(tupdesc, trigger->tgargs[0]);
	if ( attno == SPI_ERROR_NOATTRIBUTE )
		elog(ERROR, "trigger %s can't find attribute %s",
		     trigger->tgname, trigger->tgargs[0]);

	/* Find number of requested argument */
	if ( strcmp(SPI_gettype(tupdesc, attno), "geometry") )
		elog(ERROR, "trigger %s requested to apply to a non-geometry field (%s)", trigger->tgname, trigger->tgargs[0]);

	/* Get input lwgeom */
	in = SPI_getbinval(rettuple, tupdesc, attno, &isnull);

	if ( ! isnull )
	{
		out = PointerGetDatum(DirectFunctionCall1(LWGEOM_addBBOX, in));

		rettuple = SPI_modifytuple(trigdata->tg_relation, rettuple,
		                           1, &attno, &out, NULL);
	}

	/* Disconnect from SPI */
	SPI_finish();

	return PointerGetDatum(rettuple);
}
