/*-------------------------------------------------------------------------
 *
 * model.h
 *    Interface for the adhoc client model
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/bin/psql/model.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MODEL_H
#define MODEL_H

#include "adhoc_util.h"
#include "rowmap.h"

/* 
 * The model part of the adhoc client MVC.
 *
 * All row operations pass through here, and this module is mainly responsible
 * for updating the rowmap, and keeping track of the maximum length for
 * each column.
 *
 * The lengths are used by the view part (Screen) to format the display
 *
 */

typedef struct Model
{
	RowMap  *rowmap;
	size_t  *maxlens;
	size_t  nfields;
	Row     header;
} Model;

extern Model *ModelInit(void);
extern void ModelDestroy(Model *m);

/* row ops */
extern void ModelAddRow(Model *m, Row *r);
extern void ModelInsertRow(Model *m, Row *r);
extern void ModelDeleteRow(Model *m, Row *r);

/* column names */
extern void ModelSetHeader(Model *m, Row *r);

/* set the columns that make up the key */
extern void ModelSetKey(Model *m, Row *r);

/* debug funcs */
extern void ModelDump(Model *m);
extern void ModelAddRowFromString(Model *m, const char *s);

#endif
