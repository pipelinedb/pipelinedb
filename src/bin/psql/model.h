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
 * The lengths are used by the view part (Screen) to format the display.
 *
 * Maxlen of a column is a high watermark. It never shrinks.
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

extern void ModelHeaderRow(Model *m, Row *r);
extern void ModelKeyRow(Model *m, Row *r);
extern void ModelInsertRow(Model *m, Row *r);
extern void ModelUpdateRow(Model *m, Row *r);
extern void ModelDeleteRow(Model *m, Row *r);

/* debug funcs */
extern void ModelDump(Model *m);
extern void ModelInsertRowFromString(Model *m, const char *s);

#endif
