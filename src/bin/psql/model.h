#ifndef MODEL_H_4D955674
#define MODEL_H_4D955674

#include "adhoc_util.h"
#include "rowmap.h"

typedef struct Model
{
	RowMap  *rowmap;
	size_t  *maxlens;
	size_t  nfields;
	Row     header;
} Model;

extern Model *ModelInit(void);
extern void ModelDestroy(Model *m);

extern void ModelUpdateLens(Model *m, Row *r);
extern void ModelAddRow(Model *m, Row *r);
extern void ModelInsertRow(Model *m, Row *r);
extern void ModelDeleteRow(Model *m, Row *r);

extern void ModelSetHeader(Model *m, Row *r);
extern void ModelSetKey(Model *m, Row *r);

extern void ModelDump(Model *m);
extern void ModelAddRowFromString(Model *m, const char *s);

#endif
