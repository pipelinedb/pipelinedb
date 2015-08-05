#ifndef MODEL_H_4D955674
#define MODEL_H_4D955674

#include "adhoc_util.h"
#include "rowmap.h"

typedef struct Model
{
	RowMap *rowmap;
	size_t* maxlens;
	size_t nfields;

	Row header;

} Model;

#define MIN(a, b)  (((a) < (b)) ? (a) : (b))
#define MAX(a, b)  (((a) > (b)) ? (a) : (b))

Model* ModelInit(void);
void ModelDestroy(Model* m);

void ModelUpdateLens(Model *m, Row* r);
void ModelAddRow(Model *m, Row* r);
void ModelDeleteRow(Model *m, Row* r);

void ModelSetHeader(Model *m, Row* r);
void ModelSetKey(Model *m, Row* r);

void ModelDump(Model *m);
void add_row(Model *m, const char* s);

#endif
