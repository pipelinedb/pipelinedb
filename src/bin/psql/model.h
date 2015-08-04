#ifndef MODEL_H_4D955674
#define MODEL_H_4D955674

#include "rowmap.h"

typedef struct Model
{
	RowMap *rowmap;
	size_t* maxlens;
	size_t nfields;

} Model;

#define MIN(a, b)  (((a) < (b)) ? (a) : (b))
#define MAX(a, b)  (((a) > (b)) ? (a) : (b))

void ModelUpdateLens(Model *m, Row* r);
void ModelAddRow(Model *m, Row* r);
void ModelDump(Model *m);

void add_row(Model *m, const char* s);


#endif
