#include "postgres_fe.h"
#include "model.h"

void ModelUpdateLens(Model *m, Row* r)
{
	size_t i = 0;

	if (m->nfields < RowSize(r))
	{
		m->maxlens = pg_realloc(m->maxlens, RowSize(r) * sizeof(size_t));

		for (i = m->nfields; i < RowSize(r); ++i)
		{
			m->maxlens[i] = 0;
		}

		m->nfields = RowSize(r);
	}

	for (i = 0; i < m->nfields; ++i)
	{
		m->maxlens[i] = MAX(m->maxlens[i], RowFieldLength(r, i)); 
	}
}

Model* ModelInit()
{
	Model* m = pg_malloc(sizeof(Model));
	memset(m, 0, sizeof(Model));

	m->rowmap = RowMapInit();

	return m;
}

void ModelDestroy(Model *m)
{
	RowMapDestroy(m->rowmap);
	RowCleanup(&m->header);

	pg_free(m->maxlens);
	memset(m, 0, sizeof(Model));

	pg_free(m);

	RowKeyReset();
}

void ModelAddRow(Model *m, Row* r)
{
	ModelUpdateLens(m, r);
	RowMapUpdate(m->rowmap, r);
}

void ModelDeleteRow(Model *m, Row* r)
{
	RowMapErase(m->rowmap, r);
	RowCleanup(r);
}

static size_t FindInRow(Row *r, const char* s)
{
	size_t i = 0;

	for (i = 0; i < RowSize(r); ++i)
	{
		if (strcmp(RowFieldValue(r,i), s) == 0)
		{
			break;
		}
	}

	return i;
}

void ModelSetKey(Model *m, Row* r)
{
	// key names must exist in the header.
	
	size_t i = 0;

	RowKeyReset();

	for (i = 0; i < RowSize(r); ++i)
	{
		size_t ki = FindInRow(&m->header, RowFieldValue(r, i));

		if (ki == RowSize(&m->header))
		{
			FATAL_ERROR("key %s does not exist in header", RowFieldValue(r,i));
		}

		RowKeyAdd(ki);
	}

	RowCleanup(r);
}

void ModelSetHeader(Model *m, Row* r)
{
	ModelUpdateLens(m, r);

	RowCleanup(&m->header);
	m->header = *r;
}

void ModelDump(Model *m)
{
	size_t i = 0;

	printf("nfield %zu\n", m->nfields);

	for (i = 0; i < m->nfields; ++i)
	{
		printf("field %zu %zu\n", i, m->maxlens[i]);
	}
}

size_t spaces(const char* s);

size_t spaces(const char* s)
{
	size_t cnt = 0;

	while (*s)
	{
		if (*s == ' ')
		{
			cnt++;
		}

		s++;
	}

	return cnt;
}

Row make_row(const char* s);

Row make_row(const char* s)
{
	// fields, n;
	
	Row row = {0,0,0};
	size_t n = spaces(s) + 1;
	char *d = strdup(s);
	char *sptr = d;

	size_t i = 0;

	row.ptr = d;
	row.fields = pg_malloc(n * sizeof(Field));
	row.n = n;

	for (i = 0; i < n; ++i)
	{
		char* tok = strtok(sptr, " ");
		sptr = 0;

		row.fields[i].data = tok;
		row.fields[i].n = strlen(tok);
	}

	return row;
}

void add_row(Model *m, const char* s)
{
	Row row = make_row(s);
	ModelAddRow(m, &row);
}
