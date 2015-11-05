#include "postgres_fe.h"
#include "model.h"

/* utility function for creating a row from a string */
Row
make_row(const char *s)
{
	Row row = {0,0,0};
	size_t n = spaces(s) + 1;
	char *d = pg_strdup(s);
	char *sptr = d;

	size_t i = 0;

	row.ptr = d;
	row.fields = pg_malloc(n * sizeof(Field));
	row.n = n;

	for (i = 0; i < n; ++i)
	{
		char *tok = strtok(sptr, " ");
		sptr = 0;

		row.fields[i].data = tok;
		row.fields[i].n = strlen(tok);
	}

	return row;
}

/* tracks the maximum length of each column. */
static void
model_update_lens(Model *m, Row *r)
{
	size_t i = 0;

	if (m->nfields < RowSize(r))
	{
		m->maxlens = pg_realloc(m->maxlens, RowSize(r) * sizeof(size_t));

		for (i = m->nfields; i < RowSize(r); ++i)
			m->maxlens[i] = 0;

		m->nfields = RowSize(r);
	}

	for (i = 0; i < m->nfields; ++i)
		m->maxlens[i] = Max(m->maxlens[i], RowFieldLength(r, i));
}

/* allocates and initializes the model. */
Model *ModelInit()
{
	Model *m = pg_malloc(sizeof(Model));
	memset(m, 0, sizeof(Model));

	m->rowmap = RowMapInit();

	return m;
}

/* cleans up resources and calls pg_free on m */
void
ModelDestroy(Model *m)
{
	RowMapDestroy(m->rowmap);
	RowCleanup(&m->header);

	pg_free(m->maxlens);
	memset(m, 0, sizeof(Model));

	pg_free(m);
	RowKeyReset();
}

/* Set the column names  */
void
ModelHeaderRow(Model *m, Row *r)
{
	model_update_lens(m, r);

	RowCleanup(&m->header);
	m->header = *r;
}

/* Set the columns used in the key */
void
ModelKeyRow(Model *m, Row *r)
{
	/* key names must exist in the header */
	size_t i = 0;

	RowKeyReset();

	for (i = 0; i < RowSize(r); ++i)
	{
		size_t ki = atoi(RowFieldValue(r, i)) - 1;

		RowKeyAdd(ki);
	}

	RowCleanup(r);
}

void
ModelInsertRow(Model *m, Row *r)
{
	model_update_lens(m, r);
	RowMapUpdate(m->rowmap, r);
}

void
ModelUpdateRow(Model *m, Row *r)
{
	model_update_lens(m, r);
	RowMapUpdate(m->rowmap, r);
}

void
ModelDeleteRow(Model *m, Row *key)
{
	RowMapErase(m->rowmap, key);
	RowCleanup(key);
}

/* Debugging utilities */
void
ModelDump(Model *m)
{
	size_t i = 0;

	printf("nfield %zu\n", m->nfields);

	for (i = 0; i < m->nfields; ++i)
		printf("field %zu %zu\n", i, m->maxlens[i]);
}

void
ModelInsertRowFromString(Model *m, const char *s)
{
	Row row = make_row(s);
	ModelInsertRow(m, &row);
}
