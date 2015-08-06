#include "postgres_fe.h"
#include "screen.h"

Screen *ScreenInit(Model *model)
{
	const char *tty = "/dev/tty";
	const char *term_type = 0;
	Screen *self = pg_malloc(sizeof(Screen));
	memset(self, 0, sizeof(Screen));

	self->model = model;
	memset(&self->key, 0, sizeof(Row));

	term_type = getenv("TERM");

	if (term_type == NULL || *term_type == '\0') {
		term_type = "unknown";
	}

	self->term_in = fopen(tty, "r");

	if (self->term_in == NULL) {
		FATAL_ERROR("could not open %s", tty);
	}

	self->fd = fileno(self->term_in);
	self->nterm = newterm(term_type, stdout, self->term_in);
	set_term(self->nterm);

	timeout(0);
	clear();
	noecho();
	cbreak();
	curs_set(0);

	keypad(stdscr, TRUE);
	refresh();

	self->x_pos = 0;
	self->x_col = 0;

	self->pause = 0;

	return self;
}

void
ScreenDestroy(Screen *s)
{
	endwin();
	delscreen(s->nterm);

	fclose(s->term_in);
	RowCleanup(&s->key);

	memset(s, 0, sizeof(Screen));
	pg_free(s);
}

int
ScreenFd(Screen *s)
{
	return s->fd;
}

static void
ScreenSync(Screen *s)
{
	refresh();
}

static inline RowMap *
rowmap(Screen *s)
{
	return s->model->rowmap;
}

static inline Row *
key(Screen *s)
{
	return &s->key;
}

static inline int
lines(Screen *s)
{
	return LINES;
}

static inline size_t
maxlen(Screen *s, size_t i)
{
	return s->model->maxlens[i];
}

static inline size_t
numfields(Screen *s)
{
	return s->model->nfields;
}

static inline size_t
rightmost(Screen *s)
{
	return s->model->nfields - 1;
}

static inline void
printwpad(int n)
{
	int i = 0;

	for (i = 0; i < n; ++i) {
		printw("%c", ' ');
	}
}

static inline void
draw_row(Screen *s, Row *row, char sep)
{
	size_t i = 0;

	for (i = s->x_col; i < RowSize(row); ++i)
	{
		size_t padlen = maxlen(s, i);

		char *fv = RowFieldValue(row, i);
		size_t fn = RowFieldLength(row, i);

		if (i == s->x_col) {

			if (s->x_pos < fn) {

				printw("%s", fv + s->x_pos);
				printwpad(padlen - fn);
			}
			else {
				printwpad(padlen - s->x_pos);
			}
		}
		else
		{
			printw("%s", fv);
			printwpad(padlen - fn);
		}

		if (i != (RowSize(row) - 1))
			printw("%c", sep);
	}

	printw("\n");
	clrtoeol();
}

static void
ScreenRender(Screen *s)
{
	RowIterator iter = RowMapLowerBound(rowmap(s), key(s));
	RowIterator end = RowMapEnd(rowmap(s));

	int i = 0;
	int ctr = 0;
	move(ctr, 0);

	attron(A_REVERSE);
	draw_row(s, &s->model->header, '|');
	attroff(A_REVERSE);
	ctr++;

	for (; iter != end && ctr < lines(s); iter++, ++ctr)
	{
		draw_row(s, iter, ' ');
		clrtoeol();
	}

	for (i = ctr; i < lines(s); ++i) {
		printw("\n");
		clrtoeol();
	}
}

void
ScreenUpdate(Screen *s)
{
	ScreenRender(s);
	ScreenSync(s);
}

static void inline
set_key(Screen *s, Row r)
{
	RowCleanup(&s->key);
	s->key = r;
}

static void inline
clear_key(Screen *s)
{
	RowCleanup(&s->key);
}

static void
ScreenScrollUp(Screen *s)
{
	RowIterator iter = RowMapLowerBound(rowmap(s), key(s));

	if (iter != RowMapBegin(rowmap(s))) {
		iter--;

		set_key(s, RowGetKey(iter));
	} else {
		clear_key(s);
	}
}

static void
ScreenScrollDown(Screen *s)
{
	RowIterator iter = RowMapLowerBound(rowmap(s), key(s));

	if (iter != RowMapEnd(rowmap(s)))
	{
		iter++;

		if (iter != RowMapEnd(rowmap(s)))
			set_key(s, RowGetKey(iter));
	}
}

static void
ScreenScrollLeft(Screen *s)
{
	s->x_pos--;

	if (s->x_pos < 0) {

		s->x_col--;

		if (s->x_col < 0) {
			s->x_col = 0;
			s->x_pos = 0;
		}
		else
		{
			s->x_pos = maxlen(s, s->x_col);
		}
	}
}

static void
ScreenScrollRight(Screen *s)
{
	s->x_pos++;

	if (s->x_pos > maxlen(s,s->x_col)) {

		s->x_pos = 0;
		s->x_col++;

		if (s->x_col >= numfields(s)) {

			s->x_col = rightmost(s);
			s->x_pos = maxlen(s,s->x_col);
		}
	}
}

static void
ScreenPageUp(Screen *s)
{
	int i = 0;

	for (i = 0; i < lines(s); ++i) {
		ScreenScrollUp(s);
	}
}

static void
ScreenPageDown(Screen *s)
{
	int i = 0;

	for (i = 0; i < lines(s); ++i) {
		ScreenScrollDown(s);
	}
}

static void
ScreenNextCol(Screen *s)
{
	s->x_pos = 0;
	s->x_col++;

	if (s->x_col >= numfields(s)) {
		s->x_col = rightmost(s);
	}
}

static void
ScreenPrevCol(Screen *s)
{
	if (s->x_pos != 0)
	{
		s->x_pos = 0;
	}
	else
	{
		s->x_pos = 0;
		s->x_col--;

		if (s->x_col < 0)
			s->x_col = 0;
	}
}

static void
ScreenHome(Screen *s)
{
	s->x_col = 0;
	s->x_pos = 0;
}

static void
ScreenEnd(Screen *s)
{
	s->x_col = rightmost(s);
	s->x_pos = 0;
}

static void
ScreenTogglePause(Screen *s)
{
	s->pause = !s->pause;
}

void
ScreenHandleInput(Screen *s)
{
	int c = getch();

	switch(c)
	{
		case KEY_UP:
			ScreenScrollUp(s);
			break;
		case KEY_DOWN:
			ScreenScrollDown(s);
			break;
		case KEY_LEFT:
			ScreenScrollLeft(s);
			break;
		case KEY_RIGHT:
			ScreenScrollRight(s);
			break;
		case KEY_NPAGE:
			ScreenPageDown(s);
			break;
		case KEY_HOME:
			ScreenHome(s);
			break;
		case KEY_END:
			ScreenEnd(s);
			break;
		case KEY_PPAGE:
			ScreenPageUp(s);
			break;
		case KEY_RESIZE:
			break;
		case 9: /* TAB */
			ScreenNextCol(s);
			break;
		case 353: /* SHIFT+TAB */
			ScreenPrevCol(s);
			break;
		case 'p':
			ScreenTogglePause(s);
			break;
		case ERR:
			break;
		default:
			break;
	}

	ScreenUpdate(s);
}

bool
ScreenIsPaused(Screen *s)
{
	return s->pause;
}
