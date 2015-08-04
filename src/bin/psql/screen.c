#include "screen.h"

#include <stdlib.h>
#include <string.h>

void ScreenSync(Screen *s);
void ScreenRender(Screen *s);

void ScreenScrollUp(Screen *s);
void ScreenScrollDown(Screen *s);
void ScreenScrollLeft(Screen *s);
void ScreenScrollRight(Screen *s);
void ScreenPageDown(Screen *s);
void ScreenHome(Screen *s);
void ScreenEnd(Screen *s);
void ScreenPageUp(Screen *s);
void ScreenNextCol(Screen *s);
void ScreenPrevCol(Screen *s);
void ScreenTogglePause(Screen *s);

Screen* ScreenInit(Model *model)
{
	const char* term_type = 0;
	Screen* self = malloc(sizeof(Screen));

	memset(self, 0, sizeof(Screen));

	self->model = model;
	init_flex(&self->key);

	term_type = getenv("TERM");

	if (term_type == NULL || *term_type == '\0') {
		term_type = "unknown";
	}

	self->term_in = fopen("/dev/tty", "r");

	if (self->term_in == NULL) {
		die("fopen");
	}

	self->fd = fileno(self->term_in);
	set_term(newterm(term_type, stdout, self->term_in));

	timeout(0);
	clear();
	noecho();
	cbreak();
	curs_set(0);

	keypad(stdscr, TRUE);
	refresh();

	self->x_pos = 0;
	self->x_col = 0;

	return self;
}

int ScreenFd(Screen *s)
{
	return s->fd;
}

void ScreenDestroy(Screen* s)
{
}

void ScreenSync(Screen *s)
{
	refresh();
}

static inline RowMap* rowmap(Screen *s)
{
	return s->model->rowmap;
}

static inline const char* key(Screen *s)
{
	return s->key.buf;
}

static inline int lines(Screen *s)
{
	return LINES;
}

static inline size_t maxlen(Screen *s, size_t i)
{
	return s->model->maxlens[i];
}

static inline void printwpad(int n)
{
	int i = 0;

	for (i = 0; i < n; ++i) {
		printw("%c", ' ');
	}
}

static inline void draw_row(Screen *s, Row *row, char sep)
{
	size_t i = 0;

	for (i = s->x_col; i < RowSize(row); ++i)
	{
		size_t padlen = maxlen(s, i);

		char* fv = RowFieldValue(row, i);
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
		{
			printw("%c", sep);
		}
	}

	printw("\n");
	clrtoeol();
}

void ScreenRender(Screen *s)
{
	RowIterator iter = RowMapLowerBound(rowmap(s), key(s));
	RowIterator end = RowMapEnd(rowmap(s));

	int i = 0;
	int ctr = 0;
	move(ctr, 0);

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

void ScreenUpdate(Screen *s)
{
	ScreenRender(s);
	ScreenSync(s);
}

static void inline set_key(Screen *s, const char* str)
{
	reset_flex(&s->key);
	append_flex(&s->key, str, strlen(str));
}

void ScreenScrollUp(Screen *s)
{
	RowIterator iter = RowMapLowerBound(rowmap(s), key(s));

	if (iter != RowMapBegin(rowmap(s))) {
		iter--;

		set_key(s, RowGetKey(iter));
	} else {
		set_key(s, "");
	}
}

void ScreenScrollDown(Screen *s)
{
	RowIterator iter = RowMapLowerBound(rowmap(s), key(s));

	if (iter != RowMapEnd(rowmap(s)))
	{
		iter++;

		if (iter != RowMapEnd(rowmap(s))) {

			set_key(s, RowGetKey(iter));
		}
	}
}

void ScreenScrollLeft(Screen *s) {} 
void ScreenScrollRight(Screen *s) {} 
void ScreenPageDown(Screen *s) {} 
void ScreenHome(Screen *s) {} 
void ScreenEnd(Screen *s) {} 
void ScreenPageUp(Screen *s) {} 
void ScreenNextCol(Screen *s) {} 
void ScreenPrevCol(Screen *s) {} 
void ScreenTogglePause(Screen *s) {} 

void ScreenHandleInput(Screen *s)
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

		case 9: // TAB
			ScreenNextCol(s);
			break;

		case 353: // SHIFT+TAB
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