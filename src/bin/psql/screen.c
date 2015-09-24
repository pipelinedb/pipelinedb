#include "postgres_fe.h"
#include "screen.h"

#define NCURSES_ENABLE_STDBOOL_H 0
#include <ncurses.h>
#undef bool

FILE* debug_log = 0;

/*
 * Allocates and initializes a new Screen.
 */
Screen *ScreenInit(Model *model)
{
	const char *tty = "/dev/tty";
	const char *term_type = 0;

	Screen *self = pg_malloc(sizeof(Screen));
	memset(self, 0, sizeof(Screen));

	debug_log = fopen("/tmp/debug_log.txt", "w");

	self->model = model;
	memset(&self->key, 0, sizeof(Row));

	/*
	 * Open /dev/tty and set that up as the ncurses terminal. We use newterm
	 * instead of initscr so that we can read row events from stdin.
	 */

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

	/* make ncurses non blocking */
	timeout(0);

	/* clear the tty internal state */
	clear();

	/* disable character echo */
	noecho();

	/* disable input line buffering */
	cbreak();

	/* hide cursor */
	curs_set(0);

	/* enable extra keys */
	keypad(stdscr, TRUE);

	/* updates the screen (should be blank now) */
	refresh();

	self->x_pos = 0;
	self->x_col = 0;
	self->pause = 0;

	return self;
}

/*
 * Cleans up resources and calls pg_free on s
 */
void
ScreenDestroy(Screen *s)
{
	/* resets the tty to original state */
	endwin();

	/* ncurses resource cleanup */
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
screen_sync(Screen *s)
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

/*
 * Render a row, taking column, x_pos and padding into account.
 *
 * Note - render in this context means send the data to ncurses, the tty
 * 		  won't be updated until refresh() is called
 */
static inline void
render_row(Screen *s, Row *row, char sep)
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

	clrtoeol();
	printw("\n");
}

/*
 * Render the whole screen by querying the model.
 */
static void
screen_render(Screen *s)
{
	/* determine the visible set of rows using key */
	RowIterator iter = RowMapLowerBound(rowmap(s), key(s));
	RowIterator end = RowMapEnd(rowmap(s));

	/* reposition the cursor to the origin */
	int i = 0;
	int ctr = 0;
	move(ctr, 0);

	/* display the header row in reverse video at the top of screen */
	attron(A_REVERSE);
	render_row(s, &s->model->header, '|');
	attroff(A_REVERSE);
	ctr++;

	/* render visible set */
	for (; !RowIteratorEqual(iter,end) && ctr < lines(s); 
		 iter = RowIteratorNext(rowmap(s), iter), ++ctr)
	{
		render_row(s, GetRow(iter), ' ');
	}

	/* blank out any remaining rows below the last row */
	for (i = ctr; i < lines(s); ++i) {
		printw("\n");
		clrtoeol();
	}
}

/* Send data to ncurses and make it update the terminal */
void
ScreenUpdate(Screen *s)
{
	screen_render(s);
	screen_sync(s);
}

static void inline
set_key(Screen *s, Row r)
{
//	PQExpBuffer exp = createPQExpBuffer();
//	RowDumpToString(&r, exp);
//	fprintf(debug_log, "%s\n", exp->data);
//	fflush(debug_log);
//	destroyPQExpBuffer(exp);

	RowCleanup(&s->key);
	s->key = r;
}

static void inline
clear_key(Screen *s)
{
	RowCleanup(&s->key);
}

/*
 * Scroll up/down by figuring out the pred/succ key in the model
 */
static void
screen_scroll_up(Screen *s)
{
	RowIterator iter = RowMapLowerBound(rowmap(s), key(s));

	if (!RowIteratorEqual(iter, RowMapBegin(rowmap(s)))) {

		iter = RowIteratorPrev(rowmap(s), iter);
		set_key(s, RowGetKey(GetRow(iter)));

	} else {
		clear_key(s);
	}
}

static void
screen_scroll_down(Screen *s)
{
	RowIterator iter = RowMapLowerBound(rowmap(s), key(s));

	if (!RowIteratorEqual(iter, RowMapEnd(rowmap(s))))
	{
		iter = RowIteratorNext(rowmap(s), iter);

		if (!RowIteratorEqual(iter, RowMapEnd(rowmap(s))))
			set_key(s, RowGetKey(GetRow(iter)));
	}
}

/*
 * Scroll left/right by adjusting both the current column and column offset
 */
static void
screen_scroll_left(Screen *s)
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
screen_scroll_right(Screen *s)
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
screen_page_up(Screen *s)
{
	int i = 0;

	for (i = 0; i < lines(s); ++i) {
		screen_scroll_up(s);
	}
}

static void
screen_page_down(Screen *s)
{
	int i = 0;

	for (i = 0; i < lines(s); ++i) {
		screen_scroll_down(s);
	}
}

static void
screen_next_col(Screen *s)
{
	s->x_pos = 0;
	s->x_col++;

	if (s->x_col >= numfields(s)) {
		s->x_col = rightmost(s);
	}
}

static void
screen_prev_col(Screen *s)
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
screen_home(Screen *s)
{
	s->x_col = 0;
	s->x_pos = 0;
}

static void
screen_end(Screen *s)
{
	s->x_col = rightmost(s);
	s->x_pos = 0;
}

static void
screen_toggle_pause(Screen *s)
{
	s->pause = !s->pause;
}

/* Read the key press using ncurses, and perform the requested action */
void
ScreenHandleInput(Screen *s)
{
	/* this is non-blocking */
	int c = getch();

	switch(c)
	{
		case KEY_UP:
			screen_scroll_up(s);
			break;
		case KEY_DOWN:
			screen_scroll_down(s);
			break;
		case KEY_LEFT:
			screen_scroll_left(s);
			break;
		case KEY_RIGHT:
			screen_scroll_right(s);
			break;
		case KEY_NPAGE:
			screen_page_down(s);
			break;
		case KEY_HOME:
			screen_home(s);
			break;
		case KEY_END:
			screen_end(s);
			break;
		case KEY_PPAGE:
			screen_page_up(s);
			break;
		case KEY_RESIZE:
			break;
		case 9: /* TAB */
			screen_next_col(s);
			break;
		case 353: /* SHIFT+TAB */
			screen_prev_col(s);
			break;
		case 'p':
			screen_toggle_pause(s);
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
