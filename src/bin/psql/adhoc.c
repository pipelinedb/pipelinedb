#include "postgres_fe.h"
#include "adhoc_compat.h"

#include <curses.h>
#include <utils/rbtree.h>

#include <unistd.h>
#include <fcntl.h>
#include <poll.h>

/*
 * pipeline adhoc query client
 */

void die(const char* s);

void die(const char* s) {

	fprintf(stderr, "%s\n", s);
	exit(1);
}

typedef struct Field {

	int len;
	char* data;
} Field;

typedef struct Row {

	char* ptr; // points to original malloced chunk.
	int num_fields;

	Field* fields; // array of fields
} Row;

typedef struct Node {

	RBNode node;
	Row row;

} Node;

typedef void (*row_processor) (void* ctx, Row row);

typedef struct RowStream
{
	int fd;
	char buf[4096];

	FlexString flex;

	row_processor process_row;
	void* pr_ctx;

} RowStream;

void test_read(void);
int nodeCompare(const RBNode* ra, const RBNode *rb, void *arg);
void nodeCombine(RBNode* ra, const RBNode *rb, void *arg);
RBNode* nodeAlloc(void* arg);
void nodeFree(RBNode *x, void* arg);
void cleanup_row(Row *row);

//void process_row(const char* row_buf);

int nodeCompare(const RBNode* ra, const RBNode *rb, void *arg)
{
	Node* a = (Node*) (ra);
	Node* b = (Node*) (rb);

	return strcmp(a->row.fields[1].data, 
				  b->row.fields[1].data);
}

RBNode* nodeAlloc(void *arg)
{
	return malloc(sizeof(Node));
}

void nodeFree(RBNode *x, void* arg) 
{
	free(x);
}

void cleanup_row(Row* row) {

	free(row->fields);
	free((void*) row->ptr);

	memset(row, 0, sizeof(Row));
}

void nodeCombine(RBNode* ra, const RBNode *rb, void *arg) 
{
	Node* a = (Node*) (ra);
	Node* b = (Node*) (rb);

	cleanup_row(&(a->row));
	a->row = b->row;
}

void append_data(RowStream *stream, const char* buf, size_t n);

typedef struct Screen
{
	FILE* term_in;
	SCREEN* main_screen;
	int fd;

	RBTree* tree;

	FlexString key;

} Screen;

void collect(Screen* screen);

void collect(Screen* screen)
{
	int ctr = 0;
	RBTree* tree = screen->tree;

	move(ctr, 0);

	rb_begin_iterate(tree, LeftRightWalk);

	{
		Node* node = (Node*) rb_iterate(tree);

		for (; node && ctr < LINES; node = (Node*)rb_iterate(tree), ++ctr)
		{
			move(ctr, 0);
			printw("%s", node->row.fields[1].data);
			clrtoeol();
		}
	}

	{
		int i = 0;

		for (i = ctr; i < LINES; ++i) {

			mvprintw(i, 0, "");
			clrtoeol();
		}
	}
}

//	attron(A_REVERSE);
//	draw_row(header_row, '|');
//	attroff(A_REVERSE);
//	ctr++;
//	for (; iter != drows.end() && ctr < LINES; ++iter, ++ctr)
//	{
//		move(ctr, 0);
//		const Row &row = iter->second;
//		draw_row(row, ' ');
//	}
//
//	// kill off any remainder
//
//	for (int i = ctr; i < LINES; ++i) {
//
//		mvprintw(i, 0, "");
//		clrtoeol();
//	}
//
//	const char* fkey = drows.empty() ? "" : drows.begin()->first.c_str();
//	const char* lkey = drows.empty() ? "" : drows.rbegin()->first.c_str();
//	clrtoeol();

Screen* init_screen(RBTree* tree);

Screen* init_screen(RBTree* tree)
{
	Screen* screen = malloc(sizeof(Screen));
	const char* term_type = 0;

	memset(screen, 0, sizeof(Screen));


	term_type = getenv("TERM");

	if (term_type == NULL || *term_type == '\0') {
		term_type = "unknown";
	}

	screen->term_in = 0;
	screen->term_in = fopen("/dev/tty", "r");

	if (screen->term_in == NULL) {
		die("could not open /dev/tty");
	}

	screen->main_screen = newterm(term_type, stdout, screen->term_in);
	set_term(screen->main_screen);
	screen->fd = fileno(screen->term_in);
	screen->tree = tree;

	timeout(0);
	clear();
	noecho();
	cbreak();	/* Line buffering disabled. pass on everything */
	curs_set(0);

	keypad(stdscr, TRUE);
	refresh();

	init_flex(&screen->key);

	return screen;
}

void destroy_screen(Screen* s);
void destroy_screen(Screen* s)
{
	endwin();
	memset(s, 0, sizeof(Screen));
	free(s);
}


RowStream* init_row_stream(row_processor proc, void *pctx);

RowStream* init_row_stream(row_processor proc, void *pctx)
{
	RowStream *stream = malloc(sizeof(RowStream));
	memset(stream, 0, sizeof(RowStream));

	stream->fd = STDIN_FILENO;
	fcntl(stream->fd, F_SETFL, O_NONBLOCK);

	stream->process_row = proc;
	stream->pr_ctx = pctx;

	init_flex(&stream->flex);

	return stream;
}

int num_fields(const char* line);

int num_fields(const char* line)
{
	const char* s = line;
	int cnt = 0;

	while (*s)
	{
		if (*s == ' ') cnt++;
		s++;
	}

	return cnt + 1;
}

Row parse_text_row(const char* line);

Row parse_text_row(const char* line)
{
	Row row = {0,0,0};
	int i = 0;
	char* sptr = 0;

	row.ptr = strdup(line);
	row.num_fields = num_fields(line);
	row.fields = malloc(sizeof(Field) * row.num_fields);

	sptr = (char*) row.ptr;

	while (true)
	{
		char* tok = strtok(sptr, " ");

		if (!tok) 
			break;

		row.fields[i].len = strlen(tok);
		row.fields[i].data = tok;
		i++;

		sptr = 0;
	}

	return row;
}

void append_data(RowStream *stream, const char* buf, size_t nr)
{
	size_t i = 0;

	for (i = 0; i < nr; ++i) {

		append_flex(&(stream->flex), buf + i, 1);

		if (buf[i] == '\n') {

			Row row = {0};

			stream->flex.buf[stream->flex.n-1] = '\0';
			row = parse_text_row(stream->flex.buf);

			stream->process_row(stream->pr_ctx, row);
			reset_flex(&stream->flex);
		}
	}
}

// returns true if finished

bool handle_row_stream(RowStream *stream);

bool handle_row_stream(RowStream *stream)
{
	while (true)
	{
		ssize_t nr = read(stream->fd, stream->buf, 4096);

		if (nr == 0) {
			return true;
		}

		if (nr == -1) {
			return false;
		}

		append_data(stream, stream->buf, nr);
	}

	return false;
}

void destroy_row_stream(RowStream *stream);

void destroy_row_stream(RowStream *stream)
{
}

typedef struct AppCtx {

	// tree.
	// top key
	
	RBTree* tree;

	Screen* screen;

} AppCtx;

AppCtx* init_app_ctx(void);

AppCtx* init_app_ctx()
{
	void *aux = 0;
	AppCtx *ctx = malloc(sizeof(AppCtx));

	ctx->tree = rb_create(sizeof(Node), nodeCompare, nodeCombine,
				 	 	  nodeAlloc, nodeFree, aux);

	ctx->screen = 0;

	return ctx;
}

char* getvalue(Row row, int fieldnum);

char* getvalue(Row row, int fieldnum)
{
	assert(fieldnum < row.num_fields);
	return row.fields[fieldnum].data;
}

void update_row(AppCtx* ctx, Row row);

void update_row(AppCtx* ctx, Row row)
{
	Node data;
	bool is_new = false;

	data.row = row;
	rb_insert(ctx->tree, (const RBNode*) &data, &is_new);
}

void delete_row(AppCtx* ctx, Row key);

void delete_row(AppCtx* ctx, Row key)
{
	Node data;
	RBNode* node;
	Row row;

	data.row = key;
	
	node = rb_find(ctx->tree, (const RBNode*) &data);

	if (!node)
		return;

	// stash a copy before it gets trashed.
	row = ((Node*)node)->row;
	rb_delete(ctx->tree, node);

	cleanup_row(&row);
}

void print_row(void* v, Row row);
void app_refresh_screen(AppCtx* ctx);
void refresh_screen(Screen *screen);

void print_row(void* v, Row row)
{
	AppCtx *ctx = (AppCtx*) v;
	char* event_field = getvalue(row, 0);
	char event_code = event_field[0];

	switch (event_code) 
	{
		case 'h':
			break;
		case 'i':
			update_row(ctx, row);
			break;
		case 'u':
			update_row(ctx, row);
			break;
		case 'd':
			delete_row(ctx, row);
			break;

		default:
			die("unknown");
	}

	app_refresh_screen(ctx);
}

void app_refresh_screen(AppCtx* ctx)
{
	if (!ctx->screen)
		return;

	refresh_screen(ctx->screen);
}

void handle_screen(Screen* screen);

void refresh_screen(Screen* screen)
{
	collect(screen);
	refresh();
}

void handle_screen(Screen* screen)
{
	int c = getch();

	switch(c)
	{	
		case KEY_UP:
//			scroll_up();
			break;
		case KEY_DOWN:
//			scroll_down();
			break;
		case KEY_LEFT:
//			scroll_left();
			break;
		case KEY_RIGHT:
//			scroll_right();
			break;
		case KEY_NPAGE:
//			page_down();
			break;

		case KEY_HOME:
//			home();
			break;
		case KEY_END:
//			end();
			break;

		case KEY_PPAGE:
//			page_up();
			break;

		case KEY_RESIZE:
//			clear();
//			collect();
//			refresh();
			break;

		case 9: // TAB
//			next_col();
			break;

		case 353: // SHIFT+TAB
//			prev_col();
			break;

		case 'p':
//			toggle_pause();
			break;

		case ERR:

			die("wtf");
			break;

		default:
			break;
	}

	refresh_screen(screen);
}

void test_screen(void);

void test_screen()
{
	AppCtx *app_ctx = init_app_ctx();
	RowStream* stream = init_row_stream(print_row, app_ctx);

	Screen* screen = init_screen(app_ctx->tree);
	app_ctx->screen = screen;

	// main event loop, handles screen and row updates.

	while (true)
	{
		struct pollfd pfd[2];
		int rc = 0;
		memset(pfd, 0, sizeof(pfd));

		pfd[0].fd = stream->fd;
		pfd[0].events = POLLIN;
		pfd[0].revents = 0;

		pfd[1].fd = screen->fd;
		pfd[1].events = POLLIN;
		pfd[1].revents = 0;

		rc = poll(pfd, 2, -1);

		if (rc < 0) {
			die("poll error");
		}

		if (pfd[0].revents & POLLIN)
		{
			bool fin = handle_row_stream(stream);

			if (fin) {
				break;
			}
		}
		
		if (pfd[1].revents & POLLIN) {
			handle_screen(screen);
		}
	}

	destroy_screen(screen);
	destroy_row_stream(stream);
}

void test_read()
{
	char buf[4096];
	int fd = 0;
	bool finished = false;

	fd = STDIN_FILENO;
	fcntl(fd, F_SETFL, O_NONBLOCK);

	while (!finished)
	{
		int rc = 0;
		struct pollfd pfd;

		pfd.fd = fd;
		pfd.events = POLLIN;
		pfd.revents = 0;

		rc = poll(&pfd, 1, -1);

		if (rc < 0) {
			break;
		}

		if (rc == 0) {
			continue;
		}

		if (pfd.revents | POLLIN) {

			while (true) {

				ssize_t nr = read(fd, buf, 4096);

				if (nr == 0) {
					finished = true;
					break;
				}

				if (nr == -1) {
					break;
				}

				append_data(0, buf, nr);
			}
		}
	}
}

int
main(int argc, char *argv[])
{
	test_screen();

//	const char* term_type = 0;
//	FILE* term_in = 0;
//	SCREEN* main_screen = 0;
//
//	term_type = getenv("TERM");
//
//	if (term_type == NULL || *term_type == '\0') {
//		term_type = "unknown";
//	}
//
//	term_in = fopen("/dev/tty", "r");
//
//	if (term_in == NULL) {
//
//		perror("fopen");
//		return 1;
//	}
//
//	main_screen = newterm(term_type, stdout, term_in);
//	set_term(main_screen);

//	SCREEN* main_screen = newterm(term_type, stdout, term_in);
//	set_term(main_screen);
//
//	void* aux = 0;
//	int foo = 0;
//	tree = rb_create(sizeof(Node), nodeCompare, nodeCombine, nodeAlloc, nodeFree, aux);
//
//	test_read();
//
//	rb_begin_iterate(tree, LeftRightWalk);
//
//	{
//		Node* node;
//
//		while ((node = (Node*) rb_iterate(tree))) {
//
//			printf("got node %s\n", node->row.fields[0].data);
//		}
//	}
//
	return 0;

}
