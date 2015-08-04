#include "postgres_fe.h"
#include "adhoc_compat.h"

#include <curses.h>
#include <utils/rbtree.h>

#include <unistd.h>
#include <fcntl.h>
#include <poll.h>

#include "oldrowmap.h"
#include "rowmap.h"
#include "rowstream.h"
#include "screen.h"

#include "model.h"

/*
 * pipeline adhoc query client
 */

//void test_read(void);
//
//typedef struct AppCtx {
//
//	RBTree* tree;
//	Screen* screen;
//
//} AppCtx;
//
//AppCtx* init_app_ctx(void);
//
//AppCtx* init_app_ctx()
//{
//	void *aux = 0;
//	AppCtx *ctx = malloc(sizeof(AppCtx));
//
//	ctx->tree = rb_create(sizeof(Node), nodeCompare, nodeCombine,
//				 	 	  nodeAlloc, nodeFree, aux);
//
//	ctx->screen = 0;
//
//	return ctx;
//}
//
//char* getvalue(OldRow row, int fieldnum);
//
//char* getvalue(OldRow row, int fieldnum)
//{
//	assert(fieldnum < row.num_fields);
//	return row.fields[fieldnum].data;
//}
//
//void update_row(AppCtx* ctx, OldRow row);
//
//void update_row(AppCtx* ctx, OldRow row)
//{
//	Node data;
//	bool is_new = false;
//
//	data.row = row;
//	rb_insert(ctx->tree, (const RBNode*) &data, &is_new);
//}
//
//void delete_row(AppCtx* ctx, OldRow key);
//
//void delete_row(AppCtx* ctx, OldRow key)
//{
//	Node data;
//	RBNode* node;
//	OldRow row;
//
//	data.row = key;
//	
//	node = rb_find(ctx->tree, (const RBNode*) &data);
//
//	if (!node)
//		return;
//
//	// stash a copy before it gets trashed.
//	row = ((Node*)node)->row;
//	rb_delete(ctx->tree, node);
//
//	cleanup_row(&row);
//}
//
//void print_row(void* v, OldRow row);
//void app_refresh_screen(AppCtx* ctx);
//void refresh_screen(Screen *screen);
//
//void print_row(void* v, OldRow row)
//{
//	AppCtx *ctx = (AppCtx*) v;
//	char* event_field = getvalue(row, 0);
//	char event_code = event_field[0];
//
//	switch (event_code) 
//	{
//		case 'h':
//			break;
//		case 'i':
//			update_row(ctx, row);
//			break;
//		case 'u':
//			update_row(ctx, row);
//			break;
//		case 'd':
//			delete_row(ctx, row);
//			break;
//
//		default:
//			die("unknown");
//	}
//
//	app_refresh_screen(ctx);
//}
//
//void app_refresh_screen(AppCtx* ctx)
//{
//	if (!ctx->screen)
//		return;
//
//	refresh_screen(ctx->screen);
//}
//
//void handle_screen(Screen* screen);
//
//void refresh_screen(Screen* screen)
//{
//	collect(screen);
//	refresh();
//}
//
//void handle_screen(Screen* screen)
//{
//	int c = getch();
//
//	switch(c)
//	{	
//		case KEY_UP:
////			scroll_up();
//			break;
//		case KEY_DOWN:
////			scroll_down();
//			break;
//		case KEY_LEFT:
////			scroll_left();
//			break;
//		case KEY_RIGHT:
////			scroll_right();
//			break;
//		case KEY_NPAGE:
////			page_down();
//			break;
//
//		case KEY_HOME:
////			home();
//			break;
//		case KEY_END:
////			end();
//			break;
//
//		case KEY_PPAGE:
////			page_up();
//			break;
//
//		case KEY_RESIZE:
////			clear();
////			collect();
////			refresh();
//			break;
//
//		case 9: // TAB
////			next_col();
//			break;
//
//		case 353: // SHIFT+TAB
////			prev_col();
//			break;
//
//		case 'p':
////			toggle_pause();
//			break;
//
//		case ERR:
//
//			die("wtf");
//			break;
//
//		default:
//			break;
//	}
//
//	refresh_screen(screen);
//}
//
//void test_screen(void);
//
//void test_screen()
//{
//	AppCtx *app_ctx = init_app_ctx();
//	RowStream* stream = init_row_stream(print_row, app_ctx);
//
//	Screen* screen = init_screen(app_ctx->tree);
//	app_ctx->screen = screen;
//
//	// main event loop, handles screen and row updates.
//
//	while (true)
//	{
//		struct pollfd pfd[2];
//		int rc = 0;
//		memset(pfd, 0, sizeof(pfd));
//
//		pfd[0].fd = stream->fd;
//		pfd[0].events = POLLIN;
//		pfd[0].revents = 0;
//
//		pfd[1].fd = screen->fd;
//		pfd[1].events = POLLIN;
//		pfd[1].revents = 0;
//
//		rc = poll(pfd, 2, -1);
//
//		if (rc < 0) {
//			die("poll error");
//		}
//
//		if (pfd[0].revents & POLLIN)
//		{
//			bool fin = handle_row_stream(stream);
//
//			if (fin) {
//				break;
//			}
//		}
//		
//		if (pfd[1].revents & POLLIN) {
//			handle_screen(screen);
//		}
//	}
//
//	destroy_screen(screen);
//	destroy_row_stream(stream);
//}

//void test_read()
//{
//	char buf[4096];
//	int fd = 0;
//	bool finished = false;
//
//	fd = STDIN_FILENO;
//	fcntl(fd, F_SETFL, O_NONBLOCK);
//
//	while (!finished)
//	{
//		int rc = 0;
//		struct pollfd pfd;
//
//		pfd.fd = fd;
//		pfd.events = POLLIN;
//		pfd.revents = 0;
//
//		rc = poll(&pfd, 1, -1);
//
//		if (rc < 0) {
//			break;
//		}
//
//		if (rc == 0) {
//			continue;
//		}
//
//		if (pfd.revents | POLLIN) {
//
//			while (true) {
//
//				ssize_t nr = read(fd, buf, 4096);
//
//				if (nr == 0) {
//					finished = true;
//					break;
//				}
//
//				if (nr == -1) {
//					break;
//				}
//
//				append_data(0, buf, nr);
//			}
//		}
//	}
//}

// screen needs
//
// rowmap
// maxlens
// header

typedef struct App
{
	Model* model;
	Screen* screen;
} App;

void row_callback(void* ctx, int type, Row* row);

void row_callback(void* ctx, int type, Row* row)
{
	App *app = (App*)(ctx);

	switch (type) {

		case 'i': case 'u':
			ModelAddRow(app->model, row);
			ScreenUpdate(app->screen);
			break;
		case 'd':
			ModelDeleteRow(app->model, row);
			ScreenUpdate(app->screen);
			break;
		case 'h':
			ModelSetHeader(app->model, row);
			ScreenUpdate(app->screen);
			break;
	}
}

int
main(int argc, char *argv[])
{
	App app = {0,0};

	RowMap *m = RowMapInit();
	Model model = {m, 0, 0, {0,0,0}};

	Screen *screen = ScreenInit(&model);

	app.model = &model;
	app.screen = screen;

	{
		RowStream *stream = RowStreamInit(row_callback, &app);

		while (true)
		{
			struct pollfd pfd[2];
			int rc = 0;

			memset(&pfd, 0, sizeof(pfd));

			pfd[0].fd = RowStreamFd(stream);
			pfd[0].events = POLLIN;
			pfd[0].revents = 0;

			pfd[1].fd = ScreenFd(screen);
			pfd[1].events = POLLIN;
			pfd[1].revents = 0;

			rc = poll(pfd, 2, -1);

			if (rc < 0)
				die("poll error");

			if (pfd[0].revents & POLLIN)
			{
				bool fin = RowStreamHandleInput(stream);

				if (fin)
					break;
			}

			if (pfd[1].revents & POLLIN)
			{
				ScreenHandleInput(screen);
			}
		}
	}

//	{
//
//		while (true)
//		{
//			struct pollfd pfd;
//			int rc = 0;
//
//			memset(&pfd, 0, sizeof(pfd));
//
//			pfd.fd = ScreenFd(screen);
//			pfd.events = POLLIN;
//			pfd.revents = 0;
//
//			rc = poll(&pfd, 1, -1);
//
//			if (rc < 0)
//				die("poll error");
//
//			if (pfd.revents & POLLIN)
//			{
//				ScreenHandleInput(screen);
//			}
//		}
//	}

	return 0;
}
