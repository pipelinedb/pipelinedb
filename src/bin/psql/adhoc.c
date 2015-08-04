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

int
main(int argc, char *argv[])
{
	RowMap *m = RowMapInit();
	Model model = {m, 0, 0};

	add_row(&model, "sensatorial 42613 x 67 1438662000");
	add_row(&model, "creekside 97639 xxxxxxxxxx 83 1438662000");
	add_row(&model, "gait 80471 xxxxxxx 88 1438662000");
	add_row(&model, "clithral 80186 xx 39 1438662000");
	add_row(&model, "remissively 89688 xx 37 1438662000");
	add_row(&model, "uncomplicated 6921 xxxxx 80 1438662000");
	add_row(&model, "uncomplicated 83966 x 82 1438662000");
	add_row(&model, "cloakmaking 31921 xxxx 29 1438662000");
	add_row(&model, "lungflower 80782 xxxxxxxx 32 1438662000");
	add_row(&model, "mesmerian 84665 x 31 1438662000");
	add_row(&model, "semifashion 96766 xxx 55 1438662001");
	add_row(&model, "milvus 92288 xxxxx 53 1438662001");
	add_row(&model, "supplicate 3560 xxxxx 17 1438662001");
	add_row(&model, "abed 46522 xxxxxxxxx 1 1438662001");
	add_row(&model, "reddy 61353 xxxxxxxxxx 35 1438662001");
	add_row(&model, "gait 47883 xxx 91 1438662001");
	add_row(&model, "constance 51811 xxxxxx 12 1438662001");
	add_row(&model, "abietene 12372 xxxxx 6 1438662001");
	add_row(&model, "semifashion 62079 xxxxx 3 1438662001");
	add_row(&model, "delegate 9396 xxxxxxxxx 78 1438662001");
	add_row(&model, "cloakmaking 44259 xxxxxxxx 79 1438662001");
	add_row(&model, "previous 79701 x 49 1438662001");
	add_row(&model, "lungflower 71331 x 95 1438662001");
	add_row(&model, "discanonize 55153 xxx 36 1438662001");
	add_row(&model, "preliquidate 81757 xxxx 98 1438662001");
	add_row(&model, "flandan 86384 xxxxxxxx 21 1438662001");
	add_row(&model, "foundery 13824 xxx 43 1438662001");
	add_row(&model, "amgarn 61570 xxxxxxxxxx 98 1438662001");
	add_row(&model, "enlightened 96990 xxxx 82 1438662001");
	add_row(&model, "abietene 89236 xx 50 1438662001");
	add_row(&model, "phylloxera 31785 xx 36 1438662001");
	add_row(&model, "ployment 78696 xxx 51 1438662001");
	add_row(&model, "gait 70838 x 38 1438662001");
	add_row(&model, "stipe 92648 xxxxxxxxx 2 1438662001");
	add_row(&model, "vesiculopustular 18728 xxxxxxxxx 19 1438662001");
	add_row(&model, "sonancy 50586 xxx 52 1438662001");
	add_row(&model, "frigidarium 7997 x 4 1438662001");
	add_row(&model, "rollichie 92299 xxxxxxx 77 1438662001");
	add_row(&model, "bedur 42512 xxxxxxxxx 29 1438662001");
	add_row(&model, "gaspy 17340 xxxxxxxxxx 61 1438662001");
	add_row(&model, "agile 10088 x 70 1438662001");
	add_row(&model, "cytozoon 74253 xxxxxxxx 56 1438662001");
	add_row(&model, "kukulcan 15912 xxxxxxx 79 1438662001");
	add_row(&model, "cytozoon 79983 xxxxxxxxxx 1 1438662001");
	add_row(&model, "mesmerian 11052 xxxxxxxxx 24 1438662001");
	add_row(&model, "nondiscordant 5103 xxxx 76 1438662001");
	add_row(&model, "bradypus 71382 xxxxxx 87 1438662001");
	add_row(&model, "patristic 89099 xxxxxxxxx 84 1438662001");
	add_row(&model, "spina 28651 xxxxxx 28 1438662001");
	add_row(&model, "rimy 74498 xxxx 89 1438662001");
	add_row(&model, "amgarn 69939 xxxxx 53 1438662001");
	add_row(&model, "abietene 76323 xxxxxxxx 9 1438662001");
	add_row(&model, "skite 89987 xxx 3 1438662001");
	add_row(&model, "rollichie 82119 xxxxxxxx 61 1438662001");
	add_row(&model, "brehon 76578 xxx 88 1438662001");
	add_row(&model, "cocksureism 63720 xxxxx 0 1438662001");
	add_row(&model, "dialogize 73925 xxxxxx 80 1438662001");
	add_row(&model, "indris 26813 xx 81 1438662001");
	add_row(&model, "carboxylase 79224 xxxxxxxxx 36 1438662001");
	add_row(&model, "delegate 86339 xxxxxxxx 35 1438662001");
	add_row(&model, "rimy 87069 xx 33 1438662001");
	add_row(&model, "thymelical 80212 xxx 69 1438662001");
	add_row(&model, "itacolumite 78709 xxx 91 1438662001");
	add_row(&model, "eaglet 65483 xxxxxxx 30 1438662001");
	add_row(&model, "anthela 99271 xxx 92 1438662001");
	add_row(&model, "kukulcan 33201 xxxxxxxxxx 93 1438662001");
	add_row(&model, "creekside 44889 xxx 96 1438662001");
	add_row(&model, "eaglet 85532 xx 58 1438662001");
	add_row(&model, "milvus 97855 x 36 1438662001");
	add_row(&model, "retroactive 72308 xxxx 51 1438662001");
	add_row(&model, "kukulcan 27741 xxxxxxxx 32 1438662001");
	add_row(&model, "enlightened 55751 xxxxxxxx 13 1438662001");
	add_row(&model, "gaspy 38008 xxxxx 55 1438662001");
	add_row(&model, "cypress 99036 xxxxx 25 1438662001");
	add_row(&model, "agile 77582 xxxxxxx 28 1438662001");
	add_row(&model, "milvus 52548 xxxxxxx 99 1438662001");
	add_row(&model, "spheniscus 33415 xxxxxxxxx 61 1438662001");
	add_row(&model, "caresser 57600 xxxxxxxxx 79 1438662001");
	add_row(&model, "stipe 39335 xxxxx 49 1438662001");
	add_row(&model, "spheniscus 66275 xxxxxxxxxx 18 1438662001");
	add_row(&model, "stipe 48691 xxxxxx 58 1438662001");
	add_row(&model, "unreversed 9724 xxx 45 1438662001");
	add_row(&model, "constance 11590 xxxxxx 48 1438662001");
	add_row(&model, "sensatorial 14672 xxxx 58 1438662001");
	add_row(&model, "perchloroethane 73929 xxxxxxxxxx 70 1438662001");
	add_row(&model, "ployment 66439 x 22 1438662001");
	add_row(&model, "stipe 14297 xxxxxxxxx 99 1438662001");
	add_row(&model, "rollichie 42439 xxxxx 89 1438662001");
	add_row(&model, "itacolumite 52280 xxx 23 1438662001");
	add_row(&model, "abed 35428 xx 87 1438662001");
	add_row(&model, "enlightened 90265 xxxxx 97 1438662001");
	add_row(&model, "starboard 45463 xx 53 1438662001");
	add_row(&model, "lungflower 70486 xxxxxxx 7 1438662001");
	add_row(&model, "triphony 30857 xxxxxxx 0 1438662001");
	add_row(&model, "bedur 5028 xx 20 1438662001");
	add_row(&model, "starboard 90238 xx 20 1438662001");
	add_row(&model, "bedur 82032 xxxxxxxxx 94 1438662001");
	add_row(&model, "lungflower 65599 xxxxx 70 1438662001");
	add_row(&model, "bedur 19055 xxxx 83 1438662001");

//	RowMapDump(m);
//	ModelDump(&model);

	{
		Screen *screen = ScreenInit(&model);
		ScreenUpdate(screen);

		while (true)
		{
			struct pollfd pfd;
			int rc = 0;

			memset(&pfd, 0, sizeof(pfd));

			pfd.fd = ScreenFd(screen);
			pfd.events = POLLIN;
			pfd.revents = 0;

			rc = poll(&pfd, 1, -1);

			if (rc < 0)
				die("poll error");

			if (pfd.revents & POLLIN)
			{
				ScreenHandleInput(screen);
			}
		}
	}

	return 0;
}
