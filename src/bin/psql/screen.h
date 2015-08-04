#ifndef SCREEN_H_BFB9AC05
#define SCREEN_H_BFB9AC05

#include "adhoc_compat.h"
#include "model.h"

#include <stdio.h>
#include <ncurses.h>

typedef struct Screen
{
	Model* model;
	FlexString key;

	FILE* term_in;
	int fd;

	int x_pos;
	int x_col;

} Screen;

Screen* ScreenInit(Model *m);
void ScreenDestroy(Screen*);

int ScreenFd(Screen *s);
void ScreenHandleInput(Screen* s);

void ScreenUpdate(Screen *s);

#endif
