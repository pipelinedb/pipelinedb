#ifndef SCREEN_H_BFB9AC05
#define SCREEN_H_BFB9AC05

#include "model.h"

#include <stdio.h>
#include <ncurses.h>

typedef struct Screen
{
	Model   *model;
	Row     key;
	FILE    *term_in;
	int     fd;
	SCREEN  *nterm;
	int     x_pos;
	int     x_col;
	int     pause;
} Screen;

Screen *ScreenInit(Model *m);
void ScreenDestroy(Screen *s);

int ScreenFd(Screen *s);
void ScreenHandleInput(Screen *s);

void ScreenUpdate(Screen *s);
bool ScreenIsPaused(Screen *s);

#endif
