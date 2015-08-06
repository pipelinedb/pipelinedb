/*-------------------------------------------------------------------------
 *
 * screen.h
 *    Interface for the adhoc client ncurses ui
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/bin/psql/screen.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SCREEN_H
#define SCREEN_H

#include "model.h"

#include <stdio.h>
#include <ncurses.h>

/*
 * This module is responsible for handling key presses, and displaying
 * row data to the terminal. It uses ncurses.
 */

typedef struct Screen
{
	Model   *model;
	Row     key;  /* key refers to the top visible row on the screen */
	FILE    *term_in;
	int     fd;
	SCREEN  *nterm;
	int     x_pos;
	int     x_col;
	int     pause;
} Screen;

extern Screen *ScreenInit(Model *m);
extern void ScreenDestroy(Screen *s);

/* required for poll/select */
extern int ScreenFd(Screen *s);

/* to be called when the fd is ready */
extern void ScreenHandleInput(Screen *s);

/* redraws the screen and updates the terminal */
extern void ScreenUpdate(Screen *s);

extern bool ScreenIsPaused(Screen *s);

#endif
