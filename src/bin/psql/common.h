/*
 * psql - the PostgreSQL interactive terminal
 *
 * Copyright (c) 2000-2014, PostgreSQL Global Development Group
 *
 * src/bin/psql/common.h
 */
#ifndef COMMON_H
#define COMMON_H

#include "postgres_fe.h"
#include <setjmp.h>
#include "libpq-fe.h"

#define atooid(x)  ((Oid) strtoul((x), NULL, 10))

extern bool setQFout(const char *fname);

extern void
psql_error(const char *fmt,...)
/* This lets gcc check the format string for consistency. */
__attribute__((format(PG_PRINTF_ATTRIBUTE, 1, 2)));

extern void NoticeProcessor(void *arg, const char *message);

extern volatile bool sigint_interrupt_enabled;

extern sigjmp_buf sigint_interrupt_jmp;

extern volatile bool cancel_pressed;

/* Note: cancel_pressed is defined in print.c, see that file for reasons */

extern void setup_cancel_handler(void);

extern void SetCancelConn(void);
extern void ResetCancelConn(void);

extern PGresult *PSQLexec(const char *query, bool start_xact);

extern bool SendQuery(const char *query);

extern bool is_superuser(void);
extern bool standard_strings(void);
extern const char *session_username(void);

extern void expand_tilde(char **filename);

#endif   /* COMMON_H */
