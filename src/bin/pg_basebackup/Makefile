#-------------------------------------------------------------------------
#
# Makefile for src/bin/pg_basebackup
#
# Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2013-2015, PipelineDB
#
# src/bin/pg_basebackup/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "pipeline-basebackup - takes a streaming base backup of a PipelineDB instance"
PGAPPICON=win32

subdir = src/bin/pg_basebackup
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)

OBJS=receivelog.o streamutil.o $(WIN32RES)

all: pg_basebackup pg_receivexlog pg_recvlogical

pg_basebackup: pg_basebackup.o $(OBJS) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) pg_basebackup.o $(OBJS) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

pg_receivexlog: pg_receivexlog.o $(OBJS) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) pg_receivexlog.o $(OBJS) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

pg_recvlogical: pg_recvlogical.o $(OBJS) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) pg_recvlogical.o $(OBJS) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) pg_basebackup$(X) '$(DESTDIR)$(bindir)/pipeline-basebackup$(X)'
	$(INSTALL_PROGRAM) pg_receivexlog$(X) '$(DESTDIR)$(bindir)/pipeline-receivexlog$(X)'
	$(INSTALL_PROGRAM) pg_recvlogical$(X) '$(DESTDIR)$(bindir)/pipeline-recvlogical$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pipeline-basebackup$(X)'
	rm -f '$(DESTDIR)$(bindir)/pipeline-receivexlog$(X)'
	rm -f '$(DESTDIR)$(bindir)/pipeline-recvlogical$(X)'

clean distclean maintainer-clean:
	rm -f pg_basebackup$(X) pg_receivexlog$(X) pg_recvlogical$(X) \
		pg_basebackup.o pg_receivexlog.o pg_recvlogical.o \
		$(OBJS)
	rm -rf tmp_check

check: all
	$(prove_check)

installcheck:
	$(prove_installcheck)
