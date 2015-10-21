#-------------------------------------------------------------------------
#
# Makefile for src/bin/padhoc
#
# Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2013-2015, PipelineDB
#
# src/bin/pipeline/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "padhoc - the PipelineDB interactive terminal"

subdir = src/bin/pipeline
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I. -I$(srcdir) -I$(libpq_srcdir) -I$(top_srcdir)/src/bin/pg_dump $(CPPFLAGS)

OBJS= adhoc.o adhoc_options.o adhoc_util.o model.o bsd_rbtree.o rowmap.o rowstream.o pg_rowstream.o screen.o variables.o

all: padhoc

padhoc: $(OBJS) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) $(OBJS) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) -lncurses $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) padhoc$(X) '$(DESTDIR)$(bindir)/padhoc$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)' '$(DESTDIR)$(datadir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/padhoc$(X)'
