#-------------------------------------------------------------------------
#
# Makefile for src/bin/initgtm
#
# Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/bin/initgtm/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "initgtm - initialize a new GTM node in cluster"
PGAPPICON=win32

subdir = src/bin/initgtm
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -DFRONTEND -I$(libpq_srcdir) $(CPPFLAGS)

OBJS=	initgtm.o pqsignal.o $(WIN32RES)

all: initgtm

initgtm: $(OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $(OBJS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

pqsignal.c: % : $(top_srcdir)/src/port/%
	rm -f $@ && $(LN_S) $< .

install: all installdirs
	$(INSTALL_PROGRAM) initgtm$(X) '$(DESTDIR)$(bindir)/initgtm$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/initgtm$(X)'

clean distclean maintainer-clean:
	rm -f initgtm$(X) $(OBJS) pqsignal.c


# ensure that changes in datadir propagate into object file
initgtm.o: initgtm.c $(top_builddir)/src/Makefile.global
