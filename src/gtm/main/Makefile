#----------------------------------------------------------------------------
#
# Postgres-XC GTM main Makefile
#
# Copyright(c) 2010-2012 Postgres-XC Development Group
#
# src/gtm/main/Makefile
#
#-----------------------------------------------------------------------------
top_builddir=../../..
include $(top_builddir)/src/Makefile.global
subdir=src/gtm/main

ifneq ($(PORTNAME), win32)
override CFLAGS += $(PTHREAD_CFLAGS)
endif

SUBDIRS = $(top_builddir)/src/gtm/client \
	$(top_builddir)/src/gtm/common \
	$(top_builddir)/src/gtm/config \
	$(top_builddir)/src/gtm/libpq \
	$(top_builddir)/src/gtm/path \
	$(top_builddir)/src/gtm/recovery

include $(top_srcdir)/src/backend/common.mk

OBJS = $(SUBDIROBJS) \
	$(top_builddir)/src/port/libpgport_srv.a \
	main.o gtm_thread.o gtm_txn.o gtm_seq.o gtm_snap.o gtm_time.o \
	gtm_standby.o gtm_opt.o register_gtm.o replication.o gtm_backup.o

LIBS += $(PTHREAD_LIBS)

all: gtm

gtm: $(OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $(LDFLAGS) $(LIBS) $(call expand_subsys,$^) -o $@

install: all installdirs
	$(INSTALL_PROGRAM) gtm$(X) '$(DESTDIR)$(bindir)/gtm$(X)'
	$(INSTALL_DATA) $(srcdir)/gtm.conf.sample '$(DESTDIR)$(datadir)/gtm.conf.sample'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)' '$(DESTDIR)$(datadir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/gtm$(X)' '$(DESTDIR)$(datadir)/gtm.conf.sample'

clean distclean maintainer-clean:
	rm -f gtm$(X) $(OBJS)

$(top_builddir)/src/port/libpgport_srv.a: | submake-libpgport
