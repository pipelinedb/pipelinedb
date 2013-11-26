#----------------------------------------------------------------------------
#
# Postgres-XC GTM proxy Makefile
#
# Copyright(c) 2010-2012 Postgres-XC Development Group
#
# src/gtm/proxy/Makefile
#
#-----------------------------------------------------------------------------
top_builddir=../../..
include $(top_builddir)/src/Makefile.global
subdir=src/gtm/proxy

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
	$(top_builddir)/src/port/pgsleep.o \
	proxy_main.o proxy_thread.o proxy_utils.o gtm_proxy_opt.o

LIBS += $(PTHREAD_LIBS)

all: gtm_proxy

gtm_proxy: $(OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $(LDFLAGS) $(LIBS) $(call expand_subsys,$^) -o $@

install: all installdirs
	$(INSTALL_PROGRAM) gtm_proxy$(X) '$(DESTDIR)$(bindir)/gtm_proxy$(X)'
	$(INSTALL_DATA) $(srcdir)/gtm_proxy.conf.sample '$(DESTDIR)$(datadir)/gtm_proxy.conf.sample'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)' '$(DESTDIR)$(datadir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/gtm_proxy$(X)' '$(DESTDIR)$(datadir)/gtm_proxy.conf.sample'

clean distclean maintainer-clean:
	rm -f gtm_proxy$(X) $(OBJS)

$(top_builddir)/src/port/libpgport_srv.a: | submake-libpgport
