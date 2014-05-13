#----------------------------------------------------------------------------
#
# Postgres-XC GTM gtm_ctl makefile
#
# Copyright(c) 2010-2012 Postgres-XC Development Group
#
# src/bin/gtm_ctl/Makefile
#
#-----------------------------------------------------------------------------
top_builddir=../../..
include $(top_builddir)/src/Makefile.global
subdir=src/bin/gtm_ctl

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)

OBJS = gtm_ctl.o \
	$(top_builddir)/src/gtm/client/libgtmclient.a \
	$(top_builddir)/src/gtm/common/libgtmcommon.a \
	$(top_builddir)/src/interfaces/libpq/libpq.a

all:gtm_ctl

gtm_ctl: $(OBJS) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) $^ $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) gtm_ctl$(X) '$(DESTDIR)$(bindir)/gtm_ctl$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/gtm_ctl$(X)'

clean distclean maintainer-clean:
	rm -f gtm_ctl$(X) $(OBJS)

# Be sure that the necessary archives are compiled
$(top_builddir)/src/interfaces/libpq/libpq.a:
	$(MAKE) -C $(top_builddir)/src/interfaces/libpq libpq.a

$(top_builddir)/src/gtm/common/libgtmcommon.a:
	$(MAKE) -C $(top_builddir)/src/gtm/common libgtmcommon.a

$(top_builddir)/src/gtm/client/libgtmclient.a:
	$(MAKE) -C $(top_builddir)/src/gtm/client libgtmclient.a
