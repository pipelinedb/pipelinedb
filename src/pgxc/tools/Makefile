#----------------------------------------------------------------------------
#
# Postgres-XC documentation tool makefile
#
# Copyright (c) 2010-2012 Postgres-XC Development Group
#
# doc-xc/Makefile
#
#----------------------------------------------------------------------------

subdir = src/pgxc/tools
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

SUBDIRS = makesgml

all distprep html man install installdirs uninstall clean distclean maintainer-clean maintainer-check:
	$(MAKE) -C $(SUBDIRS) $@
