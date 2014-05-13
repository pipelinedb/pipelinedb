#----------------------------------------------------------------------------
#
# Postgres-XC GTM path makefile
#
# Copyright(c) 2010-2012 Postgres-XC Development Group
#
# src/gtm/path/Makefile
#
#-----------------------------------------------------------------------------
top_builddir=../../..
include $(top_builddir)/src/Makefile.global
subdir=src/gtm/path

include $(top_srcdir)/src/backend/common.mk

OBJS = path.o

all: libgtmpath.a

libgtmpath.a: $(OBJS)
	$(AR) $(AROPT) $@ $^

clean:
	rm -f $(OBJS) libgtmpath.a

distclean: clean

maintainer-clean: distclean
