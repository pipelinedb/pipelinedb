#----------------------------------------------------------------------------
#
# Postgres-XC GTM libpq makefile
#
# Copyright(c) 2010-2012 Postgres-XC Development Group
#
# src/gtm/libpq/Makefile
#
#-----------------------------------------------------------------------------
top_builddir=../../..
include $(top_builddir)/src/Makefile.global
subdir=src/gtm/libpq

include $(top_srcdir)/src/backend/common.mk

OBJS = ip.o pqcomm.o pqformat.o strlcpy.o pqsignal.o

all: libgtmpq.a

libgtmpq.a: $(OBJS)
	$(AR) $(AROPT) $@ $^

clean:
	rm -f $(OBJS) libgtmpq.a

distclean: clean

maintainer-clean: distclean
