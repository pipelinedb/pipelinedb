#----------------------------------------------------------------------------
#
# Postgres-XC GTM client makefile
#
# Copyright(c) 2010-2012 Postgres-XC Development Group
#
# src/gtm/client/Makefile
#
#-----------------------------------------------------------------------------
top_builddir=../../..
include $(top_builddir)/src/Makefile.global
subdir=src/gtm/client

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)
LIBS += $(PTHREAD_LIBS)

include $(top_srcdir)/src/backend/common.mk

OBJS = fe-misc.o fe-connect.o gtm_client.o fe-protocol.o ip.o pqexpbuffer.o

all: libgtmclient.a

libgtmclient.a: $(OBJS)
	$(AR) $(AROPT) $@ $^

clean:
	rm -f $(OBJS) libgtmclient.a

distclean: clean

maintainer-clean: distclean
