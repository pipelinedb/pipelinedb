#----------------------------------------------------------------------------
#
# Postgres-XC GTM common makefile
#
# Copyright(c) 2010-2012 Postgres-XC Development Group
#
# src/gtm/common/Makefile
#
#-----------------------------------------------------------------------------
top_builddir=../../..
include $(top_builddir)/src/Makefile.global
subdir=src/gtm/common

override CPPFLAGS := -I. -I$(libpq_srcdir) $(CPPFLAGS)
LIBS += $(PTHREAD_LIBS)

include $(top_srcdir)/src/backend/common.mk

OBJS = gtm_utils.o gtm_lock.o gtm_serialize.o gtm_serialize_debug.o \
	aset.o assert.o elog.o mcxt.o stringinfo.o gtm_list.o

all: libgtmcommon.a

libgtmcommon.a: $(OBJS)
	$(AR) $(AROPT) $@ $^

clean:
	rm -f $(OBJS) libgtmcommon.a

distclean: clean

maintainer-clean: distclean
