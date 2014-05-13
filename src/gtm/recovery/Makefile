#----------------------------------------------------------------------------
#
# Postgres-XC GTM recovery makefile
#
# Copyright(c) 2010-2012 Postgres-XC Development Group
#
# src/gtm/recovery/Makefile
#
#-----------------------------------------------------------------------------
top_builddir=../../..
include $(top_builddir)/src/Makefile.global
subdir=src/gtm/recovery

include $(top_srcdir)/src/backend/common.mk

OBJS = register_common.o standby_utils.o

all: libgtmrecovery.a

libgtmrecovery.a: $(OBJS)
	$(AR) $(AROPT) $@ $^

clean:
	rm -f $(OBJS) libgtmrecovery.a

distclean: clean

maintainer-clean: distclean

