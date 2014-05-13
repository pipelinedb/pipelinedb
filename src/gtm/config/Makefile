#----------------------------------------------------------------------------
#
# Postgres-XC GTM conf makefile
#
# Copyright(c) 2010-2012 Postgres-XC Development Group
#
# src/gtm/config/Makefile
#
#-----------------------------------------------------------------------------
top_builddir=../../..
include $(top_builddir)/src/Makefile.global
subdir=src/gtm/config

override CPPFLAGS := -I. -I$(libpq_srcdir) $(CPPFLAGS)
LIBS += $(PTHREAD_LIBS)

include $(top_srcdir)/src/backend/common.mk

OBJS = gtm_opt_handler.o

all: libgtmconfig.a

libgtmconfig.a: $(OBJS)
	$(AR) $(AROPT) $@ $^

gtm_opt_handler.o: gtm_opt_scanner.c

gtm_opt_scanner.c: gtm_opt_scanner.l
ifdef FLEX
	$(FLEX) $(FLEXFLAGS) -o'$@' $<
else
	@$(missing) flex $< $@
endif

# Note that gtm_opt_scanner.c is not deleted by make clean as we want it in distribution tarballs
clean:
	rm -f $(OBJS) libgtmconfig.a

distclean: clean

maintainer-clean: distclean
