#-------------------------------------------------------------------------
#
# Makefile for contrib/pgxc_ctl
#
# Portions Copyright (c) 2013 Postgres-XC Development Group
#
# $PostgreSQL$
#
#-------------------------------------------------------------------------

PGFILEDESC = "pgxc_ctl - Provide XC configuration and opeation"
PGAPPICON = win32

PROGRAM= pgxc_ctl
OBJS= bash_handler.o config.o pgxc_ctl.o pgxc_ctl_bash.o variables.o pgxc_ctl_log.o do_command.o \
	 utils.o do_shell.o gtm_cmd.o coord_cmd.o datanode_cmd.o gtm_util.o mcxt.o monitor.o


#Include GTM objects
gtm_builddir = $(top_builddir)/src/gtm
EX_OBJS = $(gtm_builddir)/common/assert.o \
	  $(gtm_builddir)/client/libgtmclient.a \
	  $(gtm_builddir)/common/gtm_serialize.o

PG_CPPFLAGS  = -DFRONTEND -DDLSUFFIX=\"$(DLSUFFIX)\" -I$(srcdir) -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport) $(PTHREAD_LIBS) $(EX_OBJS)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pgxc_clean
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

pgxc_ctl_bash.c: pgxc_ctl_conf_part pgxc_ctl_bash_2
	./make_signature
	touch pgxc_ctl_bash.c
	touch pgxc_ctl_bash_conf_part

clean_script:
	rm -f pgxc_ctl_bash.c
