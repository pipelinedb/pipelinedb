#-------------------------------------------------------------------------
#
# Makefile for src/bin/pg_config
#
# Portions Copyright (c) 1998-2014, PostgreSQL Global Development Group
# Portions Copyright (c) 2013-2015, PipelineDB
#
# src/bin/pg_config/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "pg_config - report configuration information"
PGAPPICON=win32

subdir = src/bin/pg_config
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS=   pg_config.o $(WIN32RES)

# don't include subdirectory-path-dependent -I and -L switches
STD_CPPFLAGS := $(filter-out -I$(top_srcdir)/src/include -I$(top_builddir)/src/include,$(CPPFLAGS))
STD_LDFLAGS := $(filter-out -L$(top_builddir)/src/port,$(LDFLAGS))

override CPPFLAGS += -DVAL_CONFIGURE="\"$(configure_args)\""
override CPPFLAGS += -DVAL_CC="\"$(CC)\""
override CPPFLAGS += -DVAL_CPPFLAGS="\"$(STD_CPPFLAGS)\""
override CPPFLAGS += -DVAL_CFLAGS="\"$(CFLAGS)\""
override CPPFLAGS += -DVAL_CFLAGS_SL="\"$(CFLAGS_SL)\""
override CPPFLAGS += -DVAL_LDFLAGS="\"$(STD_LDFLAGS)\""
override CPPFLAGS += -DVAL_LDFLAGS_EX="\"$(LDFLAGS_EX)\""
override CPPFLAGS += -DVAL_LDFLAGS_SL="\"$(LDFLAGS_SL)\""
override CPPFLAGS += -DVAL_LIBS="\"$(LIBS)\""

all: pg_config

pg_config: $(OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $(OBJS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_SCRIPT) pg_config$(X) '$(DESTDIR)$(bindir)/pg_config$(X)'
	@rm -f '$(DESTDIR)$(bindir)/pipeline-config$(X)'
	$(LN_S) pg_config $(DESTDIR)$(bindir)/pipeline-config

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_config$(X)'
	rm -f '$(DESTDIR)$(bindir)/pipeline-config$(X)'

clean distclean maintainer-clean:
	rm -f pg_config$(X) $(OBJS)
	rm -rf tmp_check

check: all
	$(prove_check)

installcheck:
	$(prove_installcheck)
