#----------------------------------------------------------------------------
#
# Postgres-XC makesgml tool makefile
#
# Copyright(c) 2010-2012 Postgres-XC Development Group
#
# src/pgxc/tools/makesgml/Makefile
#
#-----------------------------------------------------------------------------

PGFILEDESC = "makesgml - convert .sgmlin to .sgml"
PGAPPICON=win32

subdir = src/pgxc/tools/makesgml
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I. -I$(srcdir) $(CPPFLAGS)

OBJS = makesgml.o

makesgml: $(OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $^ -o $@$(X)

all distprep html man: makesgml

install: all installdirs
	$(INSTALL_PROGRAM) makesgml$(X) '$(DESTDIR)$(bindir)/makesgml$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -rf '$(DESTDIR)$(bindir)/makesgml$(X)'

clean distclean maintenance-clean maintainer-clean:
	rm -rf makesgml$(X) $(OBJS)
