#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for unit tests
#
# IDENTIFICATION
#    src/test/unit/Makefile
#
#-------------------------------------------------------------------------

backend = ../../backend
subdir = src/test/unit
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

SUBDIRS = $(backend)/access $(backend)/bootstrap \
$(backend)/catalog $(backend)/parser $(backend)/commands \
$(backend)/executor $(backend)/foreign $(backend)/lib \
$(backend)/libpq $(backend)/nodes $(backend)/optimizer \
$(backend)/pipeline $(backend)/port $(backend)/postmaster \
$(backend)/regex $(backend)/replication $(backend)/rewrite \
$(backend)/storage $(backend)/tcop $(backend)/tsearch $(backend)/utils \
$(top_builddir)/src/timezone

subsysfilename = objfiles.txt

SUBDIROBJS = $(SUBDIRS:%=%/$(subsysfilename))

# top-level backend directory obviously has its own "all" target
#ifneq ($(subdir), src/backend)
#all: $(subsysfilename)
#endif

SUBSYS.o: $(SUBDIROBJS) $(OBJS)
	$(LD) $(LDREL) $(LDOUT) $@ $^

objfiles.txt: Makefile $(SUBDIROBJS) $(OBJS)
# Don't rebuild the list if only the OBJS have changed.
	$(if $(filter-out $(OBJS),$?),( $(if $(SUBDIROBJS),cat $(SUBDIROBJS); )echo $(addprefix $(subdir)/,$(OBJS)) ) >$@,touch $@)

# make function to expand objfiles.txt contents
expand_subsys = $(foreach file,$(1),$(if $(filter %/objfiles.txt,$(file)),$(patsubst ../../src/backend/%,%,$(addprefix $(top_builddir)/,$(shell cat $(file)))),$(file)))

# Parallel make trickery
$(SUBDIROBJS): $(SUBDIRS:%=%-recursive) ;

.PHONY: $(SUBDIRS:%=%-recursive)
$(SUBDIRS:%=%-recursive):
	$(MAKE) -C $(subst -recursive,,$@) all

# As of 1/2010:
# The probes.o file is necessary for dtrace support on Solaris, and on recent
# versions of systemtap.  (Older systemtap releases just produce an empty
# file, but that's okay.)  However, OS X's dtrace doesn't use it and doesn't
# even recognize the -G option.  So, build probes.o except on Darwin.
# This might need adjustment as other platforms add dtrace support.
ifneq ($(PORTNAME), darwin)
ifeq ($(enable_dtrace), yes)
LOCALOBJS += utils/probes.o
endif
endif

TEST_OBJS = test_tdigest.o runner.o

OBJS = $(SUBDIROBJS) $(LOCALOBJS) $(top_builddir)/src/port/libpgport_srv.a \
       $(top_builddir)/src/common/libpgcommon_srv.a \
       $(top_builddir)/src/hll/MurmurHash3.o $(top_builddir)/src/hll/hll.o \
       $(TEST_OBJS)

# We put libpgport and libpgcommon into OBJS, so remove it from LIBS; also add
# libldap
LIBS := $(filter-out -lpgport -lpgcommon, $(LIBS)) $(LDAP_LIBS_BE) -lcheck -lpthread

# The backend doesn't need everything that's in LIBS, however
LIBS := $(filter-out -lz -lreadline -ledit -ltermcap -lncurses -lcurses, $(LIBS))

##########################################################################

all: submake-libpgport submake-schemapg unit_tests $(POSTGRES_IMP)

ifneq ($(PORTNAME), cygwin)
ifneq ($(PORTNAME), win32)
ifneq ($(PORTNAME), aix)

unit_tests: $(OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(LDFLAGS_EX) $(export_dynamic) $(call expand_subsys,$^) $(LIBS) -o $@

endif
endif
endif

ifeq ($(PORTNAME), cygwin)

unit_tests: $(OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(LDFLAGS_EX) $(export_dynamic) -Wl,--stack,$(WIN32_STACK_RLIMIT) -Wl,--export-all-symbols -Wl,--out-implib=libpostgres.a $(call expand_subsys,$^) $(LIBS) -o $@

# There is no correct way to write a rule that generates two files.
# Rules with two targets don't have that meaning, they are merely
# shorthand for two otherwise separate rules.  To be safe for parallel
# make, we must chain the dependencies like this.  The semicolon is
# important, otherwise make will choose some built-in rule.

libpostgres.a: postgres ;

endif # cygwin

ifeq ($(PORTNAME), win32)
LIBS += -lsecur32

unit_tests: $(OBJS) $(WIN32RES)
	$(CC) $(CFLAGS) $(LDFLAGS) $(LDFLAGS_EX) -Wl,--stack=$(WIN32_STACK_RLIMIT) -Wl,--export-all-symbols -Wl,--out-implib=libpostgres.a $(call expand_subsys,$(OBJS)) $(WIN32RES) $(LIBS) -o $@$(X)

libpostgres.a: postgres ;

endif # win32

ifeq ($(PORTNAME), aix)

unit_tests: $(POSTGRES_IMP)
	$(CC) $(CFLAGS) $(LDFLAGS) $(LDFLAGS_EX) $(call expand_subsys,$(OBJS)) -Wl,-bE:$(top_builddir)/src/backend/$(POSTGRES_IMP) $(LIBS) -o $@

$(POSTGRES_IMP): $(OBJS)
	$(LD) $(LDREL) $(LDOUT) SUBSYS.o $(call expand_subsys,$^)
ifeq ($(host_os), aix3.2.5)
	$(MKLDEXPORT) SUBSYS.o $(bindir)/postgres > $@
else
ifneq (,$(findstring aix4.1, $(host_os)))
	$(MKLDEXPORT) SUBSYS.o $(bindir)/postgres > $@
else
	$(MKLDEXPORT) SUBSYS.o . > $@
endif
endif
	@rm -f SUBSYS.o

endif # aix

# Update the commonly used headers before building the subdirectories
$(SUBDIRS:%=%-recursive): $(top_builddir)/src/include/parser/gram.h $(top_builddir)/src/include/catalog/schemapg.h $(top_builddir)/src/include/utils/fmgroids.h $(top_builddir)/src/include/utils/errcodes.h $(top_builddir)/src/include/utils/probes.h

# run this unconditionally to avoid needing to know its dependencies here:
submake-schemapg:
	$(MAKE) -C $(backend)/catalog schemapg.h

# src/port needs a convenient way to force errcodes.h to get built
submake-errcodes: $(top_builddir)/src/include/utils/errcodes.h

.PHONY: submake-schemapg submake-errcodes

catalog/schemapg.h: | submake-schemapg

$(top_builddir)/src/port/libpgport_srv.a: | submake-libpgport


# The postgres.o target is needed by the rule in Makefile.global that
# creates the exports file when MAKE_EXPORTS = true.
postgres.o: $(OBJS)
	$(CC) $(LDREL) $(LDFLAGS) $(LDFLAGS_EX) $(call expand_subsys,$^) $(LIBS) -o $@


# The following targets are specified in make commands that appear in
# the make files in our subdirectories. Note that it's important we
# match the dependencies shown in the subdirectory makefiles!

$(backend)/parser/gram.h: parser/gram.y
	$(MAKE) -C $(backend)/parser gram.h

$(backend)/utils/fmgroids.h: $(backend)/utils/Gen_fmgrtab.pl $(backend)/catalog/Catalog.pm $(top_srcdir)/src/include/catalog/pg_proc.h
	$(MAKE) -C $(backend)/utils fmgroids.h

$(backend)/utils/errcodes.h: $(backend)/utils/generate-errcodes.pl $(backend)/utils/errcodes.txt
	$(MAKE) -C $(backend)/utils errcodes.h

$(backend)/utils/probes.h: $(backend)/utils/probes.d
	$(MAKE) -C $(backend)/utils probes.h

# Make symlinks for these headers in the include directory. That way
# we can cut down on the -I options. Also, a symlink is automatically
# up to date when we update the base file.
#
# The point of the prereqdir incantation in some of the rules below is to
# force the symlink to use an absolute path rather than a relative path.
# For headers which are generated by make distprep, the actual header within
# src/backend will be in the source tree, while the symlink in src/include
# will be in the build tree, so a simple ../.. reference won't work.
# For headers generated during regular builds, we prefer a relative symlink.

$(top_builddir)/src/include/parser/gram.h: $(backend)/parser/gram.h
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	  cd '$(dir $@)' && rm -f $(notdir $@) && \
	  $(LN_S) "$$prereqdir/$(notdir $<)" .

$(top_builddir)/src/include/catalog/schemapg.h: $(backend)/catalog/schemapg.h
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	  cd '$(dir $@)' && rm -f $(notdir $@) && \
	  $(LN_S) "$$prereqdir/$(notdir $<)" .

$(top_builddir)/src/include/utils/errcodes.h: $(backend)/utils/errcodes.h
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	  cd '$(dir $@)' && rm -f $(notdir $@) && \
	  $(LN_S) "$$prereqdir/$(notdir $<)" .

$(top_builddir)/src/include/utils/fmgroids.h: $(backend)/utils/fmgroids.h
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	  cd '$(dir $@)' && rm -f $(notdir $@) && \
	  $(LN_S) "$$prereqdir/$(notdir $<)" .

$(top_builddir)/src/include/utils/probes.h: $(backend)/utils/probes.h
	cd '$(dir $@)' && rm -f $(notdir $@) && \
	    $(LN_S) "$(backend)/utils/probes.h" .

utils/probes.o: utils/probes.d $(SUBDIROBJS)
	$(DTRACE) $(DTRACEFLAGS) -C -G -s $(call expand_subsys,$^) -o $@

check: all
	$(top_builddir)/$(subdir)/unit_tests

clean:
	rm -f $(TEST_OBJS) unit_tests
