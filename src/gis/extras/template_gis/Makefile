include ../../Makefile.config

#
# PostGIS template_gis Makefile
#

SCRIPTS_IN:=$(wildcard *.in)
SCRIPTS:=$(SCRIPTS_IN:.in=)
PUBLIC_SCRIPTS:=mktemplate_gis rmtemplate_gis createdb.postgis
SONAME:=$(shell grep "^REL_MAJOR_VERSION" ../../Version.config | cut -d= -f2)

SUBSTITUTE=-e s!@bindir@!$(SUBBINDIR)!g \
           -e s!@datadir@!$(datadir)!g \
           -e s!@prefix@!$(prefix)!g \
           -e s!@SONAME@!$(SONAME)!g
SCRIPT_PREPARE=cat $(SCRIPT).in | sed $(SUBSTITUTE) > $(SCRIPT)
SCRIPT_TARGETDIR=$(DESTDIR)$(bindir)
SCRIPT_TARGET=$(SCRIPT_TARGETDIR)/$(SCRIPT)
SCRIPT_INSTALL=$(INSTALL_BIN) $(SCRIPT) $(SCRIPT_TARGET)
SCRIPT_LINKDIR=$(DESTDIR)$(prefix)/bin
SCRIPT_LINK=$(SCRIPT_LINKDIR)/$(SCRIPT)
SCRIPT_DOLINK=rm -f $(SCRIPT_LINK); \
              $(LN_S) $(SCRIPT_TARGET) $(SCRIPT_LINKDIR)
SCRIPT_UNINSTALL=rm -f $(SCRIPT_TARGET)
SCRIPT_UNLINK=rm -f $(SCRIPT_LINK)

# would probably be nice to link these somewhere as well
MANPAGES1:=$(wildcard *.1)
MAN1_TARGETDIR=$(DESTDIR)$(mandir)/man1
MAN1_TARGET=$(MAN1_TARGETDIR)/$(MAN1)
MAN1_INSTALL=$(INSTALL_DATA) $(MAN1) $(MAN1_TARGET)
MAN1_UNINSTALL=rm -f $(MAN1_TARGET)

#DEFAULTS_DIR=$(DESTDIR)/etc/default
DEFAULTS_DIR=$(DESTDIR)$(datadir)/default
SCRIPT_DEFAULTS=$(DEFAULTS_DIR)/postgis

all: $(SCRIPTS_IN)
	$(foreach SCRIPT, $(SCRIPTS), $(SCRIPT_PREPARE);)

install: all
	@mkdir -p $(SCRIPT_TARGETDIR)
	@mkdir -p $(SCRIPT_LINKDIR)
	$(foreach SCRIPT, $(SCRIPTS), $(SCRIPT_INSTALL);)
	$(foreach SCRIPT, $(PUBLIC_SCRIPTS), $(SCRIPT_DOLINK);)
	@mkdir -p $(DEFAULTS_DIR)
	cp profile $(SCRIPT_DEFAULTS)
	rm -f $(SCRIPT_TARGETDIR)/profile
	@mkdir -p $(MAN1_TARGETDIR)
	$(foreach MAN1, $(MANPAGES1), $(MAN1_INSTALL);)

uninstall:
	$(foreach SCRIPT, $(PUBLIC_SCRIPTS), $(SCRIPT_UNLINK);)
	$(foreach SCRIPT, $(SCRIPTS), $(SCRIPT_UNINSTALL);)
	$(foreach MAN1, $(MANPAGES1), $(MAN1_UNINSTALL);)

purge: uninstall
	rm -f $(SCRIPT_DEFAULTS)

clean distclean:
	rm -f $(SCRIPTS)

.PHONY: all install uninstall purge clean distclean
