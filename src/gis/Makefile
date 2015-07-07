
# this file copied and adapted from PostgreSQL source
# to allow easy build on BSD systems

all install uninstall clean distclean maintainer-clean test check docs docs-install docs-uninstall utils: GNUmakefile
	@IFS=':' ; \
	 for dir in $$PATH; do \
	   for prog in gmake gnumake make; do \
	     if [ -f $$dir/$$prog ] && ( $$dir/$$prog -f /dev/null --version 2>/dev/null | grep GNU >/dev/null 2>&1 ) ; then \
	       GMAKE=$$dir/$$prog; \
	       break 2; \
	     fi; \
	   done; \
	 done; \
	\
	 if [ x"$${GMAKE+set}" = xset ]; then \
	   echo "Using GNU make found at $${GMAKE}"; \
	   $${GMAKE} $@ ; \
	 else \
	   echo "You must use GNU make to build PostGIS." ; \
	   false; \
	 fi

configure: configure.in
	./autogen.sh

GNUmakefile: GNUmakefile.in
	./configure
