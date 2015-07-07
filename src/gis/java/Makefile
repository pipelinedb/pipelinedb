# **********************************************************************
# *
# * PostGIS - Spatial Types for PostgreSQL
# * http://postgis.net
# *
# * Copyright (C) 2012 Sandro Santilli <strk@keybit.net>
# *
# * This is free software; you can redistribute and/or modify it under
# * the terms of the GNU General Public Licence. See the COPYING file.
# *
# **********************************************************************

# TODO: add all subdirs
SUBDIRS = jdbc

all install uninstall clean distclean check maintainer-clean:
	for s in $(SUBDIRS); do \
		echo "---- Making $@ in $${s}"; \
		$(MAKE) -C $${s} $@ || exit 1; \
	done;
