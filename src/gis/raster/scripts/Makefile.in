#############################################################################
#
# Copyright (c) 2011 Regents of the University of California
#   <bkpark@ucdavis.edu>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software Foundation,
# Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#
#############################################################################

RT_CORE=../rt_core
RT_PG=../rt_pg

all: plpgsql python

plpgsql:
#	$(MAKE) -C plpgsql

python:
	$(MAKE) -C python

install:
#	$(MAKE) -C plpgsql
	$(MAKE) -C python

uninstall:

clean:
#	$(MAKE) -C plpgsql $@
	$(MAKE) -C python $@

distclean: clean
#	$(MAKE) -C plpgsql $@
	$(MAKE) -C python $@
	rm -f Makefile
