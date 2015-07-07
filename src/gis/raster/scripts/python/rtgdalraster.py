#! /usr/bin/env python
#
#
# A simple utility for passing parameters to ST_AsGDALRaster
# for a GDAL raster output
# This utility is handy for debugging purposes.
#
# Example:
#   rtgdalraster.py -d "dbname=postgres" -r "(SELECT rast FROM pele LIMIT 1)" -f JPEG -o pele.jpg
#
#
###############################################################################
# Copyright (C) 2011 Regents of the University of California
#   <bkpark@ucdavis.edu>
# Copyright (C) 2009 Mateusz Loskot <mateusz@loskot.net>
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
###############################################################################
from optparse import OptionParser
import sys
import os
import psycopg2

def logit(msg):
	if VERBOSE is True:
		sys.stderr.write("LOG - " + str(msg) + "\n")

###############################################################################
try:
	prs = OptionParser(version="%prog",
		usage="%prog -d <DB> -r <RASTER> -o <FILE> [-f <FORMAT>] [-c <OPTIONS>]",
		description="Output raster in a GDAL raster format")
	prs.add_option("-d", "--db", dest="db", action="store", default=None,
		help="PostgreSQL database connection string, required")
	prs.add_option("-r", "--raster", dest="raster", action="store", default=None,
		help="sql expression to create or access a existing raster, required")
	prs.add_option("-o", "--output", dest="output", action="store", default=None,
		help="file to put the output of ST_AsGDALRaster if successful, required")
	prs.add_option("-f", "--format", dest="format", action="store", default="GTiff",
		help="format of GDAL raster output, optional, default=GTiff")
	prs.add_option("-c", "--config", dest="config", action="store", default=None,
		help="comma separated list of GDAL raster creation options, optional")
	prs.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False,
		help="be excessively verbose and useful for debugging")

	(opts, args) = prs.parse_args()

	if opts.db is None:
		prs.error("use -d option to specify database connection string")
	if opts.raster is None:
		prs.error("use -r option to specify a sql expression for a raster")
	if opts.output is None:
		prs.error("use -o option to specify raster output file")

	if opts.config is not None:
		opts.cfg = opts.config.split(',')
	else:
		opts.cfg = None

	global VERBOSE
	VERBOSE = opts.verbose

	conn = psycopg2.connect(opts.db)
	logit("Connected to %s" % opts.db)
	logit("Raster expression is %s" % opts.raster)

	cur = conn.cursor()
	sql = "SELECT ST_AsGDALRaster(%s, %%s, %%s::text[], NULL::integer)" % (opts.raster)
	cur.execute(sql, (opts.format, opts.cfg))
	logit("Number of rows: %i" % cur.rowcount)
	rec = cur.fetchone()[0];
	logit("size of raster (bytes): %i" % len(rec))

	fh = open(opts.output, 'wb', -1)
	fh.write(rec);
	fh.flush();
	fh.close();
	logit("size of %s (bytes): %i" % (opts.output, os.stat(opts.output)[6]));

	cur.close();
	conn.close();

	print "raster outputted to %s" % opts.output;

except Exception, e:
    print "ERROR: ", e
