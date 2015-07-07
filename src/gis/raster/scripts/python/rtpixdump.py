#! /usr/bin/env python
#
#
# Brute-force dump of all pixels of all bands in WKT Raster field/row to text.
# This utility is handy for debugging purposes.
#
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
#
###############################################################################
import rtreader
from optparse import OptionParser
import sys

def logit(msg):
    if VERBOSE is True:
        sys.stderr.write("LOG - " + msg + "\n")

###############################################################################
try:

    prs = OptionParser(version="%prog $Revision$",
                       usage="%prog -d <DB> -t <TABLE> [-c <COLUMN>]",
                       description="Brute-force dump of all pixel values of WKT Raster dataset")
    prs.add_option("-d", "--db", dest="db", action="store", default=None,
            help="PostgreSQL database connection string, required")
    prs.add_option("-t", "--table", dest="table", action="store", default=None,
            help="table with raster column [<schema>.]<table>, required")
    prs.add_option("-c", "--column", dest="column", action="store", default="rast",
          help="raster column, optional, default=rast")
    prs.add_option("-w", "--where", dest="where", action="store", default=None,
            help="SQL WHERE clause to filter record - NOT IMPLEMENTED")
    prs.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False,
            help="be excessively verbose and useful for debugging")

    (opts, args) = prs.parse_args()

    if opts.db is None:
        prs.error("use -d option to specify database connection string")
    if opts.table is None:
        prs.error("use -t option to specify raster table")
    if opts.column is None:
        prs.error("use -c option to specify raster column in raster table")

    global VERBOSE
    VERBOSE = opts.verbose

    rast = rtreader.RasterReader(opts.db, opts.table, opts.column)
    logit("Connected to %s" % opts.db)
    logit("Raster width=%d, height=%d, bands=%d" %(rast.width, rast.height, rast.num_bands))
    
    for band in range(1, rast.num_bands + 1):
        logit("--- BAND %d ---------------------------------" % band)
        sys.stderr.write("\n")
        for y in range(1, rast.height + 1):
            scanline = ""
            for x in range(1, rast.width + 1):
                scanline += str(int(rast.get_value(band, x, y))) + '\t'
            print scanline
        print # Bands separator

except rtreader.RasterError, e:
    print "ERROR - ", e
