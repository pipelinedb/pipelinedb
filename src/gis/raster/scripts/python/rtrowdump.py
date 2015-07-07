#! /usr/bin/env python
#
#
# Brute-force dump of single row from WKT Raster table as GeoTIFF.
# This utility is handy for debugging purposes.
#
# WARNING: Tha main purpose of this program is to test and
# debug WKT Raster implementation. It is NOT supposed to be an
# efficient performance killer, by no means.
#
###############################################################################
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
import rtreader
import numpy
import osgeo.gdalconst
from osgeo import gdal
from optparse import OptionParser
import sys

def logit(msg):
    if VERBOSE is True:
        sys.stderr.write("LOG - " + str(msg) + "\n")

def pt2gdt(pt):
    """Translate WKT Raster pixel type to GDAL type"""
    pixtypes = {
        '8BUI' : osgeo.gdalconst.GDT_Byte,
        '16BSI' : osgeo.gdalconst.GDT_Int16,
        '16BUI' : osgeo.gdalconst.GDT_UInt16,
        '32BSI' : osgeo.gdalconst.GDT_Int32,
        '32BUI' : osgeo.gdalconst.GDT_UInt32,
        '32BF' : osgeo.gdalconst.GDT_Float32,
        '64BF' : osgeo.gdalconst.GDT_Float64
        }
    return pixtypes.get(pt, 'UNKNOWN')

def pt2numpy(pt):
    """Translate WKT Raster pixel type to NumPy data type"""
    numpytypes = {
        '8BUI' : numpy.uint8,
        '16BSI' : numpy.int16,
        '16BUI' : numpy.uint16,
        '32BSI' : numpy.int32,
        '32BUI' : numpy.uint32,
        '32BF' : numpy.float32,
        '64BF' : numpy.float64
        }
    return numpytypes.get(pt, numpy.uint8)

###############################################################################
try:

    prs = OptionParser(version="%prog $Revision: 4037 $",
                       usage="%prog -d <DB> -t <TABLE> [-c <COLUMN>]",
                       description="Brute-force dump of single row from WKT Raster table as GeoTIF")
    prs.add_option("-d", "--db", dest="db", action="store", default=None,
            help="PostgreSQL database connection string, required")
    prs.add_option("-t", "--table", dest="table", action="store", default=None,
            help="table with raster column [<schema>.]<table>, required")
    prs.add_option("-c", "--column", dest="column", action="store", default="rast",
          help="raster column, optional, default=rast")
    prs.add_option("-w", "--where", dest="where", action="store", default="",
            help="SQL WHERE clause to filter record")
    prs.add_option("-o", "--output", dest="output", action="store", default=None,
            help="GeoTIFF output file for pixel data read from WKT Raster table")
    prs.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False,
            help="be excessively verbose and useful for debugging")

    (opts, args) = prs.parse_args()

    if opts.db is None:
        prs.error("use -d option to specify database connection string")
    if opts.table is None:
        prs.error("use -t option to specify raster table")
    if opts.column is None:
        prs.error("use -c option to specify raster column in raster table")
    if opts.output is None:
        prs.error("use -o option to specify raster output file")

    global VERBOSE
    VERBOSE = opts.verbose

    rt = rtreader.RasterReader(opts.db, opts.table, opts.column, opts.where)
    if VERBOSE is True:
        rt.logging = True

    logit("Connected to %s" % opts.db)
    logit("Source WKT raster:")
    logit("\trow=%s" % opts.where)
    logit("\twidth=%d, height=%d, bands=%d, pixel types=%s" \
          %(rt.width, rt.height, rt.num_bands, str(rt.pixel_types)))
    logit("Target GeoTIFF: %s" % opts.output)

    out_format = "GTiff"
    out_driver = gdal.GetDriverByName(out_format)
    out_data_type = pt2gdt(rt.pixel_types[0])
    out_ds = out_driver.Create(opts.output, rt.width, rt.height, rt.num_bands, out_data_type)
    

    for b in range(1, rt.num_bands +1):
        logit("--- BAND %d ---------------------------------" % b)

        ### Be careful!!
        ### Zeros function's input parameter can be a (height x width) array,
        ### not (width x height): http://docs.scipy.org/doc/numpy/reference/generated/numpy.zeros.html?highlight=zeros#numpy.zeros
        raster = numpy.zeros((rt.height, rt.width), pt2numpy(out_data_type))
        for width_index in range(0, rt.width):
            for height_index in range(0, rt.height):
                pixel = rt.get_value(b, width_index + 1, height_index + 1)
                raster[height_index, width_index] = pixel

        logit(str(raster))
            
        band = out_ds.GetRasterBand(b)
        assert band is not None
        band.WriteArray(raster)

except rtreader.RasterError, e:
    print "ERROR - ", e
