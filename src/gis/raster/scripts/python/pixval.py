#! /usr/bin/env python
#
#
# This is a simple script based on GDAL and used to retrieve value of single raster pixel.
# It is used in WKTRaster testing to compare raster samples.
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

from osgeo import gdal
from osgeo import osr
import osgeo.gdalconst as gdalc
import struct
import sys

def pt2fmt(pt):
    fmttypes = {
        gdalc.GDT_Byte: 'B',
        gdalc.GDT_Int16: 'h',
        gdalc.GDT_UInt16: 'H',
        gdalc.GDT_Int32: 'i',
        gdalc.GDT_UInt32: 'I',
        gdalc.GDT_Float32: 'f',
        gdalc.GDT_Float64: 'f'
        }
    return fmttypes.get(pt, 'x')

if len(sys.argv) < 5 or len(sys.argv) > 6:
    print "Usage: pixval.py <raster> <band> <x> <y>"
    print "\traster - GDAL supported dataset"
    print "\tband - 1-based number of band"
    print "\toverview - optional 1-based number of overview"
    print "\tx - Pixel column - 1..N where N is raster X dimension"
    print "\ty - Pixel row - 1..N where N is raster Y dimension"
    sys.exit(0)

infile = sys.argv[1]
nband = int(sys.argv[2])
x = int(sys.argv[3])
y = int(sys.argv[4])
if len(sys.argv) > 5:
    noverview = int(sys.argv[5])
else:
    noverview = None

print "File : %s" % infile
print "Band : %d" % nband
if noverview is not None:
    print "Overview: %d" % noverview
print "Pixel: %d x %d" % (x, y)

ds = gdal.Open(infile, gdalc.GA_ReadOnly);
if ds is None:
    sys.exit('ERROR: Cannot open input file: ' + str(infile))

band = ds.GetRasterBand(nband)
if band is None:
    sys.exit('Cannot access band %d', nband)

if noverview is None:
    src_band = band
else:
    if noverview > 0 and noverview <= band.GetOverviewCount():
        src_band = band.GetOverview(noverview - 1)
    else:
        print "ERROR: Invalid overview index"
        print "Band %d consists of %d overivews" % (nband, band.GetOverviewCount())
        sys.exit(1)

if x <= 0 or x > src_band.XSize or y <= 0 or y > src_band.YSize:
    print "ERROR: Invalid pixel coordinates"
    print "Band or overview dimensions are %d x %d" % (src_band.XSize, src_band.YSize)
    sys.exit(1)

# Pixel index is 0-based
pixel = src_band.ReadRaster(x - 1, y - 1, 1, 1, 1, 1)

fmt = pt2fmt(src_band.DataType)
pixval = struct.unpack(fmt, pixel)

print "Pixel value -> %s" % str(pixval[0])
