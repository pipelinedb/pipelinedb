#! /usr/bin/env python
#
#
# This is a simple script based on GDAL to dump overview to separate file.
# It is used in WKTRaster testing to compare raster samples.
# 
# NOTE: GDAL does include Frank's (unofficial) dumpoverviews utility too,
#       which dumps overview as complete geospatial raster dataset.
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
from optparse import OptionParser
import os
import struct
import sys


prs = OptionParser(version="%prog $Revision: 4037 $",
                   usage="%prog -r <RASTER> -v <OVERVIEW>",
                   description="Dump GDAL raster overview to separate file, GeoTIF by default")
prs.add_option("-r", "--raster", dest="raster", action="store", default=None,
               help="input raster file")
prs.add_option("-v", "--overview", dest="overview", action="store", type="int", default=None,
               help="1-based index of raster overview, optional")

(opts, args) = prs.parse_args()

if opts.raster is None:
    prs.error("use option -r to specify input raster file that contains overviews")

################################################################################
# MAIN

# Source
src_ds = gdal.Open(opts.raster, gdalc.GA_ReadOnly);
if src_ds is None:
    sys.exit('ERROR: Cannot open input file: ' + opts.raster)

band = src_ds.GetRasterBand(1)
if opts.overview is None:
    nvstart = 0
    nvend = band.GetOverviewCount()
else:
    nvstart = opts.overview - 1
    nvend = opts.overview
    
for nv in range(nvstart, nvend):

    band = src_ds.GetRasterBand(1)
    if nv < 0 and nv >= band.GetOverviewCount():
        print "ERROR: Failed to find overview %d" % nv
        sys.exit(1)
    ov_band = band.GetOverview(nv)

    ovf = int(0.5 + band.XSize / float(ov_band.XSize))

    print "--- OVERVIEW #%d level = %d ---------------------------------------" % (nv + 1, ovf)

    # Destination
    dst_file = os.path.basename(opts.raster) + "_ov_" + str(ovf) + ".tif"
    dst_format = "GTiff"
    dst_drv = gdal.GetDriverByName(dst_format)
    dst_ds = dst_drv.Create(dst_file, ov_band.XSize, ov_band.YSize, src_ds.RasterCount, ov_band.DataType)

    print "Source: " + opts.raster
    print "Target: " + dst_file
    print "Exporting %d bands of size %d x %d" % (src_ds.RasterCount, ov_band.XSize, ov_band.YSize)

    # Rewrite bands of overview to output file
    ov_band = None
    band = None

    for nb in range(1, src_ds.RasterCount + 1):

        band = src_ds.GetRasterBand(nb)
        assert band is not None
        ov_band = band.GetOverview(nv)
        assert ov_band is not None

        print " Band #%d (%s) (%d x %d)" % \
              (nb, gdal.GetDataTypeName(ov_band.DataType), ov_band.XSize, ov_band.YSize)

        dst_band = dst_ds.GetRasterBand(nb)
        assert dst_band is not None
        data = ov_band.ReadRaster(0, 0, ov_band.XSize, ov_band.YSize)
        assert data is not None
        dst_band.WriteRaster(0, 0, ov_band.XSize, ov_band.YSize, data, buf_type = ov_band.DataType)

    dst_ds = None

# Cleanup
src_ds = None
