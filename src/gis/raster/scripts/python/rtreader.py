#! /usr/bin/env python
#
#
# A simple driver to read RASTER field data directly from PostGIS/WKTRaster.
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
#
# Requirements: psycopg2
#
# WARNING: Tha main purpose of the RasterReader is to test and debug
# WKT Raster implementation. It is not supposed to be an efficient
# performance killer, by no means.
#
###############################################################################
import psycopg2
import sys

###############################################################################
# RASTER driver (read-only)

class RasterError(Exception):
    def __init__(self, msg):
        self.msg = msg 
    def __str__(self):
        return self.msg

class RasterReader(object):
    """Reader of RASTER data stored in specified column and row (where) in a table"""

    # Constructors

    def __init__(self, connstr, table, column, where = ""):
        self._connstr = connstr 
        self._conn = None
        self._table = table
        self._column = column
        self._where = where
        self._sizes = None
        self._types = None
        self._logging = False
        # Connect and read RASTER header
        self._setup()

    # Public properties

    logging = property(fset = lambda self, v: setattr(self, '_logging', v))
    db = property(fget = lambda self: self._get_db())
    table = property(fget = lambda self: self._table)
    column = property(fget = lambda self: self._column)
    width = property(fget = lambda self: self._get_width())
    height = property(fget = lambda self: self._get_height())
    num_bands = property(fget = lambda self: self._get_num_bands())
    pixel_types = property(fget = lambda self: self._get_pixel_types())

    # Public methods 

    def get_value(self, band, x, y):
        return self._query_value(band, x, y)

    def copy_to(self, file, raster_format='TIFF', output_format='HEX', sep='\t'):
        """
        Proxy for SQL command COPY TO,
        Converts selected rasters to specified raster_format with output sent either to 
        single hex-based plain text file or one or more binary files in raster_format,
        one raster binary file per tuple from the raster table.
        The BIN output uses HEX output as intermediate stage.
        raster_format - TIFF|JPEG|PNG
        output_format - HEX|BIN; BIN is a binary file in raster_format
        sep - if output_format=HEX, separates rid value from hex-encoded binary.
        """
        import os.path
        filehex = file # No extension added, may be user-defiened
        with open(filehex, 'w') as f:
            select = "SELECT rid, encode(ST_As%s(%s), 'hex') As rt FROM %s" % (raster_format, self._column, self._table)
            if self._where is not None and len(self._where) > 0:
                select += ' WHERE %s' % self._where
            sql = "COPY (%s) TO STDOUT (DELIMITER '%s')" % (select, sep)        
            cur = self._conn.cursor()
            cur.copy_expert(sql, f)

        if output_format == 'BIN':
            import binascii
            with open(filehex, 'r') as f:
                dirname = os.path.dirname(file)
                ext = raster_format.lower()
                for line in f.readlines():
                    rid, raster = line.split()
                    filebin = self._table + '_' + self._column + '_' + rid + '.' + ext
                    filebin = os.path.join(dirname, filebin)
                    with open(filebin, 'w+') as fbin:
                        fbin.write(binascii.unhexlify(raster))

    # Private methods

    def _log(self, m):
        if self._logging:
            sys.stderr.write('[rtreader] ' + str(m) + '\n')

    def _get_db(self):
        n = filter(lambda db: db[:6] == 'dbname', self._connstr.split())[0].split('=')[1]
        return n.strip('\'').strip()

    def _get_width(self):
        return self._query_raster_size(0)

    def _get_height(self):
        return self._query_raster_size(1)

    def _get_num_bands(self):
        return self._query_raster_size(2)

    def _get_pixel_types(self):
        return self._query_pixel_types()

    def _setup(self):
        self._connect()

    def _connect(self):
        try:
            if self._conn is None:
                self._conn = psycopg2.connect(self._connstr)
        except Exception as e:
            raise RasterError("Falied to connect to %s: %s" % (self._connstr, e))

    def _query_single_row(self, sql):
        assert self._conn is not None
        #self._log(sql)

        try:
            cur = self._conn.cursor()
            cur.execute(sql)
        except Exception as e:
            raise RasterError("Failed to execute query %s: %s" % (sql, e))

        row = cur.fetchone()
        if row is None:
            raise RasterError("No tupes returned for query: %s" % sql)
        return row

    def _query_value(self, band, x, y):
        sql = 'SELECT st_value(%s, %d, %d, %d) FROM %s' % \
                 (self._column, band, x, y, self._table)
        if len(self._where) > 0:
            sql += ' WHERE %s' % self._where

        row = self._query_single_row(sql)
        if row is None:
            raise RasterError("Value of pixel %dx%d of band %d is none" %(x, y, band))
        return row[0]
    
    def _query_raster_size(self, dim, force = False):
        if self._sizes is None or force is True:
            sql = 'SELECT st_width(%s), st_height(%s), st_numbands(%s) FROM %s' % \
                     (self._column, self._column, self._column, self._table)
            if len(self._where) > 0:
                sql += ' WHERE %s' % self._where
                
            self._log(sql)
            self._sizes = self._query_single_row(sql)

        if self._sizes is None:
            raise RasterError("Falied to query %dx%d of band %d is none" %(x, y, band))
        return self._sizes[dim]

    def _query_pixel_types(self):

        types = []
        sql = 'SELECT '
        for i in range(0, self.num_bands):
            if i != 0:
                sql += ','
            nband = i + 1
            sql += ' st_bandpixeltype(%s, %d) ' % (self._column, nband)
        sql += ' FROM ' + self._table
        return self._query_single_row(sql)
