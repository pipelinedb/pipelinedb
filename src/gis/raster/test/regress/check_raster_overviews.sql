-----------------------------------------------------------------------
--
-- Copyright (c) 2010 Mateusz Loskot <mateusz@loskot.net>
-- Copyright (C) 2011 Regents of the University of California
--   <bkpark@ucdavis.edu>
--
-- This program is free software; you can redistribute it and/or
-- modify it under the terms of the GNU General Public License
-- as published by the Free Software Foundation; either version 2
-- of the License, or (at your option) any later version.
--
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
--
-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software Foundation,
-- Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
-----------------------------------------------------------------------

SET client_min_messages TO warning;

-----------------------------------------------------------------------
--- Test RASTER_OVERVIEWS
-----------------------------------------------------------------------

-- Check table exists
SELECT c.relname FROM pg_class c, pg_views v
  WHERE c.relname = v.viewname
    AND v.viewname = 'raster_overviews';

-----------------------------------------------------------------------
--- Test AddOverviewConstraints and DropOverviewConstraints
-----------------------------------------------------------------------

DROP TABLE IF EXISTS test_raster_columns;
CREATE TABLE test_raster_columns (
	rid integer,
	rast raster
);
DROP TABLE IF EXISTS test_raster_overviews;
CREATE OR REPLACE FUNCTION make_test_raster(
	rid integer,
	width integer DEFAULT 2,
	height integer DEFAULT 2,
	ul_x double precision DEFAULT 0,
	ul_y double precision DEFAULT 0,
	skew_x double precision DEFAULT 0,
	skew_y double precision DEFAULT 0,
	initvalue double precision DEFAULT 1,
	nodataval double precision DEFAULT 0
)
	RETURNS void
	AS $$
	DECLARE
		x int;
		y int;
		rast raster;
	BEGIN
		rast := ST_MakeEmptyRaster(width, height, ul_x, ul_y, 1, 1, skew_x, skew_y, 0);
		rast := ST_AddBand(rast, 1, '8BUI', initvalue, nodataval);


		INSERT INTO test_raster_columns VALUES (rid, rast);

		RETURN;
	END;
	$$ LANGUAGE 'plpgsql';
-- no skew
SELECT make_test_raster(0, 2, 2, -2, -2);
SELECT make_test_raster(1, 2, 2, 0, 0, 0, 0, 2);
SELECT make_test_raster(2, 2, 2, 1, -1, 0, 0, 3);
SELECT make_test_raster(3, 2, 2, 1, 1, 0, 0, 4);
SELECT make_test_raster(4, 2, 2, 2, 2, 0, 0, 5);

SELECT *
INTO test_raster_overviews
FROM test_raster_columns;
SELECT r_table_name, r_raster_column, srid, scale_x, scale_y, blocksize_x, blocksize_y, same_alignment, regular_blocking, num_bands, pixel_types, nodata_values, ST_AsEWKT(extent) FROM raster_columns WHERE r_table_name IN ('test_raster_columns', 'test_raster_overviews') ORDER BY r_table_name, r_raster_column;
SELECT o_table_name, o_raster_column, r_table_name, r_raster_column, overview_factor FROM raster_overviews WHERE o_table_name = 'test_raster_overviews';

SELECT AddRasterConstraints(current_schema(), 'test_raster_columns', 'rast'::name);
SELECT r_table_name, r_raster_column, srid, scale_x, scale_y, blocksize_x, blocksize_y, same_alignment, regular_blocking, num_bands, pixel_types, nodata_values, ST_AsEWKT(extent) FROM raster_columns WHERE r_table_name IN ('test_raster_columns', 'test_raster_overviews') ORDER BY r_table_name, r_raster_column;
SELECT o_table_name, o_raster_column, r_table_name, r_raster_column, overview_factor FROM raster_overviews WHERE o_table_name = 'test_raster_overviews';

SELECT AddOverviewConstraints('test_raster_overviews', 'rast', 'test_raster_columns', 'rast', 1);
SELECT r_table_name, r_raster_column, srid, scale_x, scale_y, blocksize_x, blocksize_y, same_alignment, regular_blocking, num_bands, pixel_types, nodata_values, ST_AsEWKT(extent) FROM raster_columns WHERE r_table_name IN ('test_raster_columns', 'test_raster_overviews') ORDER BY r_table_name, r_raster_column;
SELECT o_table_name, o_raster_column, r_table_name, r_raster_column, overview_factor FROM raster_overviews WHERE o_table_name = 'test_raster_overviews';

SELECT DropOverviewConstraints(current_schema(), 'test_raster_overviews', 'rast');
SELECT r_table_name, r_raster_column, srid, scale_x, scale_y, blocksize_x, blocksize_y, same_alignment, regular_blocking, num_bands, pixel_types, nodata_values, ST_AsEWKT(extent) FROM raster_columns WHERE r_table_name IN ('test_raster_columns', 'test_raster_overviews') ORDER BY r_table_name, r_raster_column;
SELECT o_table_name, o_raster_column, r_table_name, r_raster_column, overview_factor FROM raster_overviews WHERE o_table_name = 'test_raster_overviews';

SELECT DropRasterConstraints(current_schema(), 'test_raster_columns', 'rast'::name);
SELECT r_table_name, r_raster_column, srid, scale_x, scale_y, blocksize_x, blocksize_y, same_alignment, regular_blocking, num_bands, pixel_types, nodata_values, ST_AsEWKT(extent) FROM raster_columns WHERE r_table_name IN ('test_raster_columns', 'test_raster_overviews') ORDER BY r_table_name, r_raster_column;
SELECT o_table_name, o_raster_column, r_table_name, r_raster_column, overview_factor FROM raster_overviews WHERE o_table_name = 'test_raster_overviews';

DROP FUNCTION make_test_raster(integer, integer, integer, double precision, double precision, double precision, double precision, double precision, double precision);
DROP TABLE IF EXISTS test_raster_overviews;
DROP TABLE IF EXISTS test_raster_columns;
