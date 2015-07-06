-----------------------------------------------------------------------
--
-- Copyright (c) 2010 Mateusz Loskot <mateusz@loskot.net>
-- Copyright (C) 2011 - 2013 Regents of the University of California
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
--- Test RASTER_COLUMNS
-----------------------------------------------------------------------

-- Check table exists
SELECT
	c.relname
FROM pg_class c, pg_views v
WHERE c.relname = v.viewname
	AND v.viewname = 'raster_columns';

-----------------------------------------------------------------------
--- Test AddRasterConstraints and DropRasterConstraints
-----------------------------------------------------------------------

DROP TABLE IF EXISTS test_raster_columns;
CREATE TABLE test_raster_columns (
	rid integer,
	rast raster
);
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

SELECT AddRasterConstraints(current_schema(), 'test_raster_columns', 'rast'::name);
SELECT r_table_name, r_raster_column, srid, scale_x, scale_y, blocksize_x, blocksize_y, same_alignment, regular_blocking, num_bands, pixel_types, nodata_values, ST_AsEWKT(extent) FROM raster_columns WHERE r_table_name = 'test_raster_columns';

SELECT DropRasterConstraints(current_schema(),'test_raster_columns', 'rast'::name);
SELECT r_table_name, r_raster_column, srid, scale_x, scale_y, blocksize_x, blocksize_y, same_alignment, regular_blocking, num_bands, pixel_types, nodata_values, ST_AsEWKT(extent) FROM raster_columns WHERE r_table_name = 'test_raster_columns';

SELECT AddRasterConstraints('test_raster_columns', 'rast'::name, 'srid'::text, 'extent', 'blocksize');
SELECT r_table_name, r_raster_column, srid, scale_x, scale_y, blocksize_x, blocksize_y, same_alignment, regular_blocking, num_bands, pixel_types, nodata_values, ST_AsEWKT(extent) FROM raster_columns WHERE r_table_name = 'test_raster_columns';

SELECT DropRasterConstraints('test_raster_columns', 'rast'::name, 'scale'::text);
SELECT r_table_name, r_raster_column, srid, scale_x, scale_y, blocksize_x, blocksize_y, same_alignment, regular_blocking, num_bands, pixel_types, nodata_values, ST_AsEWKT(extent) FROM raster_columns WHERE r_table_name = 'test_raster_columns';

SELECT AddRasterConstraints('test_raster_columns', 'rast', FALSE, TRUE, TRUE, FALSE, FALSE, TRUE, FALSE, TRUE, FALSE);
SELECT r_table_name, r_raster_column, srid, scale_x, scale_y, blocksize_x, blocksize_y, same_alignment, regular_blocking, num_bands, pixel_types, nodata_values, ST_AsEWKT(extent) FROM raster_columns WHERE r_table_name = 'test_raster_columns';

SELECT DropRasterConstraints(current_schema(), 'test_raster_columns', 'rast'::name, 'scale'::text);
SELECT r_table_name, r_raster_column, srid, scale_x, scale_y, blocksize_x, blocksize_y, same_alignment, regular_blocking, num_bands, pixel_types, nodata_values, ST_AsEWKT(extent) FROM raster_columns WHERE r_table_name = 'test_raster_columns';

SELECT DropRasterConstraints(current_schema(), 'test_raster_columns', 'rast'::name);
DELETE FROM test_raster_columns;

-- regular_blocking

SELECT make_test_raster(1, 3, 3, 0, 0);
SELECT make_test_raster(2, 3, 3, 3, 0);
SELECT make_test_raster(3, 3, 3, 0, 3);
SELECT make_test_raster(4, 3, 3, 3, 3);

SELECT AddRasterConstraints(current_schema(), 'test_raster_columns', 'rast'::name);
SELECT AddRasterConstraints(current_schema(), 'test_raster_columns', 'rast'::name, 'regular_blocking');
SELECT r_table_name, r_raster_column, srid, scale_x, scale_y, blocksize_x, blocksize_y, same_alignment, regular_blocking, num_bands, pixel_types, nodata_values, ST_AsEWKT(extent) FROM raster_columns WHERE r_table_name = 'test_raster_columns';

-- spatially unique, this should fail 
SELECT make_test_raster(0, 3, 3, 0, 0);

-- coverage tile, this should fail
SELECT make_test_raster(0, 3, 3, 1, 0);

SELECT DropRasterConstraints(current_schema(), 'test_raster_columns', 'rast'::name, 'regular_blocking');
SELECT r_table_name, r_raster_column, srid, scale_x, scale_y, blocksize_x, blocksize_y, same_alignment, regular_blocking, num_bands, pixel_types, nodata_values, ST_AsEWKT(extent) FROM raster_columns WHERE r_table_name = 'test_raster_columns';

-- check spatial_index
SELECT
	CASE
		WHEN spatial_index IS FALSE
			THEN NULL
		ELSE FALSE
	END
FROM raster_columns WHERE r_table_name = 'test_raster_columns';
CREATE INDEX test_raster_columns_rast_gist
	ON test_raster_columns
	USING gist
	(st_convexhull(rast));
SELECT
	CASE
		WHEN spatial_index IS FALSE
			THEN FALSE
		ELSE NULL
	END
FROM raster_columns WHERE r_table_name = 'test_raster_columns';

-- ticket #2215
CREATE TABLE test_raster_columns_2 AS
	SELECT rid, rast FROM test_raster_columns;
SELECT AddRasterConstraints(current_schema(), 'test_raster_columns_2', 'rast'::name);
SELECT AddRasterConstraints(current_schema(), 'test_raster_columns', 'rast'::name, 'regular_blocking');
DROP TABLE IF EXISTS test_raster_columns_2;

DROP FUNCTION make_test_raster(integer, integer, integer, double precision, double precision, double precision, double precision, double precision, double precision);
DROP TABLE IF EXISTS test_raster_columns;
