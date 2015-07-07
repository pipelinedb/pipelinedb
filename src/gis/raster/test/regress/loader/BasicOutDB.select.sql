WITH foo AS (
	SELECT postgis_raster_lib_version()
)
SELECT NULL FROM foo;
SET postgis.enable_outdb_rasters = true;
SET postgis.gdal_enabled_drivers = 'GTiff';
DELETE FROM loadedrast WHERE filename != 'testraster.tif';
SELECT srid, scale_x::numeric(16, 10), scale_y::numeric(16, 10), blocksize_x, blocksize_y, same_alignment, regular_blocking, num_bands, pixel_types, nodata_values::numeric(16,10)[], out_db, ST_AsEWKT(extent) FROM raster_columns WHERE r_table_name = 'loadedrast' AND r_raster_column = 'rast';
SELECT ST_AsEWKT(geom), val FROM (SELECT (ST_PixelAsPolygons(rast, 1)).* FROM loadedrast WHERE rid = 1) foo WHERE x = 1 AND y = 1;
SELECT ST_AsEWKT(geom), val FROM (SELECT (ST_PixelAsPolygons(rast, 2)).* FROM loadedrast WHERE rid = 1) foo WHERE x = 90 AND y = 50;
SELECT ST_AsEWKT(geom), val FROM (SELECT (ST_PixelAsPolygons(rast, 3)).* FROM loadedrast WHERE rid = 1) foo WHERE x = 45 AND y = 25;
