SET client_min_messages TO warning;

DROP TABLE IF EXISTS raster_clip;
CREATE TABLE raster_clip (
	rid integer,
	rast raster
);
DROP TABLE IF EXISTS geom_clip;
CREATE TABLE geom_clip (
	gid integer,
	geom geometry
);
DROP TABLE IF EXISTS raster_clip_out;
CREATE TABLE raster_clip_out (
	tid integer,
	rid integer,
	gid integer,
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
		rast := ST_MakeEmptyRaster(width, height, ul_x, ul_y, 1, -1, skew_x, skew_y, 0);
		rast := ST_AddBand(rast, 1, '8BUI', initvalue, nodataval);

		INSERT INTO raster_clip VALUES (rid, rast);

		RETURN;
	END;
	$$ LANGUAGE 'plpgsql';

-- Define three rasters
-- The first one 1 band with a novalue defined and one pixel set to nodata value
SELECT make_test_raster(1, 4, 4, 0, 0, 0, 0, 1, 0);
UPDATE raster_clip SET rast = ST_SetValue(rast, 3, 2, NULL) WHERE rid = 1;

-- The second one 3 bands with a novalue defined for the first two band but not set for the third band and one pixel set to nodata value in every band
SELECT make_test_raster(2, 4, 4, 0, 0, 0, 0, 10, 0);
UPDATE raster_clip SET rast = ST_SetValue(rast, 3, 2, NULL) WHERE rid = 2;
UPDATE raster_clip SET rast = ST_AddBand(rast, '8BUI'::text, 2, 0) WHERE rid = 2;
UPDATE raster_clip SET rast = ST_SetValue(rast, 2, 3, 2, NULL) WHERE rid = 2;
UPDATE raster_clip SET rast = ST_AddBand(rast, '8BUI'::text, 3, NULL) WHERE rid = 2;

-- The third one 1 band skewed 40 degree, (Can't test this yet as ST_AsRaster() still produces badly aligned raster. See ticket #1574)
--SELECT make_test_raster(3, 4, 4, 0, 0, 0, 0, 1, 0);
--UPDATE raster_clip SET rast = ST_SetSkew(rast, -0.15, -0.15) WHERE rid = 3;
--UPDATE raster_clip SET rast = ST_SetValue(rast, 3, 2, NULL) WHERE rid = 3;

-- Add a first polygon small and outside the extent of the raster
INSERT INTO geom_clip VALUES (1, ST_Buffer(ST_SetSRID(ST_MakePoint(-1, 1), 0), 0.2));
-- Add a second polygon small, inside the extent of the raster but in the nodata value pixel
INSERT INTO geom_clip VALUES (2, ST_Buffer(ST_SetSRID(ST_MakePoint(2.5, -1.5), 0), 0.2));
-- Add a second polygon small but inside the extent of the raster
INSERT INTO geom_clip VALUES (3, ST_Buffer(ST_SetSRID(ST_MakePoint(1.5, -1.5), 0), 0.2));
-- Add a third polygon big cutting the raster
INSERT INTO geom_clip VALUES (4, ST_Buffer(ST_SetSRID(ST_MakePoint(4, -2.5), 0), 2.8));
-- Add a fourth polygon englobing the two first rasters
INSERT INTO geom_clip VALUES (5, ST_Buffer(ST_SetSRID(ST_MakePoint(2, -2), 0), 3));

DROP FUNCTION make_test_raster(integer, integer, integer, double precision, double precision, double precision, double precision, double precision, double precision);

-- Test 1 without trimming, without defining a nodata value
INSERT INTO raster_clip_out
SELECT 1, rid, gid, ST_Clip(rast, geom, false)
FROM raster_clip, geom_clip;

-- Test 2 with trimming, without defining a nodata value
INSERT INTO raster_clip_out
SELECT 2, rid, gid, ST_Clip(rast, geom, true)
FROM raster_clip, geom_clip;

-- Test 3 without trimming, defining a nodata value
INSERT INTO raster_clip_out
SELECT 3, rid, gid, ST_Clip(rast, geom, ARRAY[255, 254, 253], false)
FROM raster_clip, geom_clip;

-- Test 4 with trimming, defining a nodata value
INSERT INTO raster_clip_out
SELECT 4, rid, gid, ST_Clip(rast, geom, ARRAY[255, 254, 253], true)
FROM raster_clip, geom_clip;

-- Display the metadata of the resulting rasters
SELECT
	tid,
	rid,
	gid,
	round(upperleftx::numeric, 3) AS upperleftx,
	round(upperlefty::numeric, 3) AS upperlefty,
	width,
	height,
	round(scalex::numeric, 3) AS scalex,
	round(scaley::numeric, 3) AS scaley,
	round(skewx::numeric, 3) AS skewx,
	round(skewy::numeric, 3) AS skewy,
	srid,
	numbands,
	pixeltype,
	round(nodatavalue::numeric, 3) AS nodatavalue
FROM (
	SELECT  tid,
	        rid,
		gid,
		(ST_Metadata(rast)).*,
		(ST_BandMetadata(rast, 1)).*
	FROM raster_clip_out
) AS r;

-- Display the pixels and the values of the resulting rasters (raster 1)
SELECT
	tid,
	rid,
	gid,
	(gvxy).x,
	(gvxy).y,
	(gvxy).val,
	ST_AsText((gvxy).geom) geom
FROM (SELECT tid, rid, gid, ST_PixelAsPolygons(rast) gvxy
      FROM raster_clip_out
      WHERE rid = 1
) foo
ORDER BY 1, 2, 3, 4, 5, 7;

-- Display the pixels and the values of the resulting rasters (raster 2, 3 bands)
SELECT
	tid,
	rid,
	gid,
	band,
	(gvxy).x,
	(gvxy).y,
	(gvxy).val,
	ST_AsText((gvxy).geom) geom
FROM (SELECT tid, rid, gid, band, ST_PixelAsPolygons(rast, band) gvxy
      FROM raster_clip_out, generate_series(1, 3) band
      WHERE rid = 2
) foo
ORDER BY 1, 2, 3, 4, 5, 6, 8;

DROP TABLE IF EXISTS geom_clip;
DROP TABLE IF EXISTS raster_clip;
DROP TABLE IF EXISTS raster_clip_out;
