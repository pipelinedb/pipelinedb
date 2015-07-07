WITH foo AS (
	SELECT postgis_raster_lib_version()
)
SELECT NULL FROM foo;
SET postgis.enable_outdb_rasters = True;
SET postgis.gdal_enabled_drivers = 'GTiff PNG JPEG';

DO $$
DECLARE
	_srid int;
BEGIN
	SELECT srid INTO _srid FROM spatial_ref_sys WHERE srid = 4326;

	IF _srid IS NULL THEN
		INSERT INTO "spatial_ref_sys" ("srid","auth_name","auth_srid","srtext","proj4text") VALUES (4326,'EPSG',4326,'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]','+proj=longlat +datum=WGS84 +no_defs ');
	END IF;

	SELECT srid INTO _srid FROM spatial_ref_sys WHERE srid = 3857;
	IF _srid IS NULL THEN
	 	INSERT INTO "spatial_ref_sys" ("srid","auth_name","auth_srid","srtext","proj4text") VALUES (3857,'EPSG',3857,'PROJCS["WGS 84 / Pseudo-Mercator",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],PROJECTION["Mercator_1SP"],PARAMETER["central_meridian",0],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],EXTENSION["PROJ4","+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext  +no_defs"],AUTHORITY["EPSG","3857"],AXIS["X",EAST],AXIS["Y",NORTH]]','+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext  +no_defs');
	END IF;

END;
$$;

SELECT short_name FROM st_gdaldrivers() WHERE upper(short_name) = 'VRT';

-- make sure raster transforms still work
-- warp API requires VRT support
WITH foo AS (
	SELECT st_asraster(st_buffer('SRID=3857;POINT(0 0)'::geometry, 8), 0.5, 0.5) AS rast
),
transformed AS (
		SELECT st_transform(rast, 4326) AS rast FROM foo
)
SELECT ST_SRID(rast) FROM transformed;
SELECT short_name FROM st_gdaldrivers() WHERE upper(short_name) = 'VRT';

-- no VRT permitted by default
SELECT convert_from(ST_AsGDALRaster(ST_FromGDALRaster('<VRTDataset rasterXSize="1000"
rasterYSize="1"><VRTRasterBand band="1"
subClass="VRTRawRasterBand"><SourceFilename>/etc/passwd</SourceFilename></VRTRasterBand></VRTDataset>'::bytea),
'EHDR')::bytea, 'LATIN1');
SELECT short_name FROM st_gdaldrivers() WHERE upper(short_name) = 'VRT';

CREATE TEMP TABLE raster_test AS
	SELECT
		'passwd' AS lbl,
		'0100000100000000000000F03F000000000000F0BF0000000000000000000000000000000000000000000000000000000000000000110F0000E80301008400003C56525444617461736574207261737465725853697A653D223130303022207261737465725953697A653D2231223E3C56525452617374657242616E642062616E643D22312220737562436C6173733D2256525452617752617374657242616E64223E3C536F7572636546696C656E616D653E2F6574632F7061737377643C2F536F7572636546696C656E616D653E3C2F56525452617374657242616E643E3C2F565254446174617365743E00'::raster AS r
	UNION ALL
	SELECT 
		'inline' AS lbl,
		'0100000100000000000000F03F000000000000F03F00000000000020C000000000000020C000000000000000000000000000000000110F000010001000440000000000000101010101010000000000000000010101010101010101010000000000010101010101010101010101000000010101010101010101010101010100000101010101010101010101010101000101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010001010101010101010101010101010000010101010101010101010101010100000001010101010101010101010100000000000101010101010101010100000000000000000101010101010000000000'::raster AS r
;

SELECT count(convert_from(ST_AsGDALRaster(r, 'EHDR')::bytea, 'LATIN1')) as passwd from raster_test where lbl = 'passwd';
SELECT short_name FROM st_gdaldrivers() WHERE upper(short_name) = 'VRT';

SELECT count(convert_from(ST_AsGDALRaster(r, 'EHDR')::bytea, 'LATIN1')) as passwd from raster_test where lbl = 'passwd';
SELECT short_name FROM st_gdaldrivers() WHERE upper(short_name) = 'VRT';

SELECT st_srid(st_transform(r, 4326)) from raster_test where lbl = 'inline';
SELECT short_name FROM st_gdaldrivers() WHERE upper(short_name) = 'VRT';

SELECT st_srid(st_transform(r, 4326)) from raster_test where lbl = 'inline';
SELECT short_name FROM st_gdaldrivers() WHERE upper(short_name) = 'VRT';

SELECT count(convert_from(ST_AsGDALRaster(r, 'EHDR')::bytea, 'LATIN1')) as passwd from raster_test where lbl = 'passwd';
SELECT short_name FROM st_gdaldrivers() WHERE upper(short_name) = 'VRT';

SELECT count(convert_from(ST_AsGDALRaster(r, 'EHDR')::bytea, 'LATIN1')) as passwd from raster_test where lbl = 'passwd';
SELECT short_name FROM st_gdaldrivers() WHERE upper(short_name) = 'VRT';

SELECT st_srid(st_transform(r, 4326)) from raster_test where lbl = 'inline';
SELECT short_name FROM st_gdaldrivers() WHERE upper(short_name) = 'VRT';
