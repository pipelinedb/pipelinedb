--
-- These tests serve the purpose of ensuring compatibility with 
-- old versions of postgis users.
--
-- Their use rely on loading the legacy.sql script.
-- This file also serves as a testcase for uninstall_legacy.sql
--

SET client_min_messages TO WARNING;

\cd :scriptdir
\i legacy.sql

TRUNCATE spatial_ref_sys;
INSERT INTO "spatial_ref_sys" ("srid","auth_name","auth_srid","srtext","proj4text") VALUES (4326,'EPSG',4326,'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.01745329251994328,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]','+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs ');

SELECT 'Starting up MapServer/Geoserver tests...';
-- Set up the data table
SELECT 'Setting up the data table...';
CREATE TABLE wmstest ( id INTEGER );
SELECT AddGeometryColumn( 'wmstest', 'pt', 4326, 'POLYGON', 2 );
INSERT INTO wmstest SELECT lon * 100 + lat AS id, st_setsrid(st_buffer(st_makepoint(lon, lat),1.0),4326) AS pt
FROM (select lon, generate_series(-80,80, 5) AS lat FROM (SELECT generate_series(-175, 175, 5) AS lon) AS sq1) AS sq2;
ALTER TABLE wmstest add PRIMARY KEY ( id );
CREATE INDEX wmstest_geomidx ON wmstest using gist ( pt );

-- Geoserver 2.0 NG tests
SELECT 'Running Geoserver 2.0 NG tests...';
-- Run a Geoserver 2.0 NG metadata query
SELECT 'Geoserver1', TYPE FROM GEOMETRY_COLUMNS WHERE F_TABLE_SCHEMA = 'public' AND F_TABLE_NAME = 'wmstest' AND F_GEOMETRY_COLUMN = 'pt';
SELECT 'Geoserver2', SRID FROM GEOMETRY_COLUMNS WHERE F_TABLE_SCHEMA = 'public' AND F_TABLE_NAME = 'wmstest' AND F_GEOMETRY_COLUMN = 'pt';
-- Run a Geoserver 2.0 NG WMS query
SELECT 'Geoserver3', "id",substr(encode(asBinary(force_2d("pt"),'XDR'),'base64'),0,16) as "pt" FROM "public"."wmstest" WHERE "pt" && GeomFromText('POLYGON ((-6.58216065979069 -0.7685569763184591, -6.58216065979069 0.911225433349509, -3.050569931030911 0.911225433349509, -3.050569931030911 -0.7685569763184591, -6.58216065979069 -0.7685569763184591))', 4326);
-- Run a Geoserver 2.0 NG KML query
SELECT 'Geoserver4', count(*) FROM "public"."wmstest" WHERE "pt" && GeomFromText('POLYGON ((-1.504017942347938 24.0332272532341, -1.504017942347938 25.99364254836741, 1.736833353559741 25.99364254836741, 1.736833353559741 24.0332272532341, -1.504017942347938 24.0332272532341))', 4326);
SELECT 'Geoserver5', "id",substr(encode(asBinary(force_2d("pt"),'XDR'),'base64'),0,16) as "pt" FROM "public"."wmstest" WHERE "pt" && GeomFromText('POLYGON ((-1.504017942347938 24.0332272532341, -1.504017942347938 25.99364254836741, 1.736833353559741 25.99364254836741, 1.736833353559741 24.0332272532341, -1.504017942347938 24.0332272532341))', 4326);
SELECT 'Geoserver6', "id",substr(encode(asBinary(force_2d("pt"),'XDR'),'base64'),0,16) as "pt" FROM "public"."wmstest" WHERE "pt" && GeomFromText('POLYGON ((-1.507182836191598 24.031312785172446, -1.507182836191598 25.995557016429064, 1.7399982474034008 25.995557016429064, 1.7399982474034008 24.031312785172446, -1.507182836191598 24.031312785172446))', 4326);

-- MapServer 5.4 tests
select 'MapServer1', attname from pg_attribute, pg_constraint, pg_class where pg_constraint.conrelid = pg_class.oid and pg_class.oid = pg_attribute.attrelid and pg_constraint.contype = 'p' and pg_constraint.conkey[1] = pg_attribute.attnum and pg_class.relname = 'wmstest' and pg_table_is_visible(pg_class.oid) and pg_constraint.conkey[2] is null;
select 'MapServer2', "id",substr(encode(AsBinary(force_collection(force_2d("pt")),'NDR'),'base64'),0,16) as geom,"id" from wmstest where pt && GeomFromText('POLYGON((-98.5 32,-98.5 39,-91.5 39,-91.5 32,-98.5 32))',find_srid('','wmstest','pt'));

-- MapServer 5.6 tests
select * from wmstest where false limit 0;
select 'MapServer3', attname from pg_attribute, pg_constraint, pg_class where pg_constraint.conrelid = pg_class.oid and pg_class.oid = pg_attribute.attrelid and pg_constraint.contype = 'p' and pg_constraint.conkey[1] = pg_attribute.attnum and pg_class.relname = 'wmstest' and pg_table_is_visible(pg_class.oid) and pg_constraint.conkey[2] is null;
select 'MapServer4', "id",substr(encode(AsBinary(force_collection(force_2d("pt")),'NDR'),'hex'),0,16) as geom,"id" from wmstest where pt && GeomFromText('POLYGON((-98.5 32,-98.5 39,-91.5 39,-91.5 32,-98.5 32))',find_srid('','wmstest','pt'));

-- Drop the data table
SELECT 'Removing the data table...';
DROP TABLE wmstest;
DELETE FROM geometry_columns WHERE f_table_name = 'wmstest' AND f_table_schema = 'public';
SELECT 'Done.';

-- test #1869 ST_AsBinary is not unique --
SELECT 1869 As ticket_id, ST_AsText(ST_AsBinary('POINT(1 2)'));

DELETE FROM spatial_ref_sys WHERE SRID = '4326';

\i uninstall_legacy.sql
