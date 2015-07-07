SELECT 'T1', ST_Summary('POINT(0 0)'::geometry);
SELECT 'T1B', ST_Summary(postgis_addbbox('POINT(0 0)'::geometry));
SELECT 'T1S', ST_Summary('SRID=4326;POINT(0 0)'::geometry);
SELECT 'T1M', ST_Summary('POINTM(0 0 0)'::geometry);
SELECT 'T1Z', ST_Summary('POINT(0 0 0)'::geometry);
SELECT 'T1ZM', ST_Summary('POINT(0 0 0 0)'::geometry);
SELECT 'T1ZMB', ST_Summary(postgis_addbbox('POINT(0 0 0 0)'::geometry));
SELECT 'T1ZMBS', ST_Summary(postgis_addbbox(
 'SRID=4326;POINT(0 0 0 0)'::geometry));
SELECT 'T3', ST_Summary('MULTIPOINT(0 0)'::geometry);
SELECT 'T4', ST_Summary('SRID=4326;MULTIPOINT(0 0)'::geometry);
SELECT 'T5', ST_Summary('GEOMETRYCOLLECTION(
 MULTILINESTRING((0 0, 1 0),(2 0, 4 4)),MULTIPOINT(0 0)
)'::geometry);
SELECT 'T6', ST_Summary('SRID=4326;GEOMETRYCOLLECTION(
 MULTILINESTRING((0 0, 1 0),(2 0, 4 4)),MULTIPOINT(0 0)
)'::geometry);
