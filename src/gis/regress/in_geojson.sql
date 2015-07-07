-- FromGeoJSON
select 'geomfromgeojson_01',st_asewkt(st_geomfromgeojson(st_asgeojson('SRID=3005;MULTIPOINT(1 1, 1 1)')));
select 'geomfromgeojson_02',st_astext(st_geomfromgeojson(st_asgeojson('SRID=3005;MULTIPOINT(1 1, 1 1)')));
select 'geomfromgeojson_03',st_astext(st_geomfromgeojson(st_asgeojson('POINT(1 1)')));
select 'geomfromgeojson_04',st_astext(st_geomfromgeojson(st_asgeojson('LINESTRING(0 0,1 1)')));
select 'geomfromgeojson_05',st_astext(st_geomfromgeojson(st_asgeojson('POLYGON((0 0,1 1,1 0,0 0))')));
select 'geomfromgeojson_06',st_astext(st_geomfromgeojson(st_asgeojson('MULTIPOLYGON(((0 0,1 1,1 0,0 0)))')));

-- #1434
select '#1434: Next two errors';
select '#1434.1',ST_GeomFromGeoJSON('{ "type": "Point", "crashme": [100.0, 0.0] }');
select '#1434.2',ST_GeomFromGeoJSON('crashme');;
select '#1434.3',ST_GeomFromGeoJSON('');
select '#1434.4',ST_GeomFromGeoJSON('{}');
select '#1434.5',ST_GeomFromGeoJSON('{"type":"Point","coordinates":[]}'); 
select '#1434.6',ST_GeomFromGeoJSON('{"type":"MultiPoint","coordinates":[[]]}'); 
select '#1434.7',ST_GeomFromGeoJSON('{"type":"MultiPoint"}'); 
select '#1434.8',ST_GeomFromGeoJSON('{"type":"Point"}'); 

-- #2130 --
SELECT '#2130', ST_NPoints(ST_GeomFromGeoJSON('{"type":"MultiPolygon","coordinates":[[[[-117,32],[-117,32],[-117,32],[-117,32],[-117,32],[-117,32],[-117,32],[-117,32],[-117,32],[-117,32],[-117,32],[-117,32],[-117,32],[-117,33],[-117,33],[-117,33],[-117,33],[-117,33],[-117,33],[-117,33],[-117,33],[-117,33],[-117,33],[-117,33],[-117,33],[-117,32],[-117,32],[-117,32],[-117,32],[-116,32],[-116,32],[-116,32],[-116,32],[-116,32],[-116,32],[-116,32],[-116,32],[-116,32],[-116,32],[-117,32],[-117,32],[-117,32],[-117,32]],[[-117,33],[-117,33],[-117,33],[-117,33],[-117,33],[-117,32],[-117,33]]]]}'));

-- #2216 --
SELECT '#2216', ST_NPoints(ST_GeomFromGeoJSON('{"type":"MultiPolygon","coordinates":[[[[4,0],[0,-4],[-4,0],[0,4],[4,0]],[[2,0],[0,2],[-2,0],[0,-2],[2,0]]],[[[24,0],[20,-4],[16,0],[20,4],[24,0]],[[22,0],[20,2],[18,0],[20,-2],[22,0]]],[[[44,0],[40,-4],[36,0],[40,4],[44,0]],[[42,0],[40,2],[38,0],[40,-2],[42,0]]]]}'));

-- #2619 --
SELECT '#2619', ST_AsText(ST_GeomFromGeoJSON('{"type":"Polygon","bbox":[1,5,2,6],"coordinates":[]}'));
select '#2619', ST_AsText(ST_GeomFromGeoJSON('{"type":"MultiPolygon","coordinates":[[]]}'));

-- #2697 --
SELECT '#2697', ST_GeomFromGeoJSON('{"type":"Polygon","coordinates":[1]}');
SELECT '#2697', ST_GeomFromGeoJSON('{"type":"Polygon","coordinates":[1,1]}');

-- FromGeoJSON 3D

SELECT 'geomfromgeojson_z_01', ST_AsEWKT(ST_GeomFromGeoJSON('{"type":"Point","coordinates":[1,2,3]}'));
SELECT 'geomfromgeojson_z_02', ST_AsEWKT(ST_GeomFromGeoJSON('{"type":"LineString","coordinates":[[1,2,3],[2,3,4]]}'));

-- FromGeoJSON 4D

SELECT 'geomfromgeojson_zm_01', ST_AsEWKT(ST_GeomFromGeoJSON('{"type":"Point","coordinates":[1,2,3,4]}'));
SELECT 'geomfromgeojson_zm_02', ST_AsEWKT(ST_GeomFromGeoJSON('{"type":"LineString","coordinates":[[1,2,3,4],[2,3,4,5]]}'));

