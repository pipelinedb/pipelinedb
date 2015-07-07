-- FromGeoHash
SELECT 'box2dfromgeohash_01', ST_Box2dFromGeoHash('9qqj7nmxncgyy4d0dbxqz0');
SELECT 'box2dfromgeohash_02', ST_Box2dFromGeoHash('9qqj7nmxncgyy4d0dbxqz0', 0);
SELECT 'box2dfromgeohash_03', ST_Box2dFromGeoHash('9qqj7nmxncgyy4d0dbxqz0', -1);
SELECT 'box2dfromgeohash_04', ST_Box2dFromGeoHash('9qqj7nmxncgyy4d0dbxqz0', 30);

SELECT 'geomfromgeohash_01', ST_AsText(ST_GeomFromGeoHash('9qqj7nmxncgyy4d0dbxqz0'));
SELECT 'geomfromgeohash_02', ST_AsText(ST_GeomFromGeoHash('9qqj7nmxncgyy4d0dbxqz0', 0));
SELECT 'geomfromgeohash_03', ST_AsText(ST_GeomFromGeoHash('9qqj7nmxncgyy4d0dbxqz0', -1));
SELECT 'geomfromgeohash_04', ST_AsText(ST_GeomFromGeoHash('9qqj7nmxncgyy4d0dbxqz0', 30));

SELECT 'pointfromgeohash_01', ST_AsText(ST_PointFromGeoHash('9qqj7nmxncgyy4d0dbxqz0'));
SELECT 'pointfromgeohash_02', ST_AsText(ST_PointFromGeoHash('9qqj7nmxncgyy4d0dbxqz0', 0));
SELECT 'pointfromgeohash_03', ST_AsText(ST_PointFromGeoHash('9qqj7nmxncgyy4d0dbxqz0', -1));
SELECT 'pointfromgeohash_04', ST_AsText(ST_PointFromGeoHash('9qqj7nmxncgyy4d0dbxqz0', 30));
