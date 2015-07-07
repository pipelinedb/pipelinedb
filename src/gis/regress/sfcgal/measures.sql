SET postgis.backend = 'sfcgal';

select '113', ST_area2d('MULTIPOLYGON( ((0 0, 10 0, 10 10, 0 10, 0 0)),( (0 0, 10 0, 10 10, 0 10, 0 0),(5 5, 7 5, 7 7 , 5 7, 5 5) ) ,( (0 0, 10 0, 10 10, 0 10, 0 0),(5 5, 7 5, 7 7, 5 7, 5 5),(1 1,2 1, 2 2, 1 2, 1 1) ) )'::GEOMETRY) as value;

select '114', ST_perimeter2d('MULTIPOLYGON( ((0 0, 10 0, 10 10, 0 10, 0 0)),( (0 0, 10 0, 10 10, 0 10, 0 0),(5 5, 7 5, 7 7 , 5 7, 5 5) ) ,( (0 0, 10 0, 10 10, 0 10, 0 0),(5 5, 7 5, 7 7, 5 7, 5 5),(1 1,2 1, 2 2, 1 2, 1 1) ) )'::GEOMETRY) as value;

select '115', ST_3DPerimeter('MULTIPOLYGON( ((0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0)),( (0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0),(5 5 0, 7 5 0, 7 7  0, 5 7 0, 5 5 0) ) ,( (0 0 1, 10 0 1, 10 10 1, 0 10 1, 0 0 1),(5 5 1, 7 5 1, 7 7 1, 5 7 1, 5 5 1),(1 1 1,2 1 1, 2 2 1, 1 2 1, 1 1 1) ) )'::GEOMETRY) as value;


select '116', ST_length2d('MULTILINESTRING((0 0, 1 1),(0 0, 1 1, 2 2) )'::GEOMETRY) as value;
select '117', ST_3dlength('MULTILINESTRING((0 0, 1 1),(0 0, 1 1, 2 2) )'::GEOMETRY) as value;
select '118', ST_3dlength('MULTILINESTRING((0 0 0, 1 1 1),(0 0 0, 1 1 1, 2 2 2) )'::GEOMETRY) as value;

select '134', ST_Distance('POINT(1 2)', 'POINT(1 2)');
select '135', ST_Distance('POINT(5 0)', 'POINT(10 12)');

select '136', ST_Distance('POINT(0 0)', ST_translate('POINT(0 0)', 5, 12, 0));


-- postgis-users/2006-May/012174.html
select 'dist', ST_Distance(a,b), ST_Distance(b,a) from (
	select 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry as a,
		'POLYGON((11 0, 11 10, 20 10, 20 0, 11 0),
			(15 5, 17 5, 17 8, 15 8, 15 5))'::geometry as b
	) as foo;

--#1502
SELECT '#1502', ST_Dwithin(a,b,0.0)  from 
(SELECT 'LINESTRING(-97364 -97364, 9736.4 9736.4)'::geometry a, 'POINT(0 0)'::geometry b ) foo;

--st_shortestline

select 'st_shortestline_134', st_astext(st_shortestline('POINT(1 2)', 'POINT(1 2)'));
select 'st_shortestline_135', st_astext(st_shortestline('POINT(5 0)', 'POINT(10 12)'));

select 'st_shortestline_136', st_astext(st_shortestline('POINT(0 0)', ST_translate('POINT(0 0)', 5, 12, 0)));

-- postgis-users/2006-May/012174.html
select 'st_shortestline_dist', st_astext(st_shortestline(a,b)), st_astext(st_shortestline(b,a)) from (
	select 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry as a,
		'POLYGON((11 0, 12 10, 20 10, 20 0, 11 0),
			(15 5, 15 8, 17 8, 17 5, 15 5))'::geometry as b
	) as foo;


--st_maxdistance

select 'st_maxdistance_134', st_maxdistance('POINT(1 2)', 'POINT(1 2)');
select 'st_maxdistance_135', st_maxdistance('POINT(5 0)', 'POINT(10 12)');

select 'st_maxdistance_136', st_maxdistance('POINT(0 0)', ST_translate('POINT(0 0)', 5, 12, 0));

-- postgis-users/2006-May/012174.html
select 'st_maxdistance_dist', st_maxdistance(a,b), st_maxdistance(b,a) from (
	select 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry as a,
		'POLYGON((11 0, 11 10, 20 10, 20 0, 11 0),
			(15 5, 15 8, 17 8, 17 5, 15 5))'::geometry as b
	) as foo;



--st_longestline

select 'st_longestline_134', st_astext(st_longestline('POINT(1 2)', 'POINT(1 2)'));
select 'st_longestline_135', st_astext(st_longestline('POINT(5 0)', 'POINT(10 12)'));

select 'st_longestline_136', st_astext(st_longestline('POINT(0 0)', ST_translate('POINT(0 0)', 5, 12, 0)));

-- postgis-users/2006-May/012174.html
select 'st_longestline_dist', st_astext(st_longestline(a,b)), st_astext(st_longestline(b,a)) from (
	select 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry as a,
		'POLYGON((11 0, 11 10, 20 10, 20 0, 11 0),
			(15 5, 15 8, 17 8, 17 5, 15 5))'::geometry as b
	) as foo;

select 'distancetest1',		
	ST_Distance(a, b),
	st_maxdistance(a, b),
	st_astext(st_shortestline(a,b)),
	st_astext(st_shortestline(b,a)),
	st_astext(st_longestline(a,b)),
	st_astext(st_longestline(b,a)) from (
select 
	ST_GeomFromText('MULTILINESTRING((17 16, 16 17, 17 18, 17 17, 17 16), (28 35,29 39, 30 35))') as a,
	ST_GeomFromText('MULTIPOLYGON(((-1 -1, -1 25, 25 25, 25 -1, -1 -1), (14 14,19 14,19 19,14 19,14 14)),((33 35,35 35,35 40,33 40,33 35)))') as b
) as foo;

select  'distancetest2',
	ST_Distance(a, b),
	st_maxdistance(a, b),
	round(st_x(st_startpoint(st_shortestline(a,b)))::numeric, 10),
	round(st_y(st_startpoint(st_shortestline(a,b)))::numeric, 10),
	round(st_x(st_endpoint(st_shortestline(a,b)))::numeric, 10),
	round(st_y(st_endpoint(st_shortestline(a,b)))::numeric, 10),	
	st_astext(st_longestline(a,b)),
	st_astext(st_longestline(b,a)) from (
select 
	ST_GeomFromText('LINESTRING(-40 -20 , 4 2)') as a,
	ST_GeomFromText('LINESTRING(-10 20, 1 -2)') as b
) as foo;

select 'distancepoly1',		
	ST_Distance(a, b),
	st_maxdistance(a, b),
	st_astext(st_shortestline(a,b)),
	st_astext(st_shortestline(b,a)),
	st_astext(st_longestline(a,b)),
	st_astext(st_longestline(b,a)) from (
select 
	ST_GeomFromText('MULTIPOLYGON(((17 16,16 17,17 18,17 17,17 16)), ((28 35,30 35,29 39,28 35)))') as a,
	ST_GeomFromText('MULTIPOLYGON(((-1 -1, -1 25, 25 25, 25 -1, -1 -1), (14 14,19 14,19 19,14 19,14 14)),((33 35,33 40, 35 40, 35 35, 33 35)))') as b
) as foo;

select 'distancepoly2',		
		ST_Distance(a, b),
			st_maxdistance(a, b),
				st_astext(st_shortestline(a,b)),
					st_astext(st_shortestline(b,a)),
						st_astext(st_longestline(a,b)),
							st_astext(st_longestline(b,a)) from (
	select ST_GeomFromText('POLYGON((17 14, 16 17, 17 18, 17 17, 17 14))') as a,
			ST_GeomFromText('POLYGON((-1 -1, -1 25, 25 25, 25 -1, -1 -1), (14 14,19 14,19 19,14 19,14 14))') as b
	) as foo;



select 'distancepoly3',		
		ST_Distance(a, b),
			st_maxdistance(a, b),
				st_astext(st_shortestline(a,b)),
					st_astext(st_shortestline(b,a)),
						st_astext(st_longestline(a,b)),
							st_astext(st_longestline(b,a)) from (
	select ST_GeomFromText('POLYGON((17 16, 16 17, 17 19, 17 17, 17 16))') as a,
			ST_GeomFromText('POLYGON((-1 -1,-1 25, 25 25,25 -1,-1 -1), (14 14,19 14,19 19,14 19,14 14))') as b
	) as foo;


select 'distancepoly4',		
		ST_Distance(a, b),
			st_maxdistance(a, b),
				st_astext(st_shortestline(a,b)),
					st_astext(st_shortestline(b,a)),
						st_astext(st_longestline(a,b)),
							st_astext(st_longestline(b,a)) from (
	select ST_GeomFromText('POLYGON((17 16, 16 17, 16 20, 18 20, 18 17, 17 16))') as a,
			ST_GeomFromText('POLYGON((-1 -1,-1 25, 25 25,25 -1,-1 -1), (14 14,19 14,19 19,14 19,14 14))') as b
	) as foo;



select 'distancepoly5',		
		ST_Distance(a, b),
			st_maxdistance(a, b),
				st_astext(st_shortestline(a,b)),
					st_astext(st_shortestline(b,a)),
						st_astext(st_longestline(a,b)),
							st_astext(st_longestline(b,a)) from (
	select ST_GeomFromText('POLYGON((17 12, 16 17, 17 18, 17 17, 17 12))') as a,
			ST_GeomFromText('POLYGON((-1 -1,-1 25, 25 25,25 -1,-1 -1), (14 14,19 14,19 19,14 19,14 14))') as b
	) as foo;




select 'distancepoly6',		
		ST_Distance(a, b),
			st_maxdistance(a, b),
				st_astext(st_shortestline(a,b)),
					st_astext(st_shortestline(b,a)),
						st_astext(st_longestline(a,b)),
							st_astext(st_longestline(b,a)) from (
	select ST_GeomFromText('POLYGON((2 2, 2 3, 3 3, 3 2, 2 2))') as a,
			ST_GeomFromText('POLYGON((-1 -1,-1 25, 25 25,25 -1,-1 -1), (14 14,19 14,19 19,14 19,14 14))') as b
	) as foo;

--3D Distance functions


SELECT '3dDistancetest1',
	ST_3DDistance(a,b),
		ST_3DMaxDistance(a,b),
			ST_3DDWithin(a,b,5),
				ST_3DDFullyWithin(a,b,5),
					ST_ASEWKT(ST_3DShortestline(a,b)),
						ST_ASEWKT(ST_3DClosestpoint(a,b)),
							ST_ASEWKT(ST_3DLongestline(a,b)) FROM (
	SELECT 'POINT(1 1 1)'::geometry as a, 'POINT(3 2 7)'::geometry as b
	) as foo;
	
	
SELECT '3dDistancetest2',
	ST_3DDistance(a,b),
		ST_3DMaxDistance(a,b),
			ST_3DDWithin(a,b,5),
				ST_3DDFullyWithin(a,b,5),
					ST_ASEWKT(ST_3DShortestline(a,b)),
						ST_ASEWKT(ST_3DClosestpoint(a,b)),
							ST_ASEWKT(ST_3DLongestline(a,b)) FROM (
	SELECT 'POINT(1 1 1)'::geometry as a, 'LINESTRING(0 0 0, 2 2 2)'::geometry as b
	) as foo;
	
	
SELECT '3dDistancetest3',
	ST_3DDistance(a,b),
		ST_3DMaxDistance(a,b),
			ST_3DDWithin(a,b,5),
				ST_3DDFullyWithin(a,b,5),
					ST_ASEWKT(ST_SnapToGrid(ST_3DShortestline(a,b), 1e-14)),
						ST_ASEWKT(ST_3DClosestpoint(a,b)),
							ST_ASEWKT(ST_3DLongestline(a,b)) FROM (
	SELECT 'POINT(1 1 1)'::geometry as a, 'LINESTRING(5 2 6, -3 -2 4)'::geometry as b
	) as foo;
	
	
SELECT '3dDistancetest4',
	ST_3DDistance(a,b),
		ST_3DMaxDistance(a,b),
			ST_3DDWithin(a,b,5),
				ST_3DDFullyWithin(a,b,5),
					ST_ASEWKT(ST_3DShortestline(a,b)),
						ST_ASEWKT(ST_3DClosestpoint(a,b)),
							ST_ASEWKT(ST_3DLongestline(a,b)) FROM (
	SELECT  'LINESTRING(1 1 3, 5 7 8)'::geometry  as a, 'POINT(1 1 1)'::geometry as b
	) as foo;
	
	SELECT '3dDistancetest5',
	ST_3DDistance(a,b),
		ST_3DMaxDistance(a,b),
			ST_3DDWithin(a,b,5),
				ST_3DDFullyWithin(a,b,5),
					ST_ASEWKT(ST_3DShortestline(a,b)),
						ST_ASEWKT(ST_3DClosestpoint(a,b)),
							ST_ASEWKT(ST_3DLongestline(a,b)) FROM (
	SELECT  'LINESTRING(1 0 5, 11 0 5)'::geometry  as a, 'LINESTRING(5 2 0, 5 2 10, 5 0 13)'::geometry as b
	) as foo;

SELECT '3dDistancetest6',
	ST_3DDistance(a,b) FROM (
	SELECT 'LINESTRING(1 1 1 , 2 2 2)'::geometry as a, 'POLYGON((0 0 0, 2 2 2, 3 3 0, 0 0 0))'::geometry as b) as foo;	

-- Area of an empty polygon
select 'emptyPolyArea', st_area('POLYGON EMPTY');

-- Area of an empty linestring
select 'emptyLineArea', st_area('LINESTRING EMPTY');

-- Area of an empty point
select 'emptyPointArea', st_area('POINT EMPTY');

-- Area of an empty multipolygon
select 'emptyMultiPolyArea', st_area('MULTIPOLYGON EMPTY');

-- Area of an empty multilinestring
select 'emptyMultiLineArea', st_area('MULTILINESTRING EMPTY');

-- Area of an empty multilipoint
select 'emptyMultiPointArea', st_area('MULTIPOINT EMPTY');

-- Area of an empty collection
select 'emptyCollectionArea', st_area('GEOMETRYCOLLECTION EMPTY');

-- 
select 'spheroidLength1', round(st_length_spheroid('MULTILINESTRING((-118.584 38.374,-118.583 38.5),(-71.05957 42.3589 , -71.061 43))'::geometry,'SPHEROID["GRS_1980",6378137,298.257222101]'::spheroid)::numeric,5);
