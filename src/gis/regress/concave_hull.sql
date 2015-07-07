-- Tests to confirm the concave hull area is <= convex hull and 
-- covers the original geometry (can't use covers because always gives topo errors with 3.3
SELECT 
	'ST_ConcaveHull MultiPolygon 0.95', ST_Area(ST_Intersection(geom,ST_ConcaveHull(
		geom, 0.95) )) = ST_Area(geom) As encloses_geom, 
		(ST_Area(ST_ConvexHull(geom)) 
		- ST_Area(ST_ConcaveHull(geom, 0.95))) < (0.95 * ST_Area(ST_ConvexHull(geom) ) ) As reached_target
FROM ST_Union(ST_GeomFromText('POLYGON((175 150, 20 40, 
			50 60, 125 100, 175 150))'),
		ST_Buffer(ST_GeomFromText('POINT(110 170)'), 20)
		) As geom;
		
SELECT 
	'ST_ConcaveHull Lines 0.80', ST_Intersection(geom,ST_ConcaveHull(
		geom, 0.80) ) = geom As encloses_geom, 
		(ST_Area(ST_ConvexHull(geom)) 
		- ST_Area(ST_ConcaveHull(geom, 0.80))) < (0.80 * ST_Area(ST_ConvexHull(geom) ) ) As reached_target

FROM ST_GeomFromText('MULTILINESTRING((106 164,30 112,74 70,82 112,130 94,
	130 62,122 40,156 32,162 76,172 88),
(132 178,134 148,128 136,96 128,132 108,150 130,
170 142,174 110,156 96,158 90,158 88),
(22 64,66 28,94 38,94 68,114 76,112 30,
132 10,168 18,178 34,186 52,184 74,190 100,
190 122,182 148,178 170,176 184,156 164,146 178,
132 186,92 182,56 158,36 150,62 150,76 128,88 118))') As geom;

-- test holes vs. no holes - holes should still enclose but have smaller area than no holes --
SELECT 
	'ST_ConcaveHull Lines 0.80 holes', ST_Intersection(geom,ST_ConcaveHull(
		geom, 0.80, true) ) = geom As encloses_geom, 
		ST_Area(ST_ConcaveHull(geom, 0.80, true)) < ST_Area(ST_ConcaveHull(geom, 0.80)) As reached_target

FROM ST_GeomFromText('MULTILINESTRING((106 164,30 112,74 70,82 112,130 94,
	130 62,122 40,156 32,162 76,172 88),
(132 178,134 148,128 136,96 128,132 108,150 130,
170 142,174 110,156 96,158 90,158 88),
(22 64,66 28,94 38,94 68,114 76,112 30,
132 10,168 18,178 34,186 52,184 74,190 100,
190 122,182 148,178 170,176 184,156 164,146 178,
132 186,92 182,56 158,36 150,62 150,76 128,88 118))') As geom;
