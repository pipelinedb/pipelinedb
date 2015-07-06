DROP TABLE IF EXISTS raster_setvalues_rast;
CREATE TABLE raster_setvalues_rast AS
	SELECT 1 AS rid, ST_AddBand(ST_MakeEmptyRaster(5, 5, 0, 0, 1, -1, 0, 0, 0), 1, '8BUI', 0, 0) AS rast
;

DROP TABLE IF EXISTS raster_setvalues_geom;
CREATE TABLE raster_setvalues_geom AS
	SELECT 1 AS gid, 'SRID=0;POINT(2.5 -2.5)'::geometry geom UNION ALL
	SELECT 2 AS gid, 'SRID=0;POLYGON((1 -1, 4 -1, 4 -4, 1 -4, 1 -1))'::geometry geom UNION ALL
	SELECT 3 AS gid, 'SRID=0;POLYGON((0 0, 5 0, 5 -1, 1 -1, 1 -4, 0 -4, 0 0))'::geometry geom UNION ALL
	SELECT 4 AS gid, 'SRID=0;MULTIPOINT(0 0, 4 4, 4 -4)'::geometry
;

SELECT
	rid, gid, ST_DumpValues(ST_SetValue(rast, 1, geom, gid))
FROM raster_setvalues_rast t1
CROSS JOIN raster_setvalues_geom t2
ORDER BY rid, gid;

SELECT
	t1.rid, t2.gid, t3.gid, ST_DumpValues(ST_SetValues(rast, 1, ARRAY[ROW(t2.geom, t2.gid), ROW(t3.geom, t3.gid)]::geomval[]))
FROM raster_setvalues_rast t1
CROSS JOIN raster_setvalues_geom t2
CROSS JOIN raster_setvalues_geom t3
WHERE t2.gid = 1
	AND t3.gid = 2
ORDER BY t1.rid, t2.gid, t3.gid;

SELECT
	t1.rid, t2.gid, t3.gid, ST_DumpValues(ST_SetValues(rast, 1, ARRAY[ROW(t3.geom, t3.gid), ROW(t2.geom, t2.gid)]::geomval[]))
FROM raster_setvalues_rast t1
CROSS JOIN raster_setvalues_geom t2
CROSS JOIN raster_setvalues_geom t3
WHERE t2.gid = 1
	AND t3.gid = 2
ORDER BY t1.rid, t2.gid, t3.gid;

SELECT
	t1.rid, t2.gid, t3.gid, ST_DumpValues(ST_SetValues(rast, 1, ARRAY[ROW(t3.geom, t3.gid), ROW(t2.geom, t2.gid)]::geomval[]))
FROM raster_setvalues_rast t1
CROSS JOIN raster_setvalues_geom t2
CROSS JOIN raster_setvalues_geom t3
WHERE t2.gid = 1
	AND t3.gid = 3
ORDER BY t1.rid, t2.gid, t3.gid;

SELECT
	t1.rid, t2.gid, t3.gid, ST_DumpValues(ST_SetValues(rast, 1, ARRAY[ROW(t3.geom, t3.gid), ROW(t2.geom, t2.gid)]::geomval[]))
FROM raster_setvalues_rast t1
CROSS JOIN raster_setvalues_geom t2
CROSS JOIN raster_setvalues_geom t3
WHERE t2.gid = 1
	AND t3.gid = 4;

WITH foo AS (
	SELECT
		array_agg(gid) AS gid,
		ST_Union(geom) AS geom
	FROM raster_setvalues_geom
	WHERE gid IN (1,4)
)
SELECT
	t1.rid, t2.gid, ST_DumpValues(ST_SetValues(rast, 1, ARRAY[ROW(t2.geom, 99)]::geomval[]))
FROM raster_setvalues_rast t1
CROSS JOIN foo t2;

WITH foo AS (
	SELECT
		array_agg(gid) AS gid,
		ST_Union(geom) AS geom
	FROM raster_setvalues_geom
	WHERE gid IN (2,3)
)
SELECT
	t1.rid, t2.gid, ST_DumpValues(ST_SetValues(rast, 1, ARRAY[ROW(t2.geom, 99)]::geomval[]))
FROM raster_setvalues_rast t1
CROSS JOIN foo t2;

DROP TABLE IF EXISTS raster_setvalues_rast;
DROP TABLE IF EXISTS raster_setvalues_geom;
