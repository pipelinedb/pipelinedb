SET client_min_messages TO warning;

SELECT
	ST_SameAlignment(
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, 0, 0),
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, 0, 0)
	),
	ST_NotSameAlignmentReason(
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, 0, 0),
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, 0, 0)
	)
;
SELECT
	ST_SameAlignment(
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, 0, 0),
		ST_MakeEmptyRaster(1, 1, 0, 0, 1.1, 1.1, 0, 0)
	),
	ST_NotSameAlignmentReason(
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, 0, 0),
		ST_MakeEmptyRaster(1, 1, 0, 0, 1.1, 1.1, 0, 0)
	)
;
SELECT
	ST_SameAlignment(
		ST_MakeEmptyRaster(1, 1, 0, 0, 1.1, 1.1, 0, 0),
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, 0, 0)
	),
	ST_NotSameAlignmentReason(
		ST_MakeEmptyRaster(1, 1, 0, 0, 1.1, 1.1, 0, 0),
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, 0, 0)
	)
;
SELECT
	ST_SameAlignment(
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, 0, 0),
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, -0.1, 0)
	),
	ST_NotSameAlignmentReason(
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, 0, 0),
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, -0.1, 0)
	)
;
SELECT
	ST_SameAlignment(
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, 0, 0),
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, 0, 0.1)
	),
	ST_NotSameAlignmentReason(
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, 0, 0),
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, 0, 0.1)
	)
;
SELECT
	ST_SameAlignment(
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, 0, 0),
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, -0.1, 0.1)
	),
	ST_NotSameAlignmentReason(
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, 0, 0),
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, -0.1, 0.1)
	)
;
SELECT
	ST_SameAlignment(
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, 0, 0),
		ST_MakeEmptyRaster(1, 1, 1, 1, 1, 1, 0, 0)
	),
	ST_NotSameAlignmentReason(
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, 0, 0),
		ST_MakeEmptyRaster(1, 1, 1, 1, 1, 1, 0, 0)
	)
;
SELECT
	ST_SameAlignment(
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, 0, 0),
		ST_MakeEmptyRaster(1, 1, 0.1, 0.1, 1, 1, 0, 0)
	),
	ST_NotSameAlignmentReason(
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, 0, 0),
		ST_MakeEmptyRaster(1, 1, 0.1, 0.1, 1, 1, 0, 0)
	)
;
SELECT
	ST_SameAlignment(
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, -0.1, 0.1),
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, -0.1, 0.1)
	),
	ST_NotSameAlignmentReason(
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, -0.1, 0.1),
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, -0.1, 0.1)
	)
;
SELECT
	ST_SameAlignment(
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, -0.1, 0.1),
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, -0.1, 0)
	),
	ST_NotSameAlignmentReason(
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, -0.1, 0.1),
		ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, -0.1, 0)
	)
;

DROP TABLE IF EXISTS raster_alignment_test;
CREATE TABLE raster_alignment_test AS
	(SELECT 1 AS rid, ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, 0, 0) AS rast) UNION ALL
	(SELECT 2 AS rid, ST_MakeEmptyRaster(1, 1, 1, 1, 1, 1, 0, 0) AS rast) UNION ALL
	(SELECT 3 AS rid, ST_MakeEmptyRaster(1, 1, 1, -1, 1, 1, 0, 0) AS rast) UNION ALL
	(SELECT 4 AS rid, ST_MakeEmptyRaster(1, 1, -1, 1, 1, 1, 0, 0) AS rast) UNION ALL
	(SELECT 5 AS rid, ST_MakeEmptyRaster(1, 1, -1, -1, 1, 1, 0, 0) AS rast) UNION ALL
	(SELECT 6 AS rid, ST_MakeEmptyRaster(1, 1, 0, -1, 1, 1, 0, 0) AS rast) UNION ALL
	(SELECT 7 AS rid, ST_MakeEmptyRaster(1, 1, 1, 1, 1, 1, 0, 0) AS rast) UNION ALL
	(SELECT 8 AS rid, ST_MakeEmptyRaster(1, 1, 2, 1, 1, 1, 0, 0) AS rast) UNION ALL
	(SELECT 9 AS rid, ST_MakeEmptyRaster(1, 1, 3, 1, 1, 1, 0, 0) AS rast) UNION ALL
	(SELECT 10 AS rid, ST_MakeEmptyRaster(1, 1, 4, 1, 1, 1, 0, 0) AS rast) UNION ALL

	(SELECT 11 AS rid, ST_MakeEmptyRaster(1, 1, 0.1, 0, 1, 1, 0, 0) AS rast) UNION ALL
	(SELECT 12 AS rid, ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, -0.1, 0) AS rast) UNION ALL
	(SELECT 13 AS rid, ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, -0.1, 0.1) AS rast) UNION ALL
	(SELECT 14 AS rid, ST_MakeEmptyRaster(1, 1, 0, 0, 1, 1, 0, 0.1) AS rast) UNION ALL

	(SELECT 0 AS rid, NULL::raster AS rast)
;

SELECT ST_SameAlignment(rast)
FROM raster_alignment_test
WHERE rid BETWEEN 1 AND 10;

SELECT ST_SameAlignment(rast)
FROM raster_alignment_test
WHERE rid BETWEEN 1 AND 10
	OR rid = 11;

SELECT ST_SameAlignment(rast)
FROM raster_alignment_test
WHERE rid BETWEEN 1 AND 10
	OR rid = 12;

SELECT ST_SameAlignment(rast)
FROM raster_alignment_test
WHERE rid BETWEEN 1 AND 10
	OR rid = 13;

SELECT ST_SameAlignment(rast)
FROM raster_alignment_test
WHERE rid BETWEEN 1 AND 10
	OR rid = 14;

SELECT ST_SameAlignment(rast)
FROM raster_alignment_test
WHERE rid != 0;

SELECT ST_SameAlignment(rast)
FROM raster_alignment_test;

DROP TABLE IF EXISTS raster_alignment_test;
