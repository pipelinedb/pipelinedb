SET client_min_messages TO warning;

DROP TABLE IF EXISTS raster_nmapalgebra_in;
CREATE TABLE raster_nmapalgebra_in (
	rid integer,
	rast raster
);

INSERT INTO raster_nmapalgebra_in
	SELECT 0, NULL::raster AS rast UNION ALL
	SELECT 1, ST_MakeEmptyRaster(2, 2, 0, 0, 1, -1, 0, 0, 0) AS rast UNION ALL
	SELECT 2, ST_AddBand(ST_MakeEmptyRaster(2, 2, 0, 0, 1, -1, 0, 0, 0), 1, '8BUI', 1, 0) AS rast UNION ALL
	SELECT 3, ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(2, 2, 1, -1, 1, -1, 0, 0, 0), 1, '8BUI', 2, 0), 2, '32BF', 20, 0) AS rast UNION ALL
	SELECT 4, ST_AddBand(ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(2, 2, 1, -1, 1, -1, 0, 0, 0), 1, '8BUI', 2, 0), 2, '32BF', 20, 0), 3, '16BUI', 200, 0) AS rast
;

CREATE OR REPLACE FUNCTION raster_nmapalgebra_test(
	value double precision[][][],
	pos int[][],
	VARIADIC userargs text[]
)
	RETURNS double precision
	AS $$
	BEGIN
		RAISE NOTICE 'value = %', value;
		RAISE NOTICE 'pos = %', pos;
		RAISE NOTICE 'userargs = %', userargs;

		IF userargs IS NULL OR array_length(userargs, 1) < 1 THEN
			RETURN 255;
		ELSE
			RETURN userargs[array_lower(userargs, 1)];
		END IF;
	END;
	$$ LANGUAGE 'plpgsql' IMMUTABLE;

SET client_min_messages TO notice;

SELECT
	rid,
	ST_Value(
		ST_MapAlgebra(
			ARRAY[ROW(rast, 1)]::rastbandarg[],
			'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure
		),
		1, 1, 1
	) = 255
FROM raster_nmapalgebra_in
WHERE rid IN (0, 1);

SELECT
	rid,
	ST_Value(
		ST_MapAlgebra(
			ARRAY[ROW(rast, 1)]::rastbandarg[],
			'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure
		),
		1, 1, 1
	) = 255
FROM raster_nmapalgebra_in
WHERE rid IN (2,3,4);

SELECT
	rid,
	round(ST_Value(
		ST_MapAlgebra(
			ARRAY[ROW(rast, 2)]::rastbandarg[],
			'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,
			NULL,
			'INTERSECTION', NULL,
			0, 0,
			'3.14'
		),
		1, 1, 1
	)::numeric, 2) = 3.14
FROM raster_nmapalgebra_in
WHERE rid IN (3,4);

WITH foo AS (
	SELECT
		rid,
		ST_MapAlgebra(
			ARRAY[ROW(rast, 3)]::rastbandarg[],
			'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,
			'8BUI',
			'INTERSECTION', NULL,
			1, 1
		) AS rast
	FROM raster_nmapalgebra_in
	WHERE rid IN (3,4)
)
SELECT
	rid,
	(ST_Metadata(rast)),
	(ST_BandMetadata(rast, 1)),
	ST_Value(rast, 1, 1, 1)
FROM foo;

INSERT INTO raster_nmapalgebra_in
	SELECT 10, ST_AddBand(ST_MakeEmptyRaster(2, 2, 0, 0, 1, -1, 0, 0, 0), 1, '16BUI', 1, 0) AS rast UNION ALL
	SELECT 11, ST_AddBand(ST_MakeEmptyRaster(2, 2, 2, 0, 1, -1, 0, 0, 0), 1, '16BUI', 2, 0) AS rast UNION ALL
	SELECT 12, ST_AddBand(ST_MakeEmptyRaster(2, 2, 4, 0, 1, -1, 0, 0, 0), 1, '16BUI', 3, 0) AS rast UNION ALL

	SELECT 13, ST_AddBand(ST_MakeEmptyRaster(2, 2, 0, -2, 1, -1, 0, 0, 0), 1, '16BUI', 10, 0) AS rast UNION ALL
	SELECT 14, ST_AddBand(ST_MakeEmptyRaster(2, 2, 2, -2, 1, -1, 0, 0, 0), 1, '16BUI', 20, 0) AS rast UNION ALL
	SELECT 15, ST_AddBand(ST_MakeEmptyRaster(2, 2, 4, -2, 1, -1, 0, 0, 0), 1, '16BUI', 30, 0) AS rast UNION ALL

	SELECT 16, ST_AddBand(ST_MakeEmptyRaster(2, 2, 0, -4, 1, -1, 0, 0, 0), 1, '16BUI', 100, 0) AS rast UNION ALL
	SELECT 17, ST_AddBand(ST_MakeEmptyRaster(2, 2, 2, -4, 1, -1, 0, 0, 0), 1, '16BUI', 200, 0) AS rast UNION ALL
	SELECT 18, ST_AddBand(ST_MakeEmptyRaster(2, 2, 4, -4, 1, -1, 0, 0, 0), 1, '16BUI', 300, 0) AS rast
;

DO $$ DECLARE r record;
BEGIN
-- this ONLY works for PostgreSQL version 9.1 or higher
IF array_to_string(regexp_matches(split_part(version(), ' ', 2), E'([0-9]+)\.([0-9]+)'), '')::int > 90 THEN
	WITH foo AS (
		SELECT
			t1.rid,
			ST_MapAlgebra(
				ARRAY[ROW(ST_Union(t2.rast), 1)]::rastbandarg[],
				'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,
				'32BUI',
				'CUSTOM', t1.rast,
				1, 1
			) AS rast
		FROM raster_nmapalgebra_in t1
		CROSS JOIN raster_nmapalgebra_in t2
		WHERE t1.rid = 10
			AND t2.rid BETWEEN 10 AND 18
			AND ST_Intersects(t1.rast, t2.rast)
		GROUP BY t1.rid, t1.rast
	)
	SELECT
		rid,
		(ST_Metadata(rast)),
		(ST_BandMetadata(rast, 1)),
		ST_Value(rast, 1, 1, 1)
	INTO r
	FROM foo;
	RAISE NOTICE 'record = %', r;

	WITH foo AS (
		SELECT
			t1.rid,
			ST_MapAlgebra(
				ARRAY[ROW(ST_Union(t2.rast), 1)]::rastbandarg[],
				'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,
				'32BUI',
				'CUSTOM', t1.rast,
				1, 1
			) AS rast
		FROM raster_nmapalgebra_in t1
		CROSS JOIN raster_nmapalgebra_in t2
		WHERE t1.rid = 14
			AND t2.rid BETWEEN 10 AND 18
			AND ST_Intersects(t1.rast, t2.rast)
		GROUP BY t1.rid, t1.rast
	)
	SELECT
		rid,
		(ST_Metadata(rast)),
		(ST_BandMetadata(rast, 1)),
		ST_Value(rast, 1, 1, 1)
	INTO r
	FROM foo;
	RAISE NOTICE 'record = %', r;

	WITH foo AS (
		SELECT
			t1.rid,
			ST_MapAlgebra(
				ARRAY[ROW(ST_Union(t2.rast), 1)]::rastbandarg[],
				'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,
				'32BUI',
				'CUSTOM', t1.rast,
				1, 1,
				'1000'
			) AS rast
		FROM raster_nmapalgebra_in t1
		CROSS JOIN raster_nmapalgebra_in t2
		WHERE t1.rid = 17
			AND t2.rid BETWEEN 10 AND 18
			AND ST_Intersects(t1.rast, t2.rast)
		GROUP BY t1.rid, t1.rast
	)
	SELECT
		rid,
		(ST_Metadata(rast)),
		(ST_BandMetadata(rast, 1)),
		ST_Value(rast, 1, 1, 1)
	INTO r
	FROM foo;
	RAISE NOTICE 'record = %', r;

ELSE

	WITH foo AS (
		SELECT
			t1.rid,
			ST_Union(t2.rast) AS rast
		FROM raster_nmapalgebra_in t1
		JOIN raster_nmapalgebra_in t2
			ON ST_Intersects(t1.rast, t2.rast)
			AND t2.rid BETWEEN 10 AND 18
		WHERE t1.rid = 10
		GROUP BY t1.rid
	), bar AS (
		SELECT
			t1.rid,
			ST_MapAlgebra(
				ARRAY[ROW(t2.rast, 1)]::rastbandarg[],
				'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,
				'32BUI',
				'CUSTOM', t1.rast,
				1, 1
			) AS rast
		FROM raster_nmapalgebra_in t1
		JOIN foo t2
			ON t1.rid = t2.rid 
	)
	SELECT
		rid,
		(ST_Metadata(rast)),
		(ST_BandMetadata(rast, 1)),
		ST_Value(rast, 1, 1, 1)
	INTO r
	FROM bar;
	RAISE NOTICE 'record = %', r;

	WITH foo AS (
		SELECT
			t1.rid,
			ST_Union(t2.rast) AS rast
		FROM raster_nmapalgebra_in t1
		JOIN raster_nmapalgebra_in t2
			ON ST_Intersects(t1.rast, t2.rast)
			AND t2.rid BETWEEN 10 AND 18
		WHERE t1.rid = 14
		GROUP BY t1.rid
	), bar AS (
		SELECT
			t1.rid,
			ST_MapAlgebra(
				ARRAY[ROW(t2.rast, 1)]::rastbandarg[],
				'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,
				'32BUI',
				'CUSTOM', t1.rast,
				1, 1
			) AS rast
		FROM raster_nmapalgebra_in t1
		JOIN foo t2
			ON t1.rid = t2.rid 
	)
	SELECT
		rid,
		(ST_Metadata(rast)),
		(ST_BandMetadata(rast, 1)),
		ST_Value(rast, 1, 1, 1)
	INTO r
	FROM bar;
	RAISE NOTICE 'record = %', r;

	WITH foo AS (
		SELECT
			t1.rid,
			ST_Union(t2.rast) AS rast
		FROM raster_nmapalgebra_in t1
		JOIN raster_nmapalgebra_in t2
			ON ST_Intersects(t1.rast, t2.rast)
			AND t2.rid BETWEEN 10 AND 18
		WHERE t1.rid = 17
		GROUP BY t1.rid
	), bar AS (
		SELECT
			t1.rid,
			ST_MapAlgebra(
				ARRAY[ROW(t2.rast, 1)]::rastbandarg[],
				'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,
				'32BUI',
				'CUSTOM', t1.rast,
				1, 1,
				'1000'
			) AS rast
		FROM raster_nmapalgebra_in t1
		JOIN foo t2
			ON t1.rid = t2.rid 
	)
	SELECT
		rid,
		(ST_Metadata(rast)),
		(ST_BandMetadata(rast, 1)),
		ST_Value(rast, 1, 1, 1)
	INTO r
	FROM bar;
	RAISE NOTICE 'record = %', r;

END IF;

END $$;

INSERT INTO raster_nmapalgebra_in
	SELECT 20, ST_AddBand(ST_MakeEmptyRaster(2, 2, 0, 0, 1, -1, 0, 0, 0), 1, '16BUI', 1, 0) AS rast UNION ALL
	SELECT 21, ST_AddBand(ST_MakeEmptyRaster(2, 2, 1, -1, 1, -1, 0, 0, 0), 1, '16BUI', 2, 0) AS rast UNION ALL
	SELECT 22, ST_AddBand(ST_MakeEmptyRaster(2, 2, 0, -2, 1, -1, 0, 0, 0), 1, '16BUI', 3, 0) AS rast
;

WITH foo AS (
	SELECT
		t1.rid AS rid1,
		t2.rid AS rid2,
		ST_MapAlgebra(
			ARRAY[ROW(t1.rast, 1), ROW(t2.rast, 1)]::rastbandarg[],
			'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure
		) AS rast
	FROM raster_nmapalgebra_in t1
	CROSS JOIN raster_nmapalgebra_in t2
	WHERE t1.rid = 20
		AND t2.rid = 21
)
SELECT
	rid1,
	rid2,
	(ST_Metadata(rast)),
	(ST_BandMetadata(rast, 1))
FROM foo;

WITH foo AS (
	SELECT
		t1.rid AS rid1,
		t2.rid AS rid2,
		ST_MapAlgebra(
			ARRAY[ROW(t1.rast, 1), ROW(t2.rast, 1)]::rastbandarg[],
			'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure
		) AS rast
	FROM raster_nmapalgebra_in t1
	CROSS JOIN raster_nmapalgebra_in t2
	WHERE t1.rid = 20
		AND t2.rid = 22
)
SELECT
	rid1,
	rid2,
	(ST_Metadata(rast)),
	(ST_BandMetadata(rast, 1))
FROM foo;

WITH foo AS (
	SELECT
		t1.rid AS rid1,
		t2.rid AS rid2,
		ST_MapAlgebra(
			ARRAY[ROW(t1.rast, 1), ROW(t2.rast, 1)]::rastbandarg[],
			'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure
		) AS rast
	FROM raster_nmapalgebra_in t1
	CROSS JOIN raster_nmapalgebra_in t2
	WHERE t1.rid = 21
		AND t2.rid = 22
)
SELECT
	rid1,
	rid2,
	(ST_Metadata(rast)),
	(ST_BandMetadata(rast, 1))
FROM foo;

WITH foo AS (
	SELECT
		t1.rid AS rid1,
		t2.rid AS rid2,
		ST_MapAlgebra(
			ARRAY[ROW(t1.rast, 1), ROW(t2.rast, 1)]::rastbandarg[],
			'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,
			NULL,
			'UNION', NULL,
			0, 0
		) AS rast
	FROM raster_nmapalgebra_in t1
	CROSS JOIN raster_nmapalgebra_in t2
	WHERE t1.rid = 20
		AND t2.rid = 21
)
SELECT
	rid1,
	rid2,
	(ST_Metadata(rast)),
	(ST_BandMetadata(rast, 1))
FROM foo;

WITH foo AS (
	SELECT
		t1.rid AS rid1,
		t2.rid AS rid2,
		ST_MapAlgebra(
			ARRAY[ROW(t1.rast, 1), ROW(t2.rast, 1)]::rastbandarg[],
			'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,
			NULL,
			'UNION', NULL,
			0, 0
		) AS rast
	FROM raster_nmapalgebra_in t1
	CROSS JOIN raster_nmapalgebra_in t2
	WHERE t1.rid = 20
		AND t2.rid = 22
)
SELECT
	rid1,
	rid2,
	(ST_Metadata(rast)),
	(ST_BandMetadata(rast, 1))
FROM foo;

WITH foo AS (
	SELECT
		t1.rid AS rid1,
		t2.rid AS rid2,
		t3.rid AS rid3,
		ST_MapAlgebra(
			ARRAY[ROW(t1.rast, 1), ROW(t2.rast, 1), ROW(t3.rast, 1)]::rastbandarg[],
			'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,
			NULL,
			'UNION', NULL,
			0, 0
		) AS rast
	FROM raster_nmapalgebra_in t1
	CROSS JOIN raster_nmapalgebra_in t2
	CROSS JOIN raster_nmapalgebra_in t3
	WHERE t1.rid = 20
		AND t2.rid = 21
		AND t3.rid = 22
)
SELECT
	rid1,
	rid2,
	rid3,
	(ST_Metadata(rast)),
	(ST_BandMetadata(rast, 1))
FROM foo;

WITH foo AS (
	SELECT
		t1.rid AS rid1,
		t2.rid AS rid2,
		t3.rid AS rid3,
		ST_MapAlgebra(
			ARRAY[ROW(t1.rast, 1), ROW(t2.rast, 1), ROW(t3.rast, 1)]::rastbandarg[],
			'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,
			NULL,
			'FIRST', NULL,
			0, 0
		) AS rast
	FROM raster_nmapalgebra_in t1
	CROSS JOIN raster_nmapalgebra_in t2
	CROSS JOIN raster_nmapalgebra_in t3
	WHERE t1.rid = 20
		AND t2.rid = 21
		AND t3.rid = 22
)
SELECT
	rid1,
	rid2,
	rid3,
	(ST_Metadata(rast)),
	(ST_BandMetadata(rast, 1))
FROM foo;

WITH foo AS (
	SELECT
		t1.rid AS rid1,
		t2.rid AS rid2,
		t3.rid AS rid3,
		ST_MapAlgebra(
			ARRAY[ROW(t1.rast, 1), ROW(t2.rast, 1), ROW(t3.rast, 1)]::rastbandarg[],
			'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,
			NULL,
			'SECOND', NULL,
			0, 0
		) AS rast
	FROM raster_nmapalgebra_in t1
	CROSS JOIN raster_nmapalgebra_in t2
	CROSS JOIN raster_nmapalgebra_in t3
	WHERE t1.rid = 20
		AND t2.rid = 21
		AND t3.rid = 22
)
SELECT
	rid1,
	rid2,
	rid3,
	(ST_Metadata(rast)),
	(ST_BandMetadata(rast, 1))
FROM foo;

WITH foo AS (
	SELECT
		t1.rid AS rid1,
		t2.rid AS rid2,
		t3.rid AS rid3,
		ST_MapAlgebra(
			ARRAY[ROW(t1.rast, 1), ROW(t2.rast, 1), ROW(t3.rast, 1)]::rastbandarg[],
			'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,
			NULL,
			'LAST', NULL,
			0, 0
		) AS rast
	FROM raster_nmapalgebra_in t1
	CROSS JOIN raster_nmapalgebra_in t2
	CROSS JOIN raster_nmapalgebra_in t3
	WHERE t1.rid = 20
		AND t2.rid = 21
		AND t3.rid = 22
)
SELECT
	rid1,
	rid2,
	rid3,
	(ST_Metadata(rast)),
	(ST_BandMetadata(rast, 1))
FROM foo;

WITH foo AS (
	SELECT
		t1.rid AS rid1,
		t2.rid AS rid2,
		t3.rid AS rid3,
		ST_MapAlgebra(
			ARRAY[ROW(t1.rast, 1), ROW(t2.rast, 1), ROW(t3.rast, 1)]::rastbandarg[],
			'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure
		) AS rast
	FROM raster_nmapalgebra_in t1
	CROSS JOIN raster_nmapalgebra_in t2
	CROSS JOIN raster_nmapalgebra_in t3
	WHERE t1.rid = 20
		AND t2.rid = 21
		AND t3.rid = 22
)
SELECT
	rid1,
	rid2,
	rid3,
	(ST_Metadata(rast)),
	(ST_BandMetadata(rast, 1))
FROM foo;

INSERT INTO raster_nmapalgebra_in
	SELECT 30, ST_AddBand(ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(2, 2, 0, 0, 1, -1, 0, 0, 0), 1, '16BUI', 1, 0), 2, '8BUI', 10, 0), 3, '32BUI', 100, 0) AS rast UNION ALL
	SELECT 31, ST_AddBand(ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(2, 2, 0, 1, 1, -1, 0, 0, 0), 1, '16BUI', 2, 0), 2, '8BUI', 20, 0), 3, '32BUI', 300, 0) AS rast
;

WITH foo AS (
	SELECT
		t1.rid AS rid,
		ST_MapAlgebra(
			ARRAY[ROW(t1.rast, 1), ROW(t1.rast, 2), ROW(t1.rast, 3)]::rastbandarg[],
			'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure
		) AS rast
	FROM raster_nmapalgebra_in t1
	WHERE t1.rid = 30
)
SELECT
	rid,
	(ST_Metadata(rast)),
	(ST_BandMetadata(rast, 1))
FROM foo;

WITH foo AS (
	SELECT
		t1.rid AS rid,
		ST_MapAlgebra(
			ARRAY[ROW(t1.rast, 3), ROW(t1.rast, 1), ROW(t1.rast, 3)]::rastbandarg[],
			'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure
		) AS rast
	FROM raster_nmapalgebra_in t1
	WHERE t1.rid = 30
)
SELECT
	rid,
	(ST_Metadata(rast)),
	(ST_BandMetadata(rast, 1))
FROM foo;

WITH foo AS (
	SELECT
		t1.rid AS rid,
		ST_MapAlgebra(
			ARRAY[ROW(t1.rast, 2), ROW(t1.rast, 2)]::rastbandarg[],
			'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,
			'16BUI'::text
		) AS rast
	FROM raster_nmapalgebra_in t1
	WHERE t1.rid = 31
)
SELECT
	rid,
	(ST_Metadata(rast)),
	(ST_BandMetadata(rast, 1))
FROM foo;

WITH foo AS (
	SELECT
		t1.rid AS rid1,
		t2.rid AS rid2,
		ST_MapAlgebra(
			ARRAY[ROW(t1.rast, 2), ROW(t2.rast, 1), ROW(t2.rast, 2)]::rastbandarg[],
			'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,
			'16BUI'
		) AS rast
	FROM raster_nmapalgebra_in t1
	CROSS JOIN raster_nmapalgebra_in t2
	WHERE t1.rid = 30
		AND t2.rid = 31
)
SELECT
	rid1,
	rid2,
	(ST_Metadata(rast)),
	(ST_BandMetadata(rast, 1))
FROM foo;

WITH foo AS (
	SELECT
		t1.rid AS rid,
		ST_MapAlgebra(
			t1.rast, ARRAY[3, 1, 3]::int[],
			'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure
		) AS rast
	FROM raster_nmapalgebra_in t1
	WHERE t1.rid = 30
)
SELECT
	rid,
	(ST_Metadata(rast)),
	(ST_BandMetadata(rast, 1))
FROM foo;

WITH foo AS (
	SELECT
		t1.rid AS rid,
		ST_MapAlgebra(
			t1.rast, 2,
			'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure
		) AS rast
	FROM raster_nmapalgebra_in t1
	WHERE t1.rid = 30
)
SELECT
	rid,
	(ST_Metadata(rast)),
	(ST_BandMetadata(rast, 1))
FROM foo;

-- Ticket #2803
-- http://trac.osgeo.org/postgis/ticket/2803
ALTER FUNCTION raster_nmapalgebra_test(
	value double precision[][][],
	pos int[][],
	VARIADIC userargs text[]
)
	IMMUTABLE STRICT;

SELECT
	rid,
	ST_Value(
		ST_MapAlgebra(
			ARRAY[ROW(rast, 1)]::rastbandarg[],
			'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure
		),
		1, 1, 1
	) = 255
FROM raster_nmapalgebra_in
WHERE rid IN (2);

-- Ticket #2802
-- http://trac.osgeo.org/postgis/ticket/2802
CREATE OR REPLACE FUNCTION raster_nmapalgebra_test_bad_return(
	value double precision[][][],
	pos int[][],
	VARIADIC userargs text[]
)
	RETURNS text
	AS $$
	BEGIN
		RETURN 255;
	END;
	$$ LANGUAGE 'plpgsql' IMMUTABLE;

SELECT
	rid,
	ST_Value(
		ST_MapAlgebra(
			ARRAY[ROW(rast, 1)]::rastbandarg[],
			'raster_nmapalgebra_test_bad_return(double precision[], int[], text[])'::regprocedure
		),
		1, 1, 1
	) = 255
FROM raster_nmapalgebra_in
WHERE rid IN (2);

DROP FUNCTION IF EXISTS raster_nmapalgebra_test(double precision[], int[], text[]); 
DROP FUNCTION IF EXISTS raster_nmapalgebra_test_bad_return(double precision[], int[], text[]); 
DROP TABLE IF EXISTS raster_nmapalgebra_in; 
