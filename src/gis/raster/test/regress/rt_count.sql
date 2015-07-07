SELECT ST_Count(
	ST_SetValue(
		ST_SetValue(
			ST_SetValue(
				ST_AddBand(
					ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0,0)
					, 1, '64BF', 0, 0
				)
				, 1, 1, 1, -10
			)
			, 1, 5, 4, 0
		)
		, 1, 5, 5, 3.14159
	)
	, 1, TRUE
);
SELECT ST_Count(
	ST_SetValue(
		ST_SetValue(
			ST_SetValue(
				ST_AddBand(
					ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0,0)
					, 1, '64BF', 0, 0
				)
				, 1, 1, 1, -10
			)
			, 1, 5, 4, 0
		)
		, 1, 5, 5, 3.14159
	)
	, 1
);
SELECT ST_Count(
	ST_SetValue(
		ST_SetValue(
			ST_SetValue(
				ST_AddBand(
					ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0,0)
					, 1, '64BF', 0, 0
				)
				, 1, 1, 1, -10
			)
			, 1, 5, 4, 0
		)
		, 1, 5, 5, 3.14159
	)
	, FALSE
);
SELECT ST_Count(
	ST_SetValue(
		ST_SetValue(
			ST_SetValue(
				ST_AddBand(
					ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0,0)
					, 1, '64BF', 0, 0
				)
				, 1, 1, 1, -10
			)
			, 1, 5, 4, 0
		)
		, 1, 5, 5, 3.14159
	)
);
BEGIN;
CREATE TEMP TABLE test
	ON COMMIT DROP AS
	SELECT
		rast.rast
	FROM (
		SELECT ST_SetValue(
			ST_SetValue(
				ST_SetValue(
					ST_AddBand(
						ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0,0)
						, 1, '64BF', 0, 0
					)
					, 1, 1, 1, -10
				)
				, 1, 5, 4, 0
			)
			, 1, 5, 5, 3.14159
		) AS rast
	) AS rast
	FULL JOIN (
		SELECT generate_series(1, 10) AS id
	) AS id
		ON 1 = 1;
SELECT ST_Count('test', 'rast', 1, TRUE);
SELECT ST_Count('test', 'rast', 1, FALSE);
SELECT ST_Count('test', 'rast', 1);
SELECT ST_Count('test', 'rast', FALSE);
SELECT ST_Count('test', 'rast');

SELECT ST_CountAgg(rast, 1, TRUE, 1) FROM test;
SELECT ST_CountAgg(rast, 1, TRUE, 0) FROM test;
SELECT ST_CountAgg(rast, 1, FALSE, 1) FROM test;
SELECT ST_CountAgg(rast, 1, FALSE, 0) FROM test;
SELECT ST_CountAgg(rast, 1, TRUE) FROM test;
SELECT ST_CountAgg(rast, 1, FALSE) FROM test;
SELECT ST_CountAgg(rast, TRUE) FROM test;
SELECT ST_CountAgg(rast, FALSE) FROM test;

SAVEPOINT test;
SELECT ST_CountAgg(rast, 2, TRUE) FROM test;
ROLLBACK TO SAVEPOINT test;
RELEASE SAVEPOINT test;

SAVEPOINT test;
SELECT ST_CountAgg(rast, 1, TRUE, 2) FROM test;
ROLLBACK TO SAVEPOINT test;
RELEASE SAVEPOINT test;

ROLLBACK;
