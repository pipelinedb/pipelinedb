SELECT
	round(min::numeric, 3),
	round(max::numeric, 3),
	count,
	round(percent::numeric, 3)
FROM ST_Histogram(
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
SELECT
	round(min::numeric, 3),
	round(max::numeric, 3),
	count,
	round(percent::numeric, 3)
FROM ST_Histogram(
	ST_SetValue(
		ST_SetValue(
			ST_SetValue(
				ST_AddBand(
					ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0,0)
					, 1, '64BF', 0, NULL
				)
				, 1, 1, 1, -10
			)
			, 1, 5, 4, 0
		)
		, 1, 5, 5, 3.14159
	)
);
SELECT
	round(min::numeric, 3),
	round(max::numeric, 3),
	count,
	round(percent::numeric, 3)
FROM ST_Histogram(
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
	),
	1, FALSE, 0, ARRAY[]::double precision[], FALSE
);
SELECT
	round(min::numeric, 3),
	round(max::numeric, 3),
	count,
	round(percent::numeric, 3)
FROM ST_Histogram(
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
	),
	1, FALSE, 1, FALSE
);
SELECT
	round(min::numeric, 3),
	round(max::numeric, 3),
	count,
	round(percent::numeric, 3)
FROM ST_Histogram(
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
	),
	1, FALSE, 5
);
SELECT
	round(min::numeric, 3),
	round(max::numeric, 3),
	count,
	round(percent::numeric, 3)
FROM ST_Histogram(
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
	),
	1, FALSE
);
SELECT
	round(min::numeric, 3),
	round(max::numeric, 3),
	count,
	round(percent::numeric, 3)
FROM ST_Histogram(
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
	),
	1
);
SELECT
	round(min::numeric, 3),
	round(max::numeric, 3),
	count,
	round(percent::numeric, 3)
FROM ST_Histogram(
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
	),
	1, 0, ARRAY[5]::double precision[], FALSE
);
SELECT
	round(min::numeric, 3),
	round(max::numeric, 3),
	count,
	round(percent::numeric, 3)
FROM ST_Histogram(
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
	),
	1, 3, FALSE
);
SELECT
	round(min::numeric, 3),
	round(max::numeric, 3),
	count,
	round(percent::numeric, 3)
FROM ST_Histogram(
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
	),
	1, 5
);
SELECT
	round(min::numeric, 3),
	round(max::numeric, 3),
	count,
	round(percent::numeric, 3)
FROM ST_Histogram(
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
	),
	2
);
BEGIN;
CREATE TEMP TABLE test_histogram
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
SELECT
	round(min::numeric, 3),
	round(max::numeric, 3),
	count,
	round(percent::numeric, 3)
FROM ST_Histogram('test_histogram', 'rast', 1, TRUE, 0, NULL, FALSE);
SELECT
	round(min::numeric, 3),
	round(max::numeric, 3),
	count,
	round(percent::numeric, 3)
FROM ST_Histogram('test_histogram', 'rast', 1, TRUE, 0, NULL, FALSE);
SELECT
	round(min::numeric, 3),
	round(max::numeric, 3),
	count,
	round(percent::numeric, 3)
FROM ST_Histogram('test_histogram', 'rast', 1, FALSE);
SELECT
	round(min::numeric, 3),
	round(max::numeric, 3),
	count,
	round(percent::numeric, 3)
FROM ST_Histogram('test_histogram', 'rast', 1, FALSE, 5, FALSE);
SELECT
	round(min::numeric, 3),
	round(max::numeric, 3),
	count,
	round(percent::numeric, 3)
FROM ST_Histogram('test_histogram', 'rast', 1, 10);
SELECT
	round(min::numeric, 3),
	round(max::numeric, 3),
	count,
	round(percent::numeric, 3)
FROM ST_Histogram('test_histogram', 'rast', 1, 3, FALSE);
SAVEPOINT test;
SELECT
	round(min::numeric, 3),
	round(max::numeric, 3),
	count,
	round(percent::numeric, 3)
FROM ST_Histogram('test_histogram', 'rast', 2, TRUE, 0, NULL, FALSE);
ROLLBACK TO SAVEPOINT test;
RELEASE SAVEPOINT test;
SAVEPOINT test;
SELECT
	round(min::numeric, 3),
	round(max::numeric, 3),
	count,
	round(percent::numeric, 3)
FROM ST_Histogram('test1', 'rast', 1, TRUE, 0, NULL, FALSE);
ROLLBACK TO SAVEPOINT test;
RELEASE SAVEPOINT test;
SAVEPOINT test;
SELECT
	round(min::numeric, 3),
	round(max::numeric, 3),
	count,
	round(percent::numeric, 3)
FROM ST_Histogram('test_histogram', 'rast1', 1, TRUE, 0, NULL, FALSE);
ROLLBACK TO SAVEPOINT test;
RELEASE SAVEPOINT test;
ROLLBACK;
