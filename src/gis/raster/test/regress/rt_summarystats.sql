SELECT
	count,
	round(sum::numeric, 3),
	round(mean::numeric, 3),
	round(stddev::numeric, 3),
	round(min::numeric, 3),
	round(max::numeric, 3)
FROM ST_SummaryStats(
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
	, TRUE
);
SELECT
	count,
	round(sum::numeric, 3),
	round(mean::numeric, 3),
	round(stddev::numeric, 3),
	round(min::numeric, 3),
	round(max::numeric, 3)
FROM ST_SummaryStats(
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
	, TRUE
);
SELECT count FROM ST_SummaryStats(
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
	, TRUE
);
SELECT count FROM ST_SummaryStats(
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
SELECT round(mean::numeric, 3), round(stddev::numeric, 3) FROM ST_SummaryStats(
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
	, TRUE
);
SELECT round(mean::numeric, 3), round(stddev::numeric, 3) FROM ST_SummaryStats(
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
SELECT round(mean::numeric, 3), round(stddev::numeric, 3) FROM ST_SummaryStats(
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
	, 2
);
SELECT ST_ApproxSummaryStats(
	ST_Clip(
		ST_AddBand(
			ST_MakeEmptyRaster(10, 10, 0, 0, 1, 1, 0, 0, 0)
			, '16BSI'::text, 0, 0
		)
		, ST_MakeEnvelope(0, 0, 10, 5, 0)
	)
	, 1, true, 0.1
);
SELECT ST_SummaryStats(
	ST_AddBand(
		ST_MakeEmptyRaster(10, 0, 0, 0, 1, -1, 0, 0, 0)
		, '8BUI'::text, 1, 0
	)
);
BEGIN;
CREATE TEMP TABLE test_summarystats
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
	count,
	round(sum::numeric, 3),
	round(mean::numeric, 3),
	round(stddev::numeric, 3),
	round(min::numeric, 3),
	round(max::numeric, 3)
FROM ST_SummaryStats('test_summarystats', 'rast', 1, TRUE);
SELECT
	count,
	round(sum::numeric, 3),
	round(mean::numeric, 3),
	round(stddev::numeric, 3),
	round(min::numeric, 3),
	round(max::numeric, 3)
FROM ST_SummaryStats('test_summarystats', 'rast', 1, FALSE);
SELECT
	count,
	round(sum::numeric, 3),
	round(mean::numeric, 3),
	round(stddev::numeric, 3),
	round(min::numeric, 3),
	round(max::numeric, 3)
FROM ST_SummaryStats('test_summarystats', 'rast', 1);
SELECT
	count,
	round(sum::numeric, 3),
	round(mean::numeric, 3),
	round(stddev::numeric, 3),
	round(min::numeric, 3),
	round(max::numeric, 3)
FROM ST_SummaryStats('test_summarystats', 'rast');
SAVEPOINT test;
SELECT
	count,
	round(sum::numeric, 3),
	round(mean::numeric, 3),
	round(stddev::numeric, 3),
	round(min::numeric, 3),
	round(max::numeric, 3)
FROM ST_SummaryStats('test_summarystats', 'rast', 2);
ROLLBACK TO SAVEPOINT test;
RELEASE SAVEPOINT test;
SAVEPOINT test;
SELECT
	count,
	round(sum::numeric, 3),
	round(mean::numeric, 3),
	round(stddev::numeric, 3),
	round(min::numeric, 3),
	round(max::numeric, 3)
FROM ST_SummaryStats('test1', 'rast');
ROLLBACK TO SAVEPOINT test;
RELEASE SAVEPOINT test;
SAVEPOINT test;
SELECT
	count,
	round(sum::numeric, 3),
	round(mean::numeric, 3),
	round(stddev::numeric, 3),
	round(min::numeric, 3),
	round(max::numeric, 3)
FROM ST_SummaryStats('test_summarystats', 'rast1');
ROLLBACK TO SAVEPOINT test;
RELEASE SAVEPOINT test;
SAVEPOINT test;

SELECT
	(stats).count,
	round((stats).sum::numeric, 3),
	round((stats).mean::numeric, 3),
	round((stats).stddev::numeric, 3),
	round((stats).min::numeric, 3),
	round((stats).max::numeric, 3)
FROM (
	SELECT
		ST_SummaryStatsAgg(rast, 1, TRUE, 1) AS stats
	FROM test_summarystats
) foo;

ROLLBACK TO SAVEPOINT test;
RELEASE SAVEPOINT test;
SAVEPOINT test;

SELECT
	(stats).count,
	round((stats).sum::numeric, 3),
	round((stats).mean::numeric, 3),
	round((stats).stddev::numeric, 3),
	round((stats).min::numeric, 3),
	round((stats).max::numeric, 3)
FROM (
	SELECT
		ST_SummaryStatsAgg(rast, TRUE, 1) AS stats
	FROM test_summarystats
) foo;

ROLLBACK TO SAVEPOINT test;
RELEASE SAVEPOINT test;
SAVEPOINT test;

SELECT
	(stats).count,
	round((stats).sum::numeric, 3),
	round((stats).mean::numeric, 3),
	round((stats).stddev::numeric, 3),
	round((stats).min::numeric, 3),
	round((stats).max::numeric, 3)
FROM (
	SELECT
		ST_SummaryStatsAgg(rast, 1, TRUE) AS stats
	FROM test_summarystats
) foo;

ROLLBACK TO SAVEPOINT test;
RELEASE SAVEPOINT test;
SAVEPOINT test;

SELECT
	(stats).count,
	round((stats).sum::numeric, 3),
	round((stats).mean::numeric, 3),
	round((stats).stddev::numeric, 3),
	round((stats).min::numeric, 3),
	round((stats).max::numeric, 3)
FROM (
	SELECT
		ST_SummaryStatsAgg(rast, 1, FALSE, 1) AS stats
	FROM test_summarystats
) foo;

ROLLBACK TO SAVEPOINT test;
RELEASE SAVEPOINT test;
SAVEPOINT test;

SELECT
	(stats).count,
	round((stats).sum::numeric, 3),
	round((stats).mean::numeric, 3),
	round((stats).stddev::numeric, 3),
	round((stats).min::numeric, 3),
	round((stats).max::numeric, 3)
FROM (
	SELECT
		ST_SummaryStatsAgg(rast, FALSE, 1) AS stats
	FROM test_summarystats
) foo;

ROLLBACK TO SAVEPOINT test;
RELEASE SAVEPOINT test;
SAVEPOINT test;

SELECT
	(stats).count,
	round((stats).sum::numeric, 3),
	round((stats).mean::numeric, 3),
	round((stats).stddev::numeric, 3),
	round((stats).min::numeric, 3),
	round((stats).max::numeric, 3)
FROM (
	SELECT
		ST_SummaryStatsAgg(rast, 1, FALSE) AS stats
	FROM test_summarystats
) foo;

ROLLBACK TO SAVEPOINT test;
RELEASE SAVEPOINT test;
SAVEPOINT test;

SELECT
	(stats).count,
	round((stats).sum::numeric, 3),
	round((stats).mean::numeric, 3),
	round((stats).stddev::numeric, 3),
	round((stats).min::numeric, 3),
	round((stats).max::numeric, 3)
FROM (
	SELECT
		ST_SummaryStatsAgg(rast, 1, TRUE, 2) AS stats
	FROM test_summarystats
) foo;

ROLLBACK TO SAVEPOINT test;
RELEASE SAVEPOINT test;
SAVEPOINT test;

SELECT
	(stats).count,
	round((stats).sum::numeric, 3),
	round((stats).mean::numeric, 3),
	round((stats).stddev::numeric, 3),
	round((stats).min::numeric, 3),
	round((stats).max::numeric, 3)
FROM (
	SELECT
		ST_SummaryStatsAgg(rast, 2, TRUE, 1) AS stats
	FROM test_summarystats
) foo;

ROLLBACK TO SAVEPOINT test;
RELEASE SAVEPOINT test;
ROLLBACK;
