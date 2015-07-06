SELECT a, b, ST_RelateMatch(a,b) FROM
	( SELECT '101202FFF'::text as a, 'TTTTTTFFF'::text as b) as f;
SELECT a, b, ST_RelateMatch(a,b) FROM
	( SELECT '101202FFF'::text as a, 'T0T2TTFFF'::text as b) as f;
SELECT a, b, ST_RelateMatch(a,b) FROM
	( SELECT '101202FFF'::text as a, '101202FFF'::text as b) as f;
SELECT a, b, ST_RelateMatch(a,b) FROM
	( SELECT '101202FFF'::text as a, '101102FFF'::text as b) as f;
SELECT a, b, ST_RelateMatch(a,b) FROM
	( SELECT 'FFFFFFFFF'::text as a, '1FFFFFFFF'::text as b) as f;
SELECT a, b, ST_RelateMatch(a,b) FROM
	( SELECT 'FFFFFFFFF'::text as a, '*FFFFFFFF'::text as b) as f;
