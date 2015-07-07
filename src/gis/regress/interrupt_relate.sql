CREATE TEMPORARY TABLE _time AS SELECT now() t;

CREATE FUNCTION _timecheck(label text, tolerated interval) RETURNS text
AS $$
DECLARE
  ret TEXT;
  lap INTERVAL;
BEGIN
  lap := now()-t FROM _time;
  IF lap <= tolerated THEN ret := label || ' interrupted on time';
  ELSE ret := label || ' interrupted late: ' || lap;
  END IF;
  UPDATE _time SET t = now();
  RETURN ret;
END;
$$ LANGUAGE 'plpgsql' VOLATILE;

CREATE TEMP TABLE _inputs AS 
SELECT 1::int as id, ST_Collect(g) g FROM (
 SELECT ST_MakeLine(
   ST_Point(cos(radians(x)),sin(radians(270-x))), 
   ST_Point(sin(radians(x)),cos(radians(60-x)))
   ) g
 FROM generate_series(1,720) x
 ) foo
;

UPDATE _time SET t = now(); -- reset time as creating tables spends some

-----------------------------
-- IM9 based predicates
--
-- These require GEOS-3.4.3+
-----------------------------

SET statement_timeout TO 100;

select ST_Contains(g,g) from _inputs WHERE id = 1; -- 6+ seconds
SELECT _timecheck('contains', '200ms');

select ST_Covers(g,g) from _inputs WHERE id = 1; -- 6+ seconds
SELECT _timecheck('covers', '200ms');

select ST_CoveredBy(g,g) from _inputs WHERE id = 1; -- 6+ seconds
SELECT _timecheck('coveredby', '200ms');

select ST_Crosses(g,g) from _inputs WHERE id = 1; -- 6+ seconds
SELECT _timecheck('crosses', '200ms');

select ST_Equals(g,g) from _inputs WHERE id = 1; -- 6+ seconds
SELECT _timecheck('crosses', '200ms');

select ST_Intersects(g,g) from _inputs WHERE id = 1; -- 6+ seconds
SELECT _timecheck('crosses', '200ms');

select ST_Overlaps(g,g) from _inputs WHERE id = 1; -- 6+ seconds
SELECT _timecheck('crosses', '200ms');

select ST_Relate(g,g) from _inputs WHERE id = 1; -- 6+ seconds
SELECT _timecheck('relate', '200ms');


DROP FUNCTION _timecheck(text, interval);
