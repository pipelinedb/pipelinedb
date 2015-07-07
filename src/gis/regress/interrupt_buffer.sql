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

-----------------
-- ST_Buffer
-----------------

SET statement_timeout TO 100;
select ST_Buffer(g,100) from _inputs WHERE id = 1;
--( select (st_dumppoints(st_buffer(st_makepoint(0,0),10000,100000))).geom g) foo;
-- it may take some more to interrupt st_buffer, see
-- https://travis-ci.org/postgis/postgis/builds/40211116#L2222-L2223
SELECT _timecheck('buffer', '200ms');

-- Not affected by old timeout
SELECT '1', ST_NPoints(ST_Buffer('POINT(4 0)'::geometry, 2, 1));


DROP FUNCTION _timecheck(text, interval);
