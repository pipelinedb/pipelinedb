-- liblwgeom interruption

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

-----------------
-- ST_Segmentize
-----------------

SET statement_timeout TO 100;
-- would run for many seconds if uninterruptible...
SELECT ST_Segmentize(ST_MakeLine(ST_Point(4,39), ST_Point(1,41)), 1e-100);
SELECT _timecheck('segmentize', '250ms');
SET statement_timeout TO 0;
-- Not affected by old timeout
SELECT '1',ST_AsText(ST_Segmentize('LINESTRING(0 0,4 0)'::geometry, 2));


DROP FUNCTION _timecheck(text, interval);
