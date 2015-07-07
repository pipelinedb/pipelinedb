--- build a larger database
\i regress_lots_of_points.sql

--- test some of the searching capabilities

CREATE OR REPLACE FUNCTION qnodes(q text) RETURNS text
LANGUAGE 'plpgsql' AS
$$
DECLARE
  exp TEXT;
  mat TEXT[];
  ret TEXT[];
BEGIN
  FOR exp IN EXECUTE 'EXPLAIN ' || q
  LOOP
    --RAISE NOTICE 'EXP: %', exp;
    mat := regexp_matches(exp, ' *(?:-> *)?(.*Scan)');
    --RAISE NOTICE 'MAT: %', mat;
    IF mat IS NOT NULL THEN
      ret := array_append(ret, mat[1]);
    END IF;
    --RAISE NOTICE 'RET: %', ret;
  END LOOP;
  RETURN array_to_string(ret,',');
END;
$$;

-- GiST index

CREATE INDEX quick_gist on test using gist (the_geom);

set enable_indexscan = off;
set enable_bitmapscan = off;
set enable_seqscan = on;

SELECT 'scan_idx', qnodes('select * from test where the_geom && ST_MakePoint(0,0)');
 select num,ST_astext(the_geom) from test where the_geom && 'BOX3D(125 125,135 135)'::box3d order by num;

set enable_indexscan = on;
set enable_bitmapscan = off;
set enable_seqscan = off;

SELECT 'scan_seq', qnodes('select * from test where the_geom && ST_MakePoint(0,0)');
 select num,ST_astext(the_geom) from test where the_geom && 'BOX3D(125 125,135 135)'::box3d order by num;

CREATE FUNCTION estimate_error(qry text, tol int)
RETURNS text
LANGUAGE 'plpgsql' VOLATILE AS $$
DECLARE
  anl TEXT; -- analisys
  err INT; -- absolute difference between planned and actual rows
  est INT; -- estimated count
  act INT; -- actual count
  mat TEXT[];
BEGIN
  EXECUTE 'EXPLAIN ANALYZE ' || qry INTO anl;

  SELECT regexp_matches(anl, ' rows=([0-9]*) .* rows=([0-9]*) ')
  INTO mat;

  est := mat[1];
  act := mat[2];
    
  err = abs(est-act);

  RETURN act || '+=' || tol || ':' || coalesce(
    nullif((err < tol)::text,'false'),
    'false:'||err::text
    );

END;
$$;

-- There are 50000 points in the table with full extent being
-- BOX(0.0439142361 0.0197799355,999.955261 999.993652)
CREATE TABLE sample_queries AS
SELECT 1 as id, 5 as tol, 'ST_MakeEnvelope(125,125,135,135)' as box
 UNION ALL
SELECT 2, 60, 'ST_MakeEnvelope(0,0,135,135)'
 UNION ALL
SELECT 3, 500, 'ST_MakeEnvelope(0,0,500,500)'
 UNION ALL
SELECT 4, 600, 'ST_MakeEnvelope(0,0,1000,1000)'
;

-- We raise the statistics target to the limit 
ALTER TABLE test ALTER COLUMN the_geom SET STATISTICS 10000;

ANALYZE test;

SELECT estimate_error(
  'select num from test where the_geom && ' || box, tol )
  FROM sample_queries ORDER BY id;

-- Test selectivity estimation of functional indexes

CREATE INDEX expressional_gist on test using gist ( st_centroid(the_geom) );
ANALYZE test;

SELECT 'expr', estimate_error(
  'select num from test where st_centroid(the_geom) && ' || box, tol )
  FROM sample_queries ORDER BY id;

DROP TABLE test;
DROP TABLE sample_queries;

DROP FUNCTION estimate_error(text, int);

DROP FUNCTION qnodes(text);

set enable_indexscan = on;
set enable_bitmapscan = on;
set enable_seqscan = on;
