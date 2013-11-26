--
-- UNION (also INTERSECT, EXCEPT)
--

-- Simple UNION constructs

SELECT 1 AS two UNION SELECT 2 ORDER BY 1;

SELECT 1 AS one UNION SELECT 1 ORDER BY 1;

SELECT 1 AS two UNION ALL SELECT 2 ORDER BY 1;

SELECT 1 AS two UNION ALL SELECT 1 ORDER BY 1;

SELECT 1 AS three UNION SELECT 2 UNION SELECT 3 ORDER BY 1;

SELECT 1 AS two UNION SELECT 2 UNION SELECT 2 ORDER BY 1;

SELECT 1 AS three UNION SELECT 2 UNION ALL SELECT 2 ORDER BY 1;

SELECT 1.1 AS two UNION SELECT 2.2 ORDER BY 1;

-- Mixed types

SELECT 1.1 AS two UNION SELECT 2 ORDER BY 1;

SELECT 1 AS two UNION SELECT 2.2 ORDER BY 1;

SELECT 1 AS one UNION SELECT 1.0::float8 ORDER BY 1;

SELECT 1.1 AS two UNION ALL SELECT 2 ORDER BY 1;

SELECT 1.0::float8 AS two UNION ALL SELECT 1 ORDER BY 1;

SELECT 1.1 AS three UNION SELECT 2 UNION SELECT 3 ORDER BY 1;

SELECT 1.1::float8 AS two UNION SELECT 2 UNION SELECT 2.0::float8 ORDER BY 1;

SELECT 1.1 AS three UNION SELECT 2 UNION ALL SELECT 2 ORDER BY 1;

SELECT 1.1 AS two UNION (SELECT 2 UNION ALL SELECT 2) ORDER BY 1;

--
-- Try testing from tables...
--

SELECT f1 AS five FROM FLOAT8_TBL
UNION
SELECT f1 FROM FLOAT8_TBL
ORDER BY 1;

SELECT f1 AS ten FROM FLOAT8_TBL
UNION ALL
SELECT f1 FROM FLOAT8_TBL 
ORDER BY 1;

SELECT f1 AS nine FROM FLOAT8_TBL
UNION
SELECT f1 FROM INT4_TBL
ORDER BY 1;

SELECT f1 AS ten FROM FLOAT8_TBL
UNION ALL
SELECT f1 FROM INT4_TBL 
ORDER BY 1;

SELECT f1 AS five FROM FLOAT8_TBL
  WHERE f1 BETWEEN -1e6 AND 1e6
UNION
SELECT f1 FROM INT4_TBL
  WHERE f1 BETWEEN 0 AND 1000000 
  ORDER BY 1;

SELECT CAST(f1 AS char(4)) AS three FROM VARCHAR_TBL
UNION
SELECT f1 FROM CHAR_TBL
ORDER BY 1;

SELECT f1 AS three FROM VARCHAR_TBL
UNION
SELECT CAST(f1 AS varchar) FROM CHAR_TBL
ORDER BY 1;

SELECT f1 AS eight FROM VARCHAR_TBL
UNION ALL
SELECT f1 FROM CHAR_TBL 
ORDER BY 1;

SELECT f1 AS five FROM TEXT_TBL
UNION
SELECT f1 FROM VARCHAR_TBL
UNION
SELECT TRIM(TRAILING FROM f1) FROM CHAR_TBL
ORDER BY 1;

--
-- INTERSECT and EXCEPT
--

SELECT q2 FROM int8_tbl INTERSECT SELECT q1 FROM int8_tbl ORDER BY 1;

SELECT q2 FROM int8_tbl INTERSECT ALL SELECT q1 FROM int8_tbl ORDER BY 1;

SELECT q2 FROM int8_tbl EXCEPT SELECT q1 FROM int8_tbl ORDER BY 1;

SELECT q2 FROM int8_tbl EXCEPT ALL SELECT q1 FROM int8_tbl ORDER BY 1;

SELECT q2 FROM int8_tbl EXCEPT ALL SELECT DISTINCT q1 FROM int8_tbl ORDER BY 1;

SELECT q1 FROM int8_tbl EXCEPT SELECT q2 FROM int8_tbl ORDER BY 1;

SELECT q1 FROM int8_tbl EXCEPT ALL SELECT q2 FROM int8_tbl ORDER BY 1;

SELECT q1 FROM int8_tbl EXCEPT ALL SELECT DISTINCT q2 FROM int8_tbl ORDER BY 1;

--
-- Mixed types
--

SELECT f1 FROM float8_tbl INTERSECT SELECT f1 FROM int4_tbl ORDER BY 1;

SELECT f1 FROM float8_tbl EXCEPT SELECT f1 FROM int4_tbl ORDER BY 1;

--
-- Operator precedence and (((((extra))))) parentheses
--

SELECT q1 FROM int8_tbl INTERSECT SELECT q2 FROM int8_tbl UNION ALL SELECT q2 FROM int8_tbl ORDER BY 1;

SELECT q1 FROM int8_tbl INTERSECT (((SELECT q2 FROM int8_tbl UNION ALL SELECT q2 FROM int8_tbl))) ORDER BY 1;

(((SELECT q1 FROM int8_tbl INTERSECT SELECT q2 FROM int8_tbl))) UNION ALL SELECT q2 FROM int8_tbl ORDER BY 1;

SELECT q1 FROM int8_tbl UNION ALL SELECT q2 FROM int8_tbl EXCEPT SELECT q1 FROM int8_tbl ORDER BY 1;

SELECT q1 FROM int8_tbl UNION ALL (((SELECT q2 FROM int8_tbl EXCEPT SELECT q1 FROM int8_tbl ORDER BY 1))) ORDER BY 1;

(((SELECT q1 FROM int8_tbl UNION ALL SELECT q2 FROM int8_tbl))) EXCEPT SELECT q1 FROM int8_tbl ORDER BY 1;

--
-- Subqueries with ORDER BY & LIMIT clauses
--

-- In this syntax, ORDER BY/LIMIT apply to the result of the EXCEPT
SELECT q1,q2 FROM int8_tbl EXCEPT SELECT q2,q1 FROM int8_tbl
ORDER BY q2,q1;

-- This should fail, because q2 isn't a name of an EXCEPT output column
SELECT q1 FROM int8_tbl EXCEPT SELECT q2 FROM int8_tbl ORDER BY q2 LIMIT 1;

-- But this should work:
SELECT q1 FROM int8_tbl EXCEPT (((SELECT q2 FROM int8_tbl ORDER BY q2 LIMIT 1))) ORDER BY q1;

--
-- New syntaxes (7.1) permit new tests
--

(((((select * from int8_tbl  ORDER BY q1, q2)))));

--
-- Check handling of a case with unknown constants.  We don't guarantee
-- an undecorated constant will work in all cases, but historically this
-- usage has worked, so test we don't break it.
--

SELECT a.f1 FROM (SELECT 'test' AS f1 FROM varchar_tbl) a
UNION
SELECT b.f1 FROM (SELECT f1 FROM varchar_tbl) b
ORDER BY 1;

-- This should fail, but it should produce an error cursor
SELECT '3.4'::numeric UNION SELECT 'foo';

--
-- Test that expression-index constraints can be pushed down through
-- UNION or UNION ALL
--
-- Enforce use of COMMIT instead of 2PC for temporary objects
SET enforce_two_phase_commit TO off;
CREATE TEMP TABLE t1 (a text, b text);
CREATE INDEX t1_ab_idx on t1 ((a || b));
CREATE TEMP TABLE t2 (ab text primary key);
INSERT INTO t1 VALUES ('a', 'b'), ('x', 'y');
INSERT INTO t2 VALUES ('ab'), ('xy');

set enable_seqscan = off;
set enable_indexscan = on;
set enable_bitmapscan = off;

explain (num_nodes off, nodes off, costs off)
 SELECT * FROM
 (SELECT a || b AS ab FROM t1
  UNION ALL
  SELECT * FROM t2) t
 WHERE ab = 'ab';

explain (num_nodes off, nodes off, costs off)
 SELECT * FROM
 (SELECT a || b AS ab FROM t1
  UNION
  SELECT * FROM t2) t
 WHERE ab = 'ab';

reset enable_seqscan;
reset enable_indexscan;
reset enable_bitmapscan;

-- Test constraint exclusion of UNION ALL subqueries
explain (num_nodes off, nodes off, costs off)
 SELECT * FROM
  (SELECT 1 AS t, * FROM tenk1 a
   UNION ALL
   SELECT 2 AS t, * FROM tenk1 b) c
 WHERE t = 2;
