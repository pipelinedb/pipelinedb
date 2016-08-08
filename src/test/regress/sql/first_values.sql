CREATE TABLE first_values_t (x int, y int, z int);
CREATE STREAM first_values_s (x int, y int, z int);
CREATE CONTINUOUS VIEW first_values_v AS SELECT x::int, first_values(2) WITHIN GROUP (ORDER BY y::int) FROM first_values_s GROUP BY x;
\d first_values_v

INSERT INTO first_values_t (x, y, z) VALUES (1, 1, 10), (1, 1, 4), (1, 1, 5);
INSERT INTO first_values_t (x, y, z) VALUES (1, 1, -1), (1, 2, 10), (1, 2, 4);
INSERT INTO first_values_t (x, y, z) VALUES (2, 1, 10), (2, NULL, -1), (2, -1, 2);
INSERT INTO first_values_s (x, y, z) VALUES (1, 1, 10), (1, 1, 4), (1, 1, 5);
INSERT INTO first_values_s (x, y, z) VALUES (1, 1, -1), (1, 2, 10), (1, 2, 4);
INSERT INTO first_values_s (x, y, z) VALUES (2, 1, 10), (2, NULL, -1), (2, -1, 2);

SELECT x, first_values(2) WITHIN GROUP (ORDER BY y) FROM first_values_t GROUP BY x;
SELECT * FROM first_values_v;

INSERT INTO first_values_t (x, y, z) VALUES (2, 1, 10), (1, -1, -10), (2, -10, -2);
INSERT INTO first_values_s (x, y, z) VALUES (2, 1, 10), (1, -1, -10), (2, -10, -2);

SELECT x, first_values(2) WITHIN GROUP (ORDER BY y) FROM first_values_t GROUP BY x;
SELECT * FROM first_values_v;

SELECT x, first_values(2) WITHIN GROUP (ORDER BY y, z) FROM first_values_t GROUP BY x;

DROP CONTINUOUS VIEW first_values_v;
DROP TABLE first_values_t;

CREATE CONTINUOUS VIEW first_values_v_order0 AS SELECT x::int % 10 AS g, first_values(2) WITHIN GROUP (ORDER BY x::int, y::int) FROM first_values_s GROUP BY g;
\d first_values_v_order0

INSERT INTO first_values_s (x, y) SELECT x, x FROM generate_series(500, 1000) AS x;
INSERT INTO first_values_s (x, y) SELECT x, x FROM generate_series(250, 1000) AS x;
INSERT INTO first_values_s (x, y) SELECT x, x FROM generate_series(1, 1000) AS x;

SELECT * FROM first_values_v_order0 ORDER BY g;

DROP CONTINUOUS VIEW first_values_v_order0;
DROP STREAM first_values_s CASCADE;

CREATE STREAM first_values_s (t0 text, t1 text, t2 text);
CREATE CONTINUOUS VIEW first_values_v_order1 AS
	SELECT first_values(3) WITHIN GROUP (ORDER BY t0::text, t1::text, t2::text) FROM first_values_s;

INSERT INTO first_values_s (t0, t1, t2) SELECT x, x, x FROM generate_series(500, 1000) AS x;
INSERT INTO first_values_s (t0, t1, t2) SELECT x, x, x FROM generate_series(250, 1000) AS x;
INSERT INTO first_values_s (t0, t1, t2) SELECT x, x, x FROM generate_series(1, 1000) AS x;

SELECT * FROM first_values_v_order1;
SELECT combine(first_values) FROM first_values_v_order1;

DROP CONTINUOUS VIEW first_values_v_order1;
DROP STREAM first_values_s CASCADE;
