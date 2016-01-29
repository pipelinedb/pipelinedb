CREATE TABLE first_values_t (x int, y int, z int);
CREATE CONTINUOUS VIEW first_values_v AS SELECT x::int, first_values(2) WITHIN GROUP (ORDER BY y::int) FROM first_values_s GROUP BY x;

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
