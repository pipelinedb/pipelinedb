-- Repeat all tests with the new function names.
SELECT '1', ST_astext(ST_Simplifyvw('LINESTRING(0 0, 0 10, 0 51, 50 20, 30 20, 7 32)', 2));
SELECT '2', ST_astext(ST_Simplifyvw('LINESTRING(0 0 3, 0 10 6, 0 51 1, 50 20 6, 30 20 9, 7 32 10)', 100));
SELECT '3', ST_astext(ST_Simplifyvw('LINESTRINGM(0 0 3, 0 10 6, 0 51 1, 50 20 6, 30 20 9, 7 32 10)', 2));
SELECT '4', ST_astext(ST_Simplifyvw('LINESTRING(0 0 3 2, 0 10 6 1, 0 51 1 6, 50 20 6 7, 30 20 9 9, 7 32 10 5)', 100));
SELECT '5', ST_astext(ST_Simplifyvw('MULTIPOINT(0 0 3 2, 0 10 6 1, 0 51 1 6, 50 20 6 7, 30 20 9 9, 7 32 10 5)', 20));
SELECT '6', ST_astext(ST_Simplifyvw('MULTILINESTRING((0 0 3 2, 0 10 6 1, 0 51 1 6, 50 20 6 7, 30 20 9 9, 7 32 10 5), (0 0 4 3, 1 1 2 3, 20 20 5 30))', 400));
SELECT '7', ST_astext(ST_Simplifyvw('POLYGON((0 0 3 2, 0 10 6 1, 0 51 1 6, 50 20 6 7, 30 20 9 9, 7 32 10 5, 0 0 3 2), (1 1 4 3, 1 3 2 3, 18 18 5 30, 1 1 4 3))', 200));
SELECT '8', ST_astext(ST_Simplifyvw('POLYGON((0 0 3 2, 0 10 6 1, 0 51 1 6, 50 20 6 7, 30 20 9 9, 7 32 10 5, 0 0 3 2), (1 1 4 3, 1 3 2 3, 18 18 5 30, 1 1 4 3))', 100));

SELECT '9', ST_astext(ST_Simplifyvw('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0),(5 5, 5 6, 6 6, 8 5, 5 5))', 20));
SELECT '10', ST_astext(ST_Simplifyvw('LINESTRING(0 0, 0 10)', 20));
SELECT '11', ST_astext(ST_Simplifyvw('MULTIPOLYGON(((100 100, 100 130, 130 130, 130 100, 100 100)), ((0 0, 10 0, 10 10, 0 10, 0 0),(5 5, 5 6, 6 6, 8 5, 5 5)) )', 20));
SELECT '12', ST_astext(ST_Simplifyvw('MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0),(5 5, 5 6, 6 6, 8 5, 5 5)),((100 100, 100 130, 130 130, 130 100, 100 100)))', 200));
