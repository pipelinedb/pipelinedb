--this is the query to use to recompute what spatial_refs to exclude from backup
--it computes the where clause to put in mark_editable_objects.sql.in
WITH s AS -- our series
 (SELECT srid As n
 FROM spatial_ref_sys
 ),
 -- get start ranges (numbers where next is not next + 1)
n1 AS (SELECT n AS start_n
FROM s
EXCEPT
SELECT n + 1 AS start_n
FROM s),
-- for each start range find the next start range
n2 AS (SELECT n1.start_n, lead(start_n) OVER (ORDER BY start_n) As next_set_n
FROM n1 
GROUP BY n1.start_n),
-- determine end range for each start
-- end range is the last number that is before start of next range
n3 As (SELECT start_n, MAX(COALESCE(s.n,start_n)) As end_n
FROM n2 LEFT JOIN s ON( s.n >= n2.start_n AND s.n < n2.next_set_n)
GROUP BY start_n, next_set_n
ORDER BY start_n)
SELECT 'NOT (' || string_agg('srid BETWEEN ' || start_n || ' AND ' || end_n, ' OR ' ORDER BY start_n) || ') '
FROM n3 ;