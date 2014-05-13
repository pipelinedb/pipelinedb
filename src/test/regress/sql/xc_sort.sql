--
-- XC_SORT
--

-- This file contains tests for Merge Sort optimization for Postgres-XC. In this
-- optimization if possible the data is fetched ordered from the Datanodes and
-- merged at the Coordinator.

set enable_fast_query_shipping to off;

-- Testset 1 for distributed table (use hash tables so that node reduction can
-- be tested)
select create_table_nodes('xc_sort1_hash(val int, val2 int)', '{1, 2, 3}'::int[], 'hash(val)', NULL);
select create_table_nodes('xc_sort2_hash(val int, val2 int)', '{1, 2, 3}'::int[], 'hash(val)', NULL);
select create_table_nodes('xc_sort1_rep(val int, val2 int)', '{1}'::int[], 'replication', NULL);
select create_table_nodes('xc_sort2_rep(val int, val2 int)', '{1, 2}'::int[], 'replication', NULL);
insert into xc_sort1_hash values (1, 2), (2, 4), (5, 3), (7, 8), (9, 2), (1, 3), (5, 10);
insert into xc_sort2_hash values (1, 2), (2, 4), (5, 3), (7, 8), (9, 2), (1, 3), (5, 10);
insert into xc_sort1_rep values (1, 2), (2, 4), (5, 3), (7, 8), (9, 2), (1, 3), (5, 10);
insert into xc_sort2_rep values (1, 2), (2, 4), (5, 3), (7, 8), (9, 2), (1, 3), (5, 10);
-- simple test
select * from xc_sort1_hash order by val, val2;
explain (costs off, verbose on, nodes off) select * from xc_sort1_hash order by val, val2;
select val::char(3), val2 from xc_sort1_hash order by val, val2;
explain (costs off, verbose on, nodes off) select val::char(3), val2 from xc_sort1_hash order by val, val2;
select sum(val), val2 from xc_sort1_hash group by val2 order by sum(val);
explain (costs off, verbose on, nodes off) select sum(val), val2 from xc_sort1_hash group by val2 order by sum(val);
-- No need for sorting on the Coordinator, there will be only one node involved
select * from xc_sort1_hash where val = 5 order by val2;
explain (costs off, verbose on, nodes off) select * from xc_sort1_hash where val = 5 order by val2;
-- pushable JOINs
select * from xc_sort1_hash natural join xc_sort2_hash order by val, val2;
explain (costs off, verbose on, nodes off) select * from xc_sort1_hash natural join xc_sort2_hash order by val, val2;
-- unshippable sort tests
select sum(val) over w, array_agg(val) over w from xc_sort1_hash window w as (order by val, val2 rows 2 preceding) order by 1; 
explain (costs off, verbose on, nodes off) select sum(val) over w, array_agg(val) over w from xc_sort1_hash window w as (order by val, val2 rows 2 preceding) order by 1; 
-- non-pushable JOINs
select * from xc_sort1_hash join xc_sort2_hash using (val2) order by xc_sort1_hash.val, xc_sort2_hash.val, xc_sort2_hash.val2; 
explain (costs off, verbose on, nodes off) select * from xc_sort1_hash join xc_sort2_hash using (val2) order by xc_sort1_hash.val, xc_sort2_hash.val, xc_sort2_hash.val2; 
-- Test 2 replicated tables (We shouldn't need covering Sort except when the
-- underlying plan is not shippable)
-- simple test
select * from xc_sort1_rep order by val, val2;
explain (costs off, verbose on, nodes off) select * from xc_sort1_rep order by val, val2;
select val::char(3), val2 from xc_sort1_rep order by val, val2;
explain (costs off, verbose on, nodes off) select val::char(3), val2 from xc_sort1_rep order by val, val2;
select sum(val), val2 from xc_sort1_rep group by val2 order by sum(val);
explain (costs off, verbose on, nodes off) select sum(val), val2 from xc_sort1_rep group by val2 order by sum(val);
-- pushable JOINs
select * from xc_sort1_rep natural join xc_sort2_rep order by val, val2;
explain (costs off, verbose on, nodes off) select * from xc_sort1_rep natural join xc_sort2_rep order by val, val2;
-- unshippable sort tests
select sum(val) over w, array_agg(val) over w from xc_sort1_rep window w as (order by val, val2 rows 2 preceding) order by 1; 
explain (costs off, verbose on, nodes off) select sum(val) over w, array_agg(val) over w from xc_sort1_rep window w as (order by val, val2 rows 2 preceding) order by 1; 
-- non-pushable JOINs
select * from xc_sort1_rep join xc_sort2_hash using (val2) order by xc_sort1_rep.val, xc_sort2_hash.val, xc_sort2_hash.val2; 
explain (costs off, verbose on, nodes off) select * from xc_sort1_rep join xc_sort2_hash using (val2) order by xc_sort1_rep.val, xc_sort2_hash.val, xc_sort2_hash.val2; 
-- Test 3 the GUC
set enable_remotesort to off;
select * from xc_sort1_hash order by val, val2; 
explain (costs off, verbose on, nodes off) select * from xc_sort1_hash order by val, val2;  

drop table xc_sort1_hash;
drop table xc_sort2_hash;
drop table xc_sort1_rep;
drop table xc_sort2_rep;

reset enable_fast_query_shipping;
reset enable_remotesort;
