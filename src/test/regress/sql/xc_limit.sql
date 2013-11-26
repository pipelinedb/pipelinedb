--
-- XC_LIMIT
--

-- This file tests the LIMIT and OFFSET clause push down 
-- Since the LIMIT and OFFSET optimization can not be used when ORDER BY or
-- other clauses are present in the Query, we rely on count(*) and EXPLAIN
-- output to test correctness.
set enable_fast_query_shipping to off;
set enable_remotelimit to on;

-- Testcase 1: Replicated tables
create table xc_limit_tab1 (val int, val2 int) distribute by replication;
insert into xc_limit_tab1 values (generate_series(1, 10), generate_series(1, 10));
create table xc_limit_tab2 (val int, val2 int) distribute by replication;
insert into xc_limit_tab2 values (generate_series(1, 10), generate_series(1, 10));

-- simple shippable limit
select count(*) from (select * from xc_limit_tab1 limit 2) a;
explain (costs off, verbose on, nodes off) select * from xc_limit_tab1 limit 2;
select count(*) from (select * from xc_limit_tab1 limit 2 + length('some')) q;
explain (costs off, verbose on, nodes off) select * from xc_limit_tab1 limit 2 + length('some');
select count(*) from (select * from xc_limit_tab1 limit 2 offset 3) q;
explain (costs off, verbose on, nodes off) select * from xc_limit_tab1 limit 2 offset 3;
select count(*) from (select * from xc_limit_tab1 offset 3) q;
explain (costs off, verbose on, nodes off) select * from xc_limit_tab1 offset 3;
select count(*) from (select * from xc_limit_tab1 limit all) q;
explain (costs off, verbose on, nodes off) select * from xc_limit_tab1 limit all;
select count(*) from (select * from xc_limit_tab1 limit null) q;
explain (costs off, verbose on, nodes off) select * from xc_limit_tab1 limit null;
select count(*) from (select * from xc_limit_tab1 offset 0) q;
explain (costs off, verbose on, nodes off) select * from xc_limit_tab1 offset 0;

-- unshippable LIMIT or OFFSET clauses
explain (costs off, verbose on, nodes off) select * from xc_limit_tab1 limit random() * 10;
explain (costs off, verbose on, nodes off) select * from xc_limit_tab1 limit random() * 10;
-- in the presence of GROUP BY, ORDER BY etc.
select count(*) from (select count(*) from xc_limit_tab1 group by val2 % 4 limit 2) q;
explain (costs off, verbose on, nodes off) select count(*) from xc_limit_tab1 group by val2 % 4 limit 2;
select val, val2 from xc_limit_tab1 order by val2 limit 2;
explain (costs off, verbose on, nodes off) select val, val2 from xc_limit_tab1 order by val2 limit 2;

-- On top of JOIN tree
select count(*) from (select * from xc_limit_tab1 join xc_limit_tab2 using (val) limit 2) q;
explain (costs off, verbose on, nodes off) select * from xc_limit_tab1 join xc_limit_tab2 using (val) limit 2;

drop table xc_limit_tab1;
drop table xc_limit_tab2;

-- Testcase 2: distributed tables
create table xc_limit_tab1 (val int, val2 int);
insert into xc_limit_tab1 values (generate_series(1, 10), generate_series(1, 10));
create table xc_limit_tab2 (val int, val2 int);
insert into xc_limit_tab2 values (generate_series(1, 10), generate_series(1, 10));

-- simple shippable limit
select count(*) from (select * from xc_limit_tab1 limit 2) a;
explain (costs off, verbose on, nodes off) select * from xc_limit_tab1 limit 2;
select count(*) from (select * from xc_limit_tab1 limit 2 + length('some')) q;
explain (costs off, verbose on, nodes off) select * from xc_limit_tab1 limit 2 + length('some');
select count(*) from (select * from xc_limit_tab1 limit 2 offset 3) q;
explain (costs off, verbose on, nodes off) select * from xc_limit_tab1 limit 2 offset 3;
select count(*) from (select * from xc_limit_tab1 offset 3) q;
explain (costs off, verbose on, nodes off) select * from xc_limit_tab1 offset 3;
select count(*) from (select * from xc_limit_tab1 limit all) q;
explain (costs off, verbose on, nodes off) select * from xc_limit_tab1 limit all;
select count(*) from (select * from xc_limit_tab1 limit null) q;
explain (costs off, verbose on, nodes off) select * from xc_limit_tab1 limit null;
select count(*) from (select * from xc_limit_tab1 offset 0) q;
explain (costs off, verbose on, nodes off) select * from xc_limit_tab1 offset 0;

-- unshippable LIMIT or OFFSET clauses
explain (costs off, verbose on, nodes off) select * from xc_limit_tab1 limit random() * 10;
explain (costs off, verbose on, nodes off) select * from xc_limit_tab1 limit random() * 10;
-- in the presence of GROUP BY, ORDER BY etc.
select count(*) from (select count(*) from xc_limit_tab1 group by val2 % 4 limit 2) q;
explain (costs off, verbose on, nodes off) select count(*) from xc_limit_tab1 group by val2 % 4 limit 2;
select val, val2 from xc_limit_tab1 order by val2 limit 2;
explain (costs off, verbose on, nodes off) select val, val2 from xc_limit_tab1 order by val2 limit 2;

-- On top of JOIN tree
select count(*) from (select * from xc_limit_tab1 join xc_limit_tab2 using (val) limit 2) q;
explain (costs off, verbose on, nodes off) select * from xc_limit_tab1 join xc_limit_tab2 using (val) limit 2;

drop table xc_limit_tab1;
drop table xc_limit_tab2;

-- Test the working of GUC
create table xc_limit_tab1 (val int, val2 int) distribute by replication;
insert into xc_limit_tab1 values (generate_series(1, 10), generate_series(1, 10));
set enable_remotelimit to off;
select count(*) from (select * from xc_limit_tab1 limit 2) q;
explain (costs off, verbose on, nodes off) select * from xc_limit_tab1 limit 2;

drop table xc_limit_tab1;

-- Reset the GUC that we set here
reset enable_fast_query_shipping;
reset enable_remotelimit;
