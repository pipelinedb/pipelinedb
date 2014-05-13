--
-- XC_FQS
--

-- This file contains tests for Fast Query Shipping (FQS) for queries involving
-- a single table

-- Testset 1 for distributed table (by roundrobin)
select create_table_nodes('tab1_rr(val int, val2 int)', '{1, 2, 3}'::int[], 'roundrobin', NULL);
insert into tab1_rr values (1, 2);
insert into tab1_rr values (2, 4);
insert into tab1_rr values (5, 3);
insert into tab1_rr values (7, 8);
insert into tab1_rr values (9, 2);
explain (costs off, verbose on, nodes off, num_nodes on) insert into tab1_rr values (9, 2);
-- simple select
-- should get FQSed
select val, val2 + 2, case val when val2 then 'val and val2 are same' else 'val and val2 are not same' end from tab1_rr where val2 = 4;
explain (costs off, verbose on, nodes off) select val, val2 + 2, case val when val2 then 'val and val2 are same' else 'val and val2 are not same' end from tab1_rr where val2 = 4;
-- should get FQSed even within a subquery
select * from (select * from tab1_rr where val2 = 4) t1;
explain (costs off, verbose on, nodes off)
	select * from (select * from tab1_rr where val2 = 4) t1;
-- should not get FQSed because of aggregates
select sum(val), avg(val), count(*) from tab1_rr;
explain (costs off, verbose on, nodes off) select sum(val), avg(val), count(*) from tab1_rr;
-- should not get FQSed because of window functions
select first_value(val) over (partition by val2 order by val) from tab1_rr;
explain (costs off, verbose on, nodes off) select first_value(val) over (partition by val2 order by val) from tab1_rr;
-- should not get FQSed because of LIMIT clause
select * from tab1_rr where val2 = 3 limit 1;
explain (costs off, verbose on, nodes off) select * from tab1_rr where val2 = 3 limit 1;
-- should not FQSed because of OFFSET clause
select * from tab1_rr where val2 = 4 offset 1;
explain (costs off, verbose on, nodes off) select * from tab1_rr where val2 = 4 offset 1;
-- should not get FQSed because of SORT clause
select * from tab1_rr order by val;
explain (costs off, verbose on, nodes off) select * from tab1_rr order by val;
-- should not get FQSed because of DISTINCT clause
select distinct val, val2 from tab1_rr where val2 = 8;
explain (costs off, verbose on, nodes off) select distinct val, val2 from tab1_rr where val2 = 8;
-- should not get FQSed because of GROUP clause
select val, val2 from tab1_rr where val2 = 8 group by val, val2;
explain (costs off, verbose on, nodes off) select val, val2 from tab1_rr where val2 = 8 group by val, val2;
-- should not get FQSed because of presence of aggregates and HAVING clause,
select sum(val) from tab1_rr where val2 = 2 group by val2 having sum(val) > 1;
explain (costs off, verbose on, nodes off) select sum(val) from tab1_rr where val2 = 2 group by val2 having sum(val) > 1;

-- tests for node reduction by application of quals, for roundrobin node
-- reduction is not applicable. Having query not FQSed because of existence of ORDER BY,
-- implies that nodes did not get reduced.
select * from tab1_rr where val = 7;
explain (costs off, verbose on, nodes off) select * from tab1_rr where val = 7;
select * from tab1_rr where val = 7 or val = 2 order by val;
explain (costs off, verbose on, nodes off) select * from tab1_rr where val = 7 or val = 2 order by val;
select * from tab1_rr where val = 7 and val2 = 8;
explain (costs off, verbose on, nodes off) select * from tab1_rr where val = 7 and val2 = 8 order by val;
select * from tab1_rr where val = 3 + 4 and val2 = 8 order by val;
explain (costs off, verbose on, nodes off) select * from tab1_rr where val = 3 + 4 order by val;
select * from tab1_rr where val = char_length('len')+4 order by val;
explain (costs off, verbose on, nodes off) select * from tab1_rr where val = char_length('len')+4 order by val;
-- insert some more values 
insert into tab1_rr values (7, 2); 
select avg(val) from tab1_rr where val = 7;
explain (costs off, verbose on, nodes off) select avg(val) from tab1_rr where val = 7;
select val, val2 from tab1_rr where val = 7 order by val2;
explain (costs off, verbose on, nodes off) select val, val2 from tab1_rr where val = 7 order by val2;
select distinct val2 from tab1_rr where val = 7 ORDER BY 1;
explain (costs off, verbose on, nodes off) select distinct val2 from tab1_rr where val = 7;
-- DMLs
update tab1_rr set val2 = 1000 where val = 7; 
explain (costs off, verbose on, nodes off) update tab1_rr set val2 = 1000 where val = 7; 
select * from tab1_rr where val = 7;
delete from tab1_rr where val = 7; 
explain (costs off, verbose on, nodes off) delete from tab1_rr where val = 7; 
select * from tab1_rr where val = 7;

-- Testset 2 for distributed tables (by hash)
select create_table_nodes('tab1_hash(val int, val2 int)', '{1, 2, 3}'::int[], 'hash(val)', NULL);
insert into tab1_hash values (1, 2);
insert into tab1_hash values (2, 4);
insert into tab1_hash values (5, 3);
insert into tab1_hash values (7, 8);
insert into tab1_hash values (9, 2);
explain (costs off, verbose on, nodes off) insert into tab1_hash values (9, 2);
-- simple select
-- should get FQSed
select val, val2 + 2, case val when val2 then 'val and val2 are same' else 'val and val2 are not same' end from tab1_hash where val2 = 4;
explain (costs off, verbose on, nodes off) select val, val2 + 2, case val when val2 then 'val and val2 are same' else 'val and val2 are not same' end from tab1_hash where val2 = 2;
-- should not get FQSed because of aggregates
select sum(val), avg(val), count(*) from tab1_hash;
explain (costs off, verbose on, nodes off) select sum(val), avg(val), count(*) from tab1_hash;
-- should not get FQSed because of window functions
select first_value(val) over (partition by val2 order by val) from tab1_hash;
explain (costs off, verbose on, nodes off) select first_value(val) over (partition by val2 order by val) from tab1_hash;
-- should not get FQSed because of LIMIT clause
select * from tab1_hash where val2 = 3 limit 1;
explain (costs off, verbose on, nodes off) select * from tab1_hash where val2 = 3 limit 1;
-- should not FQSed because of OFFSET clause
select * from tab1_hash where val2 = 4 offset 1;
explain (costs off, verbose on, nodes off) select * from tab1_hash where val2 = 4 offset 1;
-- should not get FQSed because of SORT clause
select * from tab1_hash order by val;
explain (costs off, verbose on, nodes off) select * from tab1_hash order by val;
-- should get FQSed because of DISTINCT clause with distribution column in it
select distinct val, val2 from tab1_hash where val2 = 8;
explain (costs off, verbose on, nodes off) select distinct val, val2 from tab1_hash where val2 = 8;
-- should get FQSed because of GROUP clause with distribution column in it
select val, val2 from tab1_hash where val2 = 8 group by val, val2;
explain (costs off, verbose on, nodes off) select val, val2 from tab1_hash where val2 = 8 group by val, val2;
-- should not get FQSed because of DISTINCT clause
select distinct on (val2) val, val2 from tab1_hash where val2 = 8;
explain (costs off, verbose on, nodes off) select distinct on (val2) val, val2 from tab1_hash where val2 = 8;
-- should not get FQSed because of presence of aggregates and HAVING clause
-- withour distribution column in GROUP BY clause
select sum(val) from tab1_hash where val2 = 2 group by val2 having sum(val) > 1;
explain (costs off, verbose on, nodes off) select sum(val) from tab1_hash where val2 = 2 group by val2 having sum(val) > 1;

-- tests for node reduction by application of quals. Having query FQSed because of
-- existence of ORDER BY, implies that nodes got reduced.
select * from tab1_hash where val = 7;
explain (costs off, verbose on, nodes off, num_nodes on) select * from tab1_hash where val = 7;
select * from tab1_hash where val = 7 or val = 2 order by val;
explain (costs off, verbose on, nodes off) select * from tab1_hash where val = 7 or val = 2 order by val;
select * from tab1_hash where val = 7 and val2 = 8;
explain (costs off, verbose on, nodes off, num_nodes on) select * from tab1_hash where val = 7 and val2 = 8;
select * from tab1_hash where val = 3 + 4 and val2 = 8;
explain (costs off, verbose on, nodes off, num_nodes on) select * from tab1_hash where val = 3 + 4;
select * from tab1_hash where val = char_length('len')+4;
explain (costs off, verbose on, nodes off, num_nodes on) select * from tab1_hash where val = char_length('len')+4;
-- insert some more values 
insert into tab1_hash values (7, 2); 
select avg(val) from tab1_hash where val = 7;
explain (costs off, verbose on, nodes off, num_nodes on) select avg(val) from tab1_hash where val = 7;
select val, val2 from tab1_hash where val = 7 order by val2;
explain (costs off, verbose on, nodes off, num_nodes on) select val, val2 from tab1_hash where val = 7 order by val2;
select distinct val2 from tab1_hash where val = 7;
explain (costs off, verbose on, nodes off, num_nodes on) select distinct val2 from tab1_hash where val = 7;
-- FQS for subqueries
select * from (select avg(val) from tab1_hash where val = 7) t1;
explain (costs off, verbose on, nodes off, num_nodes on)
	select * from (select avg(val) from tab1_hash where val = 7) t1;
-- DMLs
update tab1_hash set val2 = 1000 where val = 7; 
explain (costs off, verbose on, nodes off) update tab1_hash set val2 = 1000 where val = 7; 
select * from tab1_hash where val = 7;
delete from tab1_hash where val = 7; 
explain (costs off, verbose on, nodes off) delete from tab1_hash where val = 7; 
select * from tab1_hash where val = 7;

-- Testset 3 for distributed tables (by modulo)
select create_table_nodes('tab1_modulo(val int, val2 int)', '{1, 2, 3}'::int[], 'modulo(val)', NULL);
insert into tab1_modulo values (1, 2);
insert into tab1_modulo values (2, 4);
insert into tab1_modulo values (5, 3);
insert into tab1_modulo values (7, 8);
insert into tab1_modulo values (9, 2);
explain (costs off, verbose on, nodes off) insert into tab1_modulo values (9, 2);
-- simple select
-- should get FQSed
select val, val2 + 2, case val when val2 then 'val and val2 are same' else 'val and val2 are not same' end from tab1_modulo where val2 = 4;
explain (costs off, verbose on, nodes off) select val, val2 + 2, case val when val2 then 'val and val2 are same' else 'val and val2 are not same' end from tab1_modulo where val2 = 4;
-- should not get FQSed because of aggregates
select sum(val), avg(val), count(*) from tab1_modulo;
explain (costs off, verbose on, nodes off) select sum(val), avg(val), count(*) from tab1_modulo;
-- should not get FQSed because of window functions
select first_value(val) over (partition by val2 order by val) from tab1_modulo;
explain (costs off, verbose on, nodes off) select first_value(val) over (partition by val2 order by val) from tab1_modulo;
-- should not get FQSed because of LIMIT clause
select * from tab1_modulo where val2 = 3 limit 1;
explain (costs off, verbose on, nodes off) select * from tab1_modulo where val2 = 3 limit 1;
-- should not FQSed because of OFFSET clause
select * from tab1_modulo where val2 = 4 offset 1;
explain (costs off, verbose on, nodes off) select * from tab1_modulo where val2 = 4 offset 1;
-- should not get FQSed because of SORT clause
select * from tab1_modulo order by val;
explain (costs off, verbose on, nodes off) select * from tab1_modulo order by val;
-- should get FQSed because of DISTINCT clause with distribution column in it
select distinct val, val2 from tab1_modulo where val2 = 8;
explain (costs off, verbose on, nodes off) select distinct val, val2 from tab1_modulo where val2 = 8;
-- should get FQSed because of GROUP clause with distribution column in it
select val, val2 from tab1_modulo where val2 = 8 group by val, val2;
explain (costs off, verbose on, nodes off) select val, val2 from tab1_modulo where val2 = 8 group by val, val2;
-- should not get FQSed because of DISTINCT clause without distribution column
-- in it
select distinct on (val2) val, val2 from tab1_modulo where val2 = 8;
explain (costs off, verbose on, nodes off) select distinct on (val2) val, val2 from tab1_modulo where val2 = 8;
-- should not get FQSed because of presence of aggregates and HAVING clause
-- without distribution column in GROUP BY clause
select sum(val) from tab1_modulo where val2 = 2 group by val2 having sum(val) > 1;
explain (costs off, verbose on, nodes off) select sum(val) from tab1_modulo where val2 = 2 group by val2 having sum(val) > 1;

-- tests for node reduction by application of quals. Having query FQSed because of
-- existence of ORDER BY, implies that nodes got reduced.
select * from tab1_modulo where val = 7;
explain (costs off, verbose on, nodes off, num_nodes on) select * from tab1_modulo where val = 7;
select * from tab1_modulo where val = 7 or val = 2 order by val;
explain (costs off, verbose on, nodes off) select * from tab1_modulo where val = 7 or val = 2 order by val;
select * from tab1_modulo where val = 7 and val2 = 8;
explain (costs off, verbose on, nodes off, num_nodes on) select * from tab1_modulo where val = 7 and val2 = 8;
select * from tab1_modulo where val = 3 + 4 and val2 = 8;
explain (costs off, verbose on, nodes off, num_nodes on) select * from tab1_modulo where val = 3 + 4;
select * from tab1_modulo where val = char_length('len')+4;
explain (costs off, verbose on, nodes off, num_nodes on) select * from tab1_modulo where val = char_length('len')+4;
-- insert some more values 
insert into tab1_modulo values (7, 2); 
select avg(val) from tab1_modulo where val = 7;
explain (costs off, verbose on, nodes off, num_nodes on) select avg(val) from tab1_modulo where val = 7;
select val, val2 from tab1_modulo where val = 7 order by val2;
explain (costs off, verbose on, nodes off, num_nodes on) select val, val2 from tab1_modulo where val = 7 order by val2;
select distinct val2 from tab1_modulo where val = 7;
explain (costs off, verbose on, nodes off, num_nodes on) select distinct val2 from tab1_modulo where val = 7;
-- FQS for subqueries
select * from (select avg(val) from tab1_modulo where val = 7) t1;
explain (costs off, verbose on, nodes off, num_nodes on)
	select * from (select avg(val) from tab1_modulo where val = 7) t1;
-- DMLs
update tab1_modulo set val2 = 1000 where val = 7; 
explain (costs off, verbose on, nodes off) update tab1_modulo set val2 = 1000 where val = 7; 
select * from tab1_modulo where val = 7;
delete from tab1_modulo where val = 7; 
explain (costs off, verbose on, nodes off) delete from tab1_modulo where val = 7; 
select * from tab1_modulo where val = 7;

-- Testset 4 for replicated tables, for replicated tables, unless the expression
-- is itself unshippable, any query involving a single replicated table is shippable
select create_table_nodes('tab1_replicated(val int, val2 int)', '{1, 2, 3}'::int[], 'replication', NULL);
insert into tab1_replicated values (1, 2);
insert into tab1_replicated values (2, 4);
insert into tab1_replicated values (5, 3);
insert into tab1_replicated values (7, 8);
insert into tab1_replicated values (9, 2);
explain (costs off, verbose on, nodes off) insert into tab1_replicated values (9, 2);
-- simple select
select * from tab1_replicated;
explain (costs off, num_nodes on, verbose on, nodes off) select * from tab1_replicated;
select sum(val), avg(val), count(*) from tab1_replicated;
explain (costs off, num_nodes on, verbose on, nodes off) select sum(val), avg(val), count(*) from tab1_replicated;
select first_value(val) over (partition by val2 order by val) from tab1_replicated;
explain (costs off, num_nodes on, verbose on, nodes off) select first_value(val) over (partition by val2 order by val) from tab1_replicated;
select * from tab1_replicated where val2 = 2 limit 2;
explain (costs off, num_nodes on, verbose on, nodes off) select * from tab1_replicated where val2 = 2 limit 2;
select * from tab1_replicated where val2 = 4 offset 1;
explain (costs off, num_nodes on, verbose on, nodes off) select * from tab1_replicated where val2 = 4 offset 1;
select * from tab1_replicated order by val;
explain (costs off, num_nodes on, verbose on, nodes off) select * from tab1_replicated order by val;
select distinct val, val2 from tab1_replicated;
explain (costs off, num_nodes on, verbose on, nodes off) select distinct val, val2 from tab1_replicated;
select val, val2 from tab1_replicated group by val, val2;
explain (costs off, num_nodes on, verbose on, nodes off) select val, val2 from tab1_replicated group by val, val2;
select sum(val) from tab1_replicated group by val2 having sum(val) > 1;
explain (costs off, num_nodes on, verbose on, nodes off) select sum(val) from tab1_replicated group by val2 having sum(val) > 1;
-- FQS for subqueries
select * from (select sum(val), val2 from tab1_replicated group by val2 order by val2) t1;
explain (costs off, verbose on, nodes off, num_nodes on)
	select * from (select sum(val), val2 from tab1_replicated group by val2 order by val2) t1;
-- DMLs
update tab1_replicated set val2 = 1000 where val = 7; 
explain (costs off, verbose on, nodes off) update tab1_replicated set val2 = 1000 where val = 7; 
select * from tab1_replicated where val = 7;
delete from tab1_replicated where val = 7; 
explain (costs off, verbose on, nodes off) delete from tab1_replicated where val = 7; 
select * from tab1_replicated where val = 7;

drop table tab1_rr;
drop table tab1_hash;
drop table tab1_modulo;
drop table tab1_replicated;
