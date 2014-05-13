--
-- XC_FOR_UPDATE
--

set enable_fast_query_shipping=true;

-- create some tables
create table t1(val int, val2 int);
create table t2(val int, val2 int);
create table t3(val int, val2 int);

create table p1(a int, b int);
create table c1(d int, e int) inherits (p1);

-- insert some rows in them
insert into t1 values(1,11),(2,11);
insert into t2 values(3,11),(4,11);
insert into t3 values(5,11),(6,11);

insert into p1 values(55,66),(77,88);
insert into c1 values(111,222,333,444),(123,345,567,789);

select * from t1 order by val;
select * from t2 order by val;
select * from t3 order by val;
select * from p1 order by a;
select * from c1 order by a;

-- create a view too
create view v1 as select * from t1 for update;

-- test a few queries with row marks
select * from t1 order by 1 for update of t1 nowait;
select * from t1, t2, t3 order by 1 for update;

select * from v1 order by val;
WITH q1 AS (SELECT * from t1 order by 1 FOR UPDATE) SELECT * FROM q1,t2 order by 1 FOR UPDATE;

WITH q1 AS (SELECT * from t1 order by 1) SELECT * FROM q1;
WITH q1 AS (SELECT * from t1 order by 1) SELECT * FROM q1 FOR UPDATE;
WITH q1 AS (SELECT * from t1 order by 1 FOR UPDATE) SELECT * FROM q1 FOR UPDATE;


-- confirm that in various join scenarios for update gets to the remote query
-- single table case
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1 for update of t1 nowait;

-- two table case
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1, t2 where t1.val = t2.val for update nowait;
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1, t2 where t1.val = t2.val for update;
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1, t2 where t1.val = t2.val for share;
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1, t2 where t1.val = t2.val;
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1, t2;
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1, t2 for update;
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1, t2 for update nowait;
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1, t2 for share nowait;
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1, t2 for share;
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1, t2 for share of t2;

-- three table case
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1, t2, t3;
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1, t2, t3 for update;
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1, t2, t3 for update of t1;
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1, t2, t3 for update of t1,t3;
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1, t2, t3 for update of t1,t3 nowait;
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1, t2, t3 for share of t1,t2 nowait;

-- check a few subquery cases
explain (costs off, num_nodes off, nodes off, verbose on)  select * from (select * from t1 for update of t1 nowait) as foo;
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1 where val in (select val from t2 for update of t2 nowait) for update;
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1 where val in (select val from t2 for update of t2 nowait);

-- test multiple row marks
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1, t2 for share of t2 for update of t1;
-- make sure FOR UPDATE takes prioriy over FOR SHARE when mentioned for the same table
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1 for share of t1 for update of t1;
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1 for update of t1 for share of t1;
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1 for share of t1 for share of t1 for update of t1;
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1 for share of t1 for share of t1 for share of t1;
-- make sure NOWAIT is used in remote query even if it is not mentioned with FOR UPDATE clause
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1 for share of t1 for share of t1 nowait for update of t1;
-- same table , different aliases and different row marks for different aliases
explain (costs off, num_nodes off, nodes off, verbose on)  select * from t1 a,t1 b for share of a for update of b;

-- test WITH queries
-- join of a WITH table and a normal table
explain (costs off, num_nodes off, nodes off, verbose on)  WITH q1 AS (SELECT * from t1 FOR UPDATE) SELECT * FROM q1,t2 FOR UPDATE;

explain (costs off, num_nodes off, nodes off, verbose on)  WITH q1 AS (SELECT * from t1) SELECT * FROM q1;
-- make sure row marks are no ops for queries on WITH tables
explain (costs off, num_nodes off, nodes off, verbose on)  WITH q1 AS (SELECT * from t1) SELECT * FROM q1 FOR UPDATE;
explain (costs off, num_nodes off, nodes off, verbose on)  WITH q1 AS (SELECT * from t1 FOR UPDATE) SELECT * FROM q1 FOR UPDATE;

-- test case of inheried tables
select * from p1 order by 1 for update;
explain (costs off, num_nodes off, nodes off, verbose on)  select * from p1 for update;

select * from c1 order by 1 for update;
explain (costs off, num_nodes off, nodes off, verbose on)  select * from c1 for update;

-- drop objects created
drop table c1;
drop table p1;
drop view v1;
drop table t1;
drop table t2;
drop table t3;

-------------------------------------------
-- Tests for concurrent transactions
-------------------------------------------

-- create some tables
create table mytab1(val int, val2 int, val3 int);
create table mytab3(val int, val2 int);
create table mytab4(val int, val2 int);

-- create index,rule,trgger & view on one of the tables
CREATE UNIQUE INDEX test_idx ON mytab1 (val);

CREATE RULE test_rule AS ON INSERT TO mytab1 DO ALSO SELECT * FROM mytab3;

CREATE VIEW v1 as select * from mytab1 where val = 2;

-- insert some rows
insert into mytab1 values(1,11,1122),(2,11,3344);

-------------------------------------------
-- Case 1 where we have a SELECT FOR UPDATE
-------------------------------------------

-- A transaction that holds ACCESS EXCLUSIVE lock on a table can later acquire ACCESS SHARE lock on the same table
begin;
declare c1 cursor for select * from mytab1 for update;
fetch 1 from c1;
declare c2 cursor for select * from mytab1 for share;
fetch 1 from c2;
end;

-- prepare a transaction that holds a ACCESS EXCLUSIVE (ROW SHARE) lock on a table
begin;
declare c1 cursor for select * from mytab1 for update;
fetch 1 from c1;
prepare transaction 'tbl_mytab1_locked';




set statement_timeout to 1000;

-- When a transaction holds a ACCESS EXCLUSIVE (ROW SHARE) lock on a table (Like a SELECT FOR UPDATE would do)
-- Can onther transaction do these to the same table

--  1. select some rows (Should pass)
       select * from mytab1 order by 1;
--  2. insert a row (Should pass)
       insert into mytab1 values(123,456);
--  3. update a row (Should fail)
       update mytab1 set val2=33 where val = 1;
--  4. delete a row
--     Newly Inserted (Should pass)
       delete from mytab1 where val2=456;
--     Previously Inserted (Should fail)
       delete from mytab1 where val=1;
--  5. inherit form it (Should pass)
       create table chld_mytab1(d int, e int) inherits (mytab1);
--  6. create a view on it (Should pass)
       create view v2 as select * from mytab1 where val = 1;
--  7. comment on it (Should pass)
       comment on table mytab1 is 'Hello table';
--  8. Alter it (Should fail)
       alter table mytab1 drop column val2;
--  9. drop it (Should fail)
       drop table mytab1;
-- 10. vacuum it (Should pass)
       vacuum mytab1;
-- 11. obtain any of these locks on it
--     ACCESS SHARE (Should pass)
       begin;
         lock table mytab1 in ACCESS SHARE mode nowait;
       end;
--     ROW SHARE (Should pass)
       begin;
         lock table mytab1 in ROW SHARE mode nowait;
       end;
--     ROW EXCLUSIVE (Should pass)
       begin;
         lock table mytab1 in ROW EXCLUSIVE mode nowait;
       end;
--     SHARE UPDATE EXCLUSIVE (Should pass)
       begin;
         lock table mytab1 in SHARE UPDATE EXCLUSIVE mode nowait;
       end;
--     SHARE (Should pass)
       begin;
         lock table mytab1 in SHARE mode nowait;
       end;
--     SHARE ROW EXCLUSIVE (Should pass)
       begin;
         lock table mytab1 in SHARE ROW EXCLUSIVE mode nowait;
       end;
--     EXCLUSIVE (Should fail)
       begin;
         lock table mytab1 in EXCLUSIVE mode nowait;
       end;
--     ACCESS EXCLUSIVE (Should fail)
       begin;
         lock table mytab1 in ACCESS EXCLUSIVE mode nowait;
       end;
-- 12. do a SELECT FOR SHARE on it (Should fail)
       begin;
         declare c1 cursor for select * from mytab1 for share nowait;
         fetch 1 from c1;
       end;
-- 13. do a SELECT FOR UPDATE on it (Should fail)
       begin;
         declare c1 cursor for select * from mytab1 for update nowait;
         fetch 1 from c1;
       end;
-- 14. alter already defined index on it (Should fail)
       ALTER INDEX test_idx RENAME TO mytab1_idx;
-- 15. alter already defined view on it (Should pass)
       ALTER VIEW v1 RENAME TO vv1;
-- 16. drop already defined index on it (Should fail)
       drop index test_idx;
-- 17. drop already defined rule on it (Should fail)
       drop rule test_rule on mytab1;
-- 18. drop already defined view on it (Should pass)
       drop view vv1;
-- 19. reindex an alredy defined index on it (Should fail)
       reindex index test_idx;
-- 20. truncate it (Should fail)
       truncate table mytab1;
-- 21. create rule on it (Should fail)
       CREATE RULE test_rule2 AS ON INSERT TO mytab1 DO ALSO SELECT * FROM mytab1_insert_log;

-- clean up
COMMIT PREPARED 'tbl_mytab1_locked';

drop table chld_mytab1;
drop view v2;

-- create the view again to carry out the next test case

CREATE VIEW v1 as select * from mytab1 where val = 2;

-------------------------------------------
-- Case 2 where we have a SELECT FOR SHARE
-------------------------------------------

-- A transaction that holds ACCESS SHARE lock on a table can later acquire ACCESS EXCLUSIVE lock on the same table
begin;
declare c1 cursor for select * from mytab1 for share;
fetch 1 from c1;
declare c2 cursor for select * from mytab1 for update;
fetch 1 from c2;
end;

-- prepare a transaction that holds a ACCESS SHARE (ROW SHARE) lock on a table
begin;
declare c1 cursor for select * from mytab1 for share;
fetch 1 from c1;
prepare transaction 'tbl_mytab1_locked';


set statement_timeout to 1000;

-- When a transaction holds a ACCESS SHARE (ROW SHARE) lock on a table (Like a SELECT FOR SHARE would do)
-- Can onther transaction do these to the same table

--  1. select some rows (Should pass)
       select * from mytab1 order by 1 ;
--  2. insert a row (Should pass)
       insert into mytab1 values(123,456);
--  3. update a row (Should fail)
       update mytab1 set val2=33 where val = 1;
--  4. delete a row
--     Newly Inserted (Should pass)
       delete from mytab1 where val2=456;
--     Previously Inserted (Should fail)
       delete from mytab1 where val=1;
--  5. inherit form it (Should pass)
       create table chld_mytab1(d int, e int) inherits (mytab1);
--  6. create a view on it (Should pass)
       create view v2 as select * from mytab1 where val = 1;
--  7. comment on it (Should pass)
       comment on table mytab1 is 'Hello table';
--  8. Alter it (Should fail)
       alter table mytab1 drop column val2;
--  9. drop it (Should fail)
       drop table mytab1;
-- 10. vacuum it (Should pass)
       vacuum mytab1;
-- 11. obtain any of these locks on it
--     ACCESS SHARE (Should pass)
       begin;
         lock table mytab1 in ACCESS SHARE mode nowait;
       end;
--     ROW SHARE (Should pass)
       begin;
         lock table mytab1 in ROW SHARE mode nowait;
       end;
--     ROW EXCLUSIVE (Should pass)
       begin;
         lock table mytab1 in ROW EXCLUSIVE mode nowait;
       end;
--     SHARE UPDATE EXCLUSIVE (Should pass)
       begin;
         lock table mytab1 in SHARE UPDATE EXCLUSIVE mode nowait;
       end;
--     SHARE (Should pass)
       begin;
         lock table mytab1 in SHARE mode nowait;
       end;
--     SHARE ROW EXCLUSIVE (Should pass)
       begin;
         lock table mytab1 in SHARE ROW EXCLUSIVE mode nowait;
       end;
--     EXCLUSIVE (Should fail)
       begin;
         lock table mytab1 in EXCLUSIVE mode nowait;
       end;
--     ACCESS EXCLUSIVE (Should fail)
       begin;
         lock table mytab1 in ACCESS EXCLUSIVE mode nowait;
       end;
-- 12. do a SELECT FOR SHARE on it (Should pass) 
--     This is the difference between FOR SHARE & FOR UPDATE, This test should pass in case of FOR SHARE, but fail in case of FOR UPDATE
       begin;
         declare c1 cursor for select * from mytab1 for share nowait;
         fetch 1 from c1;
       end;
-- 13. do a SELECT FOR UPDATE on it (Should fail)
       begin;
         declare c1 cursor for select * from mytab1 for update nowait;
         fetch 1 from c1;
       end;
-- 14. alter already defined index on it (Should fail)
       ALTER INDEX test_idx RENAME TO mytab1_idx;
-- 15. alter already defined view on it (Should pass)
       ALTER VIEW v1 RENAME TO vv1;
-- 16. drop already defined index on it (Should fail)
       drop index test_idx;
-- 17. drop already defined rule on it (Should fail)
       drop rule test_rule on mytab1;
-- 18. drop already defined view on it (Should pass)
       drop view vv1;
-- 19. reindex an alredy defined index on it (Should fail)
       reindex index test_idx;
-- 20. truncate it (Should fail)
       truncate table mytab1;
-- 21. create rule on it (Should fail)
       CREATE RULE test_rule2 AS ON INSERT TO mytab1 DO ALSO SELECT * FROM mytab1_insert_log;

-- clean up
COMMIT PREPARED 'tbl_mytab1_locked';

drop table chld_mytab1;
drop view v2;

drop table mytab1 cascade;
drop table mytab4 cascade;
drop table mytab3 cascade;

-- Test to make sure prepared transactions are working as expected
-- If a transcation is preared and contains only a select with for share/update, it should be preapred on data nodes

-- create some tables
create table t1(val int, val2 int) DISTRIBUTE BY REPLICATION;
create table t2(val int, val2 int) DISTRIBUTE BY REPLICATION;
create table t3(val int, val2 int) DISTRIBUTE BY REPLICATION;

create table p1(a int, b int) DISTRIBUTE BY REPLICATION;
create table c1(d int, e int) inherits (p1) DISTRIBUTE BY REPLICATION;

-- insert some rows in them
insert into t1 values(1,11),(2,11);
insert into t2 values(3,11),(4,11);
insert into t3 values(5,11),(6,11);

insert into p1 values(55,66),(77,88);
insert into c1 values(111,222,333,444),(123,345,567,789);

-- ****  

begin;
  select * from t1 order by 1 for update of t1 nowait;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from t1, t2, t3 order by 1 for update;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  WITH q1 AS (SELECT * from t1 order by 1 FOR UPDATE) SELECT * FROM q1,t2 order by 1 FOR UPDATE;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  WITH q1 AS (SELECT * from t1 order by 1) SELECT * FROM q1 FOR UPDATE;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- false

commit prepared 'pt_1';

-- ****  

begin;
  WITH q1 AS (SELECT * from t1 order by 1 FOR UPDATE) SELECT * FROM q1 FOR UPDATE;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from t1, t2 where t1.val = t2.val for share;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from t1, t2 for share of t2;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from (select * from t1 for update of t1 nowait) as foo;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from t1 where val in (select val from t2 for update of t2 nowait) for update;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from t1 where val in (select val from t2 for update of t2 nowait);
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from t1, t2 for share of t2 for update of t1;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from t1 for share of t1 for update of t1;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from t1 for update of t1 for share of t1;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from t1 for share of t1 for share of t1 for update of t1;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from t1 for share of t1 for share of t1 for share of t1;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from t1 for share of t1 for share of t1 nowait for update of t1;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from t1 a,t1 b for share of a for update of b;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from p1 order by 1 for update;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from p1 for update;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); --true

commit prepared 'pt_1';

-- ****  

begin;
  select * from c1 order by 1 for update;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from c1 for update;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- **********************************
-- repeat all tests with FQS disabled
-- **********************************

set enable_fast_query_shipping=false;

-- ****  

begin;
  select * from t1 order by 1 for update of t1 nowait;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from t1, t2, t3 order by 1 for update;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  WITH q1 AS (SELECT * from t1 order by 1 FOR UPDATE) SELECT * FROM q1,t2 order by 1 FOR UPDATE;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  WITH q1 AS (SELECT * from t1 order by 1) SELECT * FROM q1 FOR UPDATE;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- false

commit prepared 'pt_1';

-- ****  

begin;
  WITH q1 AS (SELECT * from t1 order by 1 FOR UPDATE) SELECT * FROM q1 FOR UPDATE;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from t1, t2 where t1.val = t2.val for share;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from t1, t2 for share of t2;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from (select * from t1 for update of t1 nowait) as foo;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from t1 where val in (select val from t2 for update of t2 nowait) for update;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from t1 where val in (select val from t2 for update of t2 nowait);
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from t1, t2 for share of t2 for update of t1;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from t1 for share of t1 for update of t1;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from t1 for update of t1 for share of t1;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from t1 for share of t1 for share of t1 for update of t1;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from t1 for share of t1 for share of t1 for share of t1;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from t1 for share of t1 for share of t1 nowait for update of t1;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from t1 a,t1 b for share of a for update of b;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from p1 order by 1 for update;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from p1 for update;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); --true

commit prepared 'pt_1';

-- ****  

begin;
  select * from c1 order by 1 for update;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

begin;
  select * from c1 for update;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

-- ****  

set enable_fast_query_shipping=true;

-- ****  

delete from t3 where val != 5;

PREPARE my_plan(int) as select * from t3 for update;
execute my_plan(1);

-- ****  

-- drop objects created
drop table c1;
drop table p1;
drop table t1;
drop table t2;
drop table t3;

