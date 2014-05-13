--
-- XC_PREPARED_XACTS
--

set enable_fast_query_shipping = true;

-- Test to make sure prepared transactions are working as expected
-- If a transcation is preared and contains only a select, it should NOT be preapred on data nodes

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
  select * from t1 order by val;
  select * from t2 order by val;
  select * from t3 order by val;
  select * from p1 order by a;
  select * from c1 order by a;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- false

commit prepared 'pt_1';

-- ****  

begin;
  insert into t3 values(7,11);
  insert into t3 values(8,11);
  insert into t3 values(9,11);
  insert into t3 values(0,11);
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

select * from t3 order by val;

-- ****  

begin;
  update t3 set val2 = 22;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

select * from t3 order by val;

-- ****  

begin;
  delete from t3 where val = 0;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

select * from t3 order by val;

-- ****  

begin;
  WITH q1 AS (SELECT * from t1 order by 1) SELECT * FROM q1;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- false

commit prepared 'pt_1';

-- ****  

begin;
  
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- false

commit prepared 'pt_1';

-- ****  

begin;
  select * from t1, t2 where t1.val = t2.val;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- false

commit prepared 'pt_1';

-- **********************************
-- repeat all tests with FQS disabled
-- **********************************

delete from t3;

set enable_fast_query_shipping=false;

-- ****  

begin;
  select * from t1 order by val;
  select * from t2 order by val;
  select * from t3 order by val;
  select * from p1 order by a;
  select * from c1 order by a;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- false

commit prepared 'pt_1';

-- ****  

begin;
  insert into t3 values(7,11);
  insert into t3 values(8,11);
  insert into t3 values(9,11);
  insert into t3 values(0,11);
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

select * from t3 order by val;

-- ****  

begin;
  update t3 set val2 = 22;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

select * from t3 order by val;

-- ****  

begin;
  delete from t3 where val = 7;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- true

commit prepared 'pt_1';

select * from t3 order by val;

-- ****  

begin;
  WITH q1 AS (SELECT * from t1 order by 1) SELECT * FROM q1;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- false

commit prepared 'pt_1';

-- ****  

begin;
  
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- false

commit prepared 'pt_1';

-- ****  

begin;
  select * from t1, t2 where t1.val = t2.val;
prepare transaction 'pt_1';

select gid from pg_prepared_xacts where gid = 'pt_1';
select is_prepared_on_node('pt_1', 1); -- false

commit prepared 'pt_1';

-- ****  

set enable_fast_query_shipping=true;

-- drop objects created
drop table c1;
drop table p1;
drop table t1;
drop table t2;
drop table t3;

