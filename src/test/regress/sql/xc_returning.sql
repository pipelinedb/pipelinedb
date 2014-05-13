
set client_min_messages=warning;

-- Lets not reuse int4_tbl & int8_tbl
-- so that this test could be run independently

create table xc_int8_tbl(q1 int8, q2 int8);
INSERT INTO xc_int8_tbl VALUES('  123   ','  456');
INSERT INTO xc_int8_tbl VALUES('123   ','4567890123456789');
INSERT INTO xc_int8_tbl VALUES('4567890123456789','123');
INSERT INTO xc_int8_tbl VALUES(+4567890123456789,'4567890123456789');
INSERT INTO xc_int8_tbl VALUES('+4567890123456789','-4567890123456789');

create table xc_int4_tbl(f1 int4);
INSERT INTO xc_int4_tbl(f1) VALUES ('   0  ');
INSERT INTO xc_int4_tbl(f1) VALUES ('123456     ');
INSERT INTO xc_int4_tbl(f1) VALUES ('    -123456');
INSERT INTO xc_int4_tbl(f1) VALUES ('2147483647');
INSERT INTO xc_int4_tbl(f1) VALUES ('-2147483647');

-- The tables have to be created on a well defined set of nodes
-- independent of the nodes available in the cluster
-- so that ctid returning tests produce predictable tests

select create_table_nodes('rep_foo(a int, b int)', '{1, 2}'::int[], 'replication', NULL);

select create_table_nodes('foo (f1 serial, f2 text, f3 int default 42)', '{1, 2}'::int[], 'hash(f1)', NULL);

select create_table_nodes('tp (f1 serial, f2 text, f3 int default 42)', '{1}'::int[], 'hash(f1)', NULL);
select create_table_nodes('tc (fc int) INHERITS (tp)', '{2}'::int[], 'hash(f1)', NULL);

select create_table_nodes('parent(a int, b int)', '{1, 2}'::int[], 'hash(a)', NULL);
select create_table_nodes('child (c int) INHERITS (parent)', '{1, 2}'::int[], 'hash(a)', NULL);
select create_table_nodes('grand_child (d int) INHERITS (child)', '{1, 2}'::int[], 'hash(a)', NULL);

select create_table_nodes('fp(f1 int, f2 varchar(255), f3 int)', '{1, 2}'::int[], 'hash(f1)', NULL);
select create_table_nodes('fp_child (fc int) INHERITS (fp)', '{1, 2}'::int[], 'hash(f1)', NULL);

select create_table_nodes('bar(c1 int, c2 int)', '{1, 2}'::int[], 'hash(c1)', NULL);

select create_table_nodes('ta1 (v1 int, v2 int)', '{1, 2}'::int[], 'hash(v1)', NULL);
select create_table_nodes('ta2 (v1 int, v2 int)', '{1, 2}'::int[], 'hash(v1)', NULL);

select create_table_nodes('sal_emp (name text, pay_by_quarter integer[], schedule text[][])', '{1, 2}'::int[], 'hash(name)', NULL);

select create_table_nodes('products(product_id serial PRIMARY KEY ,product_name varchar(150),price numeric(10,2) )', '{1, 2}'::int[], 'hash(product_id)', NULL);

select create_table_nodes('my_tab(f1 int, f2 text, f3 int)', '{1, 2}'::int[], 'hash(f1)', NULL);
select create_table_nodes('my_tab2(f1 int, f2 text, f3 int)', '{1, 2}'::int[], 'hash(f1)', NULL);

create or replace function fn_immutable(integer) RETURNS integer
    AS 'SELECT f3+$1 from my_tab2 where f1=1;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
create or replace function fn_volatile(integer) RETURNS integer
    AS 'SELECT f3+$1 from my_tab2 where f1=1;'
    LANGUAGE SQL
    VOLATILE
    RETURNS NULL ON NULL INPUT;
create or replace function fn_stable(integer) RETURNS integer
    AS 'SELECT $1 from my_tab2 where f1=1;'
    LANGUAGE SQL
    STABLE
    RETURNS NULL ON NULL INPUT;

select create_table_nodes('numbers(a int, b varchar(255), c int)', '{1, 2}'::int[], 'hash(a)', NULL);

select create_table_nodes('test_tab(a int, b varchar(255), c varchar(255), d int)', '{1, 2}'::int[], 'hash(a)', NULL);

------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------
-- INSERT Returning
------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------

-------------------------------------------------------------
-- insert returning from a replicated table
-------------------------------------------------------------

insert into rep_foo values(1,2)
returning b, a, b, b, b+a, b-a, ctid;

with t as 
(
  insert into rep_foo values(3,4), (5,6), (7,8) 
  returning b, a, b, b, b+a, b-a, ctid
) select * from t order by 1, 2;

truncate table rep_foo;

-------------------------------------------------------------
-- insert returning the colum that was not inserted
-------------------------------------------------------------

insert into foo values(1,'One') returning f3, ctid;

truncate table foo;

alter sequence foo_f1_seq restart with 1;

-------------------------------------------------------------
-- single insert & multi-insert returning misc values
-------------------------------------------------------------

insert into bar values(8,9) 
returning c2, c1, c2, c2, c1+c2, c2-c1 as diff, 
c2-1 as minus1, ctid, least(c1,c2);

with t as 
(
  insert into bar values(1,2), (3,4),(5,6)
  returning c2, c1, c2, c2, c1+c2, c2-c1 as diff,
    c2-1 as minus1, ctid, least(c1,c2)
) select * from t order by 1,2;

truncate table bar;

-------------------------------------------------------------
-- sub-plan and init-plan in returning list
-------------------------------------------------------------

INSERT INTO foo (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9) returning *, ctid;

with t as
(
INSERT INTO foo SELECT f1+10, f2, f3+99 FROM foo
RETURNING *, f3-f1, f1+112 IN (SELECT q1 FROM xc_int8_tbl) AS subplan, 
EXISTS(SELECT * FROM xc_int4_tbl) AS initplan
)
select * from t order by 1,2,3;

select * from foo order by 1,2;

truncate table foo;

alter sequence foo_f1_seq restart with 1;

-------------------------------------------------------------
-- Make sure returning implementation works in case of inheritance
-------------------------------------------------------------

with t as
(
INSERT INTO fp VALUES (1,'test', 42), (2,'More', 11), (3,upper('more'), 7+9) returning ctid, f3, f2, f1, f3-f1
)
select * from t order by 4,3,2;

INSERT INTO fp_child VALUES(123,'child',999,-123) returning ctid, *;

truncate table fp_child;
truncate table fp;

-------------------------------------------------------------
-- insert returning in case of an insert rule defined on table
-------------------------------------------------------------

INSERT INTO foo (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9);

create VIEW voo AS SELECT f1, f2 FROM foo;

create OR REPLACE RULE voo_i AS ON INSERT TO voo DO INSTEAD
  INSERT INTO foo VALUES(new.*, 57) RETURNING f1, f2;

INSERT INTO voo VALUES(11,'zit');
INSERT INTO voo VALUES(12,'zoo') RETURNING *, f1*2;

drop view voo;
truncate table foo;

alter sequence foo_f1_seq restart with 1;

-------------------------------------------------------------
-- insert returning in case parent and child reside on different Datanodes
-------------------------------------------------------------

with t as
(
INSERT INTO tp (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9) returning ctid, f3, f2, f1, f3-f1
)
select * from t order by 4,3,2;

INSERT INTO tc VALUES(123,'child',999,-123)
returning ctid, f3, f2, f1, f3-f1, fc;

truncate table tc;
truncate table tp;

-------------------------------------------------------------
-- scalars in returning
-------------------------------------------------------------

with t as
(
insert into numbers values(1,'one',11),(2,'Two',22),(3,'Three',33) returning b,c,a, 234, c-2, a+3, ctid
)
select * from t order by 3,2,1;

insert into numbers values(4,'four',44)
returning b,c,a,a+1,22,upper(b),c-a, ctid;

truncate table numbers;

-------------------------------------------------------------
-- Array notation in returning
-------------------------------------------------------------

INSERT INTO sal_emp VALUES ('Bill', ARRAY[10000, 10000, 10000, 10000], ARRAY[['meeting', 'lunch'], ['training', 'presentation']])
returning pay_by_quarter[3], schedule[1:2][1:1], array_lower(schedule, 1), array_length(schedule, 1), array_dims(schedule);

INSERT INTO sal_emp VALUES ('Carol', ARRAY[20000, 25000, 25000, 25000], ARRAY[['breakfast', 'consulting'], ['meeting', 'lunch']])
returning pay_by_quarter[3], schedule[1:2][1:1], array_lower(schedule, 1), array_length(schedule, 1), array_dims(schedule);

truncate table sal_emp;

-------------------------------------------------------------
-- ANY in returning
-------------------------------------------------------------

with t as
(
INSERT INTO products(product_name, price) VALUES  ('apple', 0.5) ,('cherry apple', 1.25) ,('avocado', 1.5),('octopus',20.50) ,('watermelon',2.00)
returning product_id, product_id = ANY('{1,4,5}'::int[])
)
select * from t order by 1;

truncate table products;

alter sequence products_product_id_seq restart with 1;

-------------------------------------------------------------
-- functions in returning
-------------------------------------------------------------

insert into my_tab2 values(1,'One',11) returning *;
insert into my_tab2 values(2,'Two',22) returning *;
insert into my_tab2 values(3,'Three',33) returning *;

insert into my_tab values(1,'One',11) returning f3,f2,f1,f3-f1,fn_immutable(f3);
insert into my_tab values(2,'Two',22) returning f3,f2,f1,f3-f1, fn_volatile(f3);
insert into my_tab values(3,'Three',33) returning f3,f2,f1,f3-f1, fn_stable(f3);

truncate table my_tab;
truncate table my_tab2;

-------------------------------------------------------------
-- boolean operator in returning
-------------------------------------------------------------

insert into numbers values(0,'zero',1) returning (a::bool) OR (c::bool);

insert into numbers values(0,'zero',1) returning (a::bool) AND (c::bool);

insert into numbers values(0,'zero',1) returning NOT(a::bool);

truncate table numbers;

-------------------------------------------------------------
-- use of case in returning clause 
-------------------------------------------------------------

with t as
(
insert into numbers values(1,'one',11),(2,'Two',22),(3,'Three',33) returning case when a=1 then 'First' when a=2 then 'Second' when a=3 then 'Third' else 'nth' end
)
select * from t order by 1;

truncate table numbers;

-------------------------------------------------------------
-- use of conditional expressions in returning clause 
-------------------------------------------------------------

with t as
(
insert into test_tab values(0,'zero', NULL, 1), (1,NULL,NULL,23),(2,'Two','Second',NULL),(3,'Three','Third', 2)
 returning *, COALESCE(c,b), NULLIF(b,c), greatest(a,d), least(a,d), b IS NULL, C IS NOT NULL
)
select * from t order by 1,2,3,4;

truncate table test_tab;

--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------
-- UPDATE Returning
--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------

-------------------------------------------------------------
-- update returning from a replicated table
-------------------------------------------------------------

insert into rep_foo values(1,2),(3,4), (5,6);

update rep_foo set b=b+1 where b = 2 returning b, a, b, b, b+a, b-a, ctid;

with t as
(
update rep_foo set b=b+1 returning b, a, b, b, b+a, b-a, ctid
)
select * from t order by 1,2;

truncate table rep_foo;

-------------------------------------------------------------
-- update more rows in one go and return updated values
-------------------------------------------------------------

insert into bar values(1,2), (3,4),(5,6);

with t as
(
update bar set c2=c2+1 returning c2, c1, c2, c2, c1+c2, c2-c1, c2-1, ctid
)
select * from t order by 1,2;

truncate table bar;

-------------------------------------------------------------
-- use a function in returning clause
-------------------------------------------------------------

INSERT INTO foo (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9);

update foo set f3 = f3+1 WHERE f1 < 2 RETURNING f3, f2, f1, f3-f1, least(f1,f3), ctid;

truncate table foo;

alter sequence foo_f1_seq restart with 1;

-------------------------------------------------------------
-- An example of a join where returning list contains columns from both tables
-------------------------------------------------------------

INSERT INTO foo (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9);

update foo set f3=f3*2 from xc_int4_tbl i WHERE (foo.f1 + (123456-1)) = i.f1 RETURNING foo.*, foo.f3-foo.f1,  i.f1 as "i.f1", foo.ctid, i.ctid;

truncate table foo;

alter sequence foo_f1_seq restart with 1;

-------------------------------------------------------------
-- sub-plan and init-plan in returning list
-------------------------------------------------------------

INSERT INTO foo (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9);

update foo set f3=f3*2 WHERE f1 < 2 RETURNING *, f3-f1, ctid, f1+112 IN (SELECT q1 FROM xc_int8_tbl) AS subplan, EXISTS(SELECT * FROM xc_int4_tbl) AS initplan;

truncate table foo;

alter sequence foo_f1_seq restart with 1;

-------------------------------------------------------------
-- Test * in a join case when used in returning
-------------------------------------------------------------

INSERT INTO foo (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9);

update foo set f3=f3*2 from xc_int8_tbl i  WHERE foo.f1+122 = i.q1  RETURNING *, foo.ctid;

truncate table foo;

alter sequence foo_f1_seq restart with 1;


-------------------------------------------------------------
-- Make sure returning implementation did not break update in case of inheritance
-------------------------------------------------------------

INSERT INTO fp VALUES (1,'test',42), (2,'More', 11), (3,upper('more'), 7+9);

INSERT INTO fp_child VALUES(123,'child',999,-123);
ALTER table fp ADD COLUMN f4 int8 DEFAULT 99;

explain (costs off, num_nodes off, nodes off, verbose on)
UPDATE fp SET f4 = f4 + f3 WHERE f4 = 99;

explain (costs off, num_nodes off, nodes off, verbose on)
UPDATE fp SET f3 = f3*2  FROM xc_int8_tbl i  WHERE fp.f1 = i.q2;

explain (costs off, num_nodes off, nodes off, verbose on)
update fp_child set fc=fc+2 returning *, f4-f1;

explain (costs off, num_nodes off, nodes off, verbose on)
update fp set f3 = f3 + 1 where f1<2 returning *, f3-f1;


UPDATE fp SET f4 = f4 + f3 WHERE f4 = 99;

UPDATE fp SET f3 = f3*2  FROM xc_int8_tbl i  WHERE fp.f1 = i.q2;

update fp_child set fc=fc+2 returning *, f4-f1;

update fp set f3 = f3 + 1 where f1<2 returning *, f3-f1;

select * from fp order by 1,2,3;

truncate table fp_child;
truncate table fp;

-------------------------------------------------------------
-- Update parent with returning in case of a join
-------------------------------------------------------------

INSERT INTO fp VALUES (1, 'test', 42), (2, 'More', 11), (3, upper('more'), 7+9);

INSERT INTO fp_child VALUES(123,'child',999,-123);

with t as
(
UPDATE fp SET f4 = f4 + f3 WHERE f4 = 99 returning ctid, *
)
select * from t order by 1,2,3;

with t as
(
UPDATE fp SET f3 = f3*2  FROM xc_int8_tbl i  WHERE fp.f1 = i.q2 returning *, fp.ctid, i.ctid
)
select * from t order by 1,2,3;

with t as
(
update fp set f3=i.q1 from xc_int8_tbl i  WHERE fp.f1 = i.q1  RETURNING *, fp.f1-fp.f3
)
select * from t order by 1,2,3;

truncate table fp_child;
truncate table fp;

-------------------------------------------------------------
-- update child with returning in case of a join
-------------------------------------------------------------

INSERT INTO fp VALUES (1, 'test', 42), (2, 'More', 11), (3, upper('more'), 7+9);

INSERT INTO fp_child VALUES(123,'child',999,-123);

with t as
(
UPDATE fp SET f4 = f4 + f3 WHERE f4 = 99 returning *
)
select * from t order by 1,2,3;

with t as
(
UPDATE fp SET f3 = f3*2  FROM xc_int8_tbl i  WHERE fp.f1 = i.q2 returning *
)
select * from t order by 1,2,3;

with t as
(
update fp_child set f4 = f4 + 1 from xc_int8_tbl i  WHERE fp_child.f1 = i.q1  RETURNING *, fp_child.f1-fp_child.f3
)
select * from t order by 1,2,3;

truncate table fp_child;
truncate table fp;

-------------------------------------------------------------
-- Returning in case of a rule defined on table
-------------------------------------------------------------

INSERT INTO foo (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9);

create VIEW voo AS SELECT f1, f3 FROM foo;

create OR REPLACE RULE voo_u AS ON UPDATE TO voo DO INSTEAD UPDATE foo SET f3 = new.f3 WHERE f1 = old.f1 RETURNING f3, f1;

update voo set f3 = f1 + 1;
update voo set f3 = f1 + 1 where f1 < 2 RETURNING *;

drop view voo;
truncate table foo;

alter sequence foo_f1_seq restart with 1;

-------------------------------------------------------------
-- returning in case parent and child reside on different Datanodes
-------------------------------------------------------------

INSERT INTO tp (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9);

INSERT INTO tc VALUES(123,'child',999,-123);

update tc set fc = fc + 23 from xc_int8_tbl i  WHERE tc.f1 = i.q1 returning *;

update tp set f3 = f3 + 23 from xc_int8_tbl i  WHERE tp.f1 = i.q1 returning *;

truncate table tc;
truncate table tp;

-------------------------------------------------------------
-- returning in case of 3 levels of inheritance
-------------------------------------------------------------

insert into parent values(1,2),(3,4),(5,6),(7,8);

insert into child values(11,22,33),(44,55,66);

insert into grand_child values(111,222,333,444),(555,666,777,888);

update parent set b = a + 1 from xc_int8_tbl i WHERE parent.a + 455 = i.q2  RETURNING *, b-i.q2;

update child set c=c+1 from xc_int8_tbl i  WHERE child.a + (456-44) = i.q2  RETURNING *, b-a;

update grand_child set d=d+2 from xc_int8_tbl i  WHERE grand_child.a + (456-111) = i.q2  RETURNING *, b-a;

truncate table grand_child;
truncate table child;
truncate table parent;

-------------------------------------------------------------
-- Return system columns 
-------------------------------------------------------------

insert into bar values(1,2),(3,4),(5,6),(7,8),(9,0);

update bar  set c2=c2 where c1 = 1 returning c2, c1, c2-c1, ctid, cmin, xmax, cmax;
truncate table bar;

-------------------------------------------------------------
-- scalars in returning
-------------------------------------------------------------

insert into numbers values(1,'one',11),(2,'Two',22),(3,'Three',33);

insert into numbers values(4,'four',44);
update numbers set c=a where a=4 returning b,c,a,a+1,22,upper(b),c-a, ctid;

truncate table numbers;

-------------------------------------------------------------
-- Array notation in returning
-------------------------------------------------------------

INSERT INTO sal_emp VALUES ('Bill', ARRAY[10000, 10000, 10000, 10000], ARRAY[['meeting', 'lunch'], ['training', 'presentation']]);
INSERT INTO sal_emp VALUES ('Carol', ARRAY[20000, 25000, 25000, 25000], ARRAY[['breakfast', 'consulting'], ['meeting', 'lunch']]);

update sal_emp set pay_by_quarter[3] = pay_by_quarter[2]  WHERE pay_by_quarter[1] <> pay_by_quarter[2] 
returning pay_by_quarter[3], schedule[1:2][1:1], array_lower(schedule, 1), array_length(schedule, 1), array_dims(schedule);

truncate table sal_emp;

-------------------------------------------------------------
-- ANY in returning
-------------------------------------------------------------

INSERT INTO products(product_name, price) VALUES  ('apple', 0.5) ,('cherry apple', 1.25) ,('avocado', 1.5),('octopus',20.50) ,('watermelon',2.00);

with t as
(
update products set price = price + 1.0 WHERE product_id = ANY('{1,4,5}'::int[]) returning product_id, product_id = ANY('{1,4,5}'::int[])
)
select * from t order by 1;

truncate table products;

alter sequence products_product_id_seq restart with 1;

-------------------------------------------------------------
-- functions in returning
-------------------------------------------------------------

insert into my_tab values(1,'One',11);
insert into my_tab values(2,'Two',22);
insert into my_tab values(3,'Three',33);

insert into my_tab2 values(1,'One',11);
insert into my_tab2 values(2,'Two',22);
insert into my_tab2 values(3,'Three',33);

update my_tab set f3=2*f3 where f1=1 returning f3,f2,f1,fn_immutable(f3);

update my_tab set f3=2*f3 where f1=2 returning f3,f2,f1,fn_volatile(f3);

update my_tab set f3=2*f3 where f1=3 returning f3,f2,f1,fn_stable(f3);

truncate table my_tab;
truncate table my_tab2;

-------------------------------------------------------------
-- boolean operator in returning
-------------------------------------------------------------

insert into numbers values(0,'zero',1);
update numbers set c=c-a where a=0 returning (a::bool) OR (c::bool);

update numbers set c=c-a where a=0 returning (a::bool) AND (c::bool);

update numbers set c=c-a where a=0 returning NOT(a::bool);

truncate table numbers;

-------------------------------------------------------------
-- use of case in returning clause 
-------------------------------------------------------------

insert into numbers values(1,'one',11),(2,'Two',22),(3,'Three',33);
with t as
(
update numbers set c=c-a returning case when a=1 then 'First' when a=2 then 'Second' when a=3 then 'Third' else 'nth' end
)
select * from t order by 1;

truncate table numbers;

-------------------------------------------------------------
-- use of conditional expressions in returning clause 
-------------------------------------------------------------

insert into test_tab values(0,'zero', NULL, 1), (1,NULL,NULL,23),(2,'Two','Second',NULL),(3,'Three','Third', 2);
with t as
(
update test_tab set d=d-a returning *, COALESCE(c,b), NULLIF(b,c), greatest(a,d), least(a,d), b IS NULL, C IS NOT NULL
)
select * from t order by 1,2,3,4;

truncate table test_tab;

-------------------------------------------------------------
-- Returning from both the tables in case of an update using from clause
-------------------------------------------------------------
insert into bar values(1,2);

INSERT INTO foo (f2,f3) VALUES ('test', DEFAULT);
update foo set f3=f3*2 from bar i RETURNING foo.*, foo.ctid foo_ctid, i.ctid i_ctid;

truncate table bar;
truncate table foo;

alter sequence foo_f1_seq restart with 1;

-------------------------------------------------------------
-- Another case of returning from both the tables in case of an update using from clause
-- This is a very important test case, it uncovered a bug in remote executor
-- where eof_underlying was not being set to false despite HandleDataRow getting
-- a data row from the datanode and copying it in RemoteQueryState
-------------------------------------------------------------

insert into ta1 values(1,2),(2,3),(3,4);

insert into ta2 values(1,2),(2,3),(3,4);

with t as
(
update ta1 t1 set v2=t1.v2+10 from ta1 t2 where t2.v2<=3 returning t1.ctid,t1.v1 t1_v1, t1.v2 t1_v2, t2.v1 t2_v1,t2.v2 t2_v2,t1.v1*t2.v2
)
select * from t order by 2,3;

select * from ta1 order by v1;

with t as
(
update ta1 t1 set v2=t1.v2+10 from ta2 t2 where t2.v2<=13 returning t1.ctid,t1.v1 t1_v1, t1.v2 t1_v2
)
select * from t order by 2,3;

select * from ta1 order by v1;

truncate table ta1;
truncate table ta2;

---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
-- DELETE Returning
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------

-------------------------------------------------------------
-- delete returning from a replicated table
-------------------------------------------------------------

insert into rep_foo values(1,2),(3,4), (5,6);

delete from rep_foo where a = 1 returning b, a, b, b, b+a, b-a;

with t as
(
delete from rep_foo returning b, a, b, b, b+a, b-a
)
select * from t order by 1,2;

truncate table rep_foo;

-------------------------------------------------------------
-- delete more rows in one go and return deleted values
-------------------------------------------------------------

insert into bar values(1,2), (3,4),(5,6);

with t as
(
delete from bar returning c2, c1, c2, c2, c1+c2, c2-c1, c2-1
)
select * from t order by 1,2;

truncate table bar;

-------------------------------------------------------------
-- use a function in returning clause
-------------------------------------------------------------

INSERT INTO foo (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9);

DELETE FROM foo WHERE f1 < 2 RETURNING f3, f2, f1, f3-f1, least(f1,f3);

truncate table foo;

alter sequence foo_f1_seq restart with 1;

-------------------------------------------------------------
-- An example of a join where returning list contains columns from both tables
-------------------------------------------------------------

INSERT INTO foo (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9);

DELETE FROM foo USING xc_int4_tbl i WHERE (foo.f1 + (123456-1)) = i.f1 RETURNING foo.*, foo.f3-foo.f1,  i.f1 as "i.f1";

truncate table foo;

alter sequence foo_f1_seq restart with 1;

-------------------------------------------------------------
-- sub-plan and init-plan in returning list
-------------------------------------------------------------

INSERT INTO foo (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9);

DELETE FROM foo WHERE f1 < 2 RETURNING *, f3-f1,  f1+112 IN (SELECT q1 FROM xc_int8_tbl) AS subplan, EXISTS(SELECT * FROM xc_int4_tbl) AS initplan;

truncate table foo;

alter sequence foo_f1_seq restart with 1;

-------------------------------------------------------------
-- Test * in a join case when used in returning
-------------------------------------------------------------

INSERT INTO foo (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9);

DELETE FROM foo  USING xc_int8_tbl i  WHERE foo.f1+122 = i.q1  RETURNING *;

truncate table foo;

alter sequence foo_f1_seq restart with 1;

-------------------------------------------------------------
-- Test delete returning in case of child tables and parent tables
-------------------------------------------------------------

INSERT INTO fp VALUES (1,'test',42), (2,'More', 11), (3,upper('more'), 7+9);

INSERT INTO fp_child VALUES(123,'child',999,-123);
INSERT INTO fp_child VALUES(456,'child',999,-456);

UPDATE fp SET f4 = f4 + f3 WHERE f4 = 99;

UPDATE fp SET f3 = f3*2  FROM xc_int8_tbl i  WHERE fp.f1 = i.q2;

DELETE FROM fp_child where fc = -123 returning *, f4-f1;

DELETE FROM fp where f1 < 2 returning *, f3-f1;

truncate table fp_child;
truncate table fp;

-------------------------------------------------------------
-- Delete from parent with returning in case of a join
-------------------------------------------------------------

INSERT INTO fp VALUES (1,'test',42), (2,'More', 11), (3,upper('more'), 7+9);

INSERT INTO fp_child VALUES(123,'child',999,-123);

UPDATE fp SET f4 = f4 + f3 WHERE f4 = 99;

UPDATE fp SET f3 = f3*2  FROM xc_int8_tbl i  WHERE fp.f1 = i.q2;

DELETE FROM fp  USING xc_int8_tbl i  WHERE fp.f1 = i.q1  RETURNING *, fp.f1-fp.f3;

truncate table fp_child;
truncate table fp;

-------------------------------------------------------------
-- Delete from child with returning in case of a join
-------------------------------------------------------------

INSERT INTO fp VALUES (1,'test',42), (2,'More', 11), (3,upper('more'), 7+9);

INSERT INTO fp_child VALUES(123,'child',999,-123);

UPDATE fp SET f4 = f4 + f3 WHERE f4 = 99;

UPDATE fp SET f3 = f3*2  FROM xc_int8_tbl i  WHERE fp.f1 = i.q2;

DELETE FROM fp_child USING xc_int8_tbl i  WHERE fp_child.f1 = i.q1  RETURNING *, fp_child.f1-fp_child.f3;

truncate table fp_child;
truncate table fp;

-------------------------------------------------------------
-- delete returning in case of a delete rule defined on table
-------------------------------------------------------------

INSERT INTO foo (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9);

create VIEW voo AS SELECT f1, f2 FROM foo;

create OR REPLACE RULE voo_d AS ON DELETE TO voo DO INSTEAD  DELETE FROM foo WHERE f1 = old.f1  RETURNING f1, f2;
DELETE FROM foo WHERE f1 = 1;
DELETE FROM foo WHERE f1 < 2 RETURNING *, f3-f1;

drop view voo;
truncate table foo;

alter sequence foo_f1_seq restart with 1;

-------------------------------------------------------------
-- delete returning in case parent and child reside on different Datanodes
-------------------------------------------------------------

INSERT INTO tp (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9);

INSERT INTO tc VALUES(123,'child',999,-123);

DELETE FROM tc  USING xc_int8_tbl i  WHERE tc.f1 = i.q1 returning *;

truncate table tc;
truncate table tp;

-------------------------------------------------------------
-- delete returning in case of 3 levels of inheritance
-------------------------------------------------------------


insert into parent values(1,2),(3,4),(5,6),(7,8);

insert into child values(11,22,33),(44,55,66);

insert into grand_child values(111,222,333,444),(555,666,777,888);

DELETE FROM parent  USING xc_int8_tbl i  WHERE parent.a + 455 = i.q2  RETURNING *, b-a;

DELETE FROM child  USING xc_int8_tbl i  WHERE child.a + (456-44) = i.q2  RETURNING *, b-a;

DELETE FROM grand_child  USING xc_int8_tbl i  WHERE grand_child.a + (456-111) = i.q2  RETURNING *, b-a;

truncate table grand_child;
truncate table child;
truncate table parent;

-------------------------------------------------------------
-- Return system columns while deleting
-------------------------------------------------------------

insert into bar values(1,2),(3,4),(5,6),(7,8),(9,0);

with t as
(
delete from bar returning c2, c1, c2-c1, get_xc_node_name_gen(xc_node_id)
)
select * from t order by 1,2;

truncate table bar;

-------------------------------------------------------------
-- scalars in returning
-------------------------------------------------------------

insert into numbers values(1,'one',11),(2,'Two',22),(3,'Three',33);

insert into numbers values(4,'four',44);
delete from numbers where a=4 returning b,c,a,a+1,22,upper(b),c-a;

truncate table numbers;

-------------------------------------------------------------
-- Array notation in returning
-------------------------------------------------------------

INSERT INTO sal_emp VALUES ('Bill', ARRAY[10000, 10000, 10000, 10000], ARRAY[['meeting', 'lunch'], ['training', 'presentation']]);
INSERT INTO sal_emp VALUES ('Carol', ARRAY[20000, 25000, 25000, 25000], ARRAY[['breakfast', 'consulting'], ['meeting', 'lunch']]);

delete from sal_emp WHERE pay_by_quarter[1] <> pay_by_quarter[2] 
returning pay_by_quarter[3], schedule[1:2][1:1], array_lower(schedule, 1), array_length(schedule, 1), array_dims(schedule);

truncate table sal_emp;

-------------------------------------------------------------
-- ANY in returning
-------------------------------------------------------------

INSERT INTO products(product_name, price) VALUES  ('apple', 0.5) ,('cherry apple', 1.25) ,('avocado', 1.5),('octopus',20.50) ,('watermelon',2.00);

with t as
(
DELETE FROM products WHERE product_id = ANY('{1,4,5}'::int[]) returning product_id, product_id = ANY('{1,4,5}'::int[])
)
select * from t order by 1;

truncate table products;

alter sequence products_product_id_seq restart with 1;

-------------------------------------------------------------
-- functions in returning
-------------------------------------------------------------

insert into my_tab values(1,'One',11);
insert into my_tab values(2,'Two',22);
insert into my_tab values(3,'Three',33);

insert into my_tab2 values(1,'One',11);
insert into my_tab2 values(2,'Two',22);
insert into my_tab2 values(3,'Three',33);

delete from my_tab where f1=1 returning f3,f2,f1,fn_immutable(f3);

delete from my_tab where f1=2 returning f3,f2,f1,fn_volatile(f3);

delete from my_tab where f1=3 returning f3,f2,f1,fn_stable(f3);

truncate table my_tab;
truncate table my_tab2;

-------------------------------------------------------------
-- boolean operator in returning
-------------------------------------------------------------

insert into numbers values(0,'zero',1);
delete from numbers where a=0 returning (a::bool) OR (c::bool);

insert into numbers values(0,'zero',1);
delete from numbers where a=0 returning (a::bool) AND (c::bool);

insert into numbers values(0,'zero',1);
delete from numbers where a=0 returning NOT(a::bool);

truncate table numbers;

-------------------------------------------------------------
-- use of case in returning clause 
-------------------------------------------------------------

insert into numbers values(1,'one',11),(2,'Two',22),(3,'Three',33);

with t as
(
delete from numbers returning case when a=1 then 'First' when a=2 then 'Second' when a=3 then 'Third' else 'nth' end
)
select * from t order by 1;

truncate table numbers;

-------------------------------------------------------------
-- use of conditional expressions in returning clause 
-------------------------------------------------------------

insert into test_tab values(0,'zero', NULL, 1), (1,NULL,NULL,23),(2,'Two','Second',NULL),(3,'Three','Third', 2);

with t as
(
delete from test_tab returning *, COALESCE(c,b), NULLIF(b,c), greatest(a,d), least(a,d), b IS NULL, C IS NOT NULL
)
select * from t order by 1,2,3,4;

truncate table test_tab;


-------------------------------------------------------------
-- A few tests to verify # of tuples processed.
-------------------------------------------------------------
\set QUIET false


-------------------------------------------------------------
-- Number of rows processed for query having RETURNING.
-- The UPDATE runs twice, but for the second row it does not actually update
-- the row and hence returns NULL. The UPDATE tag should show UPDATE 1,
-- as against UPDATE 2.
-------------------------------------------------------------

CREATE TABLE xcreturn_foo (c1 int, c2 int);
INSERT INTO xcreturn_foo VALUES (1,2), (3,4);
CREATE TABLE xcreturn_bar(c3 int, c4 int);
INSERT INTO xcreturn_bar VALUES(123,456);
INSERT INTO xcreturn_bar VALUES(123,789);
update xcreturn_foo set c2=c2*2 from xcreturn_bar b WHERE xcreturn_foo.c1+122 = b.c3 RETURNING *;

drop table xcreturn_foo, xcreturn_bar;

-------------------------------------------------------------
-- Number of rows processed query having WITH and RETURNING.
-- Following WITH query updates runs so should show UPDATE 2.
-------------------------------------------------------------

create table xcreturn_tab1 (id int, v varchar) distribute by replication;
create table xcreturn_tab2 (id int, v varchar) distribute by replication;
insert into xcreturn_tab1 values (1, 'firstrow'), (2, 'secondrow');
WITH wcte AS ( INSERT INTO xcreturn_tab2 VALUES (999, 'opop'), (333, 'sss') , ( 42, 'new' ), (55, 'ppp') RETURNING id AS newid )
UPDATE xcreturn_tab1 SET id = id + newid FROM wcte;

drop table xcreturn_tab1, xcreturn_tab2;

\set QUIET true


-------------------------------------------------------------
-- clean up
-------------------------------------------------------------

drop table test_tab;

drop table numbers;

drop function fn_immutable(integer);
drop function fn_volatile(integer);
drop function fn_stable(integer);

drop table my_tab;
drop table my_tab2;

drop table products;

drop table sal_emp;

drop table ta1;
drop table ta2;

drop table bar;

drop table fp_child;
drop table fp;

drop table grand_child;
drop table child;
drop table parent;

drop table tc;
drop table tp;

drop table foo;
drop table rep_foo;

drop table xc_int8_tbl;

drop table xc_int4_tbl;

reset client_min_messages;

