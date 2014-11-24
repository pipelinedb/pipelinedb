set debug_sync_stream_insert = on;
-- Create Test Table
create table T1(id integer, name text, description text, random integer);
-- Insert records
insert into T1 values (1,'Jim','does not work',55);
insert into T1 values (1,'Jim','does not work',55);
insert into T1 values (2,'Jim','does not work',55);
insert into T1 values (2,'Livelong','and prosper',66);
insert into T1 values (3,'Hesdead','Jimmyboy',77);
insert into T1 values (3,'Bones','Keep him alive',88);
insert into T1 values (4,'Spock','Khaaaaaaaaaaaaaaan!!',99);
insert into T1 values (4,'Kirk','this..is...a...test...yeah!!',99);

-- The following CVs all reference the same stream
-- The following tests have the stream node as the outer join node

-- 1 column join
create continuous view test1 as select stream.sid::integer, stream.name::text, T1.id::integer from stream join T1 on stream.sid = T1.id;
activate test1;
INSERT INTO stream (sid,name) values (1,'Jim');
deactivate test1;
select * from test1;
drop continuous view test1;

-- Where clause
create continuous view test3 as select stream.sid::integer, T1.id::integer from stream,T1 where stream.sid = T1.id;
activate test3;
INSERT INTO stream (sid) values (3);
deactivate test3;
select * from test3;

-- Count
create continuous view test4 as select SUM(stream.sid::integer) from stream join T1 on T1.id = stream.sid group by T1.name;
activate test4;
INSERT INTO stream (sid) values (4);
deactivate test4;
select * from test4;

-- Join on multiple columns
create continuous view testmulticol as select stream.sid::integer, stream.name::text from stream join T1 on T1.id = stream.sid and T1.name = stream.name;
activate testmulticol;
INSERT INTO stream (sid,name) values (1,'Jim');
deactivate testmulticol;
select * from testmulticol;

-- The following tests have the stream node as the inner join node
-- 1 column join
create continuous view test2 as select stream1.sid::integer, T1.id::integer from T1 join stream1 on T1.id = stream1.sid;
activate test2;
INSERT INTO stream1 (sid) values (2);
deactivate test2;
select * from test2;

-- where clause
create continuous view test5 as select stream2.sid::integer, T1.id::integer from stream2,T1 where stream2.sid = T1.id;
activate test5;
INSERT INTO stream2 (sid,name) values (3,'Bones');
deactivate test5;
select * from test5;

-- Count
create continuous view test6 as select SUM(stream3.sid::integer) from T1 join stream3 on stream3.sid = T1.id group by stream3.name::text;
activate test6;
INSERT INTO stream3 (sid,name) values (4,'Jim');
deactivate test6;
select * from test6;

-- Join on multiple columns
create continuous view testmulticol1 as select stream4.sid::integer, stream4.name::text from stream4 join T1 on T1.id = stream4.sid and T1.name = stream4.name;
activate testmulticol1;
INSERT INTO stream4 (sid,name) values (1,'Jim');
deactivate testmulticol1;
select * from testmulticol1;


drop continuous view testmulticol;
drop continuous view testmulticol1;
drop continuous view test2;
drop continuous view test3;
drop continuous view test4;
drop continuous view test5;
drop continuous view test6;
drop table T1;
