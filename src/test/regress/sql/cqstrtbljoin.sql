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
-- Create CV1 with 1 column join
create continuous view test1 as select stream.sid::integer, stream.name::text, T1.id::integer from stream join T1 on stream.sid = T1.id;
activate test1;
INSERT INTO stream (sid,name) values (1,'Jim');
deactivate test1;
select * from test1;
drop continuous view test1;

-- Reverse the join left/right
create continuous view test2 as select stream.sid::integer, T1.id::integer from stream join T1 on T1.id = stream.sid;
activate test2;
INSERT INTO stream (sid) values (2);
select * from test2;
deactivate test2;

-- Where clause
create continuous view test3 as select stream.sid::integer, T1.id::integer from stream,T1 where stream.sid = T1.id;
activate test3;
INSERT INTO stream (sid) values (3);
deactivate test3;
select * from test3;


-- Create CV2 with 2 column Join
-- Create CV2 with 2 column Join

-- Create CV3 with nested Join

-- Create CV3 with nested Join with some aggregates
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
drop continuous view testmulticol;
-- Repeat the above tests with ALL DIFFERNT STREAMS
drop table T1;
