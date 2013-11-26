
-- XC Test cases to verify that all supported data types are working as distribution key
-- Also verifies that the comaparison with a constant for equality is optimized.

create table ch_tab(a char) distribute by modulo(a);
insert into ch_tab values('a');
select hashchar('a');


create table nm_tab(a name) distribute by modulo(a);
insert into nm_tab values('abbas');
select hashname('abbas');

create table nu_tab(a numeric(10,5)) distribute by modulo(a);
insert into nu_tab values(123.456);
insert into nu_tab values(789.412);
select * from nu_tab order by a;
select * from nu_tab where a = 123.456;
select * from nu_tab where 789.412 = a;

explain (costs false, num_nodes true, nodes false) select * from nu_tab where a = 123.456;
explain (costs false, num_nodes true, nodes false) select * from nu_tab where 789.412 = a;


create table tx_tab(a text) distribute by modulo(a);
insert into tx_tab values('hello world');
insert into tx_tab values('Did the quick brown fox jump over the lazy dog?');
select * from tx_tab order by a;
select * from tx_tab where a = 'hello world';
select * from tx_tab where a = 'Did the quick brown fox jump over the lazy dog?';

select * from tx_tab where 'hello world' = a;
select * from tx_tab where 'Did the quick brown fox jump over the lazy dog?' = a;

explain (costs false, num_nodes true, nodes false) select * from tx_tab where a = 'hello world';
explain (costs false, num_nodes true, nodes false) select * from tx_tab where a = 'Did the quick brown fox jump over the lazy dog?';



create table vc_tab(a varchar(255)) distribute by modulo(a);
insert into vc_tab values('abcdefghijklmnopqrstuvwxyz');
insert into vc_tab values('A quick brown fox');
insert into vc_tab values(NULL);
select * from vc_tab order by a;
select * from vc_tab where a = 'abcdefghijklmnopqrstuvwxyz';
select * from vc_tab where a = 'A quick brown fox';
-- This test a bug in examine_conditions_walker where a = constant is optimized but constant = a was not
select * from vc_tab where 'A quick brown fox' = a;

explain (costs false, num_nodes true, nodes false) select * from vc_tab where a = 'abcdefghijklmnopqrstuvwxyz';
explain (costs false, num_nodes true, nodes false) select * from vc_tab where a = 'A quick brown fox';
-- This test a bug in examine_conditions_walker where a = constant is optimized but constant = a was not
explain (costs false, num_nodes true, nodes false) select * from vc_tab where 'A quick brown fox' = a;



create table f8_tab(a float8) distribute by modulo(a);
insert into f8_tab values(123.456);
insert into f8_tab values(10.987654);
select * from f8_tab order by a;
select * from f8_tab where a = 123.456;
select * from f8_tab where a = 10.987654;

select * from f8_tab where a = 123.456::float8;
select * from f8_tab where a = 10.987654::float8;



create table f4_tab(a float4) distribute by modulo(a);
insert into f4_tab values(123.456);
insert into f4_tab values(10.987654);
insert into f4_tab values(NULL);
select * from f4_tab order by a;
select * from f4_tab where a = 123.456;
select * from f4_tab where a = 10.987654;

select * from f4_tab where a = 123.456::float4;
select * from f4_tab where a = 10.987654::float4;


create table i8_tab(a int8) distribute by modulo(a);
insert into i8_tab values(8446744073709551359);
insert into i8_tab values(78902);
insert into i8_tab values(NULL);
select * from i8_tab order by a;

select * from i8_tab where a = 8446744073709551359::int8;
select * from i8_tab where a = 8446744073709551359;
select * from i8_tab where a = 78902::int8;
select * from i8_tab where a = 78902;


create table i2_tab(a int2) distribute by modulo(a);
insert into i2_tab values(123);
insert into i2_tab values(456);
select * from i2_tab order by a;

select * from i2_tab where a = 123;
select * from i2_tab where a = 456;

create table oid_tab(a oid) distribute by modulo(a);
insert into oid_tab values(23445);
insert into oid_tab values(45662);
select * from oid_tab order by a;

select * from oid_tab where a = 23445;
select * from oid_tab where a = 45662;


create table i4_tab(a int4) distribute by modulo(a);
insert into i4_tab values(65530);
insert into i4_tab values(2147483647);
select * from i4_tab order by a;

select * from i4_tab where a = 65530;
select * from i4_tab where a = 2147483647;

select * from i4_tab where 65530 = a;
select * from i4_tab where 2147483647 = a;

explain (costs false, num_nodes true, nodes false) select * from i4_tab where 65530 = a;
explain (costs false, num_nodes true, nodes false) select * from i4_tab where a = 2147483647;


create table bo_tab(a bool) distribute by modulo(a);
insert into bo_tab values(true);
insert into bo_tab values(false);
select * from bo_tab order by a;

select * from bo_tab where a = true;
select * from bo_tab where a = false;


create table bpc_tab(a char(35)) distribute by modulo(a);
insert into bpc_tab values('Hello World');
insert into bpc_tab values('The quick brown fox');
select * from bpc_tab order by a;

select * from bpc_tab where a = 'Hello World';
select * from bpc_tab where a = 'The quick brown fox';


create table byta_tab(a bytea) distribute by modulo(a);
insert into byta_tab values(E'\\000\\001\\002\\003\\004\\005\\006\\007\\010');
insert into byta_tab values(E'\\010\\011\\012\\013\\014\\015\\016\\017\\020');
select * from byta_tab order by a;

select * from byta_tab where a = E'\\000\\001\\002\\003\\004\\005\\006\\007\\010';
select * from byta_tab where a = E'\\010\\011\\012\\013\\014\\015\\016\\017\\020';

create table tim_tab(a time) distribute by modulo(a);
insert into tim_tab values('00:01:02.03');
insert into tim_tab values('23:59:59.99');
select * from tim_tab order by a;

delete from tim_tab where a = '00:01:02.03';
delete from tim_tab where a = '23:59:59.99';



create table timtz_tab(a time with time zone) distribute by modulo(a);
insert into timtz_tab values('00:01:02.03 PST');
insert into timtz_tab values('23:59:59.99 PST');
select * from timtz_tab order by a;

select * from timtz_tab where a = '00:01:02.03 PST';
select * from timtz_tab where a = '23:59:59.99 PST';



create table ts_tab(a timestamp) distribute by modulo(a);
insert into ts_tab values('May 10, 2011 00:01:02.03');
insert into ts_tab values('August 14, 2001 23:59:59.99');
select * from ts_tab order by a;

select * from ts_tab where a = 'May 10, 2011 00:01:02.03';
select * from ts_tab where a = 'August 14, 2001 23:59:59.99';


create table in_tab(a interval) distribute by modulo(a);
insert into in_tab values('1 day 12 hours 59 min 10 sec');
insert into in_tab values('0 day 4 hours 32 min 23 sec');
select * from in_tab order by a;

select * from in_tab where a = '1 day 12 hours 59 min 10 sec';
select * from in_tab where a = '0 day 4 hours 32 min 23 sec';



create table cash_tab(a money) distribute by modulo(a);
insert into cash_tab values('231.54');
insert into cash_tab values('14011.50');
select * from cash_tab order by a;

select * from cash_tab where a = '231.54';
select * from cash_tab where a = '14011.50';


create table atim_tab(a abstime) distribute by modulo(a);
insert into atim_tab values(abstime('May 10, 2011 00:01:02.03'));
insert into atim_tab values(abstime('Jun 23, 2001 23:59:59.99'));
select * from atim_tab order by a;

select * from atim_tab where a = abstime('May 10, 2011 00:01:02.03');
select * from atim_tab where a = abstime('Jun 23, 2001 23:59:59.99');


create table rtim_tab(a reltime) distribute by modulo(a);
insert into rtim_tab values(reltime('1 day 12 hours 59 min 10 sec'));
insert into rtim_tab values(reltime('0 day 5 hours 32 min 23 sec'));
select * from rtim_tab order by a;

select * from rtim_tab where a = reltime('1 day 12 hours 59 min 10 sec');
select * from rtim_tab where a = reltime('0 day 5 hours 32 min 23 sec');




create table date_tab(a date) distribute by modulo(a);
insert into date_tab values('May 10, 2011');
insert into date_tab values('August 23, 2001');
select * from date_tab order by a;

select * from date_tab where a = 'May 10, 2011';
select * from date_tab where a = 'August 23, 2001';


create table tstz_tab(a timestamp with time zone) distribute by modulo(a);
insert into tstz_tab values('May 10, 2011 00:01:02.03 PST');
insert into tstz_tab values('Jun 23, 2001 23:59:59.99 PST');
select * from tstz_tab order by a;

select * from tstz_tab where a = 'May 10, 2011 00:01:02.03 PST';
select * from tstz_tab where a = 'Jun 23, 2001 23:59:59.99 PST';



create table tstz_tab_h(a timestamp with time zone) distribute by hash(a);
insert into tstz_tab_h values('May 10, 2011 00:01:02.03 PST');
insert into tstz_tab_h values('Jun 23, 2001 23:59:59.99 PST');
select * from tstz_tab_h order by a;

select * from tstz_tab_h where a = 'May 10, 2011 00:01:02.03 PST';
select * from tstz_tab_h where a = 'Jun 23, 2001 23:59:59.99 PST';


create table my_rr_tab(a integer, b varchar(100)) distribute by roundrobin;
insert into my_rr_tab values(1 , 'One');
insert into my_rr_tab values(2, 'Two');

select * from my_rr_tab order by a;

