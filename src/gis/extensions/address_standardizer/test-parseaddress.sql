\pset pager off

drop table if exists test_parse_address cascade;
create table test_parse_address (
    id serial not null primary key,
    instring text not null,
    outstring text
);
copy test_parse_address (instring, outstring) from stdin;
@@ sttype dirs dirs words$	
123 oak ln e n mycity ny	(123,"oak ln e",,"123 oak ln e","n mycity",NY,,,US)
123 oak lane east n mycity ny	(123,"oak lane east",,"123 oak lane east","n mycity",NY,,,US)
123 oak ln e north mycity ny	(123,"oak ln e",,"123 oak ln e","north mycity",NY,,,US)
@@ sttype dirs dirs saint words$	
123 oak ln e n st marie ny	(123,"oak ln e",,"123 oak ln e","n st marie",NY,,,US)
123 oak lane east n st marie ny	(123,"oak lane east",,"123 oak lane east","n st marie",NY,,,US)
123 oak ln e north st marie ny	(123,"oak ln e",,"123 oak ln e","north st marie",NY,,,US)
123 oak ln e n saint marie ny	(123,"oak ln e",,"123 oak ln e","n saint marie",NY,,,US)
123 oak lane east n saint marie ny	(123,"oak lane east",,"123 oak lane east","n saint marie",NY,,,US)
123 oak ln e north saint marie ny	(123,"oak ln e",,"123 oak ln e","north saint marie",NY,,,US)
@@ sttype dirs saint words$	
123 oak ln e st marie ny	(123,"oak ln",,"123 oak ln","e st marie",NY,,,US)
123 oak lane east st marie ny	(123,"oak lane",,"123 oak lane","east st marie",NY,,,US)
123 oak ln e st marie ny	(123,"oak ln",,"123 oak ln","e st marie",NY,,,US)
123 oak ln e saint marie ny	(123,"oak ln",,"123 oak ln","e saint marie",NY,,,US)
123 oak lane east saint marie ny	(123,"oak lane",,"123 oak lane","east saint marie",NY,,,US)
123 oak ln e saint marie ny	(123,"oak ln",,"123 oak ln","e saint marie",NY,,,US)
@@ sttype saint words$	
123 oak ln st marie ny	(123,"oak ln",,"123 oak ln","st marie",NY,,,US)
123 oak lane st marie ny	(123,"oak lane",,"123 oak lane","st marie",NY,,,US)
123 oak ln st marie ny	(123,"oak ln",,"123 oak ln","st marie",NY,,,US)
123 oak ln saint marie ny	(123,"oak ln",,"123 oak ln","saint marie",NY,,,US)
123 oak lane saint marie ny	(123,"oak lane",,"123 oak lane","saint marie",NY,,,US)
123 oak ln saint marie ny	(123,"oak ln",,"123 oak ln","saint marie",NY,,,US)
@@ sttype words$	
123 oak ln marie ny	(123,"oak ln",,"123 oak ln",marie,NY,,,US)
123 oak ln new marie ny	(123,"oak ln",,"123 oak ln","new marie",NY,,,US)
@@ === same as above but with commas ===	
@@ sttype dirs dirs words$	
123 oak ln e, n mycity ny	(123,"oak ln e",,"123 oak ln e","n mycity",NY,,,US)
123 oak lane east, n mycity ny	(123,"oak lane east",,"123 oak lane east","n mycity",NY,,,US)
123 oak ln e, north mycity ny	(123,"oak ln e",,"123 oak ln e","north mycity",NY,,,US)
123 oak ln e n, mycity ny	(123,"oak ln e n",,"123 oak ln e n",mycity,NY,,,US)
123 oak lane east n, mycity ny	(123,"oak lane east n",,"123 oak lane east n",mycity,NY,,,US)
123 oak ln e north, mycity ny	(123,"oak ln e north",,"123 oak ln e north",mycity,NY,,,US)
@@ sttype dirs dirs saint words$	
123 oak ln e, n st marie ny	(123,"oak ln e",,"123 oak ln e","n st marie",NY,,,US)
123 oak lane east, n st marie ny	(123,"oak lane east",,"123 oak lane east","n st marie",NY,,,US)
123 oak ln e, north st marie ny	(123,"oak ln e",,"123 oak ln e","north st marie",NY,,,US)
123 oak ln e, n saint marie ny	(123,"oak ln e",,"123 oak ln e","n saint marie",NY,,,US)
123 oak lane east, n saint marie ny	(123,"oak lane east",,"123 oak lane east","n saint marie",NY,,,US)
123 oak ln e, north saint marie ny	(123,"oak ln e",,"123 oak ln e","north saint marie",NY,,,US)
@@ sttype dirs saint words$	
123 oak ln e, st marie ny	(123,"oak ln e",,"123 oak ln e","st marie",NY,,,US)
123 oak lane east, st marie ny	(123,"oak lane east",,"123 oak lane east","st marie",NY,,,US)
123 oak ln e, st marie ny	(123,"oak ln e",,"123 oak ln e","st marie",NY,,,US)
123 oak ln e, saint marie ny	(123,"oak ln e",,"123 oak ln e","saint marie",NY,,,US)
123 oak lane east, saint marie ny	(123,"oak lane east",,"123 oak lane east","saint marie",NY,,,US)
123 oak ln e, saint marie ny	(123,"oak ln e",,"123 oak ln e","saint marie",NY,,,US)
@@ sttype saint words$	
123 oak ln, st marie ny	(123,"oak ln",,"123 oak ln","st marie",NY,,,US)
123 oak lane, st marie ny	(123,"oak lane",,"123 oak lane","st marie",NY,,,US)
123 oak ln, st marie ny	(123,"oak ln",,"123 oak ln","st marie",NY,,,US)
123 oak ln, saint marie ny	(123,"oak ln",,"123 oak ln","saint marie",NY,,,US)
123 oak lane, saint marie ny	(123,"oak lane",,"123 oak lane","saint marie",NY,,,US)
123 oak ln, saint marie ny	(123,"oak ln",,"123 oak ln","saint marie",NY,,,US)
@@ sttype words$	
123 oak ln, marie ny	(123,"oak ln",,"123 oak ln",marie,NY,,,US)
123 oak ln, new marie ny	(123,"oak ln",,"123 oak ln","new marie",NY,,,US)
\.

select id, instring, outstring as expected, parse_address(instring) as got_result
  from test_parse_address
 where instring not like '@@%' and parse_address(instring)::text != outstring;

\q
