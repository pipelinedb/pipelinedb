--
-- XC_REMOTE
--

-- Test cases for Postgres-XC remote queries
-- Disable fast query shipping, all the queries go through standard planner
SET enable_fast_query_shipping TO false;

-- Create of non-Coordinator quals
CREATE FUNCTION func_stable (int) RETURNS int AS $$ SELECT $1 $$ LANGUAGE SQL STABLE;
CREATE FUNCTION func_volatile (int) RETURNS int AS $$ SELECT $1 $$ LANGUAGE SQL VOLATILE;
CREATE FUNCTION func_immutable (int) RETURNS int AS $$ SELECT $1 $$ LANGUAGE SQL IMMUTABLE;

-- Test for remote DML on different tables
CREATE TABLE rel_rep (a int, b int) DISTRIBUTE BY REPLICATION;
CREATE TABLE rel_hash (a int, b int) DISTRIBUTE BY HASH (a);
CREATE TABLE rel_rr (a int, b int) DISTRIBUTE BY ROUNDROBIN;
CREATE SEQUENCE seqtest START 10;
CREATE SEQUENCE seqtest2 START 100;

-- INSERT cases
INSERT INTO rel_rep VALUES (1,1);
INSERT INTO rel_hash VALUES (1,1);
INSERT INTO rel_rr VALUES (1,1);

-- Multiple entries with non-shippable expressions
INSERT INTO rel_rep VALUES (nextval('seqtest'), nextval('seqtest')), (1, nextval('seqtest'));
INSERT INTO rel_rep VALUES (nextval('seqtest'), 1), (nextval('seqtest'), nextval('seqtest2'));
INSERT INTO rel_hash VALUES (nextval('seqtest'), nextval('seqtest')), (1, nextval('seqtest'));
INSERT INTO rel_hash VALUES (nextval('seqtest'), 1), (nextval('seqtest'), nextval('seqtest2'));
INSERT INTO rel_rr VALUES (nextval('seqtest'), nextval('seqtest')), (1, nextval('seqtest'));
INSERT INTO rel_rr VALUES (nextval('seqtest'), 1), (nextval('seqtest'), nextval('seqtest2'));

-- Global check
SELECT a, b FROM rel_rep ORDER BY 1,2;
SELECT a, b FROM rel_hash ORDER BY 1,2;
SELECT a, b FROM rel_rr ORDER BY 1,2;

-- Some SELECT queries with some quals
-- Coordinator quals first
SELECT a, b FROM rel_rep WHERE a <= currval('seqtest') - 15 ORDER BY 1,2;
SELECT a, b FROM rel_hash WHERE a <= currval('seqtest') - 15 ORDER BY 1,2;
SELECT a, b FROM rel_rr WHERE a <= currval('seqtest') - 15 ORDER BY 1,2;
-- Non Coordinator quals
SELECT a, b FROM rel_rep WHERE a <= func_immutable(5) ORDER BY 1,2;
SELECT a, b FROM rel_hash WHERE a <= func_immutable(5) ORDER BY 1,2;
SELECT a, b FROM rel_rr WHERE a <= func_immutable(5) ORDER BY 1,2;
SELECT a, b FROM rel_rep WHERE a <= func_stable(5) ORDER BY 1,2;
SELECT a, b FROM rel_hash WHERE a <= func_stable(5) ORDER BY 1,2;
SELECT a, b FROM rel_rr WHERE a <= func_stable(5) ORDER BY 1,2;
SELECT a, b FROM rel_rep WHERE a <= func_volatile(5) ORDER BY 1,2;
SELECT a, b FROM rel_hash WHERE a <= func_volatile(5) ORDER BY 1,2;
SELECT a, b FROM rel_rr WHERE a <= func_volatile(5) ORDER BY 1,2;

-- Clean up everything
DROP SEQUENCE seqtest;
DROP SEQUENCE seqtest2;
DROP TABLE rel_rep;
DROP TABLE rel_hash;
DROP TABLE rel_rr;

-- UPDATE cases for replicated table
-- Plain case, change it completely
CREATE TABLE rel_rep (a int, b timestamp DEFAULT NULL, c boolean DEFAULT NULL) DISTRIBUTE BY REPLICATION;
CREATE SEQUENCE seqtest3 START 1;
INSERT INTO rel_rep VALUES (1),(2),(3),(4),(5);
UPDATE rel_rep SET a = nextval('seqtest3'), b = now(), c = false;
SELECT a FROM rel_rep ORDER BY 1;
-- Non-Coordinator quals
UPDATE rel_rep SET b = now(), c = true WHERE a < func_volatile(2);
SELECT a FROM rel_rep WHERE c = true ORDER BY 1;
UPDATE rel_rep SET c = false;
UPDATE rel_rep SET b = now(), c = true WHERE a < func_stable(3);
SELECT a FROM rel_rep WHERE c = true ORDER BY 1;
UPDATE rel_rep SET c = false WHERE c = true;
UPDATE rel_rep SET b = now(), c = true WHERE a < func_immutable(4);
SELECT a FROM rel_rep WHERE c = true ORDER BY 1;
UPDATE rel_rep SET c = false;
-- Coordinator quals
UPDATE rel_rep SET b = now(), c = true WHERE a < currval('seqtest3') - 3 AND b < now();
SELECT a FROM rel_rep  WHERE c = true ORDER BY 1;
DROP SEQUENCE seqtest3;

-- UPDATE cases for roundrobin table
-- Plain cases change it completely
CREATE TABLE rel_rr (a int, b timestamp DEFAULT NULL, c boolean DEFAULT NULL) DISTRIBUTE BY ROUNDROBIN;
CREATE SEQUENCE seqtest4 START 1;
INSERT INTO rel_rr VALUES (1),(2),(3),(4),(5);
UPDATE rel_rr SET a = nextval('seqtest4'), b = now(), c = false;
SELECT a FROM rel_rr ORDER BY 1;
-- Non-Coordinator quals
UPDATE rel_rr SET b = now(), c = true WHERE a < func_volatile(2);
SELECT a FROM rel_rr WHERE c = true ORDER BY 1;
UPDATE rel_rr SET c = false;
UPDATE rel_rr SET b = now(), c = true WHERE a < func_stable(3);
SELECT a FROM rel_rr WHERE c = true ORDER BY 1;
UPDATE rel_rr SET c = false WHERE c = true;
UPDATE rel_rr SET b = now(), c = true WHERE a < func_immutable(4);
SELECT a FROM rel_rr WHERE c = true ORDER BY 1;
UPDATE rel_rr SET c = false;
-- Coordinator qual
UPDATE rel_rr SET b = now(), c = true WHERE a < currval('seqtest4') - 3 AND b < now();
SELECT a FROM rel_rr WHERE c = true ORDER BY 1;
DROP SEQUENCE seqtest4;

-- UPDATE cases for hash table
-- Hash tables cannot be updated on distribution keys so insert fresh rows
CREATE TABLE rel_hash (a int, b timestamp DEFAULT now(), c boolean DEFAULT false) DISTRIBUTE BY HASH(a);
CREATE SEQUENCE seqtest5 START 1;
INSERT INTO rel_hash VALUES (nextval('seqtest5'));
INSERT INTO rel_hash VALUES (nextval('seqtest5'));
INSERT INTO rel_hash VALUES (nextval('seqtest5'));
INSERT INTO rel_hash VALUES (nextval('seqtest5'));
INSERT INTO rel_hash VALUES (nextval('seqtest5'));
SELECT a FROM rel_hash ORDER BY 1;
UPDATE rel_hash SET b = now(), c = true;
SELECT a FROM rel_hash WHERE c = true ORDER BY 1;
UPDATE rel_hash SET c = false;
-- Non-Coordinator quals
UPDATE rel_hash SET b = now(), c = true WHERE a < func_volatile(2);
SELECT a FROM rel_hash WHERE c = true ORDER BY 1;
UPDATE rel_hash SET c = false;
UPDATE rel_hash SET b = now(), c = true WHERE a < func_stable(3);
SELECT a FROM rel_hash WHERE c = true ORDER BY 1;
UPDATE rel_hash SET c = false;
UPDATE rel_hash SET b = now(), c = true WHERE a < func_immutable(4);
SELECT a FROM rel_hash WHERE c = true ORDER BY 1;
UPDATE rel_hash SET c = false;
-- Coordinator quals
UPDATE rel_hash SET b = now(), c = true WHERE a < currval('seqtest5') - 3 AND b < now();
SELECT a FROM rel_hash WHERE c = true ORDER BY 1;
DROP SEQUENCE seqtest5;

-- DELETE cases
-- Coordinator quals
CREATE SEQUENCE seqtest7 START 1;
DELETE FROM rel_rep WHERE a < nextval('seqtest7') + 1;
DELETE FROM rel_rr WHERE a < nextval('seqtest7') - 3;
DELETE FROM rel_hash WHERE a < nextval('seqtest7') - 3;
-- Plain cases
DELETE FROM rel_rep;
DELETE FROM rel_rr;
DELETE FROM rel_hash;
DROP SEQUENCE seqtest7;




-- Few more cases to test the specific scenario of quals incorrecly applied
-- to wrong element of target list (sourceforge bug 3515461)
CREATE TABLE xcrem_employee (EMPNO CHAR(6) NOT NULL, FIRSTNAME VARCHAR(12) NOT NULL, MIDINIT CHAR(1) NOT NULL, LASTNAME VARCHAR(15) NOT NULL, WORKDEPT CHAR(3),
			PHONENO CHAR(4), HIREDATE TIMESTAMP, JOB CHAR(8), EDLEVEL SMALLINT NOT NULL, SEX CHAR(1), BIRTHDATE TIMESTAMP, 
			SALARY DECIMAL(9,2), BONUS DECIMAL(9,2), COMM DECIMAL(9,2));


     INSERT INTO xcrem_employee (EMPNO,FIRSTNAME,MIDINIT,LASTNAME,WORKDEPT,PHONENO,HIREDATE,JOB,EDLEVEL,SEX,BIRTHDATE,SALARY,BONUS,COMM) VALUES(
'000010','CHRISTINE',     'I',  'HAAS',           'A00',  '3978', '1965-01-01','PRES' ,       18 , 'F', '1933-08-24',  52750.00,1000,4220);
     INSERT INTO xcrem_employee (EMPNO,FIRSTNAME,MIDINIT,LASTNAME,WORKDEPT,PHONENO,HIREDATE,JOB,EDLEVEL,SEX,BIRTHDATE,SALARY,BONUS,COMM) VALUES(
'000020','MICHAEL',       'L',  'THOMPSON',       'B01',  '3476','1973-10-10','MANAGER',    18 , 'M', '1948-02-02',  41250.00,800,3300);
     INSERT INTO xcrem_employee (EMPNO,FIRSTNAME,MIDINIT,LASTNAME,WORKDEPT,PHONENO,HIREDATE,JOB,EDLEVEL,SEX,BIRTHDATE,SALARY,BONUS,COMM) VALUES(
'000030','SALLY',         'A',  'KWAN',           'C01',  '4738','1975-04-05', 'MANAGER',   20, 'F', '1941-05-11',  38250.00,800,3060);
     INSERT INTO xcrem_employee (EMPNO,FIRSTNAME,MIDINIT,LASTNAME,WORKDEPT,PHONENO,HIREDATE,JOB,EDLEVEL,SEX,BIRTHDATE,SALARY,BONUS,COMM) VALUES(
'000050','JOHN',          'B',  'GEYER',          'E01',  '6789', '1949-08-17','MANAGER',      16,  'M', '1925-09-15',  40175.00,800,3214);
     INSERT INTO xcrem_employee (EMPNO,FIRSTNAME,MIDINIT,LASTNAME,WORKDEPT,PHONENO,HIREDATE,JOB,EDLEVEL,SEX,BIRTHDATE,SALARY,BONUS,COMM) VALUES(
'000060','IRVING',        'F',  'STERN',          'D11',  '6423', '1973-09-14','MANAGER',     16,  'M', '1945-07-07',  32250.00,500,2580);
     INSERT INTO xcrem_employee (EMPNO,FIRSTNAME,MIDINIT,LASTNAME,WORKDEPT,PHONENO,HIREDATE,JOB,EDLEVEL,SEX,BIRTHDATE,SALARY,BONUS,COMM) VALUES(
'000070','EVA',           'D',  'PULASKI',        'D21',  '7831', '1980-09-30','MANAGER',     16,  'F', '1953-05-26',  36170.00,700,2893);
     INSERT INTO xcrem_employee (EMPNO,FIRSTNAME,MIDINIT,LASTNAME,WORKDEPT,PHONENO,HIREDATE,JOB,EDLEVEL,SEX,BIRTHDATE,SALARY,BONUS,COMM) VALUES(
'000090','EILEEN',        'W',  'HENDERSON',      'E11',  '5498', '1970-08-15','MANAGER',   16,  'F', '1941-05-15',  29750.00,600,2380);
     INSERT INTO xcrem_employee (EMPNO,FIRSTNAME,MIDINIT,LASTNAME,WORKDEPT,PHONENO,HIREDATE,JOB,EDLEVEL,SEX,BIRTHDATE,SALARY,BONUS,COMM) VALUES(
'000100','THEODORE',      'Q',  'SPENSER',        'E21',  '0972', '1980-06-19','MANAGER',     14 , 'M', '1956-12-18',  26150.00,500,2092);
     INSERT INTO xcrem_employee (EMPNO,FIRSTNAME,MIDINIT,LASTNAME,WORKDEPT,PHONENO,HIREDATE,JOB,EDLEVEL,SEX,BIRTHDATE,SALARY,BONUS,COMM) VALUES(
'000110','VICENZO',       'G',  'LUCCHESSI',      'A00',  '3490', '1958-05-16','SALESREP',    19,  'M', '1929-11-05',  46500.00,900,3720);
     INSERT INTO xcrem_employee (EMPNO,FIRSTNAME,MIDINIT,LASTNAME,WORKDEPT,PHONENO,HIREDATE,JOB,EDLEVEL,SEX,BIRTHDATE,SALARY,BONUS,COMM) VALUES(
'000120','SEAN',        ' ',    'O''CONNELL',      'A00',  '2167', '1963-12-05','CLERK',       14,  'M', '1942-10-18',  29250.00,600,2340);
     INSERT INTO xcrem_employee (EMPNO,FIRSTNAME,MIDINIT,LASTNAME,WORKDEPT,PHONENO,HIREDATE,JOB,EDLEVEL,SEX,BIRTHDATE,SALARY,BONUS,COMM) VALUES(
'000130','DELORES',       'M',  'QUINTANA',       'C01',  '4578', '1971-07-28','ANALYST',     16,  'F', '1925-09-15',  23800.00,500,1904);
     INSERT INTO xcrem_employee (EMPNO,FIRSTNAME,MIDINIT,LASTNAME,WORKDEPT,PHONENO,HIREDATE,JOB,EDLEVEL,SEX,BIRTHDATE,SALARY,BONUS,COMM) VALUES(
'000140','HEATHER',       'A',  'NICHOLLS',       'C01',  '1793', '1976-12-15','ANALYST',     18,  'F', '1946-01-19',  28420.00,600,2274);
     INSERT INTO xcrem_employee (EMPNO,FIRSTNAME,MIDINIT,LASTNAME,WORKDEPT,PHONENO,HIREDATE,JOB,EDLEVEL,SEX,BIRTHDATE,SALARY,BONUS,COMM) VALUES(
'000150','BRUCE',        ' ',   'ADAMSON',        'D11',  '4510', '1972-02-12','DESIGNER',    16,  'M', '1947-05-17',  25280.00,500,2022);
     INSERT INTO xcrem_employee (EMPNO,FIRSTNAME,MIDINIT,LASTNAME,WORKDEPT,PHONENO,HIREDATE,JOB,EDLEVEL,SEX,BIRTHDATE,SALARY,BONUS,COMM) VALUES(
'000160','ELIZABETH',     'R',  'PIANKA',         'D11',  '3782', '1977-10-11','DESIGNER',    17,  'F', '1955-04-12',  22250.00,400,1780);
     INSERT INTO xcrem_employee (EMPNO,FIRSTNAME,MIDINIT,LASTNAME,WORKDEPT,PHONENO,HIREDATE,JOB,EDLEVEL,SEX,BIRTHDATE,SALARY,BONUS,COMM) VALUES(
'000170','MASATOSHI',     'J',  'YOSHIMURA',      'D11',  '2890', '1978-09-15','DESIGNER',    16,  'M', '1951-01-05',  24680.00,500,1974);
     INSERT INTO xcrem_employee (EMPNO,FIRSTNAME,MIDINIT,LASTNAME,WORKDEPT,PHONENO,HIREDATE,JOB,EDLEVEL,SEX,BIRTHDATE,SALARY,BONUS,COMM) VALUES(
'000180','MARILYN',       'S',  'SCOUTTEN',       'D11',  '1682','1973-07-07','DESIGNER',    17,  'F', '1949-02-21',  21340.00,500,1707);

create table xcrem_temptable as select * from xcrem_employee;

create or replace function volatile_func(id int) returns int as
$$begin return 3;end $$ language plpgsql;

\set EXP 'explain (verbose true, costs false, nodes false)'
\set SEL 'select empno, edlevel, lastname, salary, bonus from xcrem_employee order by empno'

-- Test UPDATE
\set stmt 'update xcrem_employee E set salary = salary + salary + 0.3 * bonus WHERE SALARY > ( SELECT AVG(SALARY) FROM xcrem_employee WHERE SUBSTRING(E.WORKDEPT,1,1) = SUBSTRING(WORKDEPT, 1,1) )'
:stmt;
:EXP :stmt;
:SEL;

\set stmt 'update xcrem_employee E set bonus = bonus + salary* 0.3 WHERE EDLEVEL > ( SELECT AVG(EDLEVEL) FROM xcrem_employee WHERE WORKDEPT = E.WORKDEPT )'
:stmt;
:EXP :stmt;
:SEL;

\set stmt 'update xcrem_employee E set lastname = lastname || ''suf'' WHERE EDLEVEL > volatile_func(2)'
:stmt;
:EXP :stmt;
:SEL;

\set stmt 'update xcrem_employee E set lastname = lastname || ''suf'', edlevel = edlevel+1  WHERE EDLEVEL > volatile_func(2)'
:stmt;
:EXP :stmt;
:SEL;


-- Reset the contents of xcrem_employee
truncate xcrem_employee;
insert into xcrem_employee select * from xcrem_temptable;

-- Now test DELETE

\set stmt 'DELETE FROM xcrem_employee E WHERE EDLEVEL > volatile_func(2)'
:stmt;
:EXP :stmt;
:SEL;

truncate xcrem_employee;
insert into xcrem_employee select * from xcrem_temptable;


\set stmt 'DELETE FROM xcrem_employee E WHERE EDLEVEL > ( SELECT AVG(EDLEVEL) FROM xcrem_employee WHERE WORKDEPT = E.WORKDEPT )'
:stmt;
:EXP :stmt;
:SEL;

truncate xcrem_employee;
insert into xcrem_employee select * from xcrem_temptable;


\set stmt 'DELETE FROM xcrem_employee E WHERE SALARY > ( SELECT AVG(SALARY) FROM xcrem_employee WHERE SUBSTRING(E.WORKDEPT,1,1) = SUBSTRING(WORKDEPT, 1,1) )'
:stmt;
:EXP :stmt;
:SEL;


-- Clean up
DROP TABLE rel_rep, rel_hash, rel_rr;
DROP FUNCTION func_stable (int);
DROP FUNCTION func_volatile (int);
DROP FUNCTION func_immutable (int);

drop table xcrem_employee, xcrem_temptable;
drop function volatile_func(int);

