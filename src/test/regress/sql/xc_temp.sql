--
-- XC_TEMP
--

-- Create TEMPORARY and normal tables
CREATE TABLE table_rep (a int, b_rep char(1)) DISTRIBUTE BY REPLICATION;
CREATE TABLE table_hash (a int, b_hash char(1)) DISTRIBUTE BY HASH(a);
CREATE TABLE table_rb (a int, b_rb char(1)) DISTRIBUTE BY ROUNDROBIN;
CREATE TEMP TABLE temptable_rep (a int, b_tprep char(1)) DISTRIBUTE BY REPLICATION;
CREATE TEMP TABLE temptable_hash (a int, b_tphash char(1)) DISTRIBUTE BY HASH(a);
CREATE TEMP TABLE temptable_rb (a int, b_tprb char(1)) DISTRIBUTE BY ROUNDROBIN;
INSERT INTO table_rep VALUES (1, 'a');
INSERT INTO table_rep VALUES (2, 'b');
INSERT INTO table_rep VALUES (3, 'c');
INSERT INTO table_rep VALUES (4, NULL);
INSERT INTO table_rep VALUES (NULL, 'e');
INSERT INTO table_hash VALUES (1, 'a');
INSERT INTO table_hash VALUES (2, 'b');
INSERT INTO table_hash VALUES (3, 'c');
INSERT INTO table_hash VALUES (4, NULL);
INSERT INTO table_hash VALUES (NULL, 'e');
INSERT INTO table_rb VALUES (1, 'a');
INSERT INTO table_rb VALUES (2, 'b');
INSERT INTO table_rb VALUES (3, 'c');
INSERT INTO table_rb VALUES (4, NULL);
INSERT INTO table_rb VALUES (NULL, 'e');
INSERT INTO temptable_rep VALUES (1, 'A');
INSERT INTO temptable_rep VALUES (2, NULL);
INSERT INTO temptable_rep VALUES (3, 'C');
INSERT INTO temptable_rep VALUES (4, 'D');
INSERT INTO temptable_rep VALUES (NULL, 'E');
INSERT INTO temptable_hash VALUES (1, 'A');
INSERT INTO temptable_hash VALUES (2, 'B');
INSERT INTO temptable_hash VALUES (3, NULL);
INSERT INTO temptable_hash VALUES (4, 'D');
INSERT INTO temptable_hash VALUES (NULL, 'E');
INSERT INTO temptable_rb VALUES (1, 'A');
INSERT INTO temptable_rb VALUES (2, 'B');
INSERT INTO temptable_rb VALUES (3, 'C');
INSERT INTO temptable_rb VALUES (4, NULL);
INSERT INTO temptable_rb VALUES (NULL, 'E');

-- Check global joins on each table combination
SELECT * FROM table_hash, temptable_hash ORDER BY 1,2,3,4;
SELECT * FROM table_hash, temptable_rep ORDER BY 1,2,3,4;
SELECT * FROM table_hash, temptable_rb ORDER BY 1,2,3,4;
SELECT * FROM table_rep, temptable_rep ORDER BY 1,2,3,4;
SELECT * FROM table_rep, temptable_rb ORDER BY 1,2,3,4;
SELECT * FROM table_rb, temptable_rb ORDER BY 1,2,3,4;

-- Equi-joins
SELECT * FROM table_hash, temptable_hash WHERE table_hash.a = temptable_hash.a ORDER BY 1,2,3,4;
SELECT * FROM table_hash, temptable_rep WHERE table_hash.a = temptable_rep.a ORDER BY 1,2,3,4;
SELECT * FROM table_hash, temptable_rb WHERE table_hash.a = temptable_rb.a ORDER BY 1,2,3,4;
SELECT * FROM table_rep, temptable_rep WHERE table_rep.a = temptable_rep.a ORDER BY 1,2,3,4;
SELECT * FROM table_rep, temptable_rb WHERE table_rep.a = temptable_rb.a ORDER BY 1,2,3,4;
SELECT * FROM table_rb, temptable_rb WHERE table_rb.a = temptable_rb.a ORDER BY 1,2,3,4;

-- Non equi-joins
SELECT * FROM table_hash JOIN temptable_hash ON (table_hash.a <= temptable_hash.a) ORDER BY 1,2,3,4;
SELECT * FROM table_hash JOIN temptable_rep ON (table_hash.a <= temptable_rep.a) ORDER BY 1,2,3,4;
SELECT * FROM table_hash JOIN temptable_rb ON (table_hash.a <= temptable_rb.a) ORDER BY 1,2,3,4;
SELECT * FROM table_rep JOIN temptable_rep ON (table_rep.a <= temptable_rep.a) ORDER BY 1,2,3,4;
SELECT * FROM table_rep JOIN temptable_rb ON (table_rep.a <= temptable_rb.a) ORDER BY 1,2,3,4;
SELECT * FROM table_rb JOIN temptable_rb ON (table_rb.a <= temptable_rb.a) ORDER BY 1,2,3,4;

-- More complicated joins
-- Hash and temp Hash
SELECT * FROM table_hash NATURAL JOIN temptable_hash ORDER BY 1,2,3;
SELECT * FROM table_hash CROSS JOIN temptable_hash ORDER BY 1,2,3,4;
SELECT * FROM table_hash INNER JOIN temptable_hash USING (a) ORDER BY 1,2,3;
SELECT * FROM table_hash LEFT OUTER JOIN temptable_hash USING (a) ORDER BY 1,2,3;
SELECT * FROM table_hash RIGHT OUTER JOIN temptable_hash USING (a) ORDER BY 1,2,3;
SELECT * FROM table_hash FULL OUTER JOIN temptable_hash USING (a) ORDER BY 1,2,3; --Fails for the time being
SELECT * FROM table_hash LEFT JOIN temptable_hash USING (a) ORDER BY 1,2,3;
SELECT * FROM table_hash RIGHT JOIN temptable_hash USING (a) ORDER BY 1,2,3;
SELECT * FROM table_hash FULL JOIN temptable_hash USING (a) ORDER BY 1,2,3; --Fails for the time being

-- Hash and temp Replication
SELECT * FROM table_hash NATURAL JOIN temptable_rep ORDER BY 1,2,3;
SELECT * FROM table_hash CROSS JOIN temptable_rep ORDER BY 1,2,3,4;
SELECT * FROM table_hash INNER JOIN temptable_rep USING (a) ORDER BY 1,2,3;
SELECT * FROM table_hash LEFT OUTER JOIN temptable_rep USING (a) ORDER BY 1,2,3;
SELECT * FROM table_hash RIGHT OUTER JOIN temptable_rep USING (a) ORDER BY 1,2,3;
SELECT * FROM table_hash FULL OUTER JOIN temptable_rep USING (a) ORDER BY 1,2,3; --Fails for the time being                                                                                                   
SELECT * FROM table_hash LEFT JOIN temptable_rep USING (a) ORDER BY 1,2,3;
SELECT * FROM table_hash RIGHT JOIN temptable_rep USING (a) ORDER BY 1,2,3;
SELECT * FROM table_hash FULL JOIN temptable_rep USING (a) ORDER BY 1,2,3; --Fails for the time being

-- Hash and temp Round Robin
SELECT * FROM table_hash NATURAL JOIN temptable_rb ORDER BY 1,2,3;
SELECT * FROM table_hash CROSS JOIN temptable_rb ORDER BY 1,2,3,4;
SELECT * FROM table_hash INNER JOIN temptable_rb USING (a) ORDER BY 1,2,3;
SELECT * FROM table_hash LEFT OUTER JOIN temptable_rb USING (a) ORDER BY 1,2,3;
SELECT * FROM table_hash RIGHT OUTER JOIN temptable_rb USING (a) ORDER BY 1,2,3;
SELECT * FROM table_hash FULL OUTER JOIN temptable_rb USING (a) ORDER BY 1,2,3; --Fails for the time being                                                                                                   
SELECT * FROM table_hash LEFT JOIN temptable_rb USING (a) ORDER BY 1,2,3;
SELECT * FROM table_hash RIGHT JOIN temptable_rb USING (a) ORDER BY 1,2,3;
SELECT * FROM table_hash FULL JOIN temptable_rb USING (a) ORDER BY 1,2,3; --Fails for the time being

-- Replication and temp Replication
SELECT * FROM table_rep NATURAL JOIN temptable_rep ORDER BY 1,2,3;
SELECT * FROM table_rep CROSS JOIN temptable_rep ORDER BY 1,2,3,4;
SELECT * FROM table_rep INNER JOIN temptable_rep USING (a) ORDER BY 1,2,3;
SELECT * FROM table_rep LEFT OUTER JOIN temptable_rep USING (a) ORDER BY 1,2,3;
SELECT * FROM table_rep RIGHT OUTER JOIN temptable_rep USING (a) ORDER BY 1,2,3;
SELECT * FROM table_rep FULL OUTER JOIN temptable_rep USING (a) ORDER BY 1,2,3; --Fails for the time being                                                                                                   
SELECT * FROM table_rep LEFT JOIN temptable_rep USING (a) ORDER BY 1,2,3;
SELECT * FROM table_rep RIGHT JOIN temptable_rep USING (a) ORDER BY 1,2,3;
SELECT * FROM table_rep FULL JOIN temptable_rep USING (a) ORDER BY 1,2,3; --Fails for the time being

-- Replication and temp Round Robin
SELECT * FROM table_rep NATURAL JOIN temptable_rb ORDER BY 1,2,3;
SELECT * FROM table_rep CROSS JOIN temptable_rb ORDER BY 1,2,3,4;
SELECT * FROM table_rep INNER JOIN temptable_rb USING (a) ORDER BY 1,2,3;
SELECT * FROM table_rep LEFT OUTER JOIN temptable_rb USING (a) ORDER BY 1,2,3;
SELECT * FROM table_rep RIGHT OUTER JOIN temptable_rb USING (a) ORDER BY 1,2,3;
SELECT * FROM table_rep FULL OUTER JOIN temptable_rb USING (a) ORDER BY 1,2,3; --Fails for the time being                                                                                                   
SELECT * FROM table_rep LEFT JOIN temptable_rb USING (a) ORDER BY 1,2,3;
SELECT * FROM table_rep RIGHT JOIN temptable_rb USING (a) ORDER BY 1,2,3;
SELECT * FROM table_rep FULL JOIN temptable_rb USING (a) ORDER BY 1,2,3; --Fails for the time being

-- Round Robin and temp Round Robin
SELECT * FROM table_rb NATURAL JOIN temptable_rb ORDER BY 1,2,3;
SELECT * FROM table_rb CROSS JOIN temptable_rb ORDER BY 1,2,3,4;
SELECT * FROM table_rb INNER JOIN temptable_rb USING (a) ORDER BY 1,2,3;
SELECT * FROM table_rb LEFT OUTER JOIN temptable_rb USING (a) ORDER BY 1,2,3;
SELECT * FROM table_rb RIGHT OUTER JOIN temptable_rb USING (a) ORDER BY 1,2,3;
SELECT * FROM table_rb FULL OUTER JOIN temptable_rb USING (a) ORDER BY 1,2,3; --Fails for the time being                                                                                                   
SELECT * FROM table_rb LEFT JOIN temptable_rb USING (a) ORDER BY 1,2,3;
SELECT * FROM table_rb RIGHT JOIN temptable_rb USING (a) ORDER BY 1,2,3;
SELECT * FROM table_rb FULL JOIN temptable_rb USING (a) ORDER BY 1,2,3; --Fails for the time being

-- Check that DROP with TEMP and non-TEMP tables fails correctly
DROP TABLE temptable_rep,table_rep;

-- Clean up everything
DROP TABLE table_rep,table_hash,table_rb;

-- Check of inheritance between temp and non-temp tables
CREATE TEMP TABLE table_parent (a int);
CREATE TABLE table_child (like table_parent, b int);
DROP TABLE table_child;
