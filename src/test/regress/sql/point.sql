--
-- POINT
--

-- Postgres-XC case: point type cannot use ORDER BY so table
-- is replicated for regression tests whatever the cluster configuration
CREATE TABLE POINT_TBL(f1 point) DISTRIBUTE BY REPLICATION;

INSERT INTO POINT_TBL(f1) VALUES ('(0.0,0.0)');

INSERT INTO POINT_TBL(f1) VALUES ('(-10.0,0.0)');

INSERT INTO POINT_TBL(f1) VALUES ('(-3.0,4.0)');

INSERT INTO POINT_TBL(f1) VALUES ('(5.1, 34.5)');

INSERT INTO POINT_TBL(f1) VALUES ('(-5.0,-12.0)');

-- bad format points
INSERT INTO POINT_TBL(f1) VALUES ('asdfasdf');

INSERT INTO POINT_TBL(f1) VALUES ('10.0,10.0');

INSERT INTO POINT_TBL(f1) VALUES ('(10.0 10.0)');

INSERT INTO POINT_TBL(f1) VALUES ('(10.0,10.0');


SELECT '' AS six, * FROM POINT_TBL;

-- left of 
SELECT '' AS three, p.* FROM POINT_TBL p WHERE p.f1 << '(0.0, 0.0)' ORDER BY p.f1[0], p.f1[1];

-- right of 
SELECT '' AS three, p.* FROM POINT_TBL p WHERE '(0.0,0.0)' >> p.f1 ORDER BY p.f1[0], p.f1[1];

-- above 
SELECT '' AS one, p.* FROM POINT_TBL p WHERE '(0.0,0.0)' >^ p.f1 ORDER BY p.f1[0], p.f1[1];

-- below 
SELECT '' AS one, p.* FROM POINT_TBL p WHERE p.f1 <^ '(0.0, 0.0)' ORDER BY p.f1[0], p.f1[1];

-- equal 
SELECT '' AS one, p.* FROM POINT_TBL p WHERE p.f1 ~= '(5.1, 34.5)' ORDER BY p.f1[0], p.f1[1];

-- point in box
SELECT '' AS three, p.* FROM POINT_TBL p
   WHERE p.f1 <@ box '(0,0,100,100)' ORDER BY p.f1[0], p.f1[1];

SELECT '' AS three, p.* FROM POINT_TBL p
   WHERE box '(0,0,100,100)' @> p.f1;

SELECT '' AS three, p.* FROM POINT_TBL p
   WHERE not p.f1 <@ box '(0,0,100,100)';

SELECT '' AS two, p.* FROM POINT_TBL p
   WHERE p.f1 <@ path '[(0,0),(-10,0),(-10,10)]' ORDER BY p.f1[0], p.f1[1];

SELECT '' AS three, p.* FROM POINT_TBL p
   WHERE not box '(0,0,100,100)' @> p.f1;

SELECT '' AS six, p.f1, p.f1 <-> point '(0,0)' AS dist
   FROM POINT_TBL p
   ORDER BY dist, p.f1[0], p.f1[1];

SELECT '' AS thirtysix, p1.f1 AS point1, p2.f1 AS point2, p1.f1 <-> p2.f1 AS dist
   FROM POINT_TBL p1, POINT_TBL p2
   ORDER BY dist, p1.f1[0], p2.f1[0];

SELECT '' AS thirty, p1.f1 AS point1, p2.f1 AS point2
   FROM POINT_TBL p1, POINT_TBL p2
   WHERE (p1.f1 <-> p2.f1) > 3 ORDER BY p1.f1[0], p1.f1[1], p2.f1[0], p2.f1[1];

-- put distance result into output to allow sorting with GEQ optimizer - tgl 97/05/10
SELECT '' AS fifteen, p1.f1 AS point1, p2.f1 AS point2, (p1.f1 <-> p2.f1) AS distance
   FROM POINT_TBL p1, POINT_TBL p2
   WHERE (p1.f1 <-> p2.f1) > 3 and p1.f1 << p2.f1
   ORDER BY distance, p1.f1[0], p2.f1[0];

-- put distance result into output to allow sorting with GEQ optimizer - tgl 97/05/10
SELECT '' AS three, p1.f1 AS point1, p2.f1 AS point2, (p1.f1 <-> p2.f1) AS distance
   FROM POINT_TBL p1, POINT_TBL p2
   WHERE (p1.f1 <-> p2.f1) > 3 and p1.f1 << p2.f1 and p1.f1 >^ p2.f1
   ORDER BY distance, p1.f1[0], p1.f1[1], p2.f1[0], p2.f1[1];
