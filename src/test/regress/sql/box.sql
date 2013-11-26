--
-- BOX
--

--
-- box logic
--	     o
-- 3	  o--|X
--	  |  o|
-- 2	+-+-+ |
--	| | | |
-- 1	| o-+-o
--	|   |
-- 0	+---+
--
--	0 1 2 3
--

-- boxes are specified by two points, given by four floats x1,y1,x2,y2

-- Postgres-XC case: box type cannot use ORDER BY so its table
-- is replicated for regression tests
CREATE TABLE BOX_TBL (f1 box) DISTRIBUTE BY REPLICATION;

INSERT INTO BOX_TBL (f1) VALUES ('(2.0,2.0,0.0,0.0)');

INSERT INTO BOX_TBL (f1) VALUES ('(1.0,1.0,3.0,3.0)');

-- degenerate cases where the box is a line or a point
-- note that lines and points boxes all have zero area
INSERT INTO BOX_TBL (f1) VALUES ('(2.5, 2.5, 2.5,3.5)');

INSERT INTO BOX_TBL (f1) VALUES ('(3.0, 3.0,3.0,3.0)');

-- badly formatted box inputs
INSERT INTO BOX_TBL (f1) VALUES ('(2.3, 4.5)');

INSERT INTO BOX_TBL (f1) VALUES ('asdfasdf(ad');


SELECT '' AS four, * FROM BOX_TBL;

SELECT '' AS four, b.*, area(b.f1) as barea
   FROM BOX_TBL b ORDER BY (b.f1[0])[0], (b.f1[0])[1], (b.f1[2])[0], (b.f1[2])[1];

-- overlap
SELECT '' AS three, b.f1
   FROM BOX_TBL b  
   WHERE b.f1 && box '(2.5,2.5,1.0,1.0)' ORDER BY (b.f1[0])[0], (b.f1[0])[1], (b.f1[2])[0], (b.f1[2])[1];

-- left-or-overlap (x only)
SELECT '' AS two, b1.*
   FROM BOX_TBL b1
   WHERE b1.f1 &< box '(2.0,2.0,2.5,2.5)' ORDER BY (b1.f1[0])[0], (b1.f1[0])[1], (b1.f1[2])[0], (b1.f1[2])[1];

-- right-or-overlap (x only)
SELECT '' AS two, b1.*
   FROM BOX_TBL b1
   WHERE b1.f1 &> box '(2.0,2.0,2.5,2.5)' ORDER BY (b1.f1[0])[0], (b1.f1[0])[1], (b1.f1[2])[0], (b1.f1[2])[1];

-- left of
SELECT '' AS two, b.f1
   FROM BOX_TBL b
   WHERE b.f1 << box '(3.0,3.0,5.0,5.0)' ORDER BY (b.f1[0])[0], (b.f1[0])[1], (b.f1[2])[0], (b.f1[2])[1];

-- area <=
SELECT '' AS four, b.f1
   FROM BOX_TBL b
   WHERE b.f1 <= box '(3.0,3.0,5.0,5.0)' ORDER BY (b.f1[0])[0], (b.f1[0])[1], (b.f1[2])[0], (b.f1[2])[1];

-- area <
SELECT '' AS two, b.f1
   FROM BOX_TBL b
   WHERE b.f1 < box '(3.0,3.0,5.0,5.0)' ORDER BY (b.f1[0])[0], (b.f1[0])[1], (b.f1[2])[0], (b.f1[2])[1];

-- area =
SELECT '' AS two, b.f1
   FROM BOX_TBL b
   WHERE b.f1 = box '(3.0,3.0,5.0,5.0)' ORDER BY (b.f1[0])[0], (b.f1[0])[1], (b.f1[2])[0], (b.f1[2])[1];

-- area >
SELECT '' AS two, b.f1
   FROM BOX_TBL b				-- zero area 
   WHERE b.f1 > box '(3.5,3.0,4.5,3.0)' ORDER BY (b.f1[0])[0], (b.f1[0])[1], (b.f1[2])[0], (b.f1[2])[1];	

-- area >=
SELECT '' AS four, b.f1
   FROM BOX_TBL b				-- zero area 
   WHERE b.f1 >= box '(3.5,3.0,4.5,3.0)' ORDER BY (b.f1[0])[0], (b.f1[0])[1], (b.f1[2])[0], (b.f1[2])[1];

-- right of
SELECT '' AS two, b.f1
   FROM BOX_TBL b
   WHERE box '(3.0,3.0,5.0,5.0)' >> b.f1 ORDER BY (b.f1[0])[0], (b.f1[0])[1], (b.f1[2])[0], (b.f1[2])[1];

-- contained in
SELECT '' AS three, b.f1
   FROM BOX_TBL b
   WHERE b.f1 <@ box '(0,0,3,3)' ORDER BY (b.f1[0])[0], (b.f1[0])[1], (b.f1[2])[0], (b.f1[2])[1];

-- contains
SELECT '' AS three, b.f1
   FROM BOX_TBL b
   WHERE box '(0,0,3,3)' @> b.f1 ORDER BY (b.f1[0])[0], (b.f1[0])[1], (b.f1[2])[0], (b.f1[2])[1];

-- box equality
SELECT '' AS one, b.f1
   FROM BOX_TBL b
   WHERE box '(1,1,3,3)' ~= b.f1 ORDER BY (b.f1[0])[0], (b.f1[0])[1], (b.f1[2])[0], (b.f1[2])[1];

-- center of box, left unary operator
SELECT '' AS four, @@(b1.f1) AS p
   FROM BOX_TBL b1 ORDER BY (b1.f1[0])[0], (b1.f1[0])[1], (b1.f1[2])[0], (b1.f1[2])[1];

-- wholly-contained
SELECT '' AS one, b1.*, b2.*
   FROM BOX_TBL b1, BOX_TBL b2 
   WHERE b1.f1 @> b2.f1 and not b1.f1 ~= b2.f1 
   ORDER BY (b1.f1[0])[0], (b1.f1[0])[1], (b1.f1[2])[0], (b1.f1[2])[1], (b2.f1[0])[0], (b2.f1[0])[1], (b2.f1[2])[0], (b2.f1[2])[1]; 

SELECT '' AS four, height(f1), width(f1) FROM BOX_TBL ORDER BY (f1[0])[0], (f1[0])[1], (f1[2])[0], (f1[2])[1];
