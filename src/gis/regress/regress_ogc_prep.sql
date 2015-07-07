---
--- Tests for GEOS/JTS prepared predicates
---
---

SELECT c, ST_Intersects(ply, pt) FROM 
( VALUES 
-- PIP - point within polygon (no cache)
('intersects099', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(5 5)'), 
-- PIP - point within polygon
('intersects100', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(5 5)'), 
-- PIP - point on polygon vertex
('intersects101', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(0 0)'), 
-- PIP - point outside polygon
('intersects102', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(-1 0)'),
-- PIP - point on polygon edge
('intersects103', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(0 5)'),
-- PIP - point in line with polygon edge
('intersects104', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(0 12)')
) AS v(c,ply,pt);

SELECT c, ST_Contains(ply, pt) FROM 
( VALUES 
-- PIP - point within polygon (no cache)
('contains099', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(5 5)'), 
-- PIP - point within polygon
('contains100', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(5 5)'), 
-- PIP - point on polygon vertex
('contains101', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(0 0)'), 
-- PIP - point outside polygon
('contains102', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(-1 0)'),
-- PIP - point on polygon edge
('contains103', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(0 5)'),
-- PIP - point in line with polygon edge
('contains104', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(0 12)')
) AS v(c,ply,pt);

SELECT c, ST_Covers(ply, pt) FROM 
( VALUES 
-- PIP - point within polygon (no cache)
('covers099', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(5 5)'), 
-- PIP - point within polygon
('covers100', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(5 5)'), 
-- PIP - point on polygon vertex
('covers101', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(0 0)'), 
-- PIP - point outside polygon
('covers102', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(-1 0)'),
-- PIP - point on polygon edge
('covers103', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(0 5)'),
-- PIP - point in line with polygon edge
('covers104', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(0 12)')
) AS v(c,ply,pt);

SELECT c, ST_ContainsProperly(ply, pt) FROM 
( VALUES 
-- PIP - point within polygon (no cache)
('containsproperly099', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(5 5)'), 
-- PIP - point within polygon
('containsproperly100', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(5 5)'), 
-- PIP - point on polygon vertex
('containsproperly101', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(0 0)'), 
-- PIP - point outside polygon
('containsproperly102', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(-1 0)'),
-- PIP - point on polygon edge
('containsproperly103', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(0 5)'),
-- PIP - point in line with polygon edge
('containsproperly104', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(0 12)')
) AS v(c,ply,pt);


-- PIP - point vertically aligned with polygon vertex, poly first
SELECT 'intersects105', ST_Intersects(p, ST_GeomFromText('POINT(521513 5377804)', 32631)) FROM 
( VALUES 
	(ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)) 
) AS v(p);
-- PIP - point vertically aligned with polygon vertex, point first
SELECT 'intersects106', ST_Intersects(ST_GeomFromText('POINT(521513 5377804)', 32631), p) FROM 
( VALUES 
	(ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)) 
) AS v(p);
-- PIP - repeated vertex, poly first
SELECT 'intersects107', ST_Intersects(p, ST_GeomFromText('POINT(521543 5377804)', 32631)) FROM 
( VALUES 
	(ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631))
) AS v(p);
-- PIP - repeated vertex, point first
SELECT 'intersects108', ST_Intersects(ST_GeomFromText('POINT(521543 5377804)', 32631), p) FROM 
( VALUES 
	(ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631))
) AS v(p);


-- PIP - point vertically aligned with polygon vertex, poly first
SELECT 'contains105', ST_Contains(p, ST_GeomFromText('POINT(521513 5377804)', 32631)) FROM 
( VALUES 
	(ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)) 
) AS v(p);
-- PIP - point vertically aligned with polygon vertex, point first
SELECT 'contains106', ST_Contains(ST_GeomFromText('POINT(521513 5377804)', 32631), p) FROM 
( VALUES 
	(ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)) 
) AS v(p);
-- PIP - repeated vertex, poly first
SELECT 'contains107', ST_Contains(p, ST_GeomFromText('POINT(521543 5377804)', 32631)) FROM 
( VALUES 
	(ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631))
) AS v(p);
-- PIP - repeated vertex, point first
SELECT 'contains108', ST_Contains(ST_GeomFromText('POINT(521543 5377804)', 32631), p) FROM 
( VALUES 
	(ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631))
) AS v(p);

-- PIP - point vertically aligned with polygon vertex, poly first
SELECT 'containsproperly105', ST_ContainsProperly(p, ST_GeomFromText('POINT(521513 5377804)', 32631)) FROM 
( VALUES 
	(ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)) 
) AS v(p);
-- PIP - point vertically aligned with polygon vertex, point first
SELECT 'containsproperly106', ST_ContainsProperly(ST_GeomFromText('POINT(521513 5377804)', 32631), p) FROM 
( VALUES 
	(ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)) 
) AS v(p);
-- PIP - repeated vertex, poly first
SELECT 'containsproperly107', ST_ContainsProperly(p, ST_GeomFromText('POINT(521543 5377804)', 32631)) FROM 
( VALUES 
	(ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631))
) AS v(p);
-- PIP - repeated vertex, point first
SELECT 'containsproperly108', ST_ContainsProperly(ST_GeomFromText('POINT(521543 5377804)', 32631), p) FROM 
( VALUES 
	(ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631))
) AS v(p);

-- PIP - point vertically aligned with polygon vertex, poly first
SELECT 'covers105', ST_Covers(p, ST_GeomFromText('POINT(521513 5377804)', 32631)) FROM 
( VALUES 
	(ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)) 
) AS v(p);
-- PIP - point vertically aligned with polygon vertex, point first
SELECT 'covers106', ST_Covers(ST_GeomFromText('POINT(521513 5377804)', 32631), p) FROM 
( VALUES 
	(ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)) 
) AS v(p);
-- PIP - repeated vertex, poly first
SELECT 'covers107', ST_Covers(p, ST_GeomFromText('POINT(521543 5377804)', 32631)) FROM 
( VALUES 
	(ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631))
) AS v(p);
-- PIP - repeated vertex, point first
SELECT 'covers108', ST_Covers(ST_GeomFromText('POINT(521543 5377804)', 32631), p) FROM 
( VALUES 
	(ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631)),
	(ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631))
) AS v(p);


SELECT c, ST_Intersects(p1, p2) AS intersects_p1p2, ST_Intersects(p2, p1) AS intersects_p2p1 FROM
( VALUES
('intersects200', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POLYGON((2 2, 2 3, 3 3, 3 2, 2 2))'), 
('intersects201', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POLYGON((2 2, 2 3, 3 3, 3 2, 2 2))'), 
('intersects202', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'),
('intersects203', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POLYGON((-5 -5, 5 -5, 5 5, -5 5, -5 -5))'), 
('intersects204', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POLYGON((-2 -2, -2 -3, -3 -3, -3 -2, -2 -2))'),
('intersects205', 'POLYGON((0 0, 0 10, 10 11, 10 0, 0 0))', 'POLYGON((2 2, 2 3, 3 3, 3 2, 2 2))'), 
('intersects206', 'POLYGON((0 0, 0 10, 10 11, 10 0, 0 0))', 'POLYGON((2 2, 2 3, 3 3, 3 2, 2 2))'), 
('intersects207', 'POLYGON((0 0, 0 10, 10 11, 10 0, 0 0))', 'POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'),
('intersects208', 'POLYGON((0 0, 0 10, 10 11, 10 0, 0 0))', 'POLYGON((-5 -5, 5 -5, 5 5, -5 5, -5 -5))'), 
('intersects209', 'POLYGON((0 0, 0 10, 10 11, 10 0, 0 0))', 'POLYGON((-2 -2, -2 -3, -3 -3, -3 -2, -2 -2))')
) AS v(c,p1,p2);

SELECT c, ST_Contains(p1, p2) AS contains_p1p2, ST_Contains(p2, p1) AS contains_p2p1 FROM
( VALUES
('contains200', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POLYGON((2 2, 2 3, 3 3, 3 2, 2 2))'), 
('contains201', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POLYGON((2 2, 2 3, 3 3, 3 2, 2 2))'), 
('contains202', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'),
('contains203', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POLYGON((-5 -5, 5 -5, 5 5, -5 5, -5 -5))'), 
('contains204', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POLYGON((-2 -2, -2 -3, -3 -3, -3 -2, -2 -2))'),
('contains205', 'POLYGON((0 0, 0 10, 10 11, 10 0, 0 0))', 'POLYGON((2 2, 2 3, 3 3, 3 2, 2 2))'), 
('contains206', 'POLYGON((0 0, 0 10, 10 11, 10 0, 0 0))', 'POLYGON((2 2, 2 3, 3 3, 3 2, 2 2))'), 
('contains207', 'POLYGON((0 0, 0 10, 10 11, 10 0, 0 0))', 'POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'),
('contains208', 'POLYGON((0 0, 0 10, 10 11, 10 0, 0 0))', 'POLYGON((-5 -5, 5 -5, 5 5, -5 5, -5 -5))'), 
('contains209', 'POLYGON((0 0, 0 10, 10 11, 10 0, 0 0))', 'POLYGON((-2 -2, -2 -3, -3 -3, -3 -2, -2 -2))')
) AS v(c,p1,p2);

SELECT c, ST_ContainsProperly(p1, p2) AS containsproperly_p1p2, ST_ContainsProperly(p2, p1) AS containsproperly_p2p1 FROM
( VALUES
('containsproperly200', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POLYGON((2 2, 2 3, 3 3, 3 2, 2 2))'), 
('containsproperly201', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POLYGON((2 2, 2 3, 3 3, 3 2, 2 2))'), 
('containsproperly202', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'),
('containsproperly203', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POLYGON((-5 -5, 5 -5, 5 5, -5 5, -5 -5))'), 
('containsproperly204', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POLYGON((-2 -2, -2 -3, -3 -3, -3 -2, -2 -2))'),
('containsproperly205', 'POLYGON((0 0, 0 10, 10 11, 10 0, 0 0))', 'POLYGON((2 2, 2 3, 3 3, 3 2, 2 2))'), 
('containsproperly206', 'POLYGON((0 0, 0 10, 10 11, 10 0, 0 0))', 'POLYGON((2 2, 2 3, 3 3, 3 2, 2 2))'), 
('containsproperly207', 'POLYGON((0 0, 0 10, 10 11, 10 0, 0 0))', 'POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'),
('containsproperly208', 'POLYGON((0 0, 0 10, 10 11, 10 0, 0 0))', 'POLYGON((-5 -5, 5 -5, 5 5, -5 5, -5 -5))'), 
('containsproperly209', 'POLYGON((0 0, 0 10, 10 11, 10 0, 0 0))', 'POLYGON((-2 -2, -2 -3, -3 -3, -3 -2, -2 -2))')
) AS v(c,p1,p2);

SELECT c, ST_Covers(p1, p2) AS covers_p1p2, ST_Covers(p2, p1) AS covers_p2p1 FROM
( VALUES
('covers200', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POLYGON((2 2, 2 3, 3 3, 3 2, 2 2))'), 
('covers201', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POLYGON((2 2, 2 3, 3 3, 3 2, 2 2))'), 
('covers202', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'),
('covers203', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POLYGON((-5 -5, 5 -5, 5 5, -5 5, -5 -5))'), 
('covers204', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POLYGON((-2 -2, -2 -3, -3 -3, -3 -2, -2 -2))'),
('covers205', 'POLYGON((0 0, 0 10, 10 10, 11 0, 0 0))', 'POLYGON((2 2, 2 3, 3 3, 3 2, 2 2))'), 
('covers206', 'POLYGON((0 0, 0 10, 10 10, 11 0, 0 0))', 'POLYGON((2 2, 2 3, 3 3, 3 2, 2 2))'), 
('covers207', 'POLYGON((0 0, 0 10, 10 10, 11 0, 0 0))', 'POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'),
('covers208', 'POLYGON((0 0, 0 10, 10 10, 11 0, 0 0))', 'POLYGON((-5 -5, 5 -5, 5 5, -5 5, -5 -5))'), 
('covers209', 'POLYGON((0 0, 0 10, 10 10, 11 0, 0 0))', 'POLYGON((-2 -2, -2 -3, -3 -3, -3 -2, -2 -2))')
) AS v(c,p1,p2);

-- UNEXPECTED GEOMETRY TYPES --

SELECT c, ST_Contains(p1, p2) AS contains_p1p2, ST_Contains(p2, p1) AS contains_p2p1, 
          ST_Covers(p1, p2) AS covers_p1p2, ST_Covers(p2, p1) AS covers_p2p1,
          ST_Intersects(p1, p2) AS intersects_p1p2, ST_Intersects(p2, p1) AS intersects_p2p1,
          ST_ContainsProperly(p1, p2) AS containsproper_p1p2, ST_ContainsProperly(p2, p1) AS containsproper_p2p1 
          FROM
( VALUES
('types100', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(5 5)'), 
('types101', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(5 5)'), 
('types102', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(5 5)'), 
('types103', 'LINESTRING(0 0, 0 10, 10 10, 10 0)', 'POINT(5 5)'), 
('types104', 'LINESTRING(0 0, 0 10, 10 10, 10 0)', 'POINT(5 5)'), 
('types105', 'LINESTRING(0 0, 0 10, 10 10, 10 0)', 'POINT(5 5)'), 
('types106', 'POINT(5 5)', 'POINT(5 5)'), 
('types107', 'POINT(5 5)', 'POINT(5 5)'), 
('types108', 'POINT(5 5)', 'POINT(5 5)'), 
('types109', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), 
('types110', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), 
('types111', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), 
('types112', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'LINESTRING(0 0, 0 10, 10 10, 10 0)'), 
('types113', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'LINESTRING(0 0, 0 10, 10 10, 10 0)'), 
('types114', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'LINESTRING(0 0, 0 10, 10 10, 10 0)') 
) AS v(c,p1,p2);


SELECT 'intersects310', ST_intersects('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', p) FROM ( VALUES 
('LINESTRING(1 10, 9 10, 9 8)'),('LINESTRING(1 10, 9 10, 9 8)'),('LINESTRING(1 10, 9 10, 9 8)')
) AS v(p);
SELECT 'intersects311', ST_intersects('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', p) FROM ( VALUES 
('LINESTRING(1 10, 10 10, 10 8)'),('LINESTRING(1 10, 10 10, 10 8)'),('LINESTRING(1 10, 10 10, 10 8)')
) AS v(p);

SELECT 'contains310', ST_Contains('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', p) FROM ( VALUES 
('LINESTRING(1 10, 9 10, 9 8)'),('LINESTRING(1 10, 9 10, 9 8)'),('LINESTRING(1 10, 9 10, 9 8)')
) AS v(p);
SELECT 'contains311', ST_Contains('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', p) FROM ( VALUES 
('LINESTRING(1 10, 10 10, 10 8)'),('LINESTRING(1 10, 10 10, 10 8)'),('LINESTRING(1 10, 10 10, 10 8)')
) AS v(p);

SELECT 'containsproperly310', ST_ContainsProperly('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', p) FROM ( VALUES 
('LINESTRING(1 10, 9 10, 9 8)'),('LINESTRING(1 10, 9 10, 9 8)'),('LINESTRING(1 10, 9 10, 9 8)')
) AS v(p);
SELECT 'containsproperly311', ST_ContainsProperly('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', p) FROM ( VALUES 
('LINESTRING(1 10, 10 10, 10 8)'),('LINESTRING(1 10, 10 10, 10 8)'),('LINESTRING(1 10, 10 10, 10 8)')
) AS v(p);

SELECT 'covers310', ST_Covers('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', p) FROM ( VALUES 
('LINESTRING(1 10, 9 10, 9 8)'),('LINESTRING(1 10, 9 10, 9 8)'),('LINESTRING(1 10, 9 10, 9 8)')
) AS v(p);
SELECT 'covers311', ST_Covers('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', p) FROM ( VALUES 
('LINESTRING(1 10, 10 10, 10 8)'),('LINESTRING(1 10, 10 10, 10 8)'),('LINESTRING(1 10, 10 10, 10 8)')
) AS v(p);

