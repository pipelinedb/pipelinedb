SELECT path, ST_AsText(geom) 
FROM (
  SELECT (ST_DumpPoints(g.geom)).* 
  FROM
    (SELECT 
       'POINT (0 9)'::geometry AS geom
    ) AS g
  ) j;
  
SELECT path, ST_AsText(geom) 
FROM (
  SELECT (ST_DumpPoints(g.geom)).* 
  FROM
    (SELECT 
       'LINESTRING (
                0 0, 
                0 9, 
                9 9, 
                9 0, 
                0 0
            )'::geometry AS geom
    ) AS g
  ) j;
  
SELECT path, ST_AsText(geom) 
FROM (
  SELECT (ST_DumpPoints(g.geom)).* 
  FROM
    (SELECT 
       'POLYGON ((
                0 0, 
                0 9, 
                9 9, 
                9 0, 
                0 0
            ))'::geometry AS geom
    ) AS g
  ) j;

SELECT path, ST_AsText(geom) 
FROM (
  SELECT (ST_DumpPoints(g.geom)).* 
  FROM
    (SELECT 
       'TRIANGLE ((
                0 0, 
                0 9, 
                9 0, 
                0 0
            ))'::geometry AS geom
    ) AS g
  ) j;

SELECT path, ST_AsText(geom) 
FROM (
  SELECT (ST_DumpPoints(g.geom)).* 
  FROM
    (SELECT 
       'POLYGON ((
                0 0, 
                0 9, 
                9 9, 
                9 0, 
                0 0
            ), (
                1 1, 
                1 3, 
                3 2, 
                1 1
            ), (
                7 6, 
                6 8, 
                8 8, 
                7 6
            ))'::geometry AS geom
    ) AS g
  ) j;

SELECT path, ST_AsText(geom) 
FROM (
  SELECT (ST_DumpPoints(g.geom)).* 
  FROM
    (SELECT 
       'MULTIPOLYGON (((
                0 0, 
                0 3, 
                4 3, 
                4 0, 
                0 0
            )), ((
                2 4, 
                1 6, 
                4 5, 
                2 4
            ), (
                7 6, 
                6 8, 
                8 8, 
                7 6
            )))'::geometry AS geom
    ) AS g
  ) j;

SELECT path, ST_AsEWKT(geom) 
FROM (
  SELECT (ST_DumpPoints(g.geom)).* 
   FROM
     (SELECT 
       'POLYHEDRALSURFACE (((
                0 0 0, 
                0 0 1, 
                0 1 1, 
                0 1 0, 
                0 0 0
            )), ((
                0 0 0, 
                0 1 0, 
                1 1 0, 
                1 0 0, 
                0 0 0
            ))
            )'::geometry AS geom
   ) AS g
  ) j;

SELECT path, ST_AsEWKT(geom) 
FROM (
  SELECT (ST_DumpPoints(g.geom)).* 
   FROM
     (SELECT 
       'TIN (((
                0 0 0, 
                0 0 1, 
                0 1 0, 
                0 0 0
            )), ((
                0 0 0, 
                0 1 0, 
                1 1 0, 
                0 0 0
            ))
            )'::geometry AS geom
   ) AS g
  ) j;

SELECT path, ST_AsText(geom) 
FROM (
  SELECT (ST_DumpPoints(g.geom)).* 
  FROM
    (SELECT 
       'GEOMETRYCOLLECTION(
          POINT(99 98), 
          LINESTRING(1 1, 3 3),
          POLYGON((0 0, 0 1, 1 1, 0 0)),
          POLYGON((0 0, 0 9, 9 9, 9 0, 0 0), (5 5, 5 6, 6 6, 5 5)),
          MULTIPOLYGON(((0 0, 0 9, 9 9, 9 0, 0 0), (5 5, 5 6, 6 6, 5 5)))
        )'::geometry AS geom
    ) AS g
  ) j;

SELECT path, ST_AsText(geom) 
FROM (
  SELECT (ST_DumpPoints(g.geom)).* 
  FROM
	(SELECT 'SRID=4326;CURVEPOLYGON(
			CIRCULARSTRING(-71.0821 42.3036, -71.4821 42.3036,
			       	-71.7821 42.7036, -71.0821 42.7036, -71.0821 42.3036),
			(-71.1821 42.4036, -71.3821 42.6036, -71.3821 42.4036, -71.1821 42.4036)
 )'::geometry as geom
) as g
) j;

SELECT path, ST_AsText(geom) 
FROM (
  SELECT (ST_DumpPoints(g.geom)).* 
  FROM (
SELECT 'CURVEPOLYGON(CIRCULARSTRING(0 0, 4 0, 4 4, 0 4, 0 0),(1 1, 3 3, 3 1, 1 1))'::geometry as geom
) as g
) j;

SELECT '#2704', ST_DumpPoints('POLYGON EMPTY'::geometry);
SELECT '#2704', ST_DumpPoints('MULTIPOLYGON EMPTY'::geometry);
SELECT '#2704', ST_DumpPoints('MULTILINESTRING EMPTY'::geometry);
SELECT '#2704', ST_DumpPoints('LINESTRING EMPTY'::geometry);
SELECT '#2704', ST_DumpPoints('GEOMETRYCOLLECTION EMPTY'::geometry);
