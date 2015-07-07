SELECT 't1', path, ST_AsText(geom) 
FROM (
  SELECT (ST_Dump(g.geom)).* 
  FROM
    (SELECT 
       'POINT (0 9)'::geometry AS geom
    ) AS g
  ) j;
  
SELECT 't2', path, ST_AsText(geom) 
FROM (
  SELECT (ST_Dump(g.geom)).* 
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
  
SELECT 't3', path, ST_AsText(geom) 
FROM (
  SELECT (ST_Dump(g.geom)).* 
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

SELECT 't4', path, ST_AsText(geom) 
FROM (
  SELECT (ST_Dump(g.geom)).* 
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

SELECT 't5', path, ST_AsText(geom) 
FROM (
  SELECT (ST_Dump(g.geom)).* 
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

SELECT 't6', path, ST_AsText(geom) 
FROM (
  SELECT (ST_Dump(g.geom)).* 
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

SELECT 't7', path, ST_AsEWKT(geom) 
FROM (
  SELECT (ST_Dump(g.geom)).* 
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

SELECT 't8', path, ST_AsEWKT(geom) 
FROM (
  SELECT (ST_Dump(g.geom)).* 
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

SELECT 't9', path, ST_AsText(geom) 
FROM (
  SELECT (ST_Dump(g.geom)).* 
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

SELECT 't10', count(*) 
FROM ST_Dump('
GEOMETRYCOLLECTION EMPTY
');

SELECT 't11', count(*) 
FROM ST_Dump('
GEOMETRYCOLLECTION (
	GEOMETRYCOLLECTION EMPTY,
	POINT EMPTY,
	LINESTRING EMPTY,
	POLYGON EMPTY,
	MULTIPOINT EMPTY,
	MULTILINESTRING EMPTY,
	MULTIPOLYGON EMPTY,
	GEOMETRYCOLLECTION (
		GEOMETRYCOLLECTION EMPTY
	)
)
');
