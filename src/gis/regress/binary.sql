SET client_min_messages TO warning;
CREATE SCHEMA tm;

CREATE TABLE tm.geoms (id serial, g geometry);

INSERT INTO tm.geoms(g) values ('POINT EMPTY');
INSERT INTO tm.geoms(g) values ('LINESTRING EMPTY');
INSERT INTO tm.geoms(g) values ('POLYGON EMPTY');
INSERT INTO tm.geoms(g) values ('MULTIPOINT EMPTY');
INSERT INTO tm.geoms(g) values ('MULTILINESTRING EMPTY');
INSERT INTO tm.geoms(g) values ('MULTIPOLYGON EMPTY');
INSERT INTO tm.geoms(g) values ('GEOMETRYCOLLECTION EMPTY');
INSERT INTO tm.geoms(g) values ('CIRCULARSTRING EMPTY');
INSERT INTO tm.geoms(g) values ('COMPOUNDCURVE EMPTY');
INSERT INTO tm.geoms(g) values ('CURVEPOLYGON EMPTY');
INSERT INTO tm.geoms(g) values ('MULTICURVE EMPTY');
INSERT INTO tm.geoms(g) values ('MULTISURFACE EMPTY');
INSERT INTO tm.geoms(g) values ('POLYHEDRALSURFACE EMPTY');
INSERT INTO tm.geoms(g) values ('TRIANGLE EMPTY');
INSERT INTO tm.geoms(g) values ('TIN EMPTY');

-- all zm flags
INSERT INTO tm.geoms(g)
SELECT st_force3dz(g) FROM tm.geoms WHERE id < 15 ORDER BY id;
INSERT INTO tm.geoms(g)
SELECT st_force3dm(g) FROM tm.geoms WHERE id < 15 ORDER BY id;
INSERT INTO tm.geoms(g)
SELECT st_force4d(g) FROM tm.geoms WHERE id < 15 ORDER BY id;

-- known srid
INSERT INTO tm.geoms(g)
SELECT st_setsrid(g,4326) FROM tm.geoms ORDER BY id;

COPY tm.geoms TO :tmpfile WITH BINARY;
CREATE TABLE tm.geoms_in AS SELECT * FROM tm.geoms LIMIT 0;
COPY tm.geoms_in FROM :tmpfile WITH BINARY;
SELECT 'geometry', count(*) FROM tm.geoms_in i, tm.geoms o WHERE i.id = o.id
 AND ST_OrderingEquals(i.g, o.g);

CREATE TABLE tm.geogs AS SELECT id,g::geography FROM tm.geoms
WHERE geometrytype(g) NOT LIKE '%CURVE%'
  AND geometrytype(g) NOT LIKE '%CIRCULAR%'
  AND geometrytype(g) NOT LIKE '%SURFACE%'
  AND geometrytype(g) NOT LIKE 'TRIANGLE%'
  AND geometrytype(g) NOT LIKE 'TIN%'
;

COPY tm.geogs TO :tmpfile WITH BINARY;
CREATE TABLE tm.geogs_in AS SELECT * FROM tm.geogs LIMIT 0;
COPY tm.geogs_in FROM :tmpfile WITH BINARY;
SELECT 'geometry', count(*) FROM tm.geogs_in i, tm.geogs o WHERE i.id = o.id
 AND ST_OrderingEquals(i.g::geometry, o.g::geometry);

DROP SCHEMA tm CASCADE;
