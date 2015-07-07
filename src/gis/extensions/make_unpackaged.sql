-- for postgis
SELECT 'ALTER EXTENSION ' || extname || ' ADD ' || regexp_replace(
    regexp_replace(pg_catalog.pg_describe_object(d.classid, d.objid, 0), E'cast from (.*) to (.*)', E'cast\(\\1 as \\2\)'),
    E'(.*) for access method (.*)', E'\\1 using \\2') || ';' AS sqladd
FROM pg_catalog.pg_depend AS d
INNER JOIN pg_extension AS e ON (d.refobjid = e.oid)
WHERE d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
AND deptype = 'e' AND e.extname = 'postgis'
ORDER BY sqladd;

-- for topology
SELECT 'ALTER EXTENSION ' || extname || ' ADD ' || regexp_replace(
    regexp_replace(pg_catalog.pg_describe_object(d.classid, d.objid, 0), E'cast from (.*) to (.*)', E'cast\(\\1 as \\2\)'),
    E'(.*) for access method (.*)', E'\\1 using \\2') || ';' AS sqladd
FROM pg_catalog.pg_depend AS d
INNER JOIN pg_extension AS e ON (d.refobjid = e.oid)
WHERE d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
AND deptype = 'e' AND e.extname = 'postgis_topology'
ORDER BY sqladd;

-- for postgis tiger geocoder
SELECT 'ALTER EXTENSION ' || extname || ' ADD ' || regexp_replace(
    regexp_replace(pg_catalog.pg_describe_object(d.classid, d.objid, 0), E'cast from (.*) to (.*)', E'cast\(\\1 as \\2\)'),
    E'(.*) for access method (.*)', E'\\1 using \\2') || ';' AS sqladd
FROM pg_catalog.pg_depend AS d
INNER JOIN pg_extension AS e ON (d.refobjid = e.oid)
WHERE d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
AND deptype = 'e' AND e.extname = 'postgis_tiger_geocoder'
ORDER BY sqladd;


