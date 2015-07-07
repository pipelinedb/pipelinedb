
-- Noding a multilinestring
SELECT 1, ST_AsText(ST_UnaryUnion('MULTILINESTRING((0 0, 10 0), (5 -5, 5 5))'));

-- Unioning a set of polygons (CascadedUnion)
SELECT 2, ST_AsText(ST_UnaryUnion('GEOMETRYCOLLECTION(POLYGON((0 0, 10 0, 10 10, 0 10, 0 0)),POLYGON((5 5, 15 5, 15 15, 5 15, 5 5)))'));

-- Unioning an heterogeneous collection of geometries
SELECT 3, ST_AsText(ST_UnaryUnion('GEOMETRYCOLLECTION(POLYGON((0 0, 10 0, 10 10, 0 10, 0 0)),POLYGON((5 5, 15 5, 15 15, 5 15, 5 5)), MULTIPOINT(5 4, -5 4),LINESTRING(2 -10, 2 20))'));

