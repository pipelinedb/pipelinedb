SET client_min_messages TO warning;

DROP TABLE IF EXISTS raster_intersects_rast;
DROP TABLE IF EXISTS raster_intersects_geom;
CREATE TABLE raster_intersects_rast (
	rid integer,
	rast raster
);
CREATE TABLE raster_intersects_geom (
	gid integer,
	geom geometry
);
CREATE OR REPLACE FUNCTION make_test_raster(rid integer, width integer DEFAULT 2, height integer DEFAULT 2, ul_x double precision DEFAULT 0, ul_y double precision DEFAULT 0, skew_x double precision DEFAULT 0, skew_y double precision DEFAULT 0)
	RETURNS void
	AS $$
	DECLARE
		x int;
		y int;
		rast raster;
	BEGIN
		rast := ST_MakeEmptyRaster(width, height, ul_x, ul_y, 1, 1, skew_x, skew_y, 0);
		rast := ST_AddBand(rast, 1, '8BUI', 1, 0);


		INSERT INTO raster_intersects_rast VALUES (rid, rast);

		RETURN;
	END;
	$$ LANGUAGE 'plpgsql';
SELECT make_test_raster(0, 2, 2, -1, -1);
SELECT make_test_raster(1, 2, 2);
SELECT make_test_raster(2, 3, 3);
DROP FUNCTION make_test_raster(integer, integer, integer, double precision, double precision, double precision, double precision);

INSERT INTO raster_intersects_rast VALUES (10, (
	SELECT
		ST_SetValue(rast, 1, 1, 1, 0)
	FROM raster_intersects_rast
	WHERE rid = 1
));
INSERT INTO raster_intersects_rast VALUES (11, (
	SELECT
		ST_SetValue(rast, 1, 2, 1, 0)
	FROM raster_intersects_rast
	WHERE rid = 1
));
INSERT INTO raster_intersects_rast VALUES (12, (
	SELECT
		ST_SetValue(
			ST_SetValue(
				ST_SetValue(rast, 1, 1, 1, 0),
				1, 2, 1, 0
			),
			1, 1, 2, 0
		)
	FROM raster_intersects_rast
	WHERE rid = 1
));
INSERT INTO raster_intersects_rast VALUES (13, (
	SELECT
		ST_SetValue(
			ST_SetValue(
				ST_SetValue(
					ST_SetValue(rast, 1, 1, 1, 0),
					1, 2, 1, 0
				),
				1, 1, 2, 0
			),
			1, 2, 2, 0
		)
	FROM raster_intersects_rast
	WHERE rid = 1
));
INSERT INTO raster_intersects_rast VALUES (14, (
	SELECT
		ST_SetUpperLeft(rast, 2, 0)
	FROM raster_intersects_rast
	WHERE rid = 1
));
INSERT INTO raster_intersects_rast VALUES (15, (
	SELECT
		ST_SetScale(
			ST_SetUpperLeft(rast, 0.1, 0.1),
			0.4, 0.4
		)
	FROM raster_intersects_rast
	WHERE rid = 1
));
INSERT INTO raster_intersects_rast VALUES (16, (
	SELECT
		ST_SetScale(
			ST_SetUpperLeft(rast, -0.1, 0.1),
			0.4, 0.4
		)
	FROM raster_intersects_rast
	WHERE rid = 1
));

INSERT INTO raster_intersects_rast VALUES (20, (
	SELECT
		ST_SetUpperLeft(rast, -2, -2)
	FROM raster_intersects_rast
	WHERE rid = 2
));
INSERT INTO raster_intersects_rast VALUES (21, (
	SELECT
		ST_SetValue(
			ST_SetValue(
				ST_SetValue(rast, 1, 1, 1, 0),
				1, 2, 2, 0
			),
			1, 3, 3, 0
		)
	FROM raster_intersects_rast
	WHERE rid = 20
));
INSERT INTO raster_intersects_rast VALUES (22, (
	SELECT
		ST_SetValue(
			ST_SetValue(
				rast, 1, 3, 2, 0
			),
			1, 2, 3, 0
		)
	FROM raster_intersects_rast
	WHERE rid = 21
));
INSERT INTO raster_intersects_rast VALUES (23, (
	SELECT
		ST_SetValue(
			ST_SetValue(
				rast, 1, 3, 1, 0
			),
			1, 1, 3, 0
		)
	FROM raster_intersects_rast
	WHERE rid = 22
));

INSERT INTO raster_intersects_rast VALUES (30, (
	SELECT
		ST_SetSkew(rast, -0.5, 0.5)
	FROM raster_intersects_rast
	WHERE rid = 2
));
INSERT INTO raster_intersects_rast VALUES (31, (
	SELECT
		ST_SetSkew(rast, -1, 1)
	FROM raster_intersects_rast
	WHERE rid = 2
));
INSERT INTO raster_intersects_rast VALUES (32, (
	SELECT
		ST_SetSkew(rast, 1, -1)
	FROM raster_intersects_rast
	WHERE rid = 2
));

SELECT
	'1.1',
	r1.rid,
	r2.rid,
	ST_Intersects(r1.rast, NULL, r2.rast, NULL)
FROM raster_intersects_rast r1
JOIN raster_intersects_rast r2
	ON r1.rid != r2.rid
WHERE r1.rid = 0;

SELECT
	'1.2',
	r1.rid,
	r2.rid,
	ST_Intersects(r1.rast, 1, r2.rast, 1)
FROM raster_intersects_rast r1
JOIN raster_intersects_rast r2
	ON r1.rid != r2.rid
WHERE r1.rid = 0;

-- point
INSERT INTO raster_intersects_geom VALUES (
	1, (
		SELECT ST_SetSRID(ST_MakePoint(0, 0), 0)
	)
), (
	2, (
		SELECT ST_SetSRID(ST_MakePoint(0.1, 0.1), 0)
	)
), (
	3, (
		SELECT ST_SetSRID(ST_MakePoint(-0.1, -0.1), 0)
	)
), (
	4, (
		SELECT ST_SetSRID(ST_MakePoint(-1, -1), 0)
	)
), (
	5, (
		SELECT ST_SetSRID(ST_MakePoint(-1.1, -1), 0)
	)
), (
	6, (
		SELECT ST_SetSRID(ST_MakePoint(-1, -1.1), 0)
	)
), (
	7, (
		SELECT ST_SetSRID(ST_MakePoint(-1.5, -1.5), 0)
	)
), (
	8, (
		SELECT ST_SetSRID(ST_MakePoint(3, 3), 0)
	)
);

-- multipoint
INSERT INTO raster_intersects_geom VALUES (
	11, (
		SELECT ST_Collect(geom) FROM raster_intersects_geom WHERE gid BETWEEN 1 AND 10
	)
), (
	12, (
		SELECT ST_Collect(geom) FROM raster_intersects_geom WHERE gid BETWEEN 3 AND 10
	)
), (
	13, (
		SELECT ST_Collect(geom) FROM raster_intersects_geom WHERE gid BETWEEN 4 AND 10
	)
), (
	14, (
		SELECT ST_Collect(geom) FROM raster_intersects_geom WHERE gid BETWEEN 5 AND 10
	)
), (
	15, (
		SELECT ST_Collect(geom) FROM raster_intersects_geom WHERE gid BETWEEN 6 AND 10
	)
);

-- linestring
INSERT INTO raster_intersects_geom VALUES (
	21, (
		SELECT ST_SetSRID(ST_MakeLine(ARRAY[
			ST_MakePoint(1, 1),
			ST_MakePoint(1, 0)
		]), 0)
	)
), (
	22, (
		SELECT ST_SetSRID(ST_MakeLine(ARRAY[
			ST_MakePoint(-1, -1),
			ST_MakePoint(1, 1),
			ST_MakePoint(1, 0)
		]), 0)
	)
), (
	23, (
		SELECT ST_SetSRID(ST_MakeLine(ARRAY[
			ST_MakePoint(-1, -1),
			ST_MakePoint(-1, 1),
			ST_MakePoint(1, 1),
			ST_MakePoint(1, -1)
		]), 0)
	)
), (
	24, (
		SELECT ST_SetSRID(ST_MakeLine(ARRAY[
			ST_MakePoint(-1.1, 1.1),
			ST_MakePoint(1.1, 1.1),
			ST_MakePoint(1.1, -1.1),
			ST_MakePoint(-1.1, -1.1),
			ST_MakePoint(-1.1, 1.1)
		]), 0)
	)
), (
	25, (
		SELECT ST_SetSRID(ST_MakeLine(ARRAY[
			ST_MakePoint(-2, 1),
			ST_MakePoint(1, 2),
			ST_MakePoint(2, -1),
			ST_MakePoint(-1, -2),
			ST_MakePoint(-2, 1)
		]), 0)
	)
), (
	26, (
		SELECT ST_SetSRID(ST_MakeLine(ARRAY[
			ST_MakePoint(-0.5, 0.5),
			ST_MakePoint(0, 0.5),
			ST_MakePoint(0, 0),
			ST_MakePoint(0, -0.5),
			ST_MakePoint(-0.5, 0.5)
		]), 0)
	)
), (
	27, (
		SELECT ST_SetSRID(ST_MakeLine(ARRAY[
			ST_MakePoint(0.5, 0.5),
			ST_MakePoint(1, 1),
			ST_MakePoint(1, 0),
			ST_MakePoint(0.5, 0.5)
		]), 0)
	)
), (
	28, (
		SELECT ST_SetSRID(ST_MakeLine(ARRAY[
			ST_MakePoint(1, 1),
			ST_MakePoint(0, 2),
			ST_MakePoint(1, 2),
			ST_MakePoint(1, 1)
		]), 0)
	)
), (
	29, (
		SELECT ST_SetSRID(ST_MakeLine(ARRAY[
			ST_MakePoint(0, 2),
			ST_MakePoint(1, 2),
			ST_MakePoint(1, 4),
			ST_MakePoint(0, 2)
		]), 0)
	)
);

-- polygon
INSERT INTO raster_intersects_geom VALUES (
	31, (
		SELECT ST_MakePolygon(geom) FROM raster_intersects_geom WHERE gid = 24
	)
), (
	32, (
		SELECT ST_MakePolygon(geom) FROM raster_intersects_geom WHERE gid = 25
	)
), (
	33, (
		SELECT ST_MakePolygon(geom) FROM raster_intersects_geom WHERE gid = 26
	)
), (
	34, (
		SELECT ST_MakePolygon(geom) FROM raster_intersects_geom WHERE gid = 27
	)
), (
	35, (
		SELECT ST_MakePolygon(geom) FROM raster_intersects_geom WHERE gid = 28
	)
), (
	36, (
		SELECT ST_MakePolygon(geom) FROM raster_intersects_geom WHERE gid = 29
	)
);

-- multipolygon
INSERT INTO raster_intersects_geom VALUES (
	41, (
		SELECT ST_Multi(ST_Union(geom)) FROM raster_intersects_geom WHERE gid BETWEEN 31 and 40
	)
), (
	42, (
		SELECT ST_Multi(ST_Union(geom)) FROM raster_intersects_geom WHERE gid BETWEEN 32 and 40
	)
), (
	43, (
		SELECT ST_Multi(ST_Union(geom)) FROM raster_intersects_geom WHERE gid BETWEEN 33 and 40
	)
), (
	44, (
		SELECT ST_Multi(ST_Union(geom)) FROM raster_intersects_geom WHERE gid BETWEEN 34 and 40
	)
), (
	45, (
		SELECT ST_Multi(ST_Union(geom)) FROM raster_intersects_geom WHERE gid BETWEEN 35 and 40
	)
), (
	46, (
		SELECT ST_Multi(ST_Union(geom)) FROM raster_intersects_geom WHERE gid BETWEEN 36 and 40
	)
);

SELECT
	'2.1',
	r1.rid,
	g1.gid,
	ST_GeometryType(g1.geom),
	ST_Intersects(r1.rast, g1.geom)
FROM raster_intersects_rast r1
CROSS JOIN raster_intersects_geom g1
WHERE r1.rid = 0;

SELECT
	'2.2',
	r1.rid,
	g1.gid,
	ST_GeometryType(g1.geom),
	ST_Intersects(g1.geom, r1.rast)
FROM raster_intersects_rast r1
CROSS JOIN raster_intersects_geom g1
WHERE r1.rid = 0;

SELECT
	'2.3',
	r1.rid,
	g1.gid,
	ST_GeometryType(g1.geom),
	ST_Intersects(r1.rast, g1.geom)
FROM raster_intersects_rast r1
CROSS JOIN raster_intersects_geom g1
WHERE r1.rid = 2;

SELECT
	'2.4',
	r1.rid,
	g1.gid,
	ST_GeometryType(g1.geom),
	ST_Intersects(g1.geom, r1.rast)
FROM raster_intersects_rast r1
CROSS JOIN raster_intersects_geom g1
WHERE r1.rid = 2;

SELECT
	'2.5',
	r1.rid,
	g1.gid,
	ST_GeometryType(g1.geom),
	ST_Intersects(r1.rast, g1.geom, 1)
FROM raster_intersects_rast r1
CROSS JOIN raster_intersects_geom g1
WHERE r1.rid = 0;

SELECT
	'2.6',
	r1.rid,
	g1.gid,
	ST_GeometryType(g1.geom),
	ST_Intersects(r1.rast, g1.geom, 1)
FROM raster_intersects_rast r1
CROSS JOIN raster_intersects_geom g1
WHERE r1.rid = 2;

DROP TABLE IF EXISTS raster_intersects_rast;
DROP TABLE IF EXISTS raster_intersects_geom;
