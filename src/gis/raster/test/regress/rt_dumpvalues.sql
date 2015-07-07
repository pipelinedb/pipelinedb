SET client_min_messages TO warning;
DROP TABLE IF EXISTS raster_dumpvalues;
CREATE TABLE raster_dumpvalues (
	rid integer,
	rast raster
);
CREATE OR REPLACE FUNCTION make_raster(
	rast raster DEFAULT NULL,
	pixtype text DEFAULT '8BUI',
	rows integer DEFAULT 3,
	columns integer DEFAULT 3,
	nodataval double precision DEFAULT 0,
	start_val double precision DEFAULT 1,
	step double precision DEFAULT 1,
	skip_expr text DEFAULT NULL
)
	RETURNS raster
	AS $$
	DECLARE
		x int;
		y int;
		value double precision;
		values double precision[][][];
		result boolean;
		expr text;
		_rast raster;
		nband int;
	BEGIN
		IF rast IS NULL THEN
			nband := 1;
			_rast := ST_AddBand(ST_MakeEmptyRaster(columns, rows, 0, 0, 1, -1, 0, 0, 0), nband, pixtype, 0, nodataval);
		ELSE
			nband := ST_NumBands(rast) + 1;
			_rast := ST_AddBand(rast, nband, pixtype, 0, nodataval);
		END IF;

		value := start_val;
		values := array_fill(NULL::double precision, ARRAY[columns, rows]);

		FOR y IN 1..columns LOOP
			FOR x IN 1..rows LOOP
				IF skip_expr IS NULL OR length(skip_expr) < 1 THEN
					result := TRUE;
				ELSE
					expr := replace(skip_expr, '[v]'::text, value::text);
					EXECUTE 'SELECT (' || expr || ')::boolean' INTO result;
				END IF;
				
				IF result IS TRUE THEN
					values[y][x] := value;
				END IF;

				value := value + step;
			END LOOP;
		END LOOP;

		_rast := ST_SetValues(_rast, nband, 1, 1, values);
		RETURN _rast;
	END;
	$$ LANGUAGE 'plpgsql';

INSERT INTO raster_dumpvalues
	SELECT 1, make_raster(NULL, '8BSI', 3, 3, 0, 1) UNION ALL
	SELECT 2, make_raster(NULL, '8BSI', 3, 3, 0, -1) UNION ALL
	SELECT 3, make_raster(NULL, '8BSI', 3, 3, 0, 1) UNION ALL
	SELECT 4, make_raster(NULL, '8BSI', 3, 3, 0, -2) UNION ALL
	SELECT 5, make_raster(NULL, '8BSI', 3, 3, 0, 2)
;

INSERT INTO raster_dumpvalues
	SELECT
		rid + 10,
		make_raster(rast, '16BSI', 3, 3, rid, (rid / 2)::integer)
	FROM raster_dumpvalues
	WHERE rid <= 10;

INSERT INTO raster_dumpvalues
	SELECT
		rid + 10,
		make_raster(rast, '32BSI', 3, 3, rid, (rid / 2)::integer)
	FROM raster_dumpvalues
	WHERE rid BETWEEN 11 AND 20;

DROP FUNCTION IF EXISTS make_raster(raster, text, integer, integer, double precision, double precision, double precision, text);

SELECT
	rid,
	(ST_DumpValues(rast)).*
FROM raster_dumpvalues
ORDER BY rid;

SELECT
	rid,
	(ST_DumpValues(rast, ARRAY[3,2,1])).*
FROM raster_dumpvalues
WHERE rid > 20
ORDER BY rid;

DROP TABLE IF EXISTS raster_dumpvalues;

SELECT (ST_DumpValues(ST_AddBand(ST_MakeEmptyRaster(0, 0, 0, 0, 1), ARRAY[ROW(NULL, '8BUI', 255, 0),ROW(NULL, '16BUI', 1, 2)]::addbandarg[]))).*;


-- #3086
DROP TABLE IF EXISTS raster_tile;
CREATE TABLE raster_tile AS
    WITH foo AS (
        SELECT ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(3, 3, 0, 0, 1, -1, 0, 0, 0), 1, '8BUI', 1, 0), 2, '8BUI', 10, 0) AS rast UNION ALL
        SELECT ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(3, 3, 3, 0, 1, -1, 0, 0, 0), 1, '8BUI', 2, 0), 2, '8BUI', 20, 0) AS rast 
    )
    SELECT ST_Union(rast) AS rast FROM foo;
WITH foo AS (SELECT ST_Tile(rast, 3, 3, TRUE) AS rast FROM raster_tile) SELECT (ST_DumpValues(rast, array[1,2,3])).* FROM foo;
DROP TABLE IF EXISTS raster_tile;
