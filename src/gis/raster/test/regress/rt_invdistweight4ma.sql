DROP TABLE IF EXISTS raster_value_arrays;
CREATE TABLE raster_value_arrays (
	id integer,
	val double precision[][]
);
CREATE OR REPLACE FUNCTION make_value_array(
	rows integer DEFAULT 3,
	columns integer DEFAULT 3,
	start_val double precision DEFAULT 1,
	step double precision DEFAULT 1,
	skip_expr text DEFAULT NULL
)
	RETURNS double precision[][][]
	AS $$
	DECLARE
		x int;
		y int;
		value double precision;
		values double precision[][][];
		result boolean;
		expr text;
	BEGIN
		value := start_val;

		values := array_fill(NULL::double precision, ARRAY[1, columns, rows]);

		FOR y IN 1..columns LOOP
			FOR x IN 1..rows LOOP
				IF skip_expr IS NULL OR length(skip_expr) < 1 THEN
					result := TRUE;
				ELSE
					expr := replace(skip_expr, '[v]'::text, value::text);
					EXECUTE 'SELECT (' || expr || ')::boolean' INTO result;
				END IF;
				
				IF result IS TRUE THEN
					values[1][y][x] := value;
				END IF;

				value := value + step;
			END LOOP;
		END LOOP;

		RETURN values;
	END;
	$$ LANGUAGE 'plpgsql';

INSERT INTO raster_value_arrays VALUES
	(1, make_value_array()),
	(2, make_value_array(5, 5)),
	(3, make_value_array(5, 5, 100)),
	(4, make_value_array(3, 3, 15, -1)),
	(5, make_value_array(5, 5, 15, -1)),
	(6, make_value_array(3, 3, 1, 2)),
	(7, make_value_array(5, 5, 1, 3)),

	(10, make_value_array(3, 3, 1, 1, '0')),
	(11, make_value_array(5, 5, 1, 1, '0')),
	(12, make_value_array(3, 3, 1, 1, '[v] % 2')),
	(13, make_value_array(5, 5, 1, 1, '[v] % 2')),
	(14, make_value_array(3, 3, 1, 1, '([v] % 2) = 0')),
	(15, make_value_array(5, 5, 1, 1, '([v] % 2) = 0')),
	(16, make_value_array(3, 3, 1, 2.1, '([v] NOT IN (7.3, 9.4, 15.7, 17.8))')),
	(17, make_value_array(3, 3, 0, 3.14, '([v] IN (3.14, 12.56, 25.12))')),
	(18, make_value_array(3, 3, 1, 1, '[v] > 8'))
;


SELECT
	id,
	val,
	round(st_invdistweight4ma(val, NULL, NULL)::numeric, 6) AS idw1,
	round(st_invdistweight4ma(val, NULL, '0')::numeric, 6) AS idw2,
	round(st_invdistweight4ma(val, NULL, '0.5')::numeric, 6) AS idw3,
	round(st_invdistweight4ma(val, NULL, '0.9')::numeric, 6) AS idw4,
	round(st_invdistweight4ma(val, NULL, '1')::numeric, 6) AS idw5,
	round(st_invdistweight4ma(val, NULL, '0.9', '1')::numeric, 6) AS idw6,
	round(st_invdistweight4ma(val, NULL, '0.9', '0.9')::numeric, 6) AS idw7,
	round(st_invdistweight4ma(val, NULL, '0.9', '0.75')::numeric, 6) AS idw8,
	round(st_invdistweight4ma(val, NULL, '0.9', '0.5')::numeric, 6) AS idw9,
	round(st_invdistweight4ma(val, NULL, '0.9', '0.25')::numeric, 6) AS idw10,
	round(st_invdistweight4ma(val, NULL, '0.9', '0.1')::numeric, 6) AS idw11,
	round(st_invdistweight4ma(val, NULL, '0.9', '0.01')::numeric, 6) AS idw12,
	round(st_invdistweight4ma(val, NULL, '0.9', '0.001')::numeric, 6) AS idw13,
	round(st_invdistweight4ma(val, NULL, '0.9', '0')::numeric, 6) AS idw14,
	round(st_mindist4ma(val, NULL)::numeric, 6) AS mindist4ma
FROM raster_value_arrays
ORDER BY id;

DROP TABLE IF EXISTS raster_value_arrays;
DROP FUNCTION IF EXISTS make_value_array(integer, integer, double precision, double precision, text);
