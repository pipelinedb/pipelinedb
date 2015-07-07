SET client_min_messages TO warning;

DROP TABLE IF EXISTS raster_nmapalgebra_in;
CREATE TABLE raster_nmapalgebra_mask_in (
	rid integer,
	rast raster
);

INSERT INTO raster_nmapalgebra_mask_in
	SELECT 0, NULL::raster AS rast UNION ALL
	SELECT 1, ST_MakeEmptyRaster(2, 2, 0, 0, 1, -1, 0, 0, 0) AS rast UNION ALL
	SELECT 2, ST_AddBand(ST_MakeEmptyRaster(2, 2, 0, 0, 1, -1, 0, 0, 0), 1, '8BUI', 1, 0) AS rast UNION ALL
	SELECT 3, ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(2, 2, 1, -1, 1, -1, 0, 0, 0), 1, '8BUI', 2, 0), 2, '32BF', 20, 0) AS rast UNION ALL
	SELECT 4, ST_AddBand(ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(2, 2, 1, -1, 1, -1, 0, 0, 0), 1, '8BUI', 2, 0), 2, '32BF', 20, 0), 3, '16BUI', 200, 0) AS rast
;

CREATE OR REPLACE FUNCTION raster_nmapalgebra_test(
	value double precision[][][],
	pos int[][],
	VARIADIC userargs text[]
)
	RETURNS double precision
	AS $$
	BEGIN
		RAISE NOTICE 'value = %', value;
		RAISE NOTICE 'pos = %', pos;
		RAISE NOTICE 'userargs = %', userargs;

		IF userargs IS NULL OR array_length(userargs, 1) < 1 THEN
			RETURN 255;
		ELSE
			RETURN userargs[array_lower(userargs, 1)];
		END IF;
	END;
	$$ LANGUAGE 'plpgsql' IMMUTABLE;

SET client_min_messages TO notice;

select st_mapalgebra(rast,1,'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,ARRAY[[1,1],[1,1],[1,1]]::double precision[],false) from raster_nmapalgebra_mask_in;

select st_mapalgebra(rast,1,'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,ARRAY[[1,1,1],[1,1,1]]::double precision[],false) from raster_nmapalgebra_mask_in;

select st_mapalgebra(rast,1,'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,ARRAY[[1,1,1,1],[1,1,1,1],[1,1,1,1],[1,1,1,1]]::double precision[],false) from raster_nmapalgebra_mask_in;

select st_mapalgebra(rast,1,'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,NULL::double precision[],false) from raster_nmapalgebra_mask_in;

select st_mapalgebra(rast,1,'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,ARRAY[NULL]::double precision[],false) from raster_nmapalgebra_mask_in;

select st_mapalgebra(rast,1,'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,ARRAY[1]::double precision[],false) from raster_nmapalgebra_mask_in;

select st_mapalgebra(rast,1,'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,ARRAY[[1]]::double precision[],false) from raster_nmapalgebra_mask_in;

select st_mapalgebra(rast,1,'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,ARRAY[[NULL]]::double precision[],false) from raster_nmapalgebra_mask_in;

select st_mapalgebra(rast,1,'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,ARRAY[[1,1,1],[1,1,1],[1,1,1]]::double precision[],false) from raster_nmapalgebra_mask_in;

select st_mapalgebra(rast,1,'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,ARRAY[[1,0,0],[0,0,0],[0,0,0]]::double precision[],false) from raster_nmapalgebra_mask_in;



select st_mapalgebra(rast,1,'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,ARRAY[[1,1],[1,1],[1,1]]::double precision[],true) from raster_nmapalgebra_mask_in;

select st_mapalgebra(rast,1,'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,ARRAY[[1,1,1],[1,1,1]]::double precision[],true) from raster_nmapalgebra_mask_in;

select st_mapalgebra(rast,1,'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,NULL::double precision[],true) from raster_nmapalgebra_mask_in;

select st_mapalgebra(rast,1,'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,ARRAY[NULL]::double precision[],true) from raster_nmapalgebra_mask_in;

select st_mapalgebra(rast,1,'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,ARRAY[1]::double precision[],true) from raster_nmapalgebra_mask_in;

select st_mapalgebra(rast,1,'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,ARRAY[[1]]::double precision[],true) from raster_nmapalgebra_mask_in;

select st_mapalgebra(rast,1,'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,ARRAY[[NULL]]::double precision[],true) from raster_nmapalgebra_mask_in;

select st_mapalgebra(rast,1,'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,ARRAY[[1,1,1],[1,1,1],[1,1,1]]::double precision[],true) from raster_nmapalgebra_mask_in;

select st_mapalgebra(rast,1,'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,ARRAY[[1,0,0],[0,0,0],[0,0,0]]::double precision[],true) from raster_nmapalgebra_mask_in;


select st_mapalgebra(rast,1,'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,ARRAY[[.5,.5],[.5,.5],[.5,.5]]::double precision[],true) from raster_nmapalgebra_mask_in;

select st_mapalgebra(rast,1,'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,ARRAY[[.5,.5,.5],[.5,.5,.5]]::double precision[],true) from raster_nmapalgebra_mask_in;

select st_mapalgebra(rast,1,'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,ARRAY[.5]::double precision[],true) from raster_nmapalgebra_mask_in;

select st_mapalgebra(rast,1,'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,ARRAY[[.5]]::double precision[],true) from raster_nmapalgebra_mask_in;

select st_mapalgebra(rast,1,'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,ARRAY[[.5,.5,.5],[.5,.5,.5],[.5,.5,.5]]::double precision[],true) from raster_nmapalgebra_mask_in;

select st_mapalgebra(rast,1,'raster_nmapalgebra_test(double precision[], int[], text[])'::regprocedure,ARRAY[[.5,0,0],[0,0,0],[0,0,0]]::double precision[],true) from raster_nmapalgebra_mask_in;

DROP FUNCTION IF EXISTS raster_nmapalgebra_test(double precision[], int[], text[]);
DROP TABLE IF EXISTS raster_nmapalgebra_mask_in;