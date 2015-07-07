--
-- A user callback function that nullifies all cells in the resulting raster.
--
CREATE OR REPLACE FUNCTION ST_Nullage(matrix float[][], nodatamode text, VARIADIC args text[])
    RETURNS float AS
    $$
    BEGIN
        RETURN NULL;
    END;
    $$
    LANGUAGE 'plpgsql' IMMUTABLE;


--
--Test rasters
--
CREATE OR REPLACE FUNCTION ST_TestRasterNgb(h integer, w integer, val float8) 
    RETURNS raster AS 
    $$
    DECLARE
    BEGIN
        RETURN ST_AddBand(ST_MakeEmptyRaster(h, w, 0, 0, 1, 1, 0, 0, 0), '32BF', val, -1);
    END;
    $$
    LANGUAGE 'plpgsql';

-- Tests
-- Test NULL Raster. Should be true.
SELECT ST_MapAlgebraFctNgb(NULL, 1, NULL, 1, 1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, 'NULL', NULL) IS NULL FROM ST_TestRasterNgb(0, 0, -1) rast;

-- Test empty Raster. Should be true.
SELECT ST_IsEmpty(ST_MapAlgebraFctNgb(ST_MakeEmptyRaster(0, 10, 0, 0, 1, 1, 1, 1, 0), 1, NULL, 1, 1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, 'NULL', NULL));

-- Test has no band raster. Should be true
SELECT ST_HasNoBand(ST_MapAlgebraFctNgb(ST_MakeEmptyRaster(10, 10, 0, 0, 1, 1, 1, 1, 0), 1, NULL, 1, 1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, 'NULL', NULL));

-- Test huge neighborhood. Original raster returned.
SELECT
  ST_Value(rast, 2, 2) = 1,
  ST_Value(ST_MapAlgebraFctNgb(
      rast, 1, NULL, 5, 5, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, 'NULL', NULL
    ), 2, 2) = 1
 FROM ST_TestRasterNgb(3, 3, 1) AS rast;

-- Test negative width neighborhood. Original raster returned.
SELECT 
  ST_Value(rast, 2, 2) = 1,
  ST_Value(
    ST_MapAlgebraFctNgb(
      rast, 1, NULL, -1, 1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, 'NULL', NULL
    ), 2, 2) = 1
 FROM ST_TestRasterNgb(3, 3, 1) AS rast;

-- Test negative height neighborhood. Original raster returned.
SELECT 
  ST_Value(rast, 2, 2) = 1,
  ST_Value(
    ST_MapAlgebraFctNgb(
      rast, 1, NULL, 1, -1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, 'NULL', NULL
    ), 2, 2) = 1
 FROM ST_TestRasterNgb(3, 3, 1) AS rast;

-- Test has no nodata value. Should return null and 7.
SELECT 
  ST_Value(rast, 2, 2) IS NULL, 
  ST_Value(
    ST_MapAlgebraFctNgb(
      ST_SetBandNoDataValue(rast, NULL), 1, NULL, 1, 1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, 'NULL', NULL
    ), 2, 2) = 7
 FROM ST_SetValue(ST_TestRasterNgb(3, 3, 1), 2, 2, NULL) AS rast;

---- Test NULL nodatamode. Should return null and null.
SELECT 
  ST_Value(rast, 2, 2) IS NULL, 
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, 'NULL', NULL), 2, 2
  ) IS NULL
 FROM ST_SetValue(ST_TestRasterNgb(3, 3, 1), 2, 2, NULL) AS rast;

---- Test default nodatamode (ignore). Should return null and 8.
SELECT 
  ST_Value(rast, 2, 2) IS NULL, 
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, NULL, NULL), 2, 2
  ) = 8
 FROM ST_SetValue(ST_TestRasterNgb(3, 3, 1), 2, 2, NULL) AS rast;

---- Test ignore nodatamode. Should return null and 8.
SELECT 
  ST_Value(rast, 2, 2) IS NULL, 
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, 'ignore', NULL), 2, 2
  ) = 8
 FROM ST_SetValue(ST_TestRasterNgb(3, 3, 1), 2, 2, NULL) AS rast;

---- Test value nodatamode. Should return null and null.
SELECT 
  ST_Value(rast, 2, 2) IS NULL, 
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, 'value', NULL), 2, 2
  ) IS NULL 
 FROM ST_SetValue(ST_TestRasterNgb(3, 3, 1), 2, 2, NULL) AS rast;

-- Test value nodatamode. Should return null and 9.
SELECT 
  ST_Value(rast, 1, 1) IS NULL, 
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, 'value', NULL), 2, 2
  ) = 9
 FROM ST_SetValue(ST_TestRasterNgb(3, 3, 1), 1, 1, NULL) AS rast;

-- Test value nodatamode. Should return null and 0.
SELECT 
  ST_Value(rast, 2, 2) IS NULL, 
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, '-8', NULL), 2, 2
  ) = 0
 FROM ST_SetValue(ST_TestRasterNgb(3, 3, 1), 2, 2, NULL) AS rast;

-- Test value nodatamode. Should return null and 128.
SELECT 
  ST_Value(rast, 2, 2) IS NULL, 
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, '120', NULL), 2, 2
  ) = 128
 FROM ST_SetValue(ST_TestRasterNgb(3, 3, 1), 2, 2, NULL) AS rast;

-- Test value nodatamode. Should return null and null.
SELECT 
  ST_Value(rast, 2, 2) IS NULL, 
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, 'abcd', NULL), 2, 2
  ) IS NULL
 FROM ST_SetValue(ST_TestRasterNgb(3, 3, 1), 2, 2, NULL) AS rast;

-- Test ST_Sum user function. Should be 1 and 9.
SELECT 
  ST_Value(rast, 2, 2) = 1, 
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, 'NULL', NULL), 2, 2
  ) = 9
 FROM ST_TestRasterNgb(3, 3, 1) AS rast;

-- Test ST_Sum user function on a no nodata value raster. Should be null and -1.
SELECT 
  ST_Value(rast, 2, 2) IS NULL, 
  ST_Value(
    ST_MapAlgebraFctNgb(ST_SetBandNoDataValue(rast, NULL), 1, NULL, 1, 1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, 'NULL', NULL), 2, 2
  ) = -1
 FROM ST_SetValue(ST_TestRasterNgb(3, 3, 0), 2, 2, NULL) AS rast;

-- Test pixeltype 1. Should return 2 and 15.
SELECT
  ST_Value(rast, 2, 2) = 2,
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, '4BUI', 1, 1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, 'NULL', NULL), 2, 2
  ) = 15
 FROM ST_SetBandNoDataValue(ST_TestRasterNgb(3, 3, 2), 1, NULL) AS rast;

-- Test pixeltype 1. No error, changed to 32BF
SELECT 
  ST_Value(rast, 2, 2) = 2, 
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, '4BUId', 1, 1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, 'NULL', NULL), 2, 2
  ) = 18
 FROM ST_TestRasterNgb(3, 3, 2) AS rast;

-- Test pixeltype 1. Should return 1 and 3.
SELECT 
  ST_Value(rast, 2, 2) = 1, 
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, '2BUI', 1, 1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, 'NULL', NULL), 2, 2
  ) = 3
 FROM ST_SetBandNoDataValue(ST_TestRasterNgb(3, 3, 1), 1, NULL) AS rast;

-- Test that the neighborhood function leaves a border of NODATA
SELECT
  COUNT(*) = 1
 FROM (SELECT
    (ST_DumpAsPolygons(
      ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, 'NULL', NULL)
    )).*
   FROM ST_TestRasterNgb(5, 5, 1) AS rast) AS foo;

-- Test that the neighborhood function leaves a border of NODATA
SELECT
  ST_Area(geom) = 8, val = 9
 FROM (SELECT
    (ST_DumpAsPolygons(
      ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, 'NULL', NULL)
    )).*
   FROM ST_SetValue(ST_TestRasterNgb(5, 5, 1), 1, 1, NULL) AS rast) AS foo;

-- Test that the neighborhood function leaves a border of NODATA
-- plus a corner where one cell has a value of 8.
SELECT
  (ST_Area(geom) = 1 AND val = 8) OR (ST_Area(geom) = 8 AND val = 9)
 FROM (SELECT
    (ST_DumpAsPolygons(
      ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, 'ignore', NULL)
    )).*
   FROM ST_SetValue(ST_TestRasterNgb(5, 5, 1), 1, 1, NULL) AS rast) AS foo;

-- Test that the neighborhood function leaves a border of NODATA
-- plus a hole where 9 cells have NODATA
-- This results in a donut: a polygon with a hole. The polygon has
-- an area of 16, with a hole that has an area of 9
SELECT
  ST_NRings(geom) = 2,
  ST_NumInteriorRings(geom) = 1,
  ST_Area(geom) = 16, 
  val = 9, 
  ST_Area(ST_BuildArea(ST_InteriorRingN(geom, 1))) = 9
 FROM (SELECT
    (ST_DumpAsPolygons(
      ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, 'NULL', NULL)
    )).*
   FROM ST_SetValue(ST_TestRasterNgb(7, 7, 1), 4, 4, NULL) AS rast) AS foo;

-- Test that the neighborhood function leaves a border of NODATA,
-- and the center pyramids when summed twice, ignoring NODATA values
SELECT
  COUNT(*) = 9, SUM(ST_Area(geom)) = 9, SUM(val) = ((36+54+36) + (54+81+54) + (36+54+36)) 
 FROM (SELECT
    (ST_DumpAsPolygons(
      ST_MapAlgebraFctNgb(
        ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, 'ignore', NULL), 1, NULL, 1, 1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, 'ignore', NULL
      )
    )).*
   FROM ST_TestRasterNgb(5, 5, 1) AS rast) AS foo;

-- Test that the neighborhood function leaves a border of NODATA,
-- and the center contains one cel when summed twice, replacing NULL with NODATA values
SELECT
  COUNT(*) = 1, SUM(ST_Area(geom)) = 1, SUM(val) = 81
 FROM (SELECT
    (ST_DumpAsPolygons(
      ST_MapAlgebraFctNgb(
        ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, 'NULL', NULL), 1, NULL, 1, 1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, 'NULL', NULL
      )
    )).*
   FROM ST_TestRasterNgb(5, 5, 1) AS rast) AS foo;

-- test a user function that nullifies everything
SELECT 
  ST_Value(rast, 2, 2) = 2,
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Nullage(float[][], text, text[])'::regprocedure, 'NULL', NULL), 2, 2
  ) IS NULL
 FROM ST_TestRasterNgb(3, 3, 2) AS rast;

-- 'dog ate my homework' test
-- raster initialized to one NODATA value, then a literal value passed in as the 'nodatamode' parameter.
-- expect that the cells processed by the neighborhoods would be replaced by the 'nodatamode' parameter value, and not NODATA.
SELECT 
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, '8BUI', 1, 1, 'ST_Sum4ma(float[][], text, text[])'::regprocedure, '120', NULL), 2, 2
  ) = 200
FROM ST_SetValue(ST_SetBandNoDataValue(ST_TestRasterNgb(3, 3, 10), 0), 2, 2, 0) AS rast;

DROP FUNCTION ST_Nullage(matrix float[][], nodatamode text, VARIADIC args text[]);
DROP FUNCTION ST_TestRasterNgb(h integer, w integer, val float8);
