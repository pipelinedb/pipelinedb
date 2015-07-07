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

-- test st_max4ma, uniform values
SELECT
  ST_Value(rast, 2, 2) = 1,
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'st_max4ma(float[][], text, text[])'::regprocedure, NULL, NULL), 2, 2
  ) = 1
  FROM ST_TestRasterNgb(3, 3, 1) AS rast;

-- test st_max4ma, obvious max value
SELECT
  ST_Value(rast, 2, 2) = 1,
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'st_max4ma(float[][], text, text[])'::regprocedure, NULL, NULL), 2, 2
  ) = 11
  FROM ST_SetValue(ST_TestRasterNgb(3, 3, 1), 1, 1, 11) AS rast;

-- test st_max4ma, value substitution
SELECT
  ST_Value(rast, 2, 2) = 1,
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'st_max4ma(float[][], text, text[])'::regprocedure, '22', NULL), 2, 2
  ) = 22 
  FROM ST_SetValue(ST_TestRasterNgb(3, 3, 1), 3, 3, NULL) AS rast;

-- test st_min4ma, uniform values
SELECT
  ST_Value(rast, 2, 2) = 1,
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'st_min4ma(float[][], text, text[])'::regprocedure, NULL, NULL), 2, 2
  ) = 1
  FROM ST_TestRasterNgb(3, 3, 1) AS rast;

-- test st_min4ma, obvious min value
SELECT
  ST_Value(rast, 2, 2) = 11,
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'st_min4ma(float[][], text, text[])'::regprocedure, NULL, NULL), 2, 2
  ) = 1
  FROM ST_SetValue(ST_TestRasterNgb(3, 3, 11), 1, 1, 1) AS rast;

-- test st_min4ma, value substitution
SELECT
  ST_Value(rast, 2, 2) = 22,
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'st_min4ma(float[][], text, text[])'::regprocedure, '2', NULL), 2, 2
  ) = 2 
  FROM ST_SetValue(ST_TestRasterNgb(3, 3, 22), 3, 3, NULL) AS rast;

-- test st_sum4ma happens in rt_mapalgebrafctngb.sql

-- test st_mean4ma, uniform values
SELECT
  ST_Value(rast, 2, 2) = 1,
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'st_mean4ma(float[][], text, text[])'::regprocedure, NULL, NULL), 2, 2
  ) = 1
  FROM ST_TestRasterNgb(3, 3, 1) AS rast;

-- test st_mean4ma, obvious min value
SELECT
  ST_Value(rast, 2, 2) = 10,
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'st_mean4ma(float[][], text, text[])'::regprocedure, NULL, NULL), 2, 2
  ) = 9
  FROM ST_SetValue(ST_TestRasterNgb(3, 3, 10), 1, 1, 1) AS rast;

-- test st_mean4ma, value substitution
SELECT
  ST_Value(rast, 2, 2) = 10,
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'st_mean4ma(float[][], text, text[])'::regprocedure, '1', NULL), 2, 2
  ) = 9 
  FROM ST_SetValue(ST_TestRasterNgb(3, 3, 10), 3, 3, NULL) AS rast;

-- test st_range4ma, uniform values
SELECT
  ST_Value(rast, 2, 2) = 1,
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'st_range4ma(float[][], text, text[])'::regprocedure, NULL, NULL), 2, 2
  ) = 0
  FROM ST_TestRasterNgb(3, 3, 1) AS rast;

-- test st_range4ma, obvious min value
SELECT
  ST_Value(rast, 2, 2) = 10,
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'st_range4ma(float[][], text, text[])'::regprocedure, NULL, NULL), 2, 2
  ) = 9
  FROM ST_SetValue(ST_TestRasterNgb(3, 3, 10), 1, 1, 1) AS rast;

-- test st_range4ma, value substitution
SELECT
  ST_Value(rast, 2, 2) = 10,
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'st_range4ma(float[][], text, text[])'::regprocedure, '1', NULL), 2, 2
  ) = 9 
  FROM ST_SetValue(ST_TestRasterNgb(3, 3, 10), 3, 3, NULL) AS rast;

-- test st_slope, flat plane
SELECT
  ST_Value(rast, 2, 2) = 1,
  ST_Value(
    ST_Slope(rast, 1, NULL), 2, 2
  ) = 0
  FROM ST_TestRasterNgb(3, 3, 1) AS rast;

-- test st_slope, corner spike
SELECT
  ST_Value(rast, 2, 2) = 1,
  round(ST_Value(
    ST_Slope(rast, 1, NULL), 2, 2
  )*100000) = 5784902
  FROM ST_SetValue(ST_TestRasterNgb(3, 3, 1), 1, 1, 10) AS rast;

-- test st_slope, corner droop
SELECT
  ST_Value(rast, 2, 2) = 10,
  round(ST_Value(
    ST_Slope(rast, 1, NULL), 2, 2
  )*100000) = 5784902
  FROM ST_SetValue(ST_TestRasterNgb(3, 3, 10), 1, 1, 1) AS rast;

-- test st_aspect, flat plane
SELECT
  ST_Value(rast, 2, 2) = 1,
  ST_Value(
    ST_Aspect(rast, 1, NULL), 2, 2
  ) = -1
  FROM ST_SetBandNoDataValue(ST_TestRasterNgb(3, 3, 1), NULL) AS rast;

-- test st_aspect, North
SELECT
  ST_Value(rast, 2, 2) = 1,
  round(ST_Value(
    ST_Aspect(rast, 1, NULL), 2, 2
  )*10000) = 0
  FROM ST_SetValue(ST_TestRasterNgb(3, 3, 1), 2, 1, 0) AS rast;

-- test st_aspect, South
SELECT
  ST_Value(rast, 2, 2) = 1,
  round(ST_Value(
    ST_Aspect(rast, 1, NULL), 2, 2
  )*10000) = 1800000
  FROM ST_SetValue(ST_TestRasterNgb(3, 3, 1), 2, 3, 0) AS rast;

-- test st_aspect, East
SELECT
  ST_Value(rast, 2, 2) = 1,
  round(ST_Value(
    ST_Aspect(rast, 1, NULL), 2, 2
  )*10000) = 900000
  FROM ST_SetValue(ST_SetScale(ST_TestRasterNgb(3, 3, 1), 1, -1), 3, 2, 0) AS rast;

-- test st_aspect, West
SELECT
  ST_Value(rast, 2, 2) = 1,
  round(ST_Value(
    ST_Aspect(rast, 1, NULL), 2, 2
  )*10000) = 2700000
  FROM ST_SetValue(ST_SetScale(ST_TestRasterNgb(3, 3, 1), 1, -1), 1, 2, 0) AS rast;

-- test st_hillshade, flat plane
SELECT
  ST_Value(rast, 2, 2) = 1,
  round(ST_Value(
    ST_Hillshade(rast, 1, NULL, 0.0, 45., 255), 2, 2
  )*10000) = 1803122
  FROM ST_TestRasterNgb(3, 3, 1) AS rast;

-- test st_hillshade, known set from: http://webhelp.esri.com/arcgiSDEsktop/9.3/index.cfm?TopicName=How%20Hillshade%20works
SELECT
  round(ST_Value(
    ST_Hillshade(rast, 1, NULL, 315.0, 45., 255, 1.0), 2, 2
  )*10000) = 1540287
  FROM ST_SetValue(
    ST_SetValue(
      ST_SetValue(
        ST_SetValue(
          ST_SetValue(
            ST_SetValue(
              ST_SetValue(
                ST_SetValue(
                  ST_SetValue(
                    ST_SetScale(ST_TestRasterNgb(3, 3, 1), 5), 1, 1, 2450
                  ), 2, 1, 2461
                ), 3, 1, 2483
              ), 1, 2, 2452
            ), 2, 2, 2461
          ), 3, 2, 2483
        ), 1, 3, 2447
      ), 2, 3, 2455
    ), 3, 3, 2477
  ) AS rast;

-- test st_hillshade, defaults on known set from: http://webhelp.esri.com/arcgiSDEsktop/9.3/index.cfm?TopicName=How%20Hillshade%20works  
SELECT
  round(ST_Value(
    ST_Hillshade(rast, 1, NULL, 315.0, 45.), 2, 2
  )*10000) = 1540287
  FROM ST_SetValue(
    ST_SetValue(
      ST_SetValue(
        ST_SetValue(
          ST_SetValue(
            ST_SetValue(
              ST_SetValue(
                ST_SetValue(
                  ST_SetValue(
                    ST_SetScale(ST_TestRasterNgb(3, 3, 1), 5), 1, 1, 2450
                  ), 2, 1, 2461
                ), 3, 1, 2483
              ), 1, 2, 2452
            ), 2, 2, 2461
          ), 3, 2, 2483
        ), 1, 3, 2447
      ), 2, 3, 2455
    ), 3, 3, 2477
  ) AS rast;

-- test st_distinct4ma, all one value
SELECT
  ST_Value(rast, 2, 2) = 10,
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'st_distinct4ma(float[][], text, text[])'::regprocedure, '1', NULL), 2, 2
  ) = 1 
  FROM ST_TestRasterNgb(3, 3, 10) AS rast;

-- test st_distinct4ma, three distinct values
SELECT
  ST_Value(rast, 2, 2) = 1,
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'st_distinct4ma(float[][], text, text[])'::regprocedure, '1', NULL), 2, 2
  ) = 3
  FROM ST_SetValue(
    ST_SetValue(
      ST_TestRasterNgb(3, 3, 1), 1, 1, 5
    ), 3, 3, 7
  ) AS rast;

-- test st_stddev4ma, all one value
SELECT
  ST_Value(rast, 2, 2) = 10,
  ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'st_stddev4ma(float[][], text, text[])'::regprocedure, '1', NULL), 2, 2
  ) = 0
  FROM ST_TestRasterNgb(3, 3, 10) AS rast;

-- test st_stddev4ma, values 1-9
SELECT
  ST_Value(rast, 2, 2) = 5,
  round(ST_Value(
    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'st_stddev4ma(float[][], text, text[])'::regprocedure, '1', NULL), 2, 2
  ) * 1000000) = 2738613
  FROM ST_SetValue(
    ST_SetValue(
      ST_SetValue(
        ST_SetValue(
          ST_SetValue(
            ST_SetValue(
              ST_SetValue(
                ST_SetValue(
                  ST_TestRasterNgb(3, 3, 1), 2, 1, 2
                ), 3, 1, 3
              ), 1, 2, 4
            ), 2, 2, 5
          ), 3, 2, 6
        ), 1, 3, 7
      ), 2, 3, 8
    ), 3, 3, 9
  ) AS rast;

DROP FUNCTION ST_Nullage(matrix float[][], nodatamode text, VARIADIC args text[]);
DROP FUNCTION ST_TestRasterNgb(h integer, w integer, val float8);
