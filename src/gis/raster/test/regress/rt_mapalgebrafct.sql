CREATE OR REPLACE FUNCTION ST_TestRaster(ulx float8, uly float8, val float8) 
    RETURNS raster AS 
    $$
    DECLARE
    BEGIN
        RETURN ST_AddBand(ST_MakeEmptyRaster(10, 10, ulx, uly, 1, 1, 0, 0, 0), '32BF', val, -1);
    END;
    $$
    LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION raster_plus_twenty(pixel FLOAT, VARIADIC args TEXT[])
    RETURNS FLOAT AS 
    $$
    BEGIN
        RETURN pixel + 20;
    END;
    $$ 
    LANGUAGE 'plpgsql' IMMUTABLE;

CREATE OR REPLACE FUNCTION raster_plus_arg1(pixel FLOAT, VARIADIC args TEXT[])
    RETURNS FLOAT AS 
    $$
    DECLARE
        x float := 0;
    BEGIN
        IF NOT args[1] IS NULL THEN
            x := args[1]::float;
        END IF;
        RETURN pixel + x;
    END;
    $$ 
    LANGUAGE 'plpgsql' IMMUTABLE;

CREATE OR REPLACE FUNCTION raster_polynomial(pixel FLOAT, VARIADIC args TEXT[])
    RETURNS FLOAT AS
    $$
    DECLARE
        m float := 1;
        b float := 0;
    BEGIN
        IF NOT args[1] is NULL THEN
            m := args[1]::float;
        END IF;
        IF NOT args[2] is NULL THEN
            b := args[2]::float;
        END IF;
        RETURN m * pixel + b;
    END;
    $$
    LANGUAGE 'plpgsql' IMMUTABLE;

 CREATE OR REPLACE FUNCTION raster_nullage(pixel FLOAT, VARIADIC args TEXT[])
    RETURNS FLOAT AS
    $$
    BEGIN
        RETURN NULL;
    END;
    $$
    LANGUAGE 'plpgsql' IMMUTABLE;

 CREATE OR REPLACE FUNCTION raster_x_plus_arg(pixel FLOAT, pos INT[], VARIADIC args TEXT[])
    RETURNS FLOAT AS
    $$
    DECLARE
        x float := 0;
    BEGIN
        IF NOT args[1] IS NULL THEN
            x := args[1]::float;
        END IF;
        RETURN pixel + pos[1] + x;
    END;
    $$
    LANGUAGE 'plpgsql' IMMUTABLE;

 CREATE OR REPLACE FUNCTION raster_y_plus_arg(pixel FLOAT, pos INT[], VARIADIC args TEXT[])
    RETURNS FLOAT AS
    $$
    DECLARE
        x float := 0;
    BEGIN
        IF NOT args[1] IS NULL THEN
            x := args[1]::float;
        END IF;
        RETURN pixel + pos[2] + x;
    END;
    $$
    LANGUAGE 'plpgsql' IMMUTABLE;

-- Test NULL raster
SELECT ST_MapAlgebraFct(NULL, 1, NULL, 'raster_plus_twenty(float, text[])'::regprocedure) IS NULL FROM ST_TestRaster(0, 0, -1) rast;
SELECT ST_MapAlgebraFct(NULL, 1, NULL, 'raster_plus_twenty(float, text[])'::regprocedure, NULL) IS NULL FROM ST_TestRaster(0, 0, -1) rast;

-- Test empty raster
SELECT ST_IsEmpty(ST_MapAlgebraFct(ST_MakeEmptyRaster(0, 10, 0, 0, 1, 1, 1, 1, 0), 1, NULL, 'raster_plus_twenty(float, text[])'::regprocedure));
SELECT ST_IsEmpty(ST_MapAlgebraFct(ST_MakeEmptyRaster(0, 10, 0, 0, 1, 1, 1, 1, 0), 1, NULL, 'raster_plus_twenty(float, text[])'::regprocedure, NULL));

-- Test hasnoband raster
SELECT ST_HasNoBand(ST_MapAlgebraFct(ST_MakeEmptyRaster(10, 10, 0, 0, 1, 1, 1, 1, 0), 1, NULL, 'raster_plus_twenty(float, text[])'::regprocedure));
SELECT ST_HasNoBand(ST_MapAlgebraFct(ST_MakeEmptyRaster(10, 10, 0, 0, 1, 1, 1, 1, 0), 1, NULL, 'raster_plus_twenty(float, text[])'::regprocedure, NULL));

-- Test hasnodata value
SELECT ST_Value(rast, 1, 1), ST_Value(ST_MapAlgebraFct(ST_SetBandNoDataValue(rast, NULL), 1, NULL, 'raster_plus_twenty(float, text[])'::regprocedure), 1, 1) FROM ST_TestRaster(0, 0, -1) rast;
SELECT ST_Value(rast, 1, 1), ST_Value(ST_MapAlgebraFct(ST_SetBandNoDataValue(rast, NULL), 1, NULL, 'raster_plus_twenty(float, text[])'::regprocedure, NULL), 1, 1) FROM ST_TestRaster(0, 0, -1) rast;

-- Test user function
SELECT ST_Value(rast, 1, 1), ST_Value(ST_MapAlgebraFct(rast, 1, NULL, 'raster_plus_twenty(float, text[])'::regprocedure), 1, 1) FROM ST_TestRaster(0, 0, -1) rast;
SELECT ST_Value(rast, 1, 1), ST_Value(ST_MapAlgebraFct(ST_SetBandNoDataValue(rast, NULL), 1, NULL, 'raster_plus_twenty(float, text[])'::regprocedure, NULL), 1, 1) FROM ST_TestRaster(0, 0, -1) rast;

-- Test pixeltype
SELECT ST_Value(rast, 1, 1), ST_Value(ST_MapAlgebraFct(rast, 1, '4BUI', 'raster_plus_twenty(float, text[])'::regprocedure), 1, 1) FROM ST_TestRaster(0, 0, 100) rast;
SELECT ST_Value(rast, 1, 1), ST_Value(ST_MapAlgebraFct(rast, 1, '4BUId', 'raster_plus_twenty(float, text[])'::regprocedure), 1, 1) FROM ST_TestRaster(0, 0, 100) rast;
SELECT ST_Value(rast, 1, 1), ST_Value(ST_MapAlgebraFct(rast, 1, '2BUI', 'raster_plus_twenty(float, text[])'::regprocedure), 1, 1) FROM ST_TestRaster(0, 0, 101) rast;

-- Test user callbacks
SELECT ST_Value(rast, 1, 1) + 13, ST_Value(ST_MapAlgebraFct(rast, 1, NULL, 'raster_plus_arg1(float, text[])'::regprocedure, '13'), 1, 1) FROM ST_TestRaster(0, 0, 200) AS rast;

SELECT ST_Value(rast, 1, 1) * 21 + 14, ST_Value(ST_MapAlgebraFct(rast, 1, NULL, 'raster_polynomial(float, text[])'::regprocedure, '21', '14'), 1, 1) FROM ST_TestRaster(0, 0, 300) AS rast;

-- Test null return from a user function = NODATA cell value
SELECT ST_Value(rast, 1, 1), ST_Value(ST_MapAlgebraFct(rast, 1, NULL, 'raster_nullage(float, text[])'::regprocedure), 1, 1) FROM ST_TestRaster(0, 0, 100) AS rast;

SELECT ST_Value(rast, 3, 8) + 13 + 3, ST_Value(ST_MapAlgebraFct(rast, 1, NULL, 'raster_x_plus_arg(float, int[], text[])'::regprocedure, '13'), 3, 8) FROM ST_TestRaster(0, 0, 100) AS rast;
SELECT ST_Value(rast, 3, 8) + 13 + 8, ST_Value(ST_MapAlgebraFct(rast, 1, NULL, 'raster_y_plus_arg(float, int[], text[])'::regprocedure, '13'), 3, 8) FROM ST_TestRaster(0, 0, 100) AS rast;

DROP FUNCTION ST_TestRaster(ulx float8, uly float8, val float8);
DROP FUNCTION raster_plus_twenty(pixel FLOAT, VARIADIC args TEXT[]);
DROP FUNCTION raster_plus_arg1(pixel FLOAT, VARIADIC args TEXT[]);
DROP FUNCTION raster_polynomial(pixel FLOAT, VARIADIC args TEXT[]);
DROP FUNCTION raster_nullage(pixel FLOAT, VARIADIC args TEXT[]);
DROP FUNCTION raster_x_plus_arg(pixel FLOAT, pos INT[], VARIADIC args TEXT[]);
DROP FUNCTION raster_y_plus_arg(pixel FLOAT, pos INT[], VARIADIC args TEXT[]);
