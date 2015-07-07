-----------------------------------------------------------------------
--
-- Copyright (c) 2010 Pierre Racine <pierre.racine@sbf.ulaval.ca>
--
-- This program is free software; you can redistribute it and/or
-- modify it under the terms of the GNU General Public License
-- as published by the Free Software Foundation; either version 2
-- of the License, or (at your option) any later version.
--
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
--
-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software Foundation,
-- Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
-----------------------------------------------------------------------

-----------------------------------------------------------------------
--- Test of "ST_AddBand".
-----------------------------------------------------------------------


-----------------------------------------------------------------------
--- ST_AddBand
-----------------------------------------------------------------------

SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '1BB', -1, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '1BB', 0, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '1BB', 1, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '1BB', 2, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '1BB', 21.46, NULL), 3, 3);

SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '2BUI', -1, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '2BUI', 0, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '2BUI', 3, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '2BUI', 4, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '2BUI', 21.46, NULL), 3, 3);

SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '4BUI', -1, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '4BUI', 0, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '4BUI', 15, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '4BUI', 16, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '4BUI', 21.46, NULL), 3, 3);

SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '8BSI', -129, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '8BSI', -128, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '8BSI', 0, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '8BSI', 127, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '8BSI', 128, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '8BSI', 21.46, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '8BSI', 210.46, NULL), 3, 3);

SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '8BUI', -1, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '8BUI', 0, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '8BUI', 255, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '8BUI', 256, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '8BUI', 21.46, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '8BUI', 410.46, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '8BUI', 255.9999999, NULL), 3, 3);

SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '16BSI', -32769, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '16BSI', -32768, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '16BSI', 0, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '16BSI', 32767, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '16BSI', 32768, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '16BSI', 21.46, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '16BSI', 210000.46, NULL), 3, 3);

SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '16BUI', -1, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '16BUI', 0, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '16BUI', 65535, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '16BUI', 65537, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '16BUI', 21.46, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '16BUI', 210000.4645643647457, NULL), 3, 3);

SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BSI', -2147483649, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BSI', -2147483648, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BSI', 0, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BSI', 2147483647, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BSI', 2147483648, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BSI', 21.46, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BSI', 210000.4645643647457, NULL), 3, 3);

SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BUI', -1, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BUI', 0, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BUI', 4294967295, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BUI', 4294967296, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BUI', 214294967296, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BUI', 21.46, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BUI', 4294967295.9999999, NULL), 3, 3);

SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BF', 0, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BF', 4294967000, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BF', 4294967000, NULL), 3, 3)::float4;
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BF', 4294967295, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BF', 4294967295, NULL), 3, 3)::float4;
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BF', 4294967296, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BF', 4294967296, NULL), 3, 3)::float4;
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BF', 21.46, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BF', 21.46, NULL), 3, 3)::float4;
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BF', 21003.1, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BF', 21003.1, NULL), 3, 3)::float4;
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BF', 123.456, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BF', 123.456, NULL), 3, 3)::float4;
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BF', 1234.567, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BF', 1234.567, NULL), 3, 3)::float4;
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BF', 210000.4645643647457, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '32BF', 210000.4645643647457, NULL), 3, 3)::float4;

SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '64BF', -1, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '64BF', 0, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '64BF', 14294967296.123456, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '64BF', 21.46, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '64BF', 21003.1, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '64BF', 123.4567, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '64BF', 1234.567, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '64BF', 210000.4645643647457, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, '64BF', 1234.4645643647457, NULL), 3, 3);
SELECT St_Value(ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0,0), 1, '64BF', 1234.5678, NULL), ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1, 1), 3, 3);
SELECT St_Value(ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0,0), 1, '64BF', 1234.5678, NULL), ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0), 1), 3, 3);
SELECT St_Value(ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0,0), 1, '64BF', 1234.5678, NULL), ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0)), 3, 3);

-- multiple addbandarg version
SELECT
	St_Value(rast, 1, 3, 3),
	St_Value(rast, 2, 3, 3)
FROM (
	SELECT
		ST_AddBand(
			ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0),
			ARRAY[
				ROW(1, '64BF', 1234.567, NULL),
				ROW(2, '8BUI', 255, NULL)
			]::addbandarg[]
		) AS rast
) foo;

SELECT * FROM ST_BandMetadata(
	ST_AddBand(
		NULL::raster,
		ARRAY[
			ST_AddBand(ST_MakeEmptyRaster(5, 5, 0, 0, 1, -1, 0, 0, 0), 1, '4BUI', 1, 0),
			ST_AddBand(ST_MakeEmptyRaster(5, 5, 0, 0, 1, -1, 0, 0, 0), 1, '8BUI', 1, 0),
			ST_AddBand(ST_MakeEmptyRaster(5, 5, 0, 0, 1, -1, 0, 0, 0), 1, '16BUI', 1, 0),
			ST_AddBand(ST_MakeEmptyRaster(5, 5, 0, 0, 1, -1, 0, 0, 0), 1, '32BUI', 1, 0),
			ST_AddBand(ST_MakeEmptyRaster(5, 5, 0, 0, 1, -1, 0, 0, 0), 1, '64BF', 1, 0)
		]::raster[]
	), ARRAY[]::int[]
);

SELECT * FROM ST_BandMetadata(
	ST_AddBand(
		ST_AddBand(ST_MakeEmptyRaster(5, 5, 0, 0, 1, -1, 0, 0, 0), 1, '2BUI', 1, 0),
		ARRAY[
			ST_AddBand(ST_MakeEmptyRaster(5, 5, 0, 0, 1, -1, 0, 0, 0), 1, '4BUI', 1, 0),
			ST_AddBand(ST_MakeEmptyRaster(5, 5, 0, 0, 1, -1, 0, 0, 0), 1, '8BUI', 1, 0),
			ST_AddBand(ST_MakeEmptyRaster(5, 5, 0, 0, 1, -1, 0, 0, 0), 1, '16BUI', 1, 0),
			ST_AddBand(ST_MakeEmptyRaster(5, 5, 0, 0, 1, -1, 0, 0, 0), 1, '32BUI', 1, 0),
			ST_AddBand(ST_MakeEmptyRaster(5, 5, 0, 0, 1, -1, 0, 0, 0), 1, '64BF', 1, 0)
		]::raster[]
	), ARRAY[]::int[]
);

SELECT * FROM ST_BandMetadata(
	ST_AddBand(
		ST_AddBand(ST_MakeEmptyRaster(5, 5, 0, 0, 1, -1, 0, 0, 0), 1, '2BUI', 1, 0),
		ARRAY[
			ST_AddBand(ST_MakeEmptyRaster(5, 5, 0, 0, 1, -1, 0, 0, 0), 1, '4BUI', 1, 0),
			ST_AddBand(ST_MakeEmptyRaster(5, 5, 0, 0, 1, -1, 0, 0, 0), 1, '8BUI', 1, 0),
			ST_AddBand(ST_MakeEmptyRaster(5, 5, 0, 0, 1, -1, 0, 0, 0), 1, '16BUI', 1, 0),
			ST_AddBand(ST_MakeEmptyRaster(5, 5, 0, 0, 1, -1, 0, 0, 0), 1, '32BUI', 1, 0),
			ST_AddBand(ST_MakeEmptyRaster(5, 5, 0, 0, 1, -1, 0, 0, 0), 1, '64BF', 1, 0)
		]::raster[],
		1,
		1
	), ARRAY[]::int[]
);

-- raster array version test
SELECT (ST_DumpAsPolygons(newrast,3)).val As b3val FROM (SELECT ST_AddBand(NULL, array_agg(rast)) AS newrast FROM (SELECT ST_AsRaster(ST_Buffer(ST_Point(10,10), 34),200,200, '8BUI',i*30) As rast FROM generate_series(1,3) As i ) As foo ) As foofoo;

-- out-db variants
SELECT
	1,
	bandnum,
	isoutdb,
	CASE
		WHEN isoutdb IS TRUE
			THEN strpos(path, 'testraster.tif') > 0
		ELSE NULL
	END
FROM ST_BandMetadata((SELECT rast FROM raster_outdb_template WHERE rid = 1), ARRAY[]::int[]);
SELECT
	2,
	bandnum,
	isoutdb,
	CASE
		WHEN isoutdb IS TRUE
			THEN strpos(path, 'testraster.tif') > 0
		ELSE NULL
	END
FROM ST_BandMetadata((SELECT rast FROM raster_outdb_template WHERE rid = 2), ARRAY[]::int[]);
SELECT
	3,
	bandnum,
	isoutdb,
	CASE
		WHEN isoutdb IS TRUE
			THEN strpos(path, 'testraster.tif') > 0
		ELSE NULL
	END
FROM ST_BandMetadata((SELECT rast FROM raster_outdb_template WHERE rid = 3), ARRAY[]::int[]);
SELECT
	4,
	bandnum,
	isoutdb,
	CASE
		WHEN isoutdb IS TRUE
			THEN strpos(path, 'testraster.tif') > 0
		ELSE NULL
	END
FROM ST_BandMetadata((SELECT rast FROM raster_outdb_template WHERE rid = 4), ARRAY[]::int[]);

-- #3020
SET postgis.gdal_enabled_drivers = 'GTiff';
SET postgis.enable_outdb_rasters = true;
WITH foo AS (
	SELECT
			path
	FROM ST_BandMetadata(
		(SELECT rast FROM raster_outdb_template WHERE rid = 1),
	 	ARRAY[]::int[]
	)
	LIMIT 1
), raster as (
SELECT
	ST_AddBand(
		ST_MakeEmptyRaster(90, 90, 0., 0., 1, -1, 0, 0, 0),
		1, foo.path, NULL::int[]
	) AS rast
FROM foo
)
SELECT
	ST_Value(rast, 1, 1)
FROM raster
UNION ALL
SELECT
	ST_Value(rast, 6, 45)
FROM raster
UNION ALL
SELECT
	ST_Value(rast, 90, 50)
FROM raster
UNION ALL
SELECT
	ST_Value(rast, 100, 100)
FROM raster
