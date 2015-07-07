-----------------------------------------------------------------------
--
-- Copyright (c) 2009-2010 Mateusz Loskot <mateusz@loskot.net>, David Zwarg <dzwarg@azavea.com>
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

CREATE TABLE rt_properties_test (
    id numeric,
    name text,
    srid integer,
    width integer,
    height integer,
    scalex double precision,
    scaley double precision,
    ipx double precision,
    ipy double precision,
    skewx double precision,
    skewy double precision,
    rast raster
);

INSERT INTO rt_properties_test 
VALUES ( 0, '10x20, ip:0.5,0.5 scale:2,3 skew:0,0 srid:10 width:10 height:20',
        10, 10, 20, --- SRID, width, height
        2, 3, 0.5, 0.5, 0, 0, --- georeference
(
'01' -- little endian (uint8 ndr)
|| 
'0000' -- version (uint16 0)
||
'0000' -- nBands (uint16 0)
||
'0000000000000040' -- scaleX (float64 2)
||
'0000000000000840' -- scaleY (float64 3)
||
'000000000000E03F' -- ipX (float64 0.5)
||
'000000000000E03F' -- ipY (float64 0.5)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0A000000' -- SRID (int32 10)
||
'0A00' -- width (uint16 10)
||
'1400' -- height (uint16 20)
)::raster
);

INSERT INTO rt_properties_test 
VALUES ( 1, '1x1, ip:2.5,2.5 scale:5,5 skew:0,0, srid:12, width:1, height:1',
        12, 1, 1, --- SRID, width, height
         5, 5, 2.5, 2.5, 0, 0, --- georeference
(
'01' -- little endian (uint8 ndr)
|| 
'0000' -- version (uint16 0)
||
'0000' -- nBands (uint16 0)
||
'0000000000001440' -- scaleX (float64 5)
||
'0000000000001440' -- scaleY (float64 5)
||
'0000000000000440' -- ipX (float64 2.5)
||
'0000000000000440' -- ipY (float64 2.5)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0C000000' -- SRID (int32 12)
||
'0100' -- width (uint16 1)
||
'0100' -- height (uint16 1)
)::raster
);

INSERT INTO rt_properties_test 
VALUES ( 2, '1x1, ip:7.5,2.5 scale:5,5 skew:0,0, srid:0, width:1, height:1',
         0, 1, 1, --- SRID, width, height
         5, 5, 7.5, 2.5, 0, 0, --- georeference
(
'01' -- little endian (uint8 ndr)
|| 
'0000' -- version (uint16 0)
||
'0000' -- nBands (uint16 0)
||
'0000000000001440' -- scaleX (float64 5)
||
'0000000000001440' -- scaleY (float64 5)
||
'0000000000001E40' -- ipX (float64 7.5)
||
'0000000000000440' -- ipY (float64 2.5)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'00000000' -- SRID (int32 0)
||
'0100' -- width (uint16 1)
||
'0100' -- height (uint16 1)
)::raster
);

INSERT INTO rt_properties_test 
VALUES ( 3, '1x1, ip:7.5,2.5 scale:5,5 skew:0,0, srid:-1, width:1, height:1',
         0, 1, 1, --- SRID, width, height
         5, 5, 7.5, 2.5, 0, 0, --- georeference
(
'01' -- little endian (uint8 ndr)
|| 
'0000' -- version (uint16 0)
||
'0000' -- nBands (uint16 0)
||
'0000000000001440' -- scaleX (float64 5)
||
'0000000000001440' -- scaleY (float64 5)
||
'0000000000001E40' -- ipX (float64 7.5)
||
'0000000000000440' -- ipY (float64 2.5)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'00000000' -- SRID (int32 0)
||
'0100' -- width (uint16 1)
||
'0100' -- height (uint16 1)
)::raster
); 

INSERT INTO rt_properties_test 
VALUES ( 4, '1x1, ip:7.5,2.5 scale:5,5 skew:1,1, srid:-1, width:1, height:1',
         0, 1, 1, --- SRID, width, height
         5, 5, 7.5, 2.5, 1, 1, --- georeference
(
'01' -- little endian (uint8 ndr)
|| 
'0000' -- version (uint16 0)
||
'0000' -- nBands (uint16 0)
||
'0000000000001440' -- scaleX (float64 5)
||
'0000000000001440' -- scaleY (float64 5)
||
'0000000000001E40' -- ipX (float64 7.5)
||
'0000000000000440' -- ipY (float64 2.5)
||
'000000000000F03F' -- skewX (float64 1)
||
'000000000000F03F' -- skewY (float64 1)
||
'00000000' -- SRID (int32 0)
||
'0100' -- width (uint16 1)
||
'0100' -- height (uint16 1)
)::raster
);

INSERT INTO rt_properties_test 
VALUES ( 5, '1x1, ip:7.5,2.5 scale:5,5 skew:3,7, srid:-1, width:1, height:1',
         0, 1, 1, --- SRID, width, height
         5, 5, 7.5, 2.5, 3, 7, --- georeference
(
'01' -- little endian (uint8 ndr)
|| 
'0000' -- version (uint16 0)
||
'0000' -- nBands (uint16 0)
||
'0000000000001440' -- scaleX (float64 5)
||
'0000000000001440' -- scaleY (float64 5)
||
'0000000000001E40' -- ipX (float64 7.5)
||
'0000000000000440' -- ipY (float64 2.5)
||
'0000000000000840' -- skewX (float64 3)
||
'0000000000001C40' -- skewY (float64 7)
||
'00000000' -- SRID (int32 0)
||
'0100' -- width (uint16 1)
||
'0100' -- height (uint16 1)
)::raster
); 

-----------------------------------------------------------------------
-- st_georeference (default)
-----------------------------------------------------------------------

SELECT 
    replace(st_georeference(rast)::text, E'\n', E'EOL'),
    replace(st_georeference(rast)::text, E'\n', E'EOL') = 
    '2.0000000000EOL0.0000000000EOL0.0000000000EOL3.0000000000EOL0.5000000000EOL0.5000000000EOL'
FROM rt_properties_test 
WHERE id = 0;

SELECT 
    replace(st_georeference(rast)::text, E'\n', E'EOL'),
    replace(st_georeference(rast)::text, E'\n', E'EOL') = 
    '5.0000000000EOL0.0000000000EOL0.0000000000EOL5.0000000000EOL2.5000000000EOL2.5000000000EOL'
FROM rt_properties_test 
WHERE id = 1;

SELECT 
    replace(st_georeference(rast)::text, E'\n', E'EOL'),
    replace(st_georeference(rast)::text, E'\n', E'EOL') = 
    '5.0000000000EOL0.0000000000EOL0.0000000000EOL5.0000000000EOL7.5000000000EOL2.5000000000EOL'
FROM rt_properties_test 
WHERE id = 2;


SELECT 
    replace(st_georeference(rast)::text, E'\n', E'EOL'),
    replace(st_georeference(rast)::text, E'\n', E'EOL') = 
    '5.0000000000EOL0.0000000000EOL0.0000000000EOL5.0000000000EOL7.5000000000EOL2.5000000000EOL'
FROM rt_properties_test 
WHERE id = 3;

SELECT 
    replace(st_georeference(rast)::text, E'\n', E'EOL'),
    replace(st_georeference(rast)::text, E'\n', E'EOL') = 
    '5.0000000000EOL1.0000000000EOL1.0000000000EOL5.0000000000EOL7.5000000000EOL2.5000000000EOL'
FROM rt_properties_test 
WHERE id = 4;

SELECT 
    replace(st_georeference(rast)::text, E'\n', E'EOL'),
    replace(st_georeference(rast)::text, E'\n', E'EOL') = 
    '5.0000000000EOL7.0000000000EOL3.0000000000EOL5.0000000000EOL7.5000000000EOL2.5000000000EOL'
FROM rt_properties_test 
WHERE id = 5;

-----------------------------------------------------------------------
-- st_georeference (GDAL)
-----------------------------------------------------------------------

SELECT 
    replace(st_georeference(rast,'GDAL')::text, E'\n', E'EOL'),
    replace(st_georeference(rast,'GDAL')::text, E'\n', E'EOL') = 
    '2.0000000000EOL0.0000000000EOL0.0000000000EOL3.0000000000EOL0.5000000000EOL0.5000000000EOL'
FROM rt_properties_test 
WHERE id = 0;

SELECT 
    replace(st_georeference(rast,'GDAL')::text, E'\n', E'EOL'),
    replace(st_georeference(rast,'GDAL')::text, E'\n', E'EOL') = 
    '5.0000000000EOL0.0000000000EOL0.0000000000EOL5.0000000000EOL2.5000000000EOL2.5000000000EOL'
FROM rt_properties_test 
WHERE id = 1;

SELECT 
    replace(st_georeference(rast,'GDAL')::text, E'\n', E'EOL'),
    replace(st_georeference(rast,'GDAL')::text, E'\n', E'EOL') = 
    '5.0000000000EOL0.0000000000EOL0.0000000000EOL5.0000000000EOL7.5000000000EOL2.5000000000EOL'
FROM rt_properties_test 
WHERE id = 2;

SELECT 
    replace(st_georeference(rast,'GDAL')::text, E'\n', E'EOL'),
    replace(st_georeference(rast,'GDAL')::text, E'\n', E'EOL') = 
    '5.0000000000EOL0.0000000000EOL0.0000000000EOL5.0000000000EOL7.5000000000EOL2.5000000000EOL'
FROM rt_properties_test 
WHERE id = 3;

SELECT 
    replace(st_georeference(rast,'GDAL')::text, E'\n', E'EOL'),
    replace(st_georeference(rast,'GDAL')::text, E'\n', E'EOL') = 
    '5.0000000000EOL1.0000000000EOL1.0000000000EOL5.0000000000EOL7.5000000000EOL2.5000000000EOL'
FROM rt_properties_test 
WHERE id = 4;

SELECT 
    replace(st_georeference(rast,'GDAL')::text, E'\n', E'EOL'),
    replace(st_georeference(rast,'GDAL')::text, E'\n', E'EOL') = 
    '5.0000000000EOL7.0000000000EOL3.0000000000EOL5.0000000000EOL7.5000000000EOL2.5000000000EOL'
FROM rt_properties_test 
WHERE id = 5;

-----------------------------------------------------------------------
-- st_georeference (ESRI)
-----------------------------------------------------------------------

SELECT
	replace(st_georeference(rast,'ESRI')::text, E'\n', E'EOL'),
	replace(st_georeference(rast,'ESRI')::text, E'\n', E'EOL') = 
    '2.0000000000EOL0.0000000000EOL0.0000000000EOL3.0000000000EOL1.5000000000EOL2.0000000000EOL'
FROM rt_properties_test 
WHERE id = 0;

SELECT
	replace(st_georeference(rast,'ESRI')::text, E'\n', E'EOL'),
	replace(st_georeference(rast,'ESRI')::text, E'\n', E'EOL') = 
    '5.0000000000EOL0.0000000000EOL0.0000000000EOL5.0000000000EOL5.0000000000EOL5.0000000000EOL'
FROM rt_properties_test 
WHERE id = 1;

SELECT
	replace(st_georeference(rast,'ESRI')::text, E'\n', E'EOL'),
	replace(st_georeference(rast,'ESRI')::text, E'\n', E'EOL') = 
    '5.0000000000EOL0.0000000000EOL0.0000000000EOL5.0000000000EOL10.0000000000EOL5.0000000000EOL'
FROM rt_properties_test 
WHERE id = 2;

SELECT
	replace(st_georeference(rast,'ESRI')::text, E'\n', E'EOL'),
	replace(st_georeference(rast,'ESRI')::text, E'\n', E'EOL') = 
    '5.0000000000EOL0.0000000000EOL0.0000000000EOL5.0000000000EOL10.0000000000EOL5.0000000000EOL'
FROM rt_properties_test 
WHERE id = 3;

SELECT
	replace(st_georeference(rast,'ESRI')::text, E'\n', E'EOL'),
	replace(st_georeference(rast,'ESRI')::text, E'\n', E'EOL') = 
    '5.0000000000EOL1.0000000000EOL1.0000000000EOL5.0000000000EOL10.0000000000EOL5.0000000000EOL'
FROM rt_properties_test 
WHERE id = 4;

SELECT
	replace(st_georeference(rast,'ESRI')::text, E'\n', E'EOL'),
	replace(st_georeference(rast,'ESRI')::text, E'\n', E'EOL') = 
    '5.0000000000EOL7.0000000000EOL3.0000000000EOL5.0000000000EOL10.0000000000EOL5.0000000000EOL'
FROM rt_properties_test 
WHERE id = 5;


-----------------------------------------------------------------------
-- st_setgeoreference (error conditions)
-----------------------------------------------------------------------

SELECT
    -- all 6 parameters must be numeric
    st_setgeoreference(rast,'4.0000000000 0.0000000000 0.0000000000 6.0000000000 1.5000000000 nodata') IS NULL
FROM rt_properties_test 
WHERE id = 0;

SELECT
    -- must have 6 parameters
    st_setgeoreference(rast,'2.0000000000 1.0000000000 2.0000000000 3.0000000000 1.5000000000') IS NULL
FROM rt_properties_test 
WHERE id = 1;

SELECT
    -- any whitespace accepted between parameters as well as ' '
    st_setgeoreference(rast,E'2.0000000000	1.0000000000\n2.0000000000\t3.0000000000	1.5000000000	2.0000000000') IS NOT NULL
FROM rt_properties_test 
WHERE id = 2;

-----------------------------------------------------------------------
-- st_setgeoreference (warning conditions)
-----------------------------------------------------------------------

SELECT
    -- raster arg is null
    st_setgeoreference(null,'4.0000000000 0.0000000000 0.0000000000 6.0000000000 1.5000000000 2.0000000000') IS NULL
FROM rt_properties_test 
WHERE id = 0;

-----------------------------------------------------------------------
-- st_setgeoreference (default)
-----------------------------------------------------------------------

SELECT
	st_scalex(rast) = 2,
	st_scaley(rast) = 3,
	st_scalex(st_setgeoreference(rast,'4.0000000000 0.0000000000 0.0000000000 6.0000000000 1.5000000000 2.0000000000')) = 4,
	st_scaley(st_setgeoreference(rast,'4.0000000000 0.0000000000 0.0000000000 6.0000000000 1.5000000000 2.0000000000')) = 6
FROM rt_properties_test 
WHERE id = 0;

SELECT
	st_skewx(rast) = 0,
	st_skewy(rast) = 0,
	st_skewx(st_setgeoreference(rast,'2.0000000000 1.0000000000 2.0000000000 3.0000000000 1.5000000000 2.0000000000')) = 2,
	st_skewy(st_setgeoreference(rast,'2.0000000000 1.0000000000 2.0000000000 3.0000000000 1.5000000000 2.0000000000')) = 1
FROM rt_properties_test 
WHERE id = 1;

SELECT
	st_upperleftx(rast) = 7.5,
	st_upperlefty(rast) = 2.5,
	st_upperleftx(st_setgeoreference(rast,'2.0000000000 1.0000000000 2.0000000000 3.0000000000 1.5000000000 2.0000000000')) = 1.5,
	st_upperlefty(st_setgeoreference(rast,'2.0000000000 1.0000000000 2.0000000000 3.0000000000 1.5000000000 2.0000000000')) = 2.0
FROM rt_properties_test 
WHERE id = 2;

-----------------------------------------------------------------------
-- st_setgeoreference (GDAL)
-----------------------------------------------------------------------

SELECT
	st_scalex(rast) = 2,
	st_scaley(rast) = 3,
	st_scalex(st_setgeoreference(rast,'4.0000000000 0.0000000000 0.0000000000 6.0000000000 1.5000000000 2.0000000000','GDAL')) = 4,
	st_scaley(st_setgeoreference(rast,'4.0000000000 0.0000000000 0.0000000000 6.0000000000 1.5000000000 2.0000000000','GDAL')) = 6
FROM rt_properties_test 
WHERE id = 0;

SELECT
	st_skewx(rast) = 0,
	st_skewy(rast) = 0,
	st_skewx(st_setgeoreference(rast,'2.0000000000 1.0000000000 2.0000000000 3.0000000000 1.5000000000 2.0000000000','GDAL')) = 2,
	st_skewy(st_setgeoreference(rast,'2.0000000000 1.0000000000 2.0000000000 3.0000000000 1.5000000000 2.0000000000','GDAL')) = 1
FROM rt_properties_test 
WHERE id = 1;

SELECT
	st_upperleftx(rast) = 7.5,
	st_upperlefty(rast) = 2.5,
	st_upperleftx(st_setgeoreference(rast,'2.0000000000 1.0000000000 2.0000000000 3.0000000000 1.5000000000 2.0000000000','GDAL')) = 1.5,
	st_upperlefty(st_setgeoreference(rast,'2.0000000000 1.0000000000 2.0000000000 3.0000000000 1.5000000000 2.0000000000','GDAL')) = 2.0
FROM rt_properties_test 
WHERE id = 2;

-----------------------------------------------------------------------
-- st_setgeoreference (ESRI)
-----------------------------------------------------------------------

SELECT
	st_scalex(rast) = 2,
	st_scaley(rast) = 3,
	st_scalex(st_setgeoreference(rast,'4.0000000000 0.0000000000 0.0000000000 6.0000000000 1.5000000000 2.0000000000','ESRI')) = 4,
	st_scaley(st_setgeoreference(rast,'4.0000000000 0.0000000000 0.0000000000 6.0000000000 1.5000000000 2.0000000000','ESRI')) = 6
FROM rt_properties_test 
WHERE id = 0;

SELECT
	st_skewx(rast) = 0,
	st_skewy(rast) = 0,
	st_skewx(st_setgeoreference(rast,'2.0000000000 1.0000000000 2.0000000000 3.0000000000 1.5000000000 2.0000000000','ESRI')) = 2,
	st_skewy(st_setgeoreference(rast,'2.0000000000 1.0000000000 2.0000000000 3.0000000000 1.5000000000 2.0000000000','ESRI')) = 1
FROM rt_properties_test 
WHERE id = 1;

SELECT
	st_upperleftx(rast) = 7.5,
	st_upperlefty(rast) = 2.5,
	st_upperleftx(st_setgeoreference(rast,'2.0000000000 0.0000000000 0.0000000000 3.0000000000 1.0000000000 2.5000000000','ESRI')) = 0,
	st_upperlefty(st_setgeoreference(rast,'2.0000000000 0.0000000000 0.0000000000 3.0000000000 1.0000000000 2.5000000000','ESRI')) = 1
FROM rt_properties_test 
WHERE id = 2;

-----------------------------------------------------------------------
-- ST_SetGeoReference(raster, double precision, ...)
-----------------------------------------------------------------------

SELECT id, ST_Metadata(rast), ST_Metadata(ST_SetGeoReference(rast, 0, 0, 1, -1, 0, 0)) FROM rt_properties_test ORDER BY id;
SELECT id, ST_Metadata(rast), ST_Metadata(ST_SetGeoReference(rast, 1, 1, 0.1, -0.1, 0, 0)) FROM rt_properties_test ORDER BY id;
SELECT id, ST_Metadata(rast), ST_Metadata(ST_SetGeoReference(rast, 0, 0.1, 1, 1, 0, 0)) FROM rt_properties_test ORDER BY id;

DROP TABLE rt_properties_test;
