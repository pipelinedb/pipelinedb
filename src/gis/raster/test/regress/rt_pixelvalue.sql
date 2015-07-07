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
--
-- Copyright (c) 2009 Pierre Racine <pierre.racine@sbf.ulaval.ca>
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
-----------------------------------------------------------------------

-----------------------------------------------------------------------
--- Test of "Get" functions for properties of the raster.
-----------------------------------------------------------------------

CREATE TABLE rt_band_properties_test (
    id numeric,
    description text,
    nbband integer,
    b1pixeltype text,
    b1hasnodatavalue boolean,
    b1nodatavalue float4,
    b1val float4,
    b2pixeltype text,
    b2hasnodatavalue boolean,
    b2nodatavalue float4,
    b2val float4,
    geomtxt text,
    rast raster
);

INSERT INTO rt_band_properties_test 
VALUES ( 1, '1x1, nbband:2 b1pixeltype:4BUI b1hasnodatavalue:true b1nodatavalue:3 b2pixeltype:16BSI b2hasnodatavalue:false b2nodatavalue:13',
        2, --- nbband
        '4BUI', true, 3, 2,   --- b1pixeltype, b1hasnodatavalue, b1nodatavalue, b1val
        '16BSI', false, 13, 4, --- b2pixeltype, b2hasnodatavalue, b2nodatavalue, b2val
        'POLYGON((782325.5 26744042.5,782330.5 26744045.5,782333.5 26744040.5,782328.5 26744037.5,782325.5 26744042.5))',
(
'01' -- big endian (uint8 xdr)
|| 
'0000' -- version (uint16 0)
||
'0200' -- nBands (uint16 2)
||
'0000000000001440' -- scaleX (float64 5)
||
'00000000000014C0' -- scaleY (float64 -5)
||
'00000000EBDF2741' -- ipX (float64 782325.5)
||
'000000A84E817941' -- ipY (float64 26744042.5)
||
'0000000000000840' -- skewX (float64 3)
||
'0000000000000840' -- skewY (float64 3)
||
'27690000' -- SRID (int32 26919 - UTM 19N)
||
'0100' -- width (uint16 1)
||
'0100' -- height (uint16 1)
||
'4' -- hasnodatavalue set to true
||
'2' -- first band type (4BUI) 
||
'03' -- novalue==3
||
'02' -- pixel(0,0)==2
||
'0' -- hasnodatavalue set to false
||
'5' -- second band type (16BSI)
||
'0D00' -- novalue==13
||
'0400' -- pixel(0,0)==4
)::raster
);

INSERT INTO rt_band_properties_test 
VALUES ( 2, '1x1, nbband:2 b1pixeltype:4BUI b1hasnodatavalue:true b1nodatavalue:3 b2pixeltype:16BSI b2hasnodatavalue:false b2nodatavalue:13',
        2, --- nbband
        '4BUI', true, 3, 2,   --- b1pixeltype, b1hasnodatavalue, b1nodatavalue, b1val
        '16BSI', false, 13, 4, --- b2pixeltype, b2hasnodatavalue, b2nodatavalue, b2val
        'POLYGON((-75.5533328537098 49.2824585505576,-75.5525268884758 49.2826703629415,-75.5523150760919 49.2818643977075,-75.553121041326 49.2816525853236,-75.5533328537098 49.2824585505576))',
(
'01' -- little endian (uint8 ndr)
|| 
'0000' -- version (uint16 0)
||
'0200' -- nBands (uint16 0)
||
'17263529ED684A3F' -- scaleX (float64 0.000805965234044584)
||
'F9253529ED684ABF' -- scaleY (float64 -0.00080596523404458)
||
'1C9F33CE69E352C0' -- ipX (float64 -75.5533328537098)
||
'718F0E9A27A44840' -- ipY (float64 49.2824585505576)
||
'ED50EB853EC32B3F' -- skewX (float64 0.000211812383858707)
||
'7550EB853EC32B3F' -- skewY (float64 0.000211812383858704)
||
'E6100000' -- SRID (int32 4326)
||
'0100' -- width (uint16 1)
||
'0100' -- height (uint16 1)
||
'4' -- hasnodatavalue set to true
||
'2' -- first band type (4BUI) 
||
'03' -- novalue==3
||
'02' -- pixel(0,0)==2
||
'0' -- hasnodatavalue set to false
||
'5' -- second band type (16BSI)
||
'0D00' -- novalue==13
||
'0400' -- pixel(0,0)==4
)::raster
);

INSERT INTO rt_band_properties_test 
VALUES ( 3, '1x1, nbband:2 b1pixeltype:4BUI b1hasnodatavalue:true b1nodatavalue:3 b2pixeltype:16BSI b2hasnodatavalue:false b2nodatavalue:13',
        2, --- nbband
        '4BUI', true, 3, 3,   --- b1pixeltype, b1hasnodatavalue, b1nodatavalue, b1val
        '16BSI', false, 13, 4, --- b2pixeltype, b2hasnodatavalue, b2nodatavalue, b2val
        'POLYGON((-75.5533328537098 49.2824585505576,-75.5525268884758 49.2826703629415,-75.5523150760919 49.2818643977075,-75.553121041326 49.2816525853236,-75.5533328537098 49.2824585505576))',
(
'01' -- little endian (uint8 ndr)
|| 
'0000' -- version (uint16 0)
||
'0200' -- nBands (uint16 0)
||
'17263529ED684A3F' -- scaleX (float64 0.000805965234044584)
||
'F9253529ED684ABF' -- scaleY (float64 -0.00080596523404458)
||
'1C9F33CE69E352C0' -- ipX (float64 -75.5533328537098)
||
'718F0E9A27A44840' -- ipY (float64 49.2824585505576)
||
'ED50EB853EC32B3F' -- skewX (float64 0.000211812383858707)
||
'7550EB853EC32B3F' -- skewY (float64 0.000211812383858704)
||
'E6100000' -- SRID (int32 4326)
||
'0100' -- width (uint16 1)
||
'0100' -- height (uint16 1)
||
'6' -- hasnodatavalue and isnodata set to true
||
'2' -- first band type (4BUI) 
||
'03' -- novalue==3
||
'03' -- pixel(0,0)==3 (same that nodata)
||
'0' -- hasnodatavalue set to false
||
'5' -- second band type (16BSI)
||
'0D00' -- novalue==13
||
'0400' -- pixel(0,0)==4
)::raster
);

INSERT INTO rt_band_properties_test 
VALUES ( 4, '1x1, nbband:2 b1pixeltype:4BUI b1hasnodatavalue:true b1nodatavalue:3 b2pixeltype:16BSI b2hasnodatavalue:false b2nodatavalue:13',
        2, --- nbband
        '4BUI', true, 3, 3,   --- b1pixeltype, b1hasnodatavalue, b1nodatavalue, b1val
        '16BSI', false, 13, 4, --- b2pixeltype, b2hasnodatavalue, b2nodatavalue, b2val
        'POLYGON((-75.5533328537098 49.2824585505576,-75.5525268884758 49.2826703629415,-75.5523150760919 49.2818643977075,-75.553121041326 49.2816525853236,-75.5533328537098 49.2824585505576))',
(
'01' -- little endian (uint8 ndr)
|| 
'0000' -- version (uint16 0)
||
'0200' -- nBands (uint16 0)
||
'17263529ED684A3F' -- scaleX (float64 0.000805965234044584)
||
'F9253529ED684ABF' -- scaleY (float64 -0.00080596523404458)
||
'1C9F33CE69E352C0' -- ipX (float64 -75.5533328537098)
||
'718F0E9A27A44840' -- ipY (float64 49.2824585505576)
||
'ED50EB853EC32B3F' -- skewX (float64 0.000211812383858707)
||
'7550EB853EC32B3F' -- skewY (float64 0.000211812383858704)
||
'E6100000' -- SRID (int32 4326)
||
'0100' -- width (uint16 1)
||
'0100' -- height (uint16 1)
||
'4' -- hasnodatavalue set to true and isnodata set to false (should be updated)
||
'2' -- first band type (4BUI) 
||
'03' -- novalue==3
||
'03' -- pixel(0,0)==3 (same that nodata)
||
'0' -- hasnodatavalue set to false
||
'5' -- second band type (16BSI)
||
'0D00' -- novalue==13
||
'0400' -- pixel(0,0)==4
)::raster
);

-----------------------------------------------------------------------
-- Test 1 - st_value(rast raster, band integer, x integer, y integer) 
-----------------------------------------------------------------------

SELECT 'test 1.1', id
    FROM rt_band_properties_test
    WHERE st_value(rast, 1, 1, 1, TRUE) != b1val;

SELECT 'test 1.2', id
    FROM rt_band_properties_test
    WHERE st_value(rast, 2, 1, 1, TRUE) != b2val;

SELECT 'test 1.3', id
    FROM rt_band_properties_test
    WHERE st_value(rast, 1, 1, 1, FALSE) != b1val;

SELECT 'test 1.4', id
    FROM rt_band_properties_test
    WHERE st_value(rast, 1, 1, 1, NULL) != b1val;

SELECT 'test 1.5', id
    FROM rt_band_properties_test
    WHERE NOT st_value(rast, 1, 1, NULL, NULL) IS NULL;

SELECT 'test 1.6', id
    FROM rt_band_properties_test
    WHERE NOT st_value(rast, 1, NULL, 1, NULL) IS NULL;

SELECT 'test 1.7', id
    FROM rt_band_properties_test
    WHERE NOT st_value(st_setbandnodatavalue(rast, b1val), 1, 1, 1, NULL) IS NULL;

SELECT 'test 1.8', id
    FROM rt_band_properties_test
    WHERE NOT st_value(st_setbandnodatavalue(rast, b1val), 1, 1, 1, TRUE) IS NULL;

SELECT 'test 1.9', id
    FROM rt_band_properties_test
    WHERE st_value(st_setbandnodatavalue(rast, b1val), 1, 1, 1, FALSE) != b1val;

-- Make sure we return only a warning when getting vlue with out of range pixel coordinates
SELECT 'test 1.10', id
    FROM rt_band_properties_test
    WHERE NOT st_value(rast, -1, -1) IS NULL;

-----------------------------------------------------------------------
-- Test 2 - st_value(rast raster, band integer, pt geometry)
-----------------------------------------------------------------------

SELECT 'test 2.1', id
    FROM rt_band_properties_test
    WHERE st_value(rast, 1, st_makepoint(st_upperleftx(rast), st_upperlefty(rast))) != b1val;

SELECT 'test 2.2', id
    FROM rt_band_properties_test
    WHERE st_value(rast, 2, st_makepoint(st_upperleftx(rast), st_upperlefty(rast))) != b2val;

-----------------------------------------------------------------------
-- Test 3 - st_pixelaspolygon(rast raster, x integer, y integer)
-----------------------------------------------------------------------

SELECT 'test 3.1', id
    FROM rt_band_properties_test
    WHERE st_astext(st_pixelaspolygon(rast, 1, 1)) != geomtxt;

-----------------------------------------------------------------------
-- Test 4 - st_setvalue(rast raster, band integer, x integer, y integer, val float8)
-----------------------------------------------------------------------

SELECT 'test 4.1', id
    FROM rt_band_properties_test
    WHERE st_value(st_setvalue(rast, 1, 1, 1, 5), 1, 1, 1) != 5;
    
SELECT 'test 4.2', id
    FROM rt_band_properties_test
    WHERE NOT st_value(st_setvalue(rast, 1, 1, 1, NULL), 1, 1, 1) IS NULL;

SELECT 'test 4.3', id
    FROM rt_band_properties_test
    WHERE st_value(st_setvalue(rast, 1, 1, 1, NULL), 1, 1, 1, FALSE) != st_bandnodatavalue(rast, 1);

SELECT 'test 4.4', id
    FROM rt_band_properties_test
    WHERE st_value(st_setvalue(st_setbandnodatavalue(rast, NULL), 1, 1, 1, NULL), 1, 1, 1) != b1val;

DROP TABLE rt_band_properties_test;
