-----------------------------------------------------------------------
--
-- Copyright (c) 2010 David Zwarg <dzwarg@azavea.com>, Pierre Racine <pierre.racine@sbf.ulaval.ca>
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
--- Test of "Set" functions for properties of the raster.
--- (Objective B03a)
-----------------------------------------------------------------------


-----------------------------------------------------------------------
--- ST_SetSRID
-----------------------------------------------------------------------

SELECT
    (srid+1) AS expected,
    st_srid(st_setsrid(rast,srid+1)) AS obtained
 FROM rt_properties_test
WHERE (srid+1) != st_srid(st_setsrid(rast,srid+1));


-----------------------------------------------------------------------
--- ST_SetScale
-----------------------------------------------------------------------

SELECT
    (scalex+2) AS expectedx,
    (scaley+3) AS expectedy,
    st_scalex(st_setscale(rast,scalex+2,scaley)) AS obtainedx,
    st_scaley(st_setscale(rast,scalex,scaley+3)) AS obtainedy
 FROM rt_properties_test
WHERE
    (scalex+2) != st_scalex(st_setscale(rast,scalex+2,scaley)) OR
    (scaley+3) != st_scaley(st_setscale(rast,scalex,scaley+3));

SELECT
    (scalex+2) AS expectedx,
    (scaley+3) AS expectedy,
    st_scalex(st_setscale(rast,scalex+2)) AS obtainedx,
    st_scaley(st_setscale(rast,scaley+3)) AS obtainedy
 FROM rt_properties_test
WHERE
    (scalex+2) != st_scalex(st_setscale(rast,scalex+2)) OR
    (scaley+3) != st_scaley(st_setscale(rast,scaley+3));

-----------------------------------------------------------------------
--- ST_SetSkew
-----------------------------------------------------------------------

SELECT
    (skewx+2) AS expectedx,
    (skewy+3) AS expectedy,
    st_skewx(st_setskew(rast,skewx+2,skewy)) AS obtainedx,
    st_skewy(st_setskew(rast,skewx,skewy+3)) AS obtainedy
 FROM rt_properties_test
WHERE
    (skewx+2) != st_skewx(st_setskew(rast,skewx+2,skewy)) OR
    (skewy+3) != st_skewy(st_setskew(rast,skewx,skewy+3));

SELECT
    (skewx+2) AS expectedx,
    (skewy+3) AS expectedy,
    st_skewx(st_setskew(rast,skewx+2)) AS obtainedx,
    st_skewy(st_setskew(rast,skewy+3)) AS obtainedy
 FROM rt_properties_test
WHERE
    (skewx+2) != st_skewx(st_setskew(rast,skewx+2)) OR
    (skewy+3) != st_skewy(st_setskew(rast,skewy+3));


-----------------------------------------------------------------------
--- ST_SetUpperLeft
-----------------------------------------------------------------------

SELECT
    (ipx+2) AS expectedx,
    (ipy+3) AS expectedy,
    st_upperleftx(st_setupperleft(rast,ipx+2,ipy)) AS obtainedx,
    st_upperlefty(st_setupperleft(rast,ipx,ipy+3)) AS obtainedy
 FROM rt_properties_test
WHERE
    (ipx+2) != st_upperleftx(st_setupperleft(rast,ipx+2,ipy)) OR
    (ipy+3) != st_upperlefty(st_setupperleft(rast,ipx,ipy+3));

DROP TABLE rt_properties_test;
