-----------------------------------------------------------------------
--
-- Copyright (c) 2009 Sandro Santilli <strk@keybit.net>, David Zwarg <dzwarg@azavea.com>
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

CREATE TABLE rt_box3d_test (
    id numeric,
    name text,
    rast raster,
    env box3d
);

-- 10x20, ip:0.5,0.5 scale:2,3
INSERT INTO rt_box3d_test 
VALUES ( 0, '10x20, ip:0.5,0.5 scale:2,3 skew:0,0',
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
,'BOX3D(0.5 0.5,20.5 60.5 0)' -- expected envelope (20x60) == (10*2 x 20*3)
);

INSERT INTO rt_box3d_test 
VALUES ( 1, '1x1, ip:2.5,2.5 scale:5,5 skew:0,0',
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
'00000000' -- SRID (int32 0)
||
'0100' -- width (uint16 1)
||
'0100' -- height (uint16 1)
)::raster
,'BOX3D(2.5 2.5,7.5 7.5 0)' -- expected envelope 
);

INSERT INTO rt_box3d_test 
VALUES ( 2, '1x1, ip:7.5,2.5 scale:5,5 skew:0,0',
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
,'BOX3D(7.5 2.5,12.5 7.5 0)' -- expected envelope 
);

-----------------------------------------------------------------------
-- test bounding box (2D)
-----------------------------------------------------------------------
SELECT
	id,
	env as expected,
	rast::box3d as obtained
FROM rt_box3d_test
WHERE
	rast::box3d::text != env::text;

SELECT
	id,
	env as expected,
	box3d(rast) as obtained
FROM rt_box3d_test
WHERE
	box3d(rast)::text != env::text;

SELECT
	id,
	env as expected,
	box3d(st_convexhull(rast)) as obtained
FROM rt_box3d_test
WHERE
	box3d(st_convexhull(rast))::text != env::text;

SELECT
	id,
	env as expected,
	box3d(st_envelope(rast)) as obtained
FROM rt_box3d_test
WHERE
	box3d(st_envelope(rast))::text != env::text;

-- Cleanup
DROP TABLE rt_box3d_test;
