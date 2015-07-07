-----------------------------------------------------------------------
--
-- Copyright (c) 2011 Jorge Arevalo <jorge.arevalo@deimos-space.com>
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

CREATE TABLE empty_raster_test (
	rid numeric,
	rast raster
);

INSERT INTO empty_raster_test
VALUES (1, ST_MakeEmptyRaster( 100, 100, 0.0005, 0.0005, 1, 1, 0, 0, 4326) );

-------------------------------------------------------------------
-- st_isempty
-----------------------------------------------------------------------
select st_isempty(rast) from empty_raster_test;

DROP TABLE empty_raster_test;
