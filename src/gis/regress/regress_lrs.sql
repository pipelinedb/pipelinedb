-- Repeat all tests with the new function names.
-- No M value
select '2d',ST_AsText(ST_LocateAlong('POINT(1 2)', 1));
select '3dz',ST_AsText(ST_LocateAlong('POINT(1 2 3)', 1));

-- Points
select 'PNTM_1',ST_AsText(ST_LocateAlong('POINTM(1 2 3)', 1));
select 'PNTM_2',ST_AsText(ST_LocateAlong('POINTM(1 2 3)', 3));
select 'PNTM_3',ST_AsText(ST_LocateBetween('POINTM(1 2 3)', 2, 3));
select 'PNTM_4',ST_AsText(ST_LocateBetween('POINTM(1 2 3)', 3, 4));
select 'PNTM_5',ST_AsText(ST_LocateBetween('POINTM(1 2 4.00001)', 3, 4));

-- Multipoints
select 'MPNT_1',ST_AsText(ST_LocateBetween('MULTIPOINTM(1 2 2)', 2, 5));
select 'MPNT_2', ST_AsText(ST_LocateBetween('MULTIPOINTM(1 2 8, 2 2 5, 2 1 0)', 2, 5));
select 'MPNT_3', ST_AsText(ST_LocateBetween('MULTIPOINTM(1 2 8, 2 2 5.1, 2 1 0)', 2, 5));
select 'MPNT_4', ST_AsText(ST_LocateBetween('MULTIPOINTM(1 2 8, 2 2 5, 2 1 0)', 4, 8));

-- Linestrings
select 'LINEZM_1', ST_AsText(ST_LocateBetween('LINESTRING(0 10 100 0, 0 0 0 10)', 2, 18));
select 'LINEZM_2', ST_AsText(ST_LocateBetween('LINESTRING(0 10 0 0, 0 0 100 10)', 2, 18));
select 'LINEZM_3', ST_AsText(ST_LocateBetween('LINESTRING(0 10 100 0, 0 0 0 10, 10 0 0 0)', 2, 18));
select 'LINEZM_4', ST_AsText(ST_LocateBetween('LINESTRING(0 10 100 0, 0 0 0 20, 10 0 0 0)', 2, 18));
select 'LINEZM_5', ST_AsText(ST_LocateBetween('LINESTRING(0 10 100 0, 0 0 0 20, 0 10 10 40, 10 0 0 0)', 2, 18));
select 'LINEZM_6', ST_AsText(ST_LocateBetween('LINESTRING(0 10 10 40, 10 0 0 0)', 2, 2));

--- line_locate_point

SELECT 'line_locate_point_1', ST_LineLocatePoint('LINESTRING(709243.393033887 163969.752725768,708943.240904444 163974.593889146,708675.634380651 163981.832927298)', 'POINT(705780 15883)');

--- postgis-users/2006-January/010613.html
select 'line_locate_point_2', ST_LineLocatePoint(ST_geomfromtext('LINESTRING(-1953743.873 471070.784,-1953735.105 471075.419,-1953720.034 471081.649)', 6269), ST_geomfromtext('POINT(-1953720.034 471081.649)', 6269));
select 'line_locate_point_3', ST_LineLocatePoint(ST_geomfromtext('LINESTRING(-1953743.873 471070.784,-1953735.105 471075.419,-1953720.034 471081.649)', 6269), ST_geomfromtext('POINT(-1953743.873 471070.784)', 6269));
--- http://trac.osgeo.org/postgis/ticket/1772#comment:2
select 'line_locate_point_4', ST_LineLocatePoint('LINESTRING(0 1, 0 1, 0 1)', 'POINT(0 1)');

--- line_substring / line_interpolate_point

--- postgis-devel/2006-January/001951.html
with substr as (
  select ST_LineSubstring(ST_geomfromewkt('SRID=4326;LINESTRING(0 0 0 0, 1 1 1 1, 2 2 2 2, 3 3 3 3, 4 4 4 4)'), 0.5, 0.8) as ln
)	
select 'line_substring_1', ST_SRID(ln), ST_AsText(ln) from substr;

select 'line_substring_2', ST_AsText(ST_LineSubstring('LINESTRING(0 0 0 0, 1 1 1 1, 2 2 2 2, 3 3 3 3, 4 4 4 4)', 0.5, 0.75));
select 'line_substring_3', ST_AsText(ST_LineSubstring('LINESTRING(0 0, 1 1, 2 2)', 0, 0.5));
select 'line_substring_4', ST_AsText(ST_LineSubstring('LINESTRING(0 0, 1 1, 2 2)', 0.5, 1));
select 'line_substring_5', ST_AsText(ST_LineSubstring('LINESTRING(0 0, 2 2)', 0.5, 1));
select 'line_substring_6', ST_AsText(ST_LineSubstring('LINESTRING(0 0, 2 2)', 0, 0.5));
select 'line_substring_7', ST_AsText(ST_LineSubstring('LINESTRING(0 0, 4 4)', .25, 0.5));
select 'line_substring_8', ST_AsText(ST_LineSubstring('LINESTRINGM(0 0 0, 4 4 4)', .25, 0.5));
select 'line_substring_9', ST_AsText(ST_LineSubstring('LINESTRINGM(0 0 4, 4 4 0)', .25, 0.5));
select 'line_substring_10', ST_AsText(ST_LineSubstring('LINESTRING(0 0 4, 4 4 0)', .25, 0.5));

select 'line_substring_11', ST_AsText(ST_LineSubstring('LINESTRING(0 0, 1 1)', 0, 0));
select 'line_substring_12', ST_AsText(ST_LineSubstring('LINESTRING(0 0 10, 1 1 5)', 0.5, .5));

--- line_interpolate_point

select 'line_interpolate_point', ST_AsText(ST_LineInterpolatePoint('LINESTRING(0 0, 1 1)', 0));
select 'line_interpolate_point', ST_AsText(ST_LineInterpolatePoint('LINESTRING(0 0 10, 1 1 5)', 0.5));
