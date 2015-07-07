SET client_min_messages TO warning;
CREATE TABLE res1 AS SELECT
  ST_AddBand(
    ST_MakeEmptyRaster(10, 10, x, y, 1, -1, 0, 0, 0)
    , 1, '8BUI', 0, 0
  ) r
FROM generate_series(-170, 160, 10) x,
     generate_series(80, -70, -10) y;
SELECT addrasterconstraints('res1', 'r');

SELECT ST_CreateOverview('res1', 'r', 2)::text = 'o_2_res1';

SELECT ST_CreateOverview('res1', 'r', 4)::text = 'o_4_res1';

SELECT ST_CreateOverview('res1', 'r', 8)::text = 'o_8_res1';

SELECT ST_CreateOverview('res1', 'r', 16)::text = 'o_16_res1';

SELECT r_table_name tab, r_raster_column c, srid s,
 scale_x sx, scale_y sy,
 blocksize_x w, blocksize_y h, same_alignment a,
 -- regular_blocking (why not regular?)
 --extent::box2d e,
 st_covers(extent::box2d, 'BOX(-170 -80,170 80)'::box2d) ec,
 st_xmin(extent::box2d) = -170 as eix,
 st_ymax(extent::box2d) = 80 as eiy,
 (st_xmax(extent::box2d) - 170) <= scale_x as eox,
 --(st_xmax(extent::box2d) - 170) eoxd,
 abs(st_ymin(extent::box2d) + 80) <= abs(scale_y) as eoy
 --,abs(st_ymin(extent::box2d) + 80) eoyd
 FROM raster_columns
WHERE r_table_name like '%res1'
ORDER BY scale_x, r_table_name;

SELECT o_table_name, o_raster_column,
       r_table_name, r_raster_column, overview_factor
FROM raster_overviews
WHERE r_table_name = 'res1'
ORDER BY overview_factor;

SELECT 'count',
(SELECT count(*) r1 from res1),
(SELECT count(*) r2 from o_2_res1),
(SELECT count(*) r4 from o_4_res1),
(SELECT count(*) r8 from o_8_res1),
(SELECT count(*) r16 from o_16_res1)
;


DROP TABLE o_16_res1;
DROP TABLE o_8_res1;
DROP TABLE o_4_res1;
DROP TABLE o_2_res1;
DROP TABLE res1;
