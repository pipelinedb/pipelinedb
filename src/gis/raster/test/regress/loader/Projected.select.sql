SELECT st_srid(rast) from loadedrast limit 1;
SELECT st_xmin(ST_SnapToGrid(rast::geometry,1e5)) from loadedrast;

SELECT st_srid(rast) from o_2_loadedrast limit 1;
SELECT st_xmin(ST_SnapToGrid(rast::geometry,1e5)) from o_2_loadedrast;
