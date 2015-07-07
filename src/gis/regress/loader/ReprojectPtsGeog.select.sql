select ST_Asewkt(ST_SnapToGrid(the_geom::geometry,0.00000001)) from loadedshp;

