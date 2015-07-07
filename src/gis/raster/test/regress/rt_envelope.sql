-- NULL raster
SELECT ST_AsText(ST_Envelope(NULL::raster));

-- width and height of zero
SELECT ST_AsText(ST_Envelope(ST_MakeEmptyRaster(0, 0, 0, 0, 1, -1, 0, 0, 0)));

-- width of zero
SELECT ST_AsText(ST_Envelope(ST_MakeEmptyRaster(0, 9, 0, 0, 1, -1, 0, 0, 0)));

-- height of zero
SELECT ST_AsText(ST_Envelope(ST_MakeEmptyRaster(9, 0, 0, 0, 1, -1, 0, 0, 0)));

-- normal raster
SELECT ST_AsText(ST_Envelope(ST_MakeEmptyRaster(9, 9, 0, 0, 1, -1, 0, 0, 0)));
