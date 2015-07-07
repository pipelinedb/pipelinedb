-----------------------------------------------------------------------
-- ST_MultiBandMapAlgebra
-- Return the same map algebra expression to all the band of a raster. 
-----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ST_MultiBandMapAlgebra(rast1 raster, 
                                            	  rast2 raster, 
                                                  expression text, 
                                            	  extentexpr text) 
    RETURNS raster AS 
    $$
    DECLARE
		numband int;
		newrast raster;
		pixeltype text;
		nodataval float;
    BEGIN
        numband := ST_NumBands(rast1);
        IF numband != ST_NumBands(rast2) THEN
            RAISE EXCEPTION 'ST_MultiBandMapAlgebra: Rasters do not have the same number of band';
        END IF;
        newrast := ST_MakeEmptyRaster(rast1);
        FOR b IN 1..numband LOOP
            pixeltype := ST_BandPixelType(rast1, b);
            nodataval := ST_BandNodataValue(rast1, b);
            newrast := ST_AddBand(newrast, NULL, ST_MapAlgebraExpr(rast1, b, rast2, b, expression, pixeltype, extentexpr, nodataval), 1);
        END LOOP;
    END;
    $$
    LANGUAGE 'plpgsql';