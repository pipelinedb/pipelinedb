WITH foo AS (
	SELECT postgis_raster_lib_version()
)
SELECT NULL FROM foo;
SET postgis.gdal_enabled_drivers = 'PNG';
SELECT ST_AsPNG(NULL) IS NULL;
SELECT CASE
	WHEN length(
		ST_AsPNG(
			ST_AddBand(ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0), 1, '8BSI', 123, NULL)
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsPNG(
			ST_AddBand(ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0), 1, '8BUI', 123, NULL)
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsPNG(
			ST_AddBand(ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0), 1, '8BSI', -123, NULL)
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsPNG(
			ST_AddBand(ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0), 1, '8BUI', 254, NULL)
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsPNG(
			ST_AddBand(
				ST_AddBand(
					ST_AddBand(
						ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0)
						, 1, '8BSI', 1, -1
					)
					, 2, '8BSI', 11, -1
				)
				, 3, '8BSI', 111, -1
			),
			ARRAY['ZLEVEL=1']
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsPNG(
			ST_AddBand(
				ST_AddBand(
					ST_AddBand(
						ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0)
						, 1, '8BSI', 1, -1
					)
					, 2, '8BSI', 11, -1
				)
				, 3, '8BSI', 111, -1
			),
			ARRAY['ZLEVEL=9']
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsPNG(
			ST_AddBand(
				ST_AddBand(
					ST_AddBand(
						ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0)
						, 1, '8BSI', 1, 1
					)
					, 2, '8BSI', 11, 1
				)
				, 3, '8BSI', 111, 1
			),
			ARRAY['ZLEVEL=9']
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsPNG(
			ST_AddBand(
				ST_AddBand(
					ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0)
					, 1, '8BUI', 1, 1
				)
				, 2, '8BUI', 11, 1
			),
			ARRAY[1],
			6
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsPNG(
			ST_AddBand(
				ST_AddBand(
					ST_AddBand(
						ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0)
						, 1, '8BUI', 1, 1
					)
					, 2, '8BUI', 11, 1
				)
				, 3, '8BUI', 111, 1
			),
			ARRAY[3,1],
			6
		)
	) > 0
		THEN 1
	ELSE 0
END;
