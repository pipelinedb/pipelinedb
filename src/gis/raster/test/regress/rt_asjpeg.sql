WITH foo AS (
	SELECT postgis_raster_lib_version()
)
SELECT NULL FROM foo;
SET postgis.gdal_enabled_drivers = 'JPEG';
SELECT ST_AsJPEG(NULL) IS NULL;
SELECT CASE
	WHEN length(
		ST_AsJPEG(
			ST_AddBand(ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0), 1, '8BSI', 123, NULL)
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsJPEG(
			ST_AddBand(ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0), 1, '8BUI', 123, NULL)
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsJPEG(
			ST_AddBand(ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0), 1, '8BSI', -123, NULL)
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsJPEG(
			ST_AddBand(ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0), 1, '8BUI', 254, NULL)
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsJPEG(
			ST_AddBand(
				ST_AddBand(
					ST_AddBand(
						ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0)
						, 1, '8BUI', 1, 255
					)
					, 2, '8BUI', 11, 0
				)
				, 3, '8BUI', 111, 127
			)
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsJPEG(
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
			ARRAY['QUALITY=90','PROGRESSIVE=ON']
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsJPEG(
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
			ARRAY[3,1],
			50
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsJPEG(
			ST_AddBand(
				ST_AddBand(
					ST_AddBand(
						ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0)
						, 1, '8BSI', 1, -1
					)
					, 2, '8BSI', 11, -1
				)
				, 3, '8BUI', 111, -1
			),
			ARRAY[3],
			10
		)
	) > 0
		THEN 1
	ELSE 0
END;
