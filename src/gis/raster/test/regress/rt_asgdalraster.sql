WITH foo AS (
	SELECT postgis_raster_lib_version()
)
SELECT NULL FROM foo;
SET postgis.gdal_enabled_drivers = 'GTiff PNG JPEG';
SELECT CASE
	WHEN length(
		ST_AsGDALRaster(
			NULL,
			'GTiff'
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsGDALRaster(
			ST_AddBand(ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0), 1, '64BF', 123.4567, NULL),
			'GTiff'
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsGDALRaster(
			ST_AddBand(
				ST_AddBand(
					ST_AddBand(
						ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0)
						, 1, '64BF', 1234.5678, NULL
					)
					, '64BF', 987.654321, NULL
				)
				, '64BF', 9876.54321, NULL
			),
			'GTiff'
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsGDALRaster(
			ST_AddBand(
				ST_AddBand(
					ST_AddBand(
						ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0)
						, 1, '64BF', 1234.5678, -9999
					)
					, '64BF', 987.654321, -9999
				)
				, '64BF', 9876.54321, -9999
			),
			'GTiff'
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsGDALRaster(
			ST_AddBand(ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0), 1, '8BSI', 123, NULL),
			'PNG'
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsGDALRaster(
			ST_AddBand(ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0), 1, '8BUI', 123, NULL),
			'PNG'
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsGDALRaster(
			ST_AddBand(ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0), 1, '8BSI', -123, NULL),
			'PNG'
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsGDALRaster(
			ST_AddBand(ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0), 1, '8BUI', 254, NULL),
			'PNG'
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsGDALRaster(
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
			'PNG',
			ARRAY['ZLEVEL=1']
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsGDALRaster(
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
			'PNG',
			ARRAY['ZLEVEL=9']
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsGDALRaster(
			ST_AddBand(ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0), 1, '8BSI', 123, NULL),
			'JPEG'
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsGDALRaster(
			ST_AddBand(ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0), 1, '8BUI', 123, NULL),
			'JPEG'
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsGDALRaster(
			ST_AddBand(ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0), 1, '8BSI', -123, NULL),
			'JPEG'
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsGDALRaster(
			ST_AddBand(ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0), 1, '8BUI', 254, NULL),
			'JPEG'
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsGDALRaster(
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
			'JPEG'
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsGDALRaster(
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
			'JPEG',
			ARRAY['QUALITY=90','PROGRESSIVE=ON']
		)
	) > 0
		THEN 1
	ELSE 0
END;
