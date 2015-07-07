WITH foo AS (
	SELECT postgis_raster_lib_version()
)
SELECT NULL FROM foo;
SET postgis.gdal_enabled_drivers = 'GTiff';
SELECT ST_AsTIFF(NULL) IS NULL;
SELECT ST_AsTIFF(NULL, 'JPEG') IS NULL;
SELECT CASE
	WHEN length(
		ST_AsTIFF(
			ST_AddBand(ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0), 1, '64BF', 123.4567, NULL)
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsTIFF(
			ST_AddBand(
				ST_AddBand(
					ST_AddBand(
						ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0)
						, 1, '64BF', 1234.5678, NULL
					)
					, '64BF', 987.654321, NULL
				)
				, '64BF', 9876.54321, NULL
			)
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsTIFF(
			ST_AddBand(
				ST_AddBand(
					ST_AddBand(
						ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0)
						, 1, '64BF', 1234.5678, -9999
					)
					, '64BF', 987.654321, -9999
				)
				, '64BF', 9876.54321, -9999
			)
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsTIFF(
			ST_AddBand(
				ST_AddBand(
					ST_AddBand(
						ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0)
						, 1, '64BF', 1234.5678, -9999
					)
					, '64BF', 987.654321, -9999
				)
				, '64BF', 9876.54321, -9999
			)
			, ARRAY[3]
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsTIFF(
			ST_AddBand(
				ST_AddBand(
					ST_AddBand(
						ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0)
						, 1, '64BF', 1234.5678, -9999
					)
					, '64BF', 987.654321, -9999
				)
				, '64BF', 9876.54321, -1
			)
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsTIFF(
			ST_AddBand(
				ST_AddBand(
					ST_AddBand(
						ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0)
						, 1, '64BF', 1234.5678, -1
					)
					, '64BF', 987.654321, -9999
				)
				, '64BF', 9876.54321, -9999
			)
		)
	) > 0
		THEN 1
	ELSE 0
END;
SELECT CASE
	WHEN length(
		ST_AsTIFF(
			ST_AddBand(
				ST_AddBand(
					ST_AddBand(
						ST_MakeEmptyRaster(200, 200, 10, 10, 2, 2, 0, 0)
						, 1, '64BF', 1234.5678, -9999
					)
					, '64BF', 987.654321, -9999
				)
				, '64BF', 9876.54321, -1
			)
			, 'JPEG90'
		)
	) > 0
		THEN 1
	ELSE 0
END;
