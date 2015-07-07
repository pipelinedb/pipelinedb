SELECT
	ST_Value(t.rast, 1, 1, 1),
	ST_Value(t.rast, 1, 10, 10),
	ST_Value(t.rast, 1, 2, 10),
	ST_BandNoDataValue(rast, 1)
FROM (
	SELECT ST_Reclass(
		ST_SetValue(
			ST_SetValue(
				ST_AddBand(
					ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0),
					1, '32BUI', 0, 0
				),
				1, 1, 499
			),
			10, 10, 12
		),
		ROW(1, '0-100:1-10, 101-500:11-150,501 - 10000: 151-254', '8BUI', 255)
	) AS rast  OFFSET 0
) AS t;
SELECT
	ST_Value(t.rast, 1, 1, 1),
	ST_Value(t.rast, 1, 10, 10),
	ST_Value(t.rast, 1, 2, 10),
	ST_BandNoDataValue(rast, 1),
	ST_Value(t.rast, 2, 1, 1),
	ST_Value(t.rast, 2, 10, 10),
	ST_Value(t.rast, 2, 2, 10),
	ST_BandNoDataValue(rast, 2)
FROM (
	SELECT ST_Reclass(
		ST_SetValue(
			ST_SetValue(
				ST_AddBand(
					ST_AddBand(
						ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0),
						1, '8BUI', 0, 1
					),
					2, '32BUI', 0, 0
				),
				2, 1, 1, 499
			),
			2, 10, 10, 12
		),
		ROW(2, '0-100:1-10, 101-500:11-150,501 - 10000: 151-254', '8BUI', 255)
	) AS rast   OFFSET 0
) AS t;
SELECT
	ST_Value(t.rast, 1, 1, 1),
	ST_Value(t.rast, 1, 10, 10),
	ST_Value(t.rast, 1, 2, 10),
	ST_BandNoDataValue(rast, 1),
	ST_Value(t.rast, 2, 1, 1),
	ST_Value(t.rast, 2, 10, 10),
	ST_Value(t.rast, 2, 2, 10),
	ST_BandNoDataValue(rast, 2)
FROM (
	SELECT ST_Reclass(
		ST_SetValue(
			ST_SetValue(
				ST_AddBand(
					ST_AddBand(
						ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0),
						1, '8BUI', 0, 1
					),
					2, '32BUI', 0, 0
				),
				2, 1, 1, 499
			),
			2, 10, 10, 12
		),
		ROW(1, '0:1', '8BUI', 255),
		ROW(2, '0-100:1-10, 101-500:11-150,501 - 10000: 151-254', '8BUI', 255)
	) AS rast   OFFSET 0
) AS t;
SELECT
	ST_Value(t.rast, 1, 1, 1),
	ST_Value(t.rast, 1, 10, 10),
	ST_BandNoDataValue(rast, 1)
FROM (
	SELECT ST_Reclass(
		ST_SetValue(
			ST_SetValue(
				ST_AddBand(
					ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0),
					1, '8BUI', 0, 0
				),
				1, 1, 1, 255
			),
			1, 10, 10, 0
		),
		ROW(1, '0-100]:200-255,(100-200]:100-200,(200-255]:0-100', '8BUI', NULL)
	) AS rast  OFFSET 0
) AS t;
SELECT
	ST_Value(t.rast, 1, 1, 1),
	ST_Value(t.rast, 1, 10, 10),
	ST_BandNoDataValue(rast, 1)
FROM (
	SELECT ST_Reclass(
		ST_SetValue(
			ST_SetValue(
				ST_AddBand(
					ST_MakeEmptyRaster(100, 100, 10, 10, 2, 2, 0, 0, 0),
					1, '32BF', 1, 0
				),
				1, 1, 1, 3.14159
			),
			1, 10, 10, 2.71828
		),
		ROW(1, '-10000--100]:1-50,(-100-1000]:50-150,(1000-10000]:150-254', '8BUI', 0)
	) AS rast  OFFSET 0
) AS t;
SELECT
	ST_Value(t.rast, 1, 1, 1),
	ST_Value(t.rast, 1, 10, 10),
	ST_BandNoDataValue(rast, 1)
FROM (
	SELECT ST_Reclass(
		ST_SetValue(
			ST_SetValue(
				ST_AddBand(
					ST_MakeEmptyRaster(100, 100, 10, 10, 2, 2, 0, 0, 0),
					1, '32BF', 1, 0
				),
				1, 1, 1, 3.14159
			),
			1, 10, 10, 2.71828
		),
		ROW(1, '-10000--100]:50-1,(-100-1000]:150-50,(1000-10000]:254-150', '8BUI', 0)
	) AS rast
) AS t;
SELECT
	ST_Value(t.rast, 1, 1, 1),
	ST_Value(t.rast, 1, 10, 10),
	ST_BandNoDataValue(rast, 1)
FROM (
	SELECT ST_Reclass(
		ST_SetValue(
			ST_SetValue(
				ST_AddBand(
					ST_MakeEmptyRaster(100, 100, 10, 10, 2, 2, 0, 0, 0),
					1, '32BF', 1, 0
				),
				1, 1, 1, 3.14159
			),
			1, 10, 10, 2.71828
		),
		ROW(1, 'a-100]:50-1,(-100-1000]:150-50,(1000-10000]:254-150', '8BUI', 0)
	) AS rast OFFSET 0
) AS t;
SELECT
	ST_Value(t.rast, 1, 1, 1),
	ST_Value(t.rast, 1, 10, 10),
	ST_BandNoDataValue(rast, 1)
FROM (
	SELECT ST_Reclass(
		ST_SetValue(
			ST_SetValue(
				ST_AddBand(
					ST_MakeEmptyRaster(100, 100, 10, 10, 2, 2, 0, 0, 0),
					1, '32BF', 1, 0
				),
				1, 1, 1, 3.14159
			),
			1, 10, 10, 2.71828
		),
		ROW(2, '-10000--100]:50-1,(-100-1000]:150-50,(1000-10000]:254-150', '8BUI', 0)
	) AS rast OFFSET 0
) AS t;

-- ticket #2555
SELECT
	ST_Value(rast, 1, 2, 2),
	ST_Value(rast, 1, 3, 3),
	ST_Value(rast, 1, 4, 4)
FROM (
	SELECT ST_Reclass(
		ST_SetValues(
		 	ST_AddBand(
			 	ST_MakeEmptyRaster(5, 5, 10, 10, 2, 2, 0, 0, 0),
				1, '32BF', 1, -9999
			),
			1, 1, 1,
			ARRAY[
				[1, 1, 1, 1, 1],
				[1, 9000, 1, 1, 1],
				[1, 1, -9000, 1, 1],
				[1, 1, 1, 9000, 1],
				[1, 1, 1, 1, 1]
			]::double precision[]
		),

		1, '[-9000-9000]:[-900-900]', '32BF'
	) AS rast OFFSET 0
) AS t;
