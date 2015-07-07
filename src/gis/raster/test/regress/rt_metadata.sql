SELECT * FROM ST_MetaData(NULL);
SELECT * FROM ST_MetaData(
	ST_AddBand(
		ST_MakeEmptyRaster(10, 10, 10, 10, 2, 2, 0, 0, 0)
		, 1, '64BF', 0, 0
	)
);
