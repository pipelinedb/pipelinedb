SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ] ; do SOURCE="$(readlink "$SOURCE")"; done
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

FILERASTER="$DIR/loader/testraster.tif"

# special handling for msys
CSYS=`uname -o | tr '[:upper:]' '[:lower:]'`
if [ "$CSYS" == "msys" ]; then
	FILERASTER=`cmd //c echo "${FILERASTER}"`
fi

SQL=" \
WITH foo AS ( \
	SELECT postgis_raster_lib_version() \
) \
SELECT NULL FROM foo; \
SET postgis.gdal_enabled_drivers = 'GTiff'; \
DROP TABLE IF EXISTS raster_outdb_template; \
CREATE TABLE raster_outdb_template AS \
SELECT \
	1 AS rid, \
	ST_AddBand( -- insert all three bands of out-db raster at index 1 \
		ST_MakeEmptyRaster(90, 50, 0., 0., 1, -1, 0, 0, 0), \
		1, '$FILERASTER'::text, NULL::int[] \
	) AS rast \
UNION ALL \
SELECT \
	2 AS rid, \
	ST_AddBand( -- append all three bands of out-db raster \
		ST_MakeEmptyRaster(90, 50, 0., 0., 1, -1, 0, 0, 0), \
		'$FILERASTER'::text, NULL::int[] \
	) AS rast \
UNION ALL \
SELECT \
	3 AS rid, \
	ST_AddBand( -- append out-db band 2 \
		ST_AddBand( \
			ST_MakeEmptyRaster(90, 50, 0., 0., 1, -1, 0, 0, 0), \
			1, '8BUI', 1, 0 \
		), \
		'$FILERASTER'::text, ARRAY[2]::int[] \
	) AS rast \
UNION ALL \
SELECT \
	4 AS rid, \
	ST_AddBand( -- append out-db band 2 \
		ST_AddBand( \
			ST_MakeEmptyRaster(90, 50, 0., 0., 1, -1, 0, 0, 0), \
			1, '8BUI', 1, 0 \
		), \
		'$FILERASTER'::text, ARRAY[2]::int[], \
		1, \
		255 \
	) AS rast \
"

echo "$SQL" > "$DIR/$TEST-pre.sql"

# no longer needed as "clean" test takes care of it
#echo "DROP TABLE IF EXISTS raster_outdb_template;" > "$DIR/$TEST-post.sql"
