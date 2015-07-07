----------------------------------------------------------------------------------------------------------------------
-- ST_SplitTable
-- Split a table into a series of table which names are composed of the concatenation of a prefix
-- and the value of a column. This function is usefull when loading many raster in one operation but 
-- still wanting to split them in different tables afterward. They must have been loaded with the -F 
-- raster2pgsql option so that different rasters are identifiable by a column.
--
-- sourcetablename   - The name of the table to split into multiple table
-- targettableschema - The schema in which to create the new set of table
-- targettableprefix - The prefix of the set of table names to create.
-- suffixcolumnname  - The name of the column providing the suffix to each table name.
--
-- Example to split the table 'test' into a set of table starting with 't_' and 
-- ending with the value of the column 'rid' to be created in the 'public' schema.
--
-- SELECT ST_SplitTable('test', 'public', 't_', 'rid');;
----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ST_SplitTable(sourcetablename text, targettableschema text, targettableprefix text, suffixcolumnname text)
RETURNS int AS
$BODY$
DECLARE
    newtablename text;
    uniqueid RECORD;
BEGIN
    FOR uniqueid IN EXECUTE 'SELECT DISTINCT ' || quote_ident(suffixcolumnname) || ' AS xyz123  FROM ' || sourcetablename LOOP
        newtablename := targettableschema || '.' || targettableprefix || uniqueid.xyz123;
    EXECUTE 'CREATE TABLE ' || quote_ident(newtablename) || ' AS SELECT * FROM ' || sourcetablename || ' WHERE ' || suffixcolumnname || ' = ' || uniqueid.xyz123;
    END LOOP;
    RETURN 1;
END;
$BODY$
LANGUAGE plpgsql VOLATILE STRICT;

---------------------------------------
-- test
CREATE TABLE test AS
SELECT 1 AS rid, ST_MakeEmptyRaster(2,2,0,0,1,1,1,1,4326) AS rast
UNION ALL
SELECT 2 AS rid, ST_MakeEmptyRaster(2,2,0,0,1,1,1,1,4326) AS rast
UNION ALL
SELECT 1 AS rid, ST_MakeEmptyRaster(2,2,0,0,1,1,1,1,4326) AS rast
UNION ALL
SELECT 2 AS rid, ST_MakeEmptyRaster(2,2,0,0,1,1,1,1,4326) AS rast

SELECT * FROM test;

SELECT ST_SplitTable('test', 'public', 't_', 'rid');
