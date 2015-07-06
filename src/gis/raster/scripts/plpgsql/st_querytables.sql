----------------------------------------------------------------------------------------------------------------------
-- ST_QueryTables
-- Execute a query on a series of table based on a prefix.
-- The string "tablename" will be replaced by the name of the table.
-- schemaname - The schema where to execute the queries.
-- prefix     - Prefix to restraint the query to tables names starting with this string.
-- inquery    - Query to execute. Can contain the 'tablename' string which will be replaced buy the name of the current table.
--
-- Example to drop a set of table
--
-- SELECT ST_QueryTables('public', 'aa', 'DROP TABLE IF EXISTS tablename');
----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ST_QueryTables(schemaname text, prefix text, inquery text)
RETURNS int AS
$BODY$
DECLARE
    tabletoquery RECORD;
BEGIN
    FOR tabletoquery IN EXECUTE 'SELECT tablename FROM pg_tables WHERE schemaname = ' || quote_literal(schemaname) || ' AND tablename LIKE ' || quote_literal(prefix || '%') LOOP
        RAISE NOTICE 'Querying %', schemaname || '.' || tabletoquery.tablename;
        EXECUTE replace(inquery, 'tablename', '"' || schemaname || '"."' || tabletoquery.tablename || '"');
    END LOOP;
    RETURN 1;
END;
$BODY$
LANGUAGE plpgsql VOLATILE STRICT;