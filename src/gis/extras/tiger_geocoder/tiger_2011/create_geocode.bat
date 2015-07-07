set PGPORT=5432
set PGHOST=localhost
set PGUSER=postgres
set PGPASSWORD=yourpasswordhere
set THEDB=geocoder
set PGBIN=C:\Program Files\PostgreSQL\9.1\bin
set PGCONTRIB=C:\Program Files\PostgreSQL\9.1\share\contrib
REM "%PGBIN%\psql" -d "%THEDB%" -f "%PGCONTRIB%\fuzzystrmatch.sql"
REM If you are using PostgreSQL 9.1 or above, use the extension syntax instead as shown below
"%PGBIN%\psql"  -d "%THEDB%" -c "CREATE EXTENSION fuzzystrmatch;" 
"%PGBIN%\psql"  -d "%THEDB%" -c "CREATE SCHEMA tiger;"
REM unremark this next line and edit if you want the search paths set as part of the install
REM "%PGBIN%\psql" -d "%THEDB%" -c "ALTER DATABASE %THEDB% SET search_path=public, tiger;"
"%PGBIN%\psql"  -d "%THEDB%" -f "create_geocode.sql"
REM "%PGBIN%\psql"  -d "%THEDB%" -f "tables\lookup_tables_2011.sql"
"%PGBIN%\psql"  -d "%THEDB%" -c "CREATE SCHEMA tiger_data;"
"%PGBIN%\psql"  -d "%THEDB%" -f "tiger_loader_2014.sql;"
"%PGBIN%\psql"  -d "%THEDB%" -f "census_loader.sql;"
"%PGBIN%\psql"  -d "%THEDB%" -c "SELECT tiger.create_census_base_tables();"
"%PGBIN%\psql"  -d "%THEDB%" -c "CREATE INDEX idx_tiger_addr_least_address ON addr USING btree (least_hn(fromhn,tohn));"
pause

