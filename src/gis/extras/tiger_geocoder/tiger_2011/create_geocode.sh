#!/bin/bash
PGPORT=5432
PGHOST=localhost
PGUSER=postgres
PGPASSWORD=yourpasswordhere
THEDB=geocoder
PSQL_CMD=/usr/bin/psql
PGCONTRIB=/usr/share/postgresql/contrib
#if you are on 9.1+ use the CREATE EXTENSION syntax instead
${PSQL_CMD} -d "${THEDB}" -f "${PGCONTRIB}/fuzzystrmatch.sql"
#${PSQL_CMD} -d "${THEDB}" -c "CREATE EXTENSION fuzzystrmatch" 
${PSQL_CMD} -d "${THEDB}" -c "CREATE SCHEMA tiger"
#unremark this next line and edit if you want the search paths set as part of the install
#${PSQL_CMD} -d "${THEDB}" -c "ALTER DATABASE ${THEDB} SET search_path=public, tiger;"
#${PSQL_CMD} -d "${THEDB}" -f "tables/lookup_tables_2010.sql"
${PSQL_CMD} -d "${THEDB}" -c "CREATE SCHEMA tiger_data"
${PSQL_CMD} -d "${THEDB}" -f "create_geocode.sql"
${PSQL_CMD} -d "${THEDB}" -f "tiger_loader_2014.sql"
${PSQL_CMD}  -d "${THEDB}" -f "census_loader.sql"
${PSQL_CMD}  -d "${THEDB}" -c "SELECT tiger.create_census_base_tables();"
${PSQL_CMD} -d "${THEDB}" -c "CREATE INDEX idx_tiger_addr_least_address ON addr USING btree (least_hn(fromhn,tohn));"