#!/bin/bash
# install cstore_fdw, pgsql_fdw, and other common tools

CSTOREVER=1.2.0
PGVER=9.4.4

# install some build tools, we'll remove these later
apt-get install -y unzip gcc make g++
apt-get install -y protobuf-c-compiler libprotobuf-c0-dev

cd /usr/local/src/

# get packages
wget -q http://api.pgxn.org/dist/cstore_fdw/${CSTOREVER}/cstore_fdw-${CSTOREVER}.zip
wget -q  https://ftp.postgresql.org/pub/source/v${PGVER}/postgresql-${PGVER}.tar.gz
unzip cstore_fdw-${CSTOREVER}.zip
tar -xzf postgresql-${PGVER}.tar.gz 

# build cstore
cd cstore_fdw-${CSTOREVER}
make PG_CONFIG=/usr/lib/pipelinedb/bin/pg_config
make PG_CONFIG=/usr/lib/pipelinedb/bin/pg_config install
cd ..

# build contrib modules
cd /usr/local/src/postgresql-${PGVER}/contrib
export USE_PGXS=1

cd postgres_fdw
make PG_CONFIG=/usr/lib/pipelinedb/bin/pg_config install

cd ../citext
make PG_CONFIG=/usr/lib/pipelinedb/bin/pg_config install

cd ../fuzzystrmatch
make PG_CONFIG=/usr/lib/pipelinedb/bin/pg_config install

cd ../hstore
make PG_CONFIG=/usr/lib/pipelinedb/bin/pg_config install

cd ../pg_trgm
make PG_CONFIG=/usr/lib/pipelinedb/bin/pg_config install

cd ../file_fdw
make PG_CONFIG=/usr/lib/pipelinedb/bin/pg_config install

cd ../tablefunc
make PG_CONFIG=/usr/lib/pipelinedb/bin/pg_config install

# clean up
apt-get remove gcc make unzip g++
apt-get autoremove
cd /
rm -rf /usr/local/src/*

