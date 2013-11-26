#!/bin/bash
#
# This script sets up test environment for pgxc_clean.
# Please note that all the prepared transactions are
# partially committed or aborted.
#
# You should configure PGPORT and PGHOST to connect, as
# well as node names for your test environment.
#
# Before you run this script, XC should be up and ready.
# Also, this may try to drop test databases.   You may need
# to run CLEAN CONNECTION satement for each coordinator in
# advance.
#

if [ $# -le 0 ]
then
	create=no
else
	if [ $1 = create ]
	then
		create=yes
	else
		create=no
	fi
fi

export PGPORT=20004
exprot PGHOST=localhost
sourcedb=postgres

if [ $create = yes ]
then
psql -e $sourcedb <<EOF
drop database if exists test1;
drop database if exists test2;
drop database if exists test3;
create database test1;
create database test2;
create database test3;
\q
EOF
fi

psql -e test1 <<EOF
drop table if exists t;
begin;
create table t (a int);
prepare transaction 'test1_1';
\q
EOF

psql -e test2 <<EOF
drop table if exists t;
begin;
create table t (a int);
prepare transaction 'test2_1';
\q
EOF

psql -e test3 <<EOF
drop table if exists t;
begin;
create table t (a int);
prepare transaction 'test3_1';
\q
EOF

psql -e test1 <<EOF
set xc_maintenance_mode = on;
execute direct on node1 'commit prepared ''test1_1'' ';
\q
EOF

psql -e test2 <<EOF
set xc_maintenance_mode = on;
execute direct on node2 'commit prepared ''test2_1'' ';
\q
EOF

psql -e test3 <<EOF
set xc_maintenance_mode = on;
execute direct on node1 'rollback prepared ''test3_1'' ';
\q
EOF
