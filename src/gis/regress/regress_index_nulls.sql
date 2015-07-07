\i regress_lots_of_nulls.sql
SET client_min_messages TO NOTICE;

CREATE INDEX "test_geom_idx" ON "test" using gist (the_geom);

DROP TABLE "test";


-- Test node splits in geometry and geography with null-filled, empty-filled, and mixed collections of rows
-- Previous issues showed crashing behaviour, so if we don't crash, we're golden.

-- Mixed null/empty on geography
drop table if exists indexnulls;
create table indexnulls (g geography, id integer);
create index nulls_gix on indexnulls using gist (g);
insert into indexnulls (id, g) with n as ( select null::geography as p ), e as (select 'point(0 0)'::geography as p), a as (select * from n union all select * from e) select generate_series as id, p as g from generate_series(1,1000),a order by generate_series;
drop table indexnulls;

-- Mixed null/empty on geometry
drop table if exists indexnulls;
create table indexnulls (g geometry, id integer);
create index nulls_gix on indexnulls using gist (g);
insert into indexnulls (id, g) with n as ( select null::geometry as p ), e as (select 'point(0 0)'::geometry as p), a as (select * from n union all select * from e) select generate_series as id, p as g from generate_series(1,1000),a order by generate_series;
drop table indexnulls;

-- All empty on geometry
drop table if exists indexempty;
create table indexempty (g geography, id integer);
create index empty_gix on indexempty using gist (g);
insert into indexempty (id, g) select generate_series, 'POINT EMPTY'::geography from generate_series(1,1000);
drop table indexempty;

-- All empty on geometry
drop table if exists indexempty;
create table indexempty (g geometry, id integer);
create index empty_gix on indexempty using gist (g);
insert into indexempty (id, g) select generate_series, 'POINT EMPTY':: geometry from generate_series(1,1000);
drop table indexempty;
