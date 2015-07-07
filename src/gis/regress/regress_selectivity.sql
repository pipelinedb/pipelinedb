
-- Check for error messages
create table no_stats ( g geometry, id integer );
create table no_stats_join ( g geometry, id integer );
select _postgis_selectivity('no_stats','g', 'LINESTRING(0 0, 1 1)');
select _postgis_stats('no_stats','g');
select _postgis_join_selectivity('no_stats', 'g', 'no_stats_join', 'g');
insert into no_stats (g, id) values ('POINT(0 0)', 0);
analyze no_stats;
select _postgis_join_selectivity('no_stats', 'g', 'no_stats_join', 'g');
drop table no_stats;
drop table no_stats_join;

-- Table with uniformly variable density, highest at 1,1, lowest at 10,10
create table regular_overdots as
with 
ij as ( select i, j from generate_series(1, 10) i, generate_series(1, 10) j),
iijj as (select generate_series(1, i) as a, generate_series(1, j) b from ij)
select st_makepoint(a, b) as g from iijj;

-- Generate the stats
analyze regular_overdots;

-- Baseline info
select 'selectivity_00', count(*) from regular_overdots;

-- First test
select 'selectivity_01', count(*) from regular_overdots where g && 'LINESTRING(0 0, 11 3.5)';
select 'selectivity_02', 'actual', round(1068.0/2127.0,3);
select 'selectivity_03', 'estimated', round(_postgis_selectivity('regular_overdots','g','LINESTRING(0 0, 11 3.5)')::numeric,3);

-- Second test
select 'selectivity_04', count(*) from regular_overdots where g && 'LINESTRING(5.5 5.5, 11 11)';
select 'selectivity_05', 'actual', round(161.0/2127.0,3);
select 'selectivity_06', 'estimated', round(_postgis_selectivity('regular_overdots','g','LINESTRING(5.5 5.5, 11 11)')::numeric,3);

-- Third test
select 'selectivity_07', count(*) from regular_overdots where g && 'LINESTRING(1.5 1.5, 2.5 2.5)';
select 'selectivity_08', 'actual', round(81.0/2127.0,3);
select 'selectivity_09', 'estimated', round(_postgis_selectivity('regular_overdots','g','LINESTRING(1.5 1.5, 2.5 2.5)')::numeric,3);

-- Fourth test
select 'selectivity_10', 'actual', 0;
select 'selectivity_09', 'estimated', _postgis_selectivity('regular_overdots','g','LINESTRING(11 11, 12 12)');

-- Fifth test
select 'selectivity_10', 'actual', 1;
select 'selectivity_09', 'estimated', _postgis_selectivity('regular_overdots','g','LINESTRING(0 0, 12 12)');

-- Clean
drop table if exists regular_overdots;

