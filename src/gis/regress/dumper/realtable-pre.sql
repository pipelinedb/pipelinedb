insert into spatial_ref_sys(srid,srtext) values (1,'fake["srs"],text');
create table c (i int, g geometry);
insert into c values (1,'SRID=1;POINT(0 0)');
