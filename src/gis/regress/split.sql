-- Split line by point of different SRID
select st_split('SRID=10;LINESTRING(0 0, 10 0)', 'SRID=5;POINT(5 1)');

-- Split line by point on the line interior
select '1',st_asewkt(st_split('SRID=10;LINESTRING(0 0, 10 0)', 'SRID=10;POINT(5 0)'));
select '1.1',st_asewkt(st_split('SRID=10;LINESTRING(10 0, 0 0)', 'SRID=10;POINT(5 0)'));

-- Split line by point on the line boundary
select '2',st_asewkt(st_split('SRID=10;LINESTRING(0 0, 10 0)', 'SRID=10;POINT(10 0)'));

-- Split line by point on the line exterior
select '3',st_asewkt(st_split('SRID=10;LINESTRING(0 0, 10 0)', 'SRID=10;POINT(5 1)'));

-- Split line by line of different SRID
select st_split('SRID=10;LINESTRING(0 0, 10 0)', 'SRID=5;LINESTRING(5 1, 10 1)');

-- Split line by disjoint line 
select '4', st_asewkt(ST_Split('SRID=10;LINESTRING(0 0, 10 0)', 'SRID=10;LINESTRING(20 0, 20 20)'));

-- Split line by touching line
select '5', st_asewkt(ST_Split('SRID=10;LINESTRING(0 0, 10 0)', 'SRID=10;LINESTRING(10 -5, 10 5)'));

-- Split line by crossing line
select '6', st_asewkt(ST_Split('SRID=10;LINESTRING(0 0, 10 0)', 'SRID=10;LINESTRING(5 -5, 5 5)'));

-- Split line by multiply-crossing line
select '7', st_asewkt(ST_Split('SRID=10;LINESTRING(0 0, 10 0, 10 10, 0 10, 0 20, 10 20)', 'SRID=10;LINESTRING(5 -5, 5 25)'));

-- Split line by overlapping line (1)
select '8.1', st_asewkt(ST_Split('SRID=10;LINESTRING(0 0, 10 0)', 'SRID=10;LINESTRING(5 0, 20 0)'));
-- Split line by contained line (2)
select '8.2', st_asewkt(ST_Split('SRID=10;LINESTRING(0 0, 10 0)', 'SRID=10;LINESTRING(5 0, 8 0)'));

-- Split exterior-only polygon by crossing line
select '20', st_asewkt(ST_Split('SRID=12;POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))', 'SRID=12;LINESTRING(5 -5, 5 15)'));

-- Split single-hole polygon by line crossing both exterior and hole
select '21', st_asewkt(ST_Split('SRID=12;POLYGON((0 0, 10 0, 10 10, 0 10, 0 0),(2 2, 8 2, 8 8, 2 8, 2 2))', 'SRID=12;LINESTRING(5 -5, 5 15)'));

-- Split single-hole polygon by line crossing only exterior 
select '22', st_asewkt(ST_Split('SRID=12;POLYGON((0 0, 10 0, 10 10, 0 10, 0 0),(5 2, 8 2, 8 8, 5 8, 5 2))', 'SRID=12;LINESTRING(2 -5, 2 15)'));

-- Split double-hole polygon by line crossing exterior and both holes
select '23', st_asewkt(ST_Split('SRID=12;POLYGON((0 0, 10 0, 10 10, 0 10, 0 0),(2 2, 8 2, 8 4, 2 4, 2 2),(2 6,8 6,8 8,2 8,2 6))', 'SRID=12;LINESTRING(5 -5, 5 15)'));

-- Split multiline by line crossing both
select '30', st_asewkt(st_split('SRID=10;MULTILINESTRING((0 0, 10 0),(0 5, 10 5))', 'SRID=10;LINESTRING(5 -5, 5 10)'));

-- Split multiline by line crossing only one of them
select '31', st_asewkt(st_split('SRID=10;MULTILINESTRING((0 0, 10 0),(0 5, 10 5))', 'SRID=10;LINESTRING(5 -5, 5 2)'));

-- Split multiline by disjoint line
select '32', st_asewkt(st_split('SRID=10;MULTILINESTRING((0 0, 10 0),(0 5, 10 5))', 'SRID=10;LINESTRING(5 10, 5 20)'));

-- Split multiline by point on one of them 
select '40', st_asewkt(st_split('SRID=10;MULTILINESTRING((0 0, 10 0),(0 5, 10 5))', 'SRID=10;POINT(5 0)'));

-- Split multipolygon by line 
select '50', st_asewkt(ST_Split('SRID=12;MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0),(2 2, 8 2, 8 4, 2 4, 2 2),(2 6,8 6,8 8,2 8,2 6)),((20 0,20 10, 30 10, 30 0, 20 0),(25 5, 28 5, 25 8, 25 5)))', 'SRID=12;LINESTRING(5 -5, 5 15)'));

-- Split geometrycollection by line 
select '60', st_asewkt(ST_Split('SRID=12;GEOMETRYCOLLECTION(MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0),(2 2, 8 2, 8 4, 2 4, 2 2),(2 6,8 6,8 8,2 8,2 6)),((20 0,20 10, 30 10, 30 0, 20 0),(25 5, 28 5, 25 8, 25 5))),MULTILINESTRING((0 0, 10 0),(0 5, 10 5)))', 'SRID=12;LINESTRING(5 -5, 5 15)'));

-- Split 3d line by 2d line 
select '70', st_asewkt(ST_Split('SRID=11;LINESTRING(1691983.26 4874594.81 312.24, 1691984.86 4874593.69 312.24, 1691979.54 4874586.09 312.24, 1691978.03 4874587.16 298.36)', 'SRID=11;LINESTRING(1691978.0 4874589.0,1691982.0 4874588.53, 1691982.0 4874591.0)'));

-- Split collapsed line by point
-- See http://trac.osgeo.org/postgis/ticket/1772
select '80', st_asewkt(ST_Split('LINESTRING(0 1, 0 1, 0 1)', 'POINT(0 1)'));
select '81', st_asewkt(ST_Split('LINESTRING(0 1, 0 1)', 'POINT(0 1)'));

-- Split long line by vertex point
-- See http://trac.osgeo.org/postgis/ticket/2173
with inp as ( SELECT 
'01020000001000000034030F8FB15866C0F2311FFD3B9A53C0571C87CF1BB65BC0182DB847DB9052C0EBD57BDEEBF658C05CA18B9FA81B52C074384E71C20552C05AD308B7C38351C0A4B3920AA7914CC0ACD200FB29784FC0F8892AEE70E14040C0C8143E325651C0234604DC104E5440EF10F2807BF850C08FEE52B6CAE15F4002BF1C6676B450C0051A57A65BB061405B9E445AEC9F50C05AF3E1D5815665405E3A4A2BB6CF51C0591DE7ECD21F66400D33BFE91C7E53C0000000E0FF7F6640000000C04E9353C0000000000080664000000000008056C000000000008066C000000000008056C000000000008066C0000000E04D9353C034030F8FB15866C0F2311FFD3B9A53C0'
  ::geometry as g, 14 as p )
select '82', st_equals(g, st_union(
    st_geometryn(st_split(g, st_pointn(g,p)), 1), 
    st_geometryn(st_split(g, st_pointn(g,p)), 2))) from inp;

-- Split line by multiline
select '83', st_asewkt(ST_Split(
  'SRID=3;LINESTRING(1 -1,1 1)',
  'SRID=3;MULTILINESTRING((10 0, 10 4),(-4 0, 4 0))'
));

-- Split line by polygon (boundary)
select '84', st_asewkt(ST_Split(
  'SRID=3;LINESTRING(1 -1,1 1)',
  'SRID=3;POLYGON((-10 -10,-10 10,10 10,10 -10,-10 -10),(-4 2,-4 0,4 0,4 2,-4 2))'
));

-- Split line by multipolygon (boundary)
select '85', st_asewkt(ST_Split(
  'SRID=3;LINESTRING(1 -2,1 1,4 1)',
  'SRID=3;MULTIPOLYGON(((0 -1,0 -3,2 -3,2 -1,0 -1)),((3 0,3 2,5 2,5 0,3 0)))'
));

-- Split multiline by multipoint
select '86', st_asewkt(ST_Split(
  'SRID=3;MULTILINESTRING((0 0,10 0),(5 -5, 5 5),(0 20,10 20))',
  'SRID=3;MULTIPOINT(2 6,5 0,5 20,2 20,8 20,8 0,5 -2,0 0, 5 -5, 10 20)'
));

-- TODO: split line by collapsed line 
