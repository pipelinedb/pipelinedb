--- regression test for postGIS



--- assume datatypes already defined



--- basic datatypes (correct)

select '1',ST_asewkt('POINT( 1 2 )'::GEOMETRY) as geom;
select '2',ST_asewkt('POINT( 1 2 3)'::GEOMETRY) as geom;

select '3',ST_asewkt('LINESTRING( 0 0, 1 1, 2 2, 3 3 , 4 4)'::GEOMETRY) as geom;
select '4',ST_asewkt('LINESTRING( 0 0 0 , 1 1 1 , 2 2 2 , 3 3 3, 4 4 4)'::GEOMETRY) as geom;
select '5',ST_asewkt('LINESTRING( 1 2 3 , 4 5 6 , 7 8 9 , 10 11 12, 13 14 15)'::GEOMETRY) as geom;

select '6',ST_asewkt('POLYGON( (0 0, 10 0, 10 10, 0 10, 0 0) )'::GEOMETRY) as geom;
select '7',ST_asewkt('POLYGON( (0 0 1 , 10 0 1, 10 10 1, 0 10 1, 0 0 1) )'::GEOMETRY) as geom;
select '8',ST_asewkt('POLYGON( (0 0, 10 0, 10 10, 0 10, 0 0),(5 5, 7 5, 7 7 , 5 7, 5 5) )'::GEOMETRY) as geom;
select '9',ST_asewkt('POLYGON( (0 0, 10 0, 10 10, 0 10, 0 0),(5 5, 7 5, 7 7 , 5 7, 5 5),(1 1,2 1, 2 2, 1 2, 1 1) )'::GEOMETRY) as geom;
select '10',ST_asewkt('POLYGON( (0 0 1, 10 0 1, 10 10 1, 0 10 1, 0 0 1),(5 5 1, 7 5 1, 7 7 1 , 5 7 1, 5 5 1) )'::GEOMETRY) as geom;
select '11',ST_asewkt('POLYGON( (0 0 1, 10 0 1, 10 10 1, 0 10 1, 0 0 1),(5 5 1, 7 5 1, 7 7 1, 5 7 1, 5 5 1),(1 1 1,2 1 1, 2 2 1, 1 2 1, 1 1 1) )'::GEOMETRY) as geom;

select '12',ST_asewkt('GEOMETRYCOLLECTION(POINT( 1 2 ))'::GEOMETRY);
select '13',ST_asewkt('GEOMETRYCOLLECTION(POINT( 1 2 3))'::GEOMETRY);
select '14',ST_asewkt('GEOMETRYCOLLECTION(LINESTRING( 0 0, 1 1, 2 2, 3 3 , 4 4))'::GEOMETRY);
select '15',ST_asewkt('GEOMETRYCOLLECTION(LINESTRING( 1 2 3 , 4 5 6 , 7 8 9 , 10 11 12, 13 14 15))'::GEOMETRY);
select '16',ST_asewkt('GEOMETRYCOLLECTION(POLYGON( (0 0 1, 10 0 1, 10 10 1, 0 10 1, 0 0 1),(5 5 1, 7 5 1, 7 7 1 , 5 7 1, 5 5 1) ))'::GEOMETRY);
select '17',ST_asewkt('GEOMETRYCOLLECTION(POINT( 1 2 0),POINT( 1 2 3) )'::GEOMETRY);
select '18',ST_asewkt('GEOMETRYCOLLECTION(LINESTRING( 0 0 0, 1 1 0, 2 2 0, 3 3 0, 4 4 0),POINT( 1 2 3) )'::GEOMETRY);
select '19',ST_asewkt('GEOMETRYCOLLECTION(POINT( 1 2 ),LINESTRING( 0 0, 1 1, 2 2, 3 3 , 4 4) )'::GEOMETRY);
select '20',ST_asewkt('GEOMETRYCOLLECTION(POINT( 1 2 0 ),POINT( 1 2 3),LINESTRING( 1 2 3 , 4 5 6 , 7 8 9 , 10 11 12, 13 14 15) )'::GEOMETRY);
select '21',ST_asewkt('GEOMETRYCOLLECTION(POINT( 1 2 0 ),POINT( 1 2 3),LINESTRING( 1 2 3 , 4 5 6 , 7 8 9 , 10 11 12, 13 14 15),POLYGON( (0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0) ) )'::GEOMETRY);
select '22',ST_asewkt('GEOMETRYCOLLECTION(POINT( 1 2 0),POINT( 1 2 3),POLYGON( (0 0 1, 10 0 1, 10 10 1, 0 10 1, 0 0 1),(5 5 1, 7 5 1, 7 7 1, 5 7 1, 5 5 1),(1 1 1,2 1 1, 2 2 1, 1 2 1, 1 1 1) ) )'::GEOMETRY);

select '23',ST_asewkt('MULTIPOINT( 1 2)'::GEOMETRY) as geom;
select '24',ST_asewkt('MULTIPOINT( 1 2 3)'::GEOMETRY) as geom;
select '25',ST_asewkt('MULTIPOINT( 1 2, 3 4, 5 6)'::GEOMETRY) as geom;
select '26',ST_asewkt('MULTIPOINT( 1 2 3, 5 6 7, 8 9 10, 11 12 13)'::GEOMETRY) as geom;
select '27',ST_asewkt('MULTIPOINT( 1 2 0, 1 2 3, 4 5 0, 6 7 8)'::GEOMETRY) as geom;
select '28',ST_asewkt('MULTIPOINT( 1 2 3,4 5 0)'::GEOMETRY) as geom;

select '29',ST_asewkt('MULTILINESTRING( (0 0, 1 1, 2 2, 3 3 , 4 4) )'::GEOMETRY) as geom;
select '30',ST_asewkt('MULTILINESTRING( (0 0, 1 1, 2 2, 3 3 , 4 4),(0 0, 1 1, 2 2, 3 3 , 4 4))'::GEOMETRY) as geom;
select '31',ST_asewkt('MULTILINESTRING( (0 0 0, 1 1 0, 2 2 0, 3 3 0, 4 4 0),(0 0 0, 1 1 0, 2 2 0, 3 3 0, 4 4 0),(1 2 3 , 4 5 6 , 7 8 9 , 10 11 12, 13 14 15) )'::GEOMETRY) as geom;
select '32',ST_asewkt('MULTILINESTRING( (1 2 3 , 4 5 6 , 7 8 9 , 10 11 12, 13 14 15),(0 0 0, 1 1 0, 2 2 0, 3 3 0, 4 4 0),(0 0 0, 1 1 0, 2 2 0, 3 3 0 , 4 4 0))'::GEOMETRY) as geom;

select '33',ST_asewkt('MULTIPOLYGON( ((0 0, 10 0, 10 10, 0 10, 0 0)) )'::GEOMETRY) as geom;
select '34',ST_asewkt('MULTIPOLYGON( ((0 0, 10 0, 10 10, 0 10, 0 0)),( (0 0, 10 0, 10 10, 0 10, 0 0),(5 5, 7 5, 7 7 , 5 7, 5 5) ) )'::GEOMETRY) as geom;
select '35',ST_asewkt('MULTIPOLYGON( ((0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0)),( (0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0),(5 5 0, 7 5 0, 7 7 0, 5 7 0, 5 5 0) ) ,( (0 0 1, 10 0 1, 10 10 1, 0 10 1, 0 0 1),(5 5 1, 7 5 1, 7 7 1, 5 7 1, 5 5 1),(1 1 1,2 1 1, 2 2 1, 1 2 1, 1 1 1) ) )'::GEOMETRY) as geom;


select '36',ST_asewkt('GEOMETRYCOLLECTION(MULTIPOINT( 1 2))'::GEOMETRY);
select '37',ST_asewkt('GEOMETRYCOLLECTION(MULTIPOINT( 1 2 3))'::GEOMETRY);
select '38',ST_asewkt('GEOMETRYCOLLECTION(MULTIPOINT( 1 2 3, 5 6 7, 8 9 10, 11 12 13))'::GEOMETRY);
select '39',ST_asewkt('GEOMETRYCOLLECTION(MULTILINESTRING( (0 0, 1 1, 2 2, 3 3 , 4 4) ))'::GEOMETRY);
select '40',ST_asewkt('GEOMETRYCOLLECTION(MULTILINESTRING( (1 2 3 , 4 5 6 , 7 8 9 , 10 11 12, 13 14 15),(0 0 0, 1 1 0, 2 2 0, 3 3 0, 4 4 0),(0 0 0, 1 1 0, 2 2 0, 3 3 0, 4 4 0)))'::GEOMETRY);
select '41',ST_asewkt('GEOMETRYCOLLECTION(MULTIPOLYGON( ((0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0)),( (0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0),(5 5 0, 7 5 0, 7 7 0, 5 7 0, 5 5 0) ) ,( (0 0 1, 10 0 1, 10 10 1, 0 10 1, 0 0 1),(5 5 1, 7 5 1, 7 7 1, 5 7 1, 5 5 1),(1 1 1,2 1 1, 2 2 1, 1 2 1, 1 1 1) ) ))'::GEOMETRY);
select '42',ST_asewkt('GEOMETRYCOLLECTION(POINT( 1 2 0),MULTIPOINT( 1 2 3))'::GEOMETRY);
select '43',ST_asewkt('GEOMETRYCOLLECTION(MULTIPOINT( 1 2 0, 3 4 0, 5 6 0),POINT( 1 2 3))'::GEOMETRY);
select '44',ST_asewkt('GEOMETRYCOLLECTION(POINT( 1 2 3),MULTILINESTRING( (0 0 0, 1 1 0, 2 2 0, 3 3 0 , 4 4 0) ))'::GEOMETRY);
select '45',ST_asewkt('GEOMETRYCOLLECTION(MULTILINESTRING( (0 0 0, 1 1 0, 2 2 0, 3 3 0 , 4 4 0) ),POINT( 1 2 3))'::GEOMETRY);
select '46',ST_asewkt('GEOMETRYCOLLECTION(POINT( 1 2 3), MULTIPOLYGON( ((0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0)),( (0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0),(5 5 0, 7 5 0, 7 7 0, 5 7 0, 5 5 0) ) ,( (0 0 1, 10 0 1, 10 10 1, 0 10 1, 0 0 1),(5 5 1, 7 5 1, 7 7 1, 5 7 1, 5 5 1),(1 1 1,2 1 1, 2 2 1, 1 2 1, 1 1 1) ) ))'::GEOMETRY);
select '47',ST_asewkt('GEOMETRYCOLLECTION(MULTIPOLYGON( ((0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0)),( (0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0),(5 5 0, 7 5 0, 7 7 0, 5 7 0, 5 5 0) ) ,( (0 0 1, 10 0 1, 10 10 1, 0 10 1, 0 0 1),(5 5 1, 7 5 1, 7 7 1, 5 7 1, 5 5 1),(1 1 1,2 1 1, 2 2 1, 1 2 1, 1 1 1) ) ),MULTILINESTRING( (0 0 0, 1 1 0, 2 2 0, 3 3 0, 4 4 0),(0 0 0, 1 1 0, 2 2 0, 3 3 0, 4 4 0),(1 2 3 , 4 5 6 , 7 8 9 , 10 11 12, 13 14 15) ),MULTIPOINT( 1 2 3, 5 6 7, 8 9 10, 11 12 13))'::GEOMETRY);

select '48',ST_asewkt('MULTIPOINT( -1 -2 -3, 5.4 6.6 7.77, -5.4 -6.6 -7.77, 1e6 1e-6 -1e6, -1.3e-6 -1.4e-5 0)'::GEOMETRY) as geom;

select '49', ST_asewkt('GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(1 1) ))'::GEOMETRY) as geom;
--- basic datatype (incorrect)

select '50', 'POINT()'::GEOMETRY as geom;
select '51', 'POINT(1)'::GEOMETRY as geom;
select '52', 'POINT(,)'::GEOMETRY as geom;
select '53', 'MULTIPOINT(,)'::GEOMETRY as geom;
select '54', 'POINT(a b)'::GEOMETRY as geom;
select '55', 'MULTIPOINT()'::GEOMETRY as geom;
select '56', ST_asewkt('POLYGON( (0 0, 10 0, 10 10, 0 10) )'::GEOMETRY);
select '57', ST_asewkt('POLYGON( (0 0, 10 0, 10 10, 0 10, 0 0),(5 5, 7 5, 7 7 , 5 7) )'::GEOMETRY);
select '58', ST_asewkt('MULTILINESTRING((0 0, 1 1),(0 0, 1 1, 2 2,) )'::GEOMETRY);


--- funny results

select '59',ST_asewkt('POINT(1 2 3, 4 5 6)'::GEOMETRY);
select '60',ST_asewkt('POINT(1 2 3 4 5 6 7)'::GEOMETRY);
select '61',ST_asewkt('LINESTRING(1 1)'::GEOMETRY);
select '62',regexp_replace(ST_asewkt('POINT( 1e700 0)'::GEOMETRY), E'(Infinity|1\.#INF)', 'inf');
select '63',regexp_replace(ST_asewkt('POINT( -1e700 0)'::GEOMETRY), E'(Infinity|1\.#INF)', 'inf');
--select '62',ST_asewkt('POINT( 1e700 0)'::GEOMETRY);
--select '63',ST_asewkt('POINT( -1e700 0)'::GEOMETRY);
select '64',ST_asewkt('MULTIPOINT(1 1, 2 2'::GEOMETRY);


--- is_same() testing

select '65','POINT(1 1)'::GEOMETRY ~= 'POINT(1 1)'::GEOMETRY as bool;
select '65a',ST_OrderingEquals('POINT(1 1)'::GEOMETRY,'POINT(1 1)'::GEOMETRY) as bool;
select '66','POINT(1 1 0)'::GEOMETRY ~= 'POINT(1 1)'::GEOMETRY as bool;
select '66a',ST_OrderingEquals('POINT(1 1 0)'::GEOMETRY,'POINT(1 1)'::GEOMETRY) as bool;
select '67','POINT(1 1 0)'::GEOMETRY ~= 'POINT(1 1 0)'::GEOMETRY as bool;
select '67a',ST_OrderingEquals('POINT(1 1 0)'::GEOMETRY,'POINT(1 1 0)'::GEOMETRY) as bool;

select '68','MULTIPOINT(1 1,2 2)'::GEOMETRY ~= 'MULTIPOINT(1 1,2 2)'::GEOMETRY as bool;
select '68a',ST_OrderingEquals('MULTIPOINT(1 1,2 2)'::GEOMETRY,'MULTIPOINT(1 1,2 2)'::GEOMETRY) as bool;
select '69','MULTIPOINT(2 2, 1 1)'::GEOMETRY ~= 'MULTIPOINT(1 1,2 2)'::GEOMETRY as bool;
select '69a',ST_OrderingEquals('MULTIPOINT(2 2, 1 1)'::GEOMETRY,'MULTIPOINT(1 1,2 2)'::GEOMETRY) as bool;

select '70','GEOMETRYCOLLECTION(POINT( 1 2 3),POINT(4 5 6))'::GEOMETRY ~= 'GEOMETRYCOLLECTION(POINT( 4 5 6),POINT(1 2 3))'::GEOMETRY as bool;
select '70a',ST_OrderingEquals('GEOMETRYCOLLECTION(POINT( 1 2 3),POINT(4 5 6))'::GEOMETRY,'GEOMETRYCOLLECTION(POINT( 4 5 6),POINT(1 2 3))'::GEOMETRY) as bool;
select '71','MULTIPOINT(4 5 6, 1 2 3)'::GEOMETRY ~= 'GEOMETRYCOLLECTION(POINT( 4 5 6),POINT(1 2 3))'::GEOMETRY as bool;
select '71a',ST_OrderingEquals('MULTIPOINT(4 5 6, 1 2 3)'::GEOMETRY,'GEOMETRYCOLLECTION(POINT( 4 5 6),POINT(1 2 3))'::GEOMETRY) as bool;
select '72','MULTIPOINT(1 2 3, 4 5 6)'::GEOMETRY ~= 'GEOMETRYCOLLECTION(POINT( 4 5 6),POINT(1 2 3))'::GEOMETRY as bool;
select '72a',ST_OrderingEquals('MULTIPOINT(1 2 3, 4 5 6)'::GEOMETRY,'GEOMETRYCOLLECTION(POINT( 4 5 6),POINT(1 2 3))'::GEOMETRY) as bool;
select '73','MULTIPOINT(1 2 3, 4 5 6)'::GEOMETRY ~= 'GEOMETRYCOLLECTION(MULTIPOINT(1 2 3, 4 5 6))'::GEOMETRY as bool;
select '73a',ST_OrderingEquals('MULTIPOINT(1 2 3, 4 5 6)'::GEOMETRY,'GEOMETRYCOLLECTION(MULTIPOINT(1 2 3, 4 5 6))'::GEOMETRY) as bool;


select '74','LINESTRING(1 1,2 2)'::GEOMETRY ~= 'POINT(1 1)'::GEOMETRY as bool;
select '74a',ST_OrderingEquals('LINESTRING(1 1,2 2)'::GEOMETRY,'POINT(1 1)'::GEOMETRY) as bool;
select '75','LINESTRING(1 1, 2 2)'::GEOMETRY ~= 'LINESTRING(2 2, 1 1)'::GEOMETRY as bool;
select '75a',ST_OrderingEquals('LINESTRING(1 1, 2 2)'::GEOMETRY,'LINESTRING(2 2, 1 1)'::GEOMETRY) as bool;
select '76','LINESTRING(1 1, 2 2)'::GEOMETRY ~= 'LINESTRING(1 1, 2 2, 3 3)'::GEOMETRY as bool;
select '76a',ST_OrderingEquals('LINESTRING(1 1, 2 2)'::GEOMETRY,'LINESTRING(1 1, 2 2, 3 3)'::GEOMETRY) as bool;

--- function testing
--- conversion function

select '106',box3d('MULTIPOINT(0 0, 7 7)'::GEOMETRY) as bvol;

-- box3d only type is only used for indexing -- NEVER use one yourself
select '107',ST_AsEWKT(geometry('BOX3D(0 0 0, 7 7 7 )'::BOX3D));

--- debug function testing

select '108',ST_NPoints('MULTIPOINT(0 0, 7 7)'::GEOMETRY) as value;
select '109',ST_NPoints('GEOMETRYCOLLECTION(POINT(1 1), LINESTRING( 1 1 , 2 2, 3 3))'::GEOMETRY) as value;

select '110', ST_NRings('MULTIPOLYGON( ((0 0, 10 0, 10 10, 0 10, 0 0)),( (0 0, 10 0, 10 10, 0 10, 0 0),(5 5, 7 5, 7 7 , 5 7, 5 5) ) ,( (0 0, 10 0, 10 10, 0 10, 0 0),(5 5, 7 5, 7 7, 5 7, 5 5),(1 1,2 1, 2 2, 1 2, 1 1) ) )'::GEOMETRY) as value;

select '111', ST_MemSize(PostGIS_DropBBOX('MULTIPOLYGON( ((0 0, 10 0, 10 10, 0 10, 0 0)),( (0 0, 10 0, 10 10, 0 10, 0 0),(5 5, 7 5, 7 7 , 5 7, 5 5) ) ,( (0 0, 10 0, 10 10, 0 10, 0 0),(5 5, 7 5, 7 7, 5 7, 5 5),(1 1,2 1, 2 2, 1 2, 1 1) ) )'::GEOMETRY)) as value;

select '112',ST_NumGeometries('GEOMETRYCOLLECTION(POINT(1 1), LINESTRING( 1 1 , 2 2, 3 3),MULTIPOINT(1 1, 2 2))'::GEOMETRY) as value;

---selection



--- TOAST testing

-- create a table with data that will be TOASTed (even after compression)
create table TEST(a GEOMETRY, b GEOMETRY);
\i regress_biginsert.sql


---test basic ops on this 

select '121',box3d(a) as box3d_a, box3d(b) as box3d_b from TEST;

select '122',a <<b from TEST;
select '123',a &<b from TEST;
select '124',a >>b from TEST;
select '125',a &>b from TEST;

select '126',a ~= b from TEST;
select '127',a @ b from TEST;
select '128',a ~ b from TEST; 

-- ST_Mem_Size was deprecated in favor of ST_MemSize in 2.2.0
-- We keep the test using the deprecated function until 2.4.0 
-- (when the function will be removed)
select '129', ST_mem_size(PostGIS_DropBBOX(a)), ST_mem_size(PostGIS_DropBBOX(b)) from TEST;

select '131', ST_X('POINT(1 2)');
select '132', ST_Y('POINT(1 2)');
select '133', ST_Z('POINT(1 2)');
select '133a', ST_Z('POINT(1 2 3)');
select '133b', ST_Z('POINTM(1 2 3)');
select '133c', ST_M('POINT(1 2)');
select '133d', ST_M('POINTM(1 2 4)');
select '133e', ST_M('POINT(1 2 4)');

select '137', box3d('GEOMETRYCOLLECTION(GEOMETRYCOLLECTION EMPTY)'::geometry);
select '138', box3d('GEOMETRYCOLLECTION(GEOMETRYCOLLECTION EMPTY, POINT(0 0))'::geometry);

select '139', ST_AsEWKT(ST_multi(ST_setsrid('GEOMETRYCOLLECTION(GEOMETRYCOLLECTION EMPTY, POINT(0 0))'::geometry, 2)));
select '140', ST_AsEWKT(ST_multi(ST_setsrid('POINT(2 2)'::geometry, 3)));
select '141', ST_AsEWKT(ST_multi(ST_setsrid('LINESTRING(2 2, 3 3)'::geometry, 4)));
select '142', ST_AsEWKT(ST_multi(ST_setsrid('LINESTRING(2 2, 3 3)'::geometry, 5)));
select '143', ST_AsEWKT(ST_multi(ST_setsrid('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'::geometry, 6)));
select '143c1', ST_AsEWKT(ST_multi('CIRCULARSTRING(0 0, 1 1, 2 2)'::geometry));
select '144', ST_AsEWKT(ST_Force3dm('POINT(1 2 3)'));
select '144d', ST_AsEWKT(ST_Force_3dm('POINT(1 2 3)'));
select '145', ST_AsEWKT(ST_Force3dz('POINTM(1 2 3)'));
select '145d', ST_AsEWKT(ST_Force_3dz('POINTM(1 2 3)'));
select '146', ST_AsEWKT(ST_Force4d('POINTM(1 2 3)'));
select '147', ST_AsEWKT(ST_Force4d('POINT(1 2 3)'));
select '147d', ST_AsEWKT(ST_Force_4d('POINT(1 2 3)'));

select '148', ST_AsText(ST_segmentize('LINESTRING(0 0, 10 0)'::geometry, 5));

select '149', ST_AsText(ST_segmentize('GEOMETRYCOLLECTION EMPTY'::geometry, 0.5));

select '150d', ST_AsEWKT(ST_ForceCollection(ST_setsrid('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'::geometry, 6)));
select '150', ST_AsEWKT(ST_ForceCollection(ST_setsrid('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'::geometry, 6)));

select '151', encode(ST_AsBinary(ST_MakeEnvelope(0, 0, 1, 1, 4326),'ndr'),'hex');
select '152', ST_SRID(ST_MakeEnvelope(0, 0, 1, 1, 4326));
select '152.1', ST_SRID(ST_MakeEnvelope(0, 0, 1, 1)) = ST_SRID('POINT(0 0)'::geometry);
select '152.2', ST_SRID(ST_SetSRID(ST_MakeEnvelope(0, 0, 1, 1), 4326));

select '153', ST_AsText(ST_CollectionExtract('GEOMETRYCOLLECTION(POINT(0 0))',1));
select '154', ST_AsText(ST_CollectionExtract('GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(0 0)))',1));
select '155', ST_AsText(ST_CollectionExtract('GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(0 0), POINT(1 1)))',1));
select '156', ST_AsText(ST_CollectionExtract('GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(LINESTRING(0 0, 1 1), POINT(1 1)))',1));
select '157', ST_AsText(ST_CollectionExtract('GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(LINESTRING(0 0, 1 1), POINT(1 1)))',2));
select '158', ST_AsText(ST_CollectionExtract('GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(LINESTRING(0 0, 1 1), POINT(1 1)),LINESTRING(2 2, 3 3))',2));
select '159', ST_AsText(ST_CollectionExtract('GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(LINESTRING(0 0, 1 1), POINT(1 1)),LINESTRING(2 2, 3 3))',3));
select '160', ST_AsText(ST_CollectionExtract('GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(LINESTRING(0 0, 1 1), POINT(1 1)),LINESTRING(2 2, 3 3))',1));
select '161', ST_AsText(ST_CollectionExtract('GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(LINESTRING(0 0, 1 1), GEOMETRYCOLLECTION(POINT(1 1))),LINESTRING(2 2, 3 3))',2));
select '162', encode(ST_AsBinary(ST_MakeLine(ST_GeomFromText('POINT(-11.1111111 40)'),ST_GeomFromText('LINESTRING(-11.1111111 70,70 -11.1111111)')),'ndr'),'hex') As result;
select '163', ST_AsEWKT('POLYGON((0 0 0, 1 0 0, 1 1 0, 0 1 0, 0 0 0))');
select '164', ST_AsEWKT('POLYGON((0 0 0, 1 0 0, 1 1 0, 0 1 0, 0 0 1))');
select '165', ST_AsEWKT('POLYGON((0 0 0, 1 0 0, 1 1 0, 0 1 0, 0 0.1 1))');
select '166', ST_AsText('POINT EMPTY');
select '167', ST_AsText('LINESTRING EMPTY');
select '168', ST_AsText('POLYGON EMPTY');
select '169', ST_AsText('CIRCULARSTRING EMPTY');
select '170', ST_AsText('COMPOUNDCURVE EMPTY');
select '171', ST_AsText('CURVEPOLYGON EMPTY');
select '172', ST_AsText('MULTIPOINT EMPTY');
select '173', ST_AsText('MULTILINESTRING EMPTY');
select '174', ST_AsText('MULTIPOLYGON EMPTY');
select '175', ST_AsText('TRIANGLE EMPTY');
select '176', ST_AsText('TIN EMPTY');
select '177', ST_AsText('POLYHEDRALSURFACE EMPTY');
select '178', ST_AsText('MULTISURFACE EMPTY');
select '179', ST_AsText('MULTICURVE EMPTY');
select '180', ST_AsText('GEOMETRYCOLLECTION EMPTY');
select '181', ST_AsText('GEOMETRYCOLLECTION(TRIANGLE EMPTY,TIN EMPTY)');


-- Drop test table
DROP table test;
