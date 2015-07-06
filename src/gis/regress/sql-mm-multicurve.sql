-- Repeat the tests with the new function names.
SELECT 'ndims01', ST_ndims(ST_geomfromewkt('MULTICURVE((
                5 5 1 3,
                3 5 2 2,
                3 3 3 1,
                0 3 1 1)
                ,CIRCULARSTRING(
                0 0 0 0, 
                0.26794919243112270647255365849413 1 3 -2, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097 1 2))'));
SELECT 'geometrytype01', geometrytype(ST_geomfromewkt('MULTICURVE((
                5 5 1 3,
                3 5 2 2,
                3 3 3 1,
                0 3 1 1)
                ,CIRCULARSTRING(
                0 0 0 0, 
                0.26794919243112270647255365849413 1 3 -2, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097 1 2))'));
SELECT 'ndims02', ST_ndims(ST_geomfromewkt('MULTICURVE((
                5 5 1,
                3 5 2,
                3 3 3,
                0 3 1)
                ,CIRCULARSTRING(
                0 0 0, 
                0.26794919243112270647255365849413 1 3, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097 1))'));
SELECT 'geometrytype02', geometrytype(ST_geomfromewkt('MULTICURVE((
                5 5 1,
                3 5 2,
                3 3 3,
                0 3 1)
                ,CIRCULARSTRING(
                0 0 0, 
                0.26794919243112270647255365849413 1 3, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097 1))'));
SELECT 'ndims03', ST_ndims(ST_geomfromewkt('MULTICURVEM((
                5 5 3,
                3 5 2,
                3 3 1,
                0 3 1)
                ,CIRCULARSTRING(
                0 0 0, 
                0.26794919243112270647255365849413 1 -2, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097 2))'));
SELECT 'geometrytype03', geometrytype(ST_geomfromewkt('MULTICURVEM((
                5 5 3,
                3 5 2,
                3 3 1,
                0 3 1)
                ,CIRCULARSTRING(
                0 0 0, 
                0.26794919243112270647255365849413 1 -2, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097 2))'));
SELECT 'ndims04', ST_ndims(ST_geomfromewkt('MULTICURVE((
                5 5,
                3 5,
                3 3,
                0 3)
                ,CIRCULARSTRING(
                0 0, 
                0.26794919243112270647255365849413 1, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097))'));
SELECT 'geometrytype04', geometrytype(ST_geomfromewkt('MULTICURVE((
                5 5,
                3 5,
                3 3,
                0 3)
                ,CIRCULARSTRING(
                0 0, 
                0.26794919243112270647255365849413 1, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097))'));

CREATE TABLE public.multicurve (id INTEGER, description VARCHAR,
the_geom_2d GEOMETRY(MULTICURVE),
the_geom_3dm GEOMETRY(MULTICURVEM),
the_geom_3dz GEOMETRY(MULTICURVEZ),
the_geom_4d GEOMETRY(MULTICURVEZM));

INSERT INTO public.multicurve (
        id,
        description
      ) VALUES (
        1, 'multicurve');
UPDATE public.multicurve
        SET the_geom_4d = ST_Geomfromewkt('MULTICURVE((
                5 5 1 3,
                3 5 2 2,
                3 3 3 1,
                0 3 1 1)
                ,CIRCULARSTRING(
                0 0 0 0, 
                0.26794919243112270647255365849413 1 3 -2, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097 1 2))');
UPDATE public.multicurve
        SET the_geom_3dz = ST_Geomfromewkt('MULTICURVE((
                5 5 1,
                3 5 2,
                3 3 3,
                0 3 1)
                ,CIRCULARSTRING(
                0 0 0, 
                0.26794919243112270647255365849413 1 3, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097 1))');
UPDATE public.multicurve        
        SET the_geom_3dm = ST_Geomfromewkt('MULTICURVEM((
                5 5 3,
                3 5 2,
                3 3 1,
                0 3 1)
                ,CIRCULARSTRING(
                0 0 0, 
                0.26794919243112270647255365849413 1 -2, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097 2))');
UPDATE public.multicurve
        SET the_geom_2d = ST_Geomfromewkt('MULTICURVE((
                5 5,
                3 5,
                3 3,
                0 3)
                ,CIRCULARSTRING(
                0 0, 
                0.26794919243112270647255365849413 1, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097))');

-- These tests will fail on different architectures
-- We need a way to handle multiple byte orderings
--SELECT 'asbinary01', encode(asbinary(the_geom_2d), 'hex') FROM public.multicurve;
--SELECT 'asbinary02', encode(asbinary(the_geom_3dm), 'hex') FROM public.multicurve;
--SELECT 'asbinary03', encode(asbinary(the_geom_3dz), 'hex') FROM public.multicurve;
--SELECT 'asbinary04', encode(asbinary(the_geom_4d), 'hex') FROM public.multicurve;
--
--SELECT 'asewkb01', encode(asewkb(the_geom_2d), 'hex') FROM public.multicurve;
--SELECT 'asewkb02', encode(asewkb(the_geom_3dm), 'hex') FROM public.multicurve;
--SELECT 'asewkb03', encode(asewkb(the_geom_3dz), 'hex') FROM public.multicurve;
--SELECT 'asewkb04', encode(asewkb(the_geom_4d), 'hex') FROM public.multicurve;

SELECT 'ST_CurveToLine-201', ST_Asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_2d, 2), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.multicurve;
SELECT 'ST_CurveToLine-202', ST_Asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_3dm, 2), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.multicurve;
SELECT 'ST_CurveToLine-203', ST_Asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_3dz, 2), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.multicurve;
SELECT 'ST_CurveToLine-204', ST_Asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_4d, 2), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.multicurve;

SELECT 'ST_CurveToLine-401', ST_Asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_2d, 4), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.multicurve;
SELECT 'ST_CurveToLine-402', ST_Asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_3dm, 4), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.multicurve;
SELECT 'ST_CurveToLine-403', ST_Asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_3dz, 4), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.multicurve;
SELECT 'ST_CurveToLine-404', ST_Asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_4d, 4), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.multicurve;

SELECT 'ST_CurveToLine01', ST_Asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_2d), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.multicurve;
SELECT 'ST_CurveToLine02', ST_Asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_3dm), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.multicurve;
SELECT 'ST_CurveToLine03', ST_Asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_3dz), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.multicurve;
SELECT 'ST_CurveToLine04', ST_Asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_4d), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.multicurve;

-- Removed due to descrepencies between hardware
--SELECT 'box2d01', box2d(the_geom_2d) FROM public.multicurve;
--SELECT 'box2d02', box2d(the_geom_3dm) FROM public.multicurve;
--SELECT 'box2d03', box2d(the_geom_3dz) FROM public.multicurve;
--SELECT 'box2d04', box2d(the_geom_4d) FROM public.multicurve;

--SELECT 'box3d01', box3d(the_geom_2d) FROM public.multicurve;
--SELECT 'box3d02', box3d(the_geom_3dm) FROM public.multicurve;
--SELECT 'box3d03', box3d(the_geom_3dz) FROM public.multicurve;
--SELECT 'box3d04', box3d(the_geom_4d) FROM public.multicurve;
-- TODO: ST_SnapToGrid is required to remove platform dependent precision
-- issues.  Until ST_SnapToGrid is updated to work against curves, these
-- tests cannot be run.
--SELECT 'ST_LineToCurve01', ST_Asewkt(ST_LineToCurve(ST_CurveToLine(the_geom_2d))) FROM public.multicurve;
--SELECT 'ST_LineToCurve02', ST_Asewkt(ST_LineToCurve(ST_CurveToLine(the_geom_3dm))) FROM public.multicurve;
--SELECT 'ST_LineToCurve03', ST_Asewkt(ST_LineToCurve(ST_CurveToLine(the_geom_3dz))) FROM public.multicurve;
--SELECT 'ST_LineToCurve04', ST_Asewkt(ST_LineToCurve(ST_CurveToLine(the_geom_4d))) FROM public.multicurve;

-- Repeat all tests with the new function names.
SELECT 'astext01', ST_astext(the_geom_2d) FROM public.multicurve;
SELECT 'astext02', ST_astext(the_geom_3dm) FROM public.multicurve;
SELECT 'astext03', ST_astext(the_geom_3dz) FROM public.multicurve;
SELECT 'astext04', ST_astext(the_geom_4d) FROM public.multicurve;

SELECT 'asewkt01', ST_asewkt(the_geom_2d) FROM public.multicurve;
SELECT 'asewkt02', ST_asewkt(the_geom_3dm) FROM public.multicurve;
SELECT 'asewkt03', ST_asewkt(the_geom_3dz) FROM public.multicurve;
SELECT 'asewkt04', ST_asewkt(the_geom_4d) FROM public.multicurve;

-- These tests will fail on different architectures
-- We need a way to handle multiple byte orderings
--SELECT 'asbinary01', encode(ST_asbinary(the_geom_2d), 'hex') FROM public.multicurve;
--SELECT 'asbinary02', encode(ST_asbinary(the_geom_3dm), 'hex') FROM public.multicurve;
--SELECT 'asbinary03', encode(ST_asbinary(the_geom_3dz), 'hex') FROM public.multicurve;
--SELECT 'asbinary04', encode(ST_asbinary(the_geom_4d), 'hex') FROM public.multicurve;
--
--SELECT 'asewkb01', encode(ST_asewkb(the_geom_2d), 'hex') FROM public.multicurve;
--SELECT 'asewkb02', encode(ST_asewkb(the_geom_3dm), 'hex') FROM public.multicurve;
--SELECT 'asewkb03', encode(ST_asewkb(the_geom_3dz), 'hex') FROM public.multicurve;
--SELECT 'asewkb04', encode(ST_asewkb(the_geom_4d), 'hex') FROM public.multicurve;

-- Removed due to descrepencies between hardware
--SELECT 'box2d01', ST_box2d(the_geom_2d) FROM public.multicurve;
--SELECT 'box2d02', ST_box2d(the_geom_3dm) FROM public.multicurve;
--SELECT 'box2d03', ST_box2d(the_geom_3dz) FROM public.multicurve;
--SELECT 'box2d04', ST_box2d(the_geom_4d) FROM public.multicurve;

--SELECT 'box3d01', ST_box3d(the_geom_2d) FROM public.multicurve;
--SELECT 'box3d02', ST_box3d(the_geom_3dm) FROM public.multicurve;
--SELECT 'box3d03', ST_box3d(the_geom_3dz) FROM public.multicurve;
--SELECT 'box3d04', ST_box3d(the_geom_4d) FROM public.multicurve;

SELECT 'isValid01', ST_isValid(the_geom_2d) FROM public.multicurve;
SELECT 'isValid02', ST_isValid(the_geom_3dm) FROM public.multicurve;
SELECT 'isValid03', ST_isValid(the_geom_3dz) FROM public.multicurve;
SELECT 'isValid04', ST_isValid(the_geom_4d) FROM public.multicurve;

SELECT 'dimension01', ST_dimension(the_geom_2d) FROM public.multicurve;
SELECT 'dimension02', ST_dimension(the_geom_3dm) FROM public.multicurve;
SELECT 'dimension03', ST_dimension(the_geom_3dz) FROM public.multicurve;
SELECT 'dimension04', ST_dimension(the_geom_4d) FROM public.multicurve;

SELECT 'numGeometries01', ST_numGeometries(the_geom_2d) FROM public.multicurve;
SELECT 'numGeometries02', ST_numGeometries(the_geom_3dm) FROM public.multicurve;
SELECT 'numGeometries03', ST_numGeometries(the_geom_3dz) FROM public.multicurve;
SELECT 'numGeometries04', ST_numGeometries(the_geom_4d) FROM public.multicurve;

SELECT 'geometryN-201', ST_asEWKT(ST_geometryN(the_geom_2d, 2)) FROM public.multicurve;
SELECT 'geometryN-202', ST_asEWKT(ST_geometryN(the_geom_3dm, 2)) FROM public.multicurve;
SELECT 'geometryN-203', ST_asEWKT(ST_geometryN(the_geom_3dz, 2)) FROM public.multicurve;
SELECT 'geometryN-204', ST_asEWKT(ST_geometryN(the_geom_4d, 2)) FROM public.multicurve;

SELECT 'geometryN-301', (ST_asEWKT(ST_geometryN(the_geom_2d, 3)) is null) FROM public.multicurve;
SELECT 'geometryN-302', (ST_asEWKT(ST_geometryN(the_geom_3dm, 3)) is null) FROM public.multicurve;
SELECT 'geometryN-303', (ST_asEWKT(ST_geometryN(the_geom_3dz, 3)) is null) FROM public.multicurve;
SELECT 'geometryN-304', (ST_asEWKT(ST_geometryN(the_geom_4d, 3)) is null) FROM public.multicurve;

SELECT DropGeometryColumn('public', 'multicurve', 'the_geom_2d');
SELECT DropGeometryColumn('public', 'multicurve', 'the_geom_3dm');
SELECT DropGeometryColumn('public', 'multicurve', 'the_geom_3dz');
SELECT DropGeometryColumn('public', 'multicurve', 'the_geom_4d');
DROP TABLE public.multicurve;

