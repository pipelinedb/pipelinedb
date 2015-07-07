SELECT 'ndims01', ST_NDims(ST_Geomfromewkt('COMPOUNDCURVE(CIRCULARSTRING(
                0 0 0 0, 
                0.26794919243112270647255365849413 1 3 -2, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097 1 2),
                (0.5857864376269049511983112757903 1.4142135623730950488016887242097 1 2,
                2 0 0 0,
                0 0 0 0))'));
SELECT 'geometrytype01', geometrytype(ST_Geomfromewkt('COMPOUNDCURVE(CIRCULARSTRING(
                0 0 0 0, 
                0.26794919243112270647255365849413 1 3 -2, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097 1 2),
                (0.5857864376269049511983112757903 1.4142135623730950488016887242097 1 2,
                2 0 0 0,
                0 0 0 0))'));
SELECT 'ndims02', ST_NDims(ST_Geomfromewkt('COMPOUNDCURVE(CIRCULARSTRING(
                0 0 0, 
                0.26794919243112270647255365849413 1 3, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097 1),
                (0.5857864376269049511983112757903 1.4142135623730950488016887242097 1,
                2 0 0,
                0 0 0))'));
SELECT 'geometrytype02', geometrytype(ST_Geomfromewkt('COMPOUNDCURVE(CIRCULARSTRING(
                0 0 0, 
                0.26794919243112270647255365849413 1 3, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097 1),
                (0.5857864376269049511983112757903 1.4142135623730950488016887242097 1,
                2 0 0,
                0 0 0))'));
SELECT 'ndims03', ST_NDims(ST_Geomfromewkt('COMPOUNDCURVEM(CIRCULARSTRING(
                0 0 0, 
                0.26794919243112270647255365849413 1 -2, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097 2),
                (0.5857864376269049511983112757903 1.4142135623730950488016887242097 2,
                2 0 0,
                0 0 0))'));
SELECT 'geometrytype03', geometrytype(ST_Geomfromewkt('COMPOUNDCURVEM(CIRCULARSTRING(
                0 0 0, 
                0.26794919243112270647255365849413 1 -2, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097 2),
                (0.5857864376269049511983112757903 1.4142135623730950488016887242097 2,
                2 0 0,
                0 0 0))'));
SELECT 'ndims04', ST_NDims(ST_Geomfromewkt('COMPOUNDCURVE(CIRCULARSTRING(
                0 0, 
                0.26794919243112270647255365849413 1, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097),
                (0.5857864376269049511983112757903 1.4142135623730950488016887242097,
                2 0,
                0 0))'));
SELECT 'geometrytype04', geometrytype(ST_Geomfromewkt('COMPOUNDCURVE(CIRCULARSTRING(
                0 0, 
                0.26794919243112270647255365849413 1, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097),
                (0.5857864376269049511983112757903 1.4142135623730950488016887242097,
                2 0,
                0 0))'));

-- Repeat tests with new function names.
SELECT 'ndims01', ST_NDims(ST_geomfromewkt('COMPOUNDCURVE(CIRCULARSTRING(
                0 0 0 0, 
                0.26794919243112270647255365849413 1 3 -2, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097 1 2),
                (0.5857864376269049511983112757903 1.4142135623730950488016887242097 1 2,
                2 0 0 0,
                0 0 0 0))'));
SELECT 'geometrytype01', geometrytype(ST_geomfromewkt('COMPOUNDCURVE(CIRCULARSTRING(
                0 0 0 0, 
                0.26794919243112270647255365849413 1 3 -2, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097 1 2),
                (0.5857864376269049511983112757903 1.4142135623730950488016887242097 1 2,
                2 0 0 0,
                0 0 0 0))'));
SELECT 'ndims02', ST_NDims(ST_geomfromewkt('COMPOUNDCURVE(CIRCULARSTRING(
                0 0 0, 
                0.26794919243112270647255365849413 1 3, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097 1),
                (0.5857864376269049511983112757903 1.4142135623730950488016887242097 1,
                2 0 0,
                0 0 0))'));
SELECT 'geometrytype02', geometrytype(ST_geomfromewkt('COMPOUNDCURVE(CIRCULARSTRING(
                0 0 0, 
                0.26794919243112270647255365849413 1 3, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097 1),
                (0.5857864376269049511983112757903 1.4142135623730950488016887242097 1,
                2 0 0,
                0 0 0))'));
SELECT 'ndims03', ST_NDims(ST_geomfromewkt('COMPOUNDCURVEM(CIRCULARSTRING(
                0 0 0, 
                0.26794919243112270647255365849413 1 -2, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097 2),
                (0.5857864376269049511983112757903 1.4142135623730950488016887242097 2,
                2 0 0,
                0 0 0))'));
SELECT 'geometrytype03', geometrytype(ST_geomfromewkt('COMPOUNDCURVEM(CIRCULARSTRING(
                0 0 0, 
                0.26794919243112270647255365849413 1 -2, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097 2),
                (0.5857864376269049511983112757903 1.4142135623730950488016887242097 2,
                2 0 0,
                0 0 0))'));
SELECT 'ndims04', ST_NDims(ST_geomfromewkt('COMPOUNDCURVE(CIRCULARSTRING(
                0 0, 
                0.26794919243112270647255365849413 1, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097),
                (0.5857864376269049511983112757903 1.4142135623730950488016887242097,
                2 0,
                0 0))'));
SELECT 'geometrytype04', geometrytype(ST_geomfromewkt('COMPOUNDCURVE(CIRCULARSTRING(
                0 0, 
                0.26794919243112270647255365849413 1, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097),
                (0.5857864376269049511983112757903 1.4142135623730950488016887242097,
                2 0,
                0 0))'));

CREATE TABLE public.compoundcurve (id INTEGER, description VARCHAR,
the_geom_2d GEOMETRY(COMPOUNDCURVE),
the_geom_3dm GEOMETRY(COMPOUNDCURVEM),
the_geom_3dz GEOMETRY(COMPOUNDCURVEZ),
the_geom_4d GEOMETRY(COMPOUNDCURVEZM)
);

INSERT INTO public.compoundcurve (
                id,
                description
              ) VALUES (
                2,
                'compoundcurve');
UPDATE public.compoundcurve
                SET the_geom_4d = ST_Geomfromewkt('COMPOUNDCURVE(CIRCULARSTRING(
                        0 0 0 0, 
                        0.26794919243112270647255365849413 1 3 -2, 
                        0.5857864376269049511983112757903 1.4142135623730950488016887242097 1 2),
                        (0.5857864376269049511983112757903 1.4142135623730950488016887242097 1 2,
                        2 0 0 0,
                        0 0 0 0))');
UPDATE public.compoundcurve
                SET the_geom_3dz = ST_Geomfromewkt('COMPOUNDCURVE(CIRCULARSTRING(
                        0 0 0, 
                        0.26794919243112270647255365849413 1 3, 
                        0.5857864376269049511983112757903 1.4142135623730950488016887242097 1),
                        (0.5857864376269049511983112757903 1.4142135623730950488016887242097 1,
                        2 0 0,
                        0 0 0))');
UPDATE public.compoundcurve
                SET the_geom_3dm = ST_Geomfromewkt('COMPOUNDCURVEM(CIRCULARSTRING(
                        0 0 0, 
                        0.26794919243112270647255365849413 1 -2, 
                        0.5857864376269049511983112757903 1.4142135623730950488016887242097 2),
                        (0.5857864376269049511983112757903 1.4142135623730950488016887242097 2,
                        2 0 0,
                        0 0 0))');
UPDATE public.compoundcurve
                SET the_geom_2d = ST_Geomfromewkt('COMPOUNDCURVE(CIRCULARSTRING(
                        0 0, 
                        0.26794919243112270647255365849413 1, 
                        0.5857864376269049511983112757903 1.4142135623730950488016887242097),
                        (0.5857864376269049511983112757903 1.4142135623730950488016887242097,
                        2 0,
                        0 0))');

SELECT 'astext01', ST_Astext(the_geom_2d) FROM public.compoundcurve;
SELECT 'astext02', ST_Astext(the_geom_3dm) FROM public.compoundcurve;
SELECT 'astext03', ST_Astext(the_geom_3dz) FROM public.compoundcurve;
SELECT 'astext04', ST_Astext(the_geom_4d) FROM public.compoundcurve;

SELECT 'asewkt01', ST_Asewkt(the_geom_2d) FROM public.compoundcurve;
SELECT 'asewkt02', ST_Asewkt(the_geom_3dm) FROM public.compoundcurve;
SELECT 'asewkt03', ST_Asewkt(the_geom_3dz) FROM public.compoundcurve;
SELECT 'asewkt04', ST_Asewkt(the_geom_4d) FROM public.compoundcurve;

-- These tests will fail on different architectures
-- We need a way to handle multiple byte orderings
--SELECT 'asbinary01', encode(asbinary(the_geom_2d), 'hex') FROM public.compoundcurve;
--SELECT 'asbinary02', encode(asbinary(the_geom_3dm), 'hex') FROM public.compoundcurve;
--SELECT 'asbinary03', encode(asbinary(the_geom_3dz), 'hex') FROM public.compoundcurve;
--SELECT 'asbinary04', encode(asbinary(the_geom_4d), 'hex') FROM public.compoundcurve;
--
--SELECT 'asewkb01', encode(asewkb(the_geom_2d), 'hex') FROM public.compoundcurve;
--SELECT 'asewkb02', encode(asewkb(the_geom_3dm), 'hex') FROM public.compoundcurve;
--SELECT 'asewkb03', encode(asewkb(the_geom_3dz), 'hex') FROM public.compoundcurve;
--SELECT 'asewkb04', encode(asewkb(the_geom_4d), 'hex') FROM public.compoundcurve;

SELECT 'ST_CurveToLine-201', ST_Asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_2d, 2), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.compoundcurve;
SELECT 'ST_CurveToLine-202', ST_Asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_3dm, 2), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.compoundcurve;
SELECT 'ST_CurveToLine-203', ST_Asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_3dz, 2), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.compoundcurve;
SELECT 'ST_CurveToLine-204', ST_Asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_4d, 2), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.compoundcurve;

SELECT 'ST_CurveToLine-401', ST_Asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_2d, 4), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.compoundcurve;
SELECT 'ST_CurveToLine-402', ST_Asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_3dm, 4), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.compoundcurve;
SELECT 'ST_CurveToLine-403', ST_Asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_3dz, 4), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.compoundcurve;
SELECT 'ST_CurveToLine-404', ST_Asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_4d, 4), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.compoundcurve;

SELECT 'ST_CurveToLine01', ST_Asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_2d), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.compoundcurve;
SELECT 'ST_CurveToLine02', ST_Asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_3dm), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.compoundcurve;
SELECT 'ST_CurveToLine03', ST_Asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_3dz), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.compoundcurve;
SELECT 'ST_CurveToLine04', ST_Asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_4d), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.compoundcurve;

-- Removed due to discrepencies between hardware
--SELECT 'box2d01', box2d(the_geom_2d) FROM public.compoundcurve;
--SELECT 'box2d02', box2d(the_geom_3dm) FROM public.compoundcurve;
--SELECT 'box2d03', box2d(the_geom_3dz) FROM public.compoundcurve;
--SELECT 'box2d04', box2d(the_geom_4d) FROM public.compoundcurve;

--SELECT 'box3d01', box3d(the_geom_2d) FROM public.compoundcurve;
--SELECT 'box3d02', box3d(the_geom_3dm) FROM public.compoundcurve;
--SELECT 'box3d03', box3d(the_geom_3dz) FROM public.compoundcurve;
--SELECT 'box3d04', box3d(the_geom_4d) FROM public.compoundcurve;

-- SELECT 'isValid01', isValid(the_geom_2d) FROM public.compoundcurve;
-- SELECT 'isValid02', isValid(the_geom_3dm) FROM public.compoundcurve;
-- SELECT 'isValid03', isValid(the_geom_3dz) FROM public.compoundcurve;
-- SELECT 'isValid04', isValid(the_geom_4d) FROM public.compoundcurve;

-- SELECT 'dimension01', dimension(the_geom_2d) FROM public.compoundcurve;
-- SELECT 'dimension02', dimension(the_geom_3dm) FROM public.compoundcurve;
-- SELECT 'dimension03', dimension(the_geom_3dz) FROM public.compoundcurve;
-- SELECT 'dimension04', dimension(the_geom_4d) FROM public.compoundcurve;

-- SELECT 'SRID01', ST_SRID(the_geom_2d) FROM public.compoundcurve;
-- SELECT 'SRID02', ST_SRID(the_geom_3dm) FROM public.compoundcurve;
-- SELECT 'SRID03', ST_SRID(the_geom_3dz) FROM public.compoundcurve;
-- SELECT 'SRID04', ST_SRID(the_geom_4d) FROM public.compoundcurve;

-- SELECT 'accessor01', isEmpty(the_geom_2d), isSimple(the_geom_2d), isClosed(the_geom_2d), isRing(the_geom_2d) FROM public.compoundcurve;
-- SELECT 'accessor02', isEmpty(the_geom_3dm), isSimple(the_geom_3dm), isClosed(the_geom_3dm), isRing(the_geom_3dm) FROM public.compoundcurve;
-- SELECT 'accessor03', isEmpty(the_geom_3dz), isSimple(the_geom_3dz), isClosed(the_geom_3dz), isRing(the_geom_3dz) FROM public.compoundcurve;
-- SELECT 'accessor04', isEmpty(the_geom_4d), isSimple(the_geom_4d), isClosed(the_geom_4d), isRing(the_geom_4d) FROM public.compoundcurve;

-- SELECT 'envelope01', ST_AsText(ST_SnapToGrid(envelope(the_geom_2d), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.compoundcurve;
-- SELECT 'envelope02', ST_AsText(ST_SnapToGrid(envelope(the_geom_3dm), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.compoundcurve;
-- SELECT 'envelope03', ST_AsText(ST_SnapToGrid(envelope(the_geom_3dz), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.compoundcurve;
-- SELECT 'envelope04', ST_AsText(ST_SnapToGrid(envelope(the_geom_4d), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.compoundcurve;

-- TODO: ST_SnapToGrid is required to remove platform dependent precision
-- issues.  Until ST_SnapToGrid is updated to work against curves, these
-- tests cannot be run.
--SELECT 'ST_LineToCurve', ST_Asewkt(ST_LineToCurve(ST_CurveToLine(the_geom_2d))) FROM public.compoundcurve;
--SELECT 'ST_LineToCurve', ST_Asewkt(ST_LineToCurve(ST_CurveToLine(the_geom_3dm))) FROM public.compoundcurve;
--SELECT 'ST_LineToCurve', ST_Asewkt(ST_LineToCurve(ST_CurveToLine(the_geom_3dz))) FROM public.compoundcurve;
--SELECT 'ST_LineToCurve', ST_Asewkt(ST_LineToCurve(ST_CurveToLine(the_geom_4d))) FROM public.compoundcurve;

-- Repeat tests on new function names.
SELECT 'astext01', ST_astext(the_geom_2d) FROM public.compoundcurve;
SELECT 'astext02', ST_astext(the_geom_3dm) FROM public.compoundcurve;
SELECT 'astext03', ST_astext(the_geom_3dz) FROM public.compoundcurve;
SELECT 'astext04', ST_astext(the_geom_4d) FROM public.compoundcurve;

SELECT 'asewkt01', ST_asewkt(the_geom_2d) FROM public.compoundcurve;
SELECT 'asewkt02', ST_asewkt(the_geom_3dm) FROM public.compoundcurve;
SELECT 'asewkt03', ST_asewkt(the_geom_3dz) FROM public.compoundcurve;
SELECT 'asewkt04', ST_asewkt(the_geom_4d) FROM public.compoundcurve;

--SELECT 'asbinary01', encode(ST_asbinary(the_geom_2d), 'hex') FROM public.compoundcurve;
--SELECT 'asbinary02', encode(ST_asbinary(the_geom_3dm), 'hex') FROM public.compoundcurve;
--SELECT 'asbinary03', encode(ST_asbinary(the_geom_3dz), 'hex') FROM public.compoundcurve;
--SELECT 'asbinary04', encode(ST_asbinary(the_geom_4d), 'hex') FROM public.compoundcurve;
--
--SELECT 'asewkb01', encode(ST_asewkb(the_geom_2d), 'hex') FROM public.compoundcurve;
--SELECT 'asewkb02', encode(ST_asewkb(the_geom_3dm), 'hex') FROM public.compoundcurve;
--SELECT 'asewkb03', encode(ST_asewkb(the_geom_3dz), 'hex') FROM public.compoundcurve;
--SELECT 'asewkb04', encode(ST_asewkb(the_geom_4d), 'hex') FROM public.compoundcurve;

-- Removed due to discrepencies between hardware
--SELECT 'box2d01', ST_box2d(the_geom_2d) FROM public.compoundcurve;
--SELECT 'box2d02', ST_box2d(the_geom_3dm) FROM public.compoundcurve;
--SELECT 'box2d03', ST_box2d(the_geom_3dz) FROM public.compoundcurve;
--SELECT 'box2d04', ST_box2d(the_geom_4d) FROM public.compoundcurve;

--SELECT 'box3d01', ST_box3d(the_geom_2d) FROM public.compoundcurve;
--SELECT 'box3d02', ST_box3d(the_geom_3dm) FROM public.compoundcurve;
--SELECT 'box3d03', ST_box3d(the_geom_3dz) FROM public.compoundcurve;
--SELECT 'box3d04', ST_box3d(the_geom_4d) FROM public.compoundcurve;

SELECT 'isValid01', ST_isValid(the_geom_2d) FROM public.compoundcurve;
SELECT 'isValid02', ST_isValid(the_geom_3dm) FROM public.compoundcurve;
SELECT 'isValid03', ST_isValid(the_geom_3dz) FROM public.compoundcurve;
SELECT 'isValid04', ST_isValid(the_geom_4d) FROM public.compoundcurve;

SELECT 'dimension01', ST_dimension(the_geom_2d) FROM public.compoundcurve;
SELECT 'dimension02', ST_dimension(the_geom_3dm) FROM public.compoundcurve;
SELECT 'dimension03', ST_dimension(the_geom_3dz) FROM public.compoundcurve;
SELECT 'dimension04', ST_dimension(the_geom_4d) FROM public.compoundcurve;

SELECT 'SRID01', ST_SRID(the_geom_2d) FROM public.compoundcurve;
SELECT 'SRID02', ST_SRID(the_geom_3dm) FROM public.compoundcurve;
SELECT 'SRID03', ST_SRID(the_geom_3dz) FROM public.compoundcurve;
SELECT 'SRID04', ST_SRID(the_geom_4d) FROM public.compoundcurve;

SELECT 'accessor01', ST_isEmpty(the_geom_2d), ST_isSimple(the_geom_2d), ST_isClosed(the_geom_2d), ST_isRing(the_geom_2d) FROM public.compoundcurve;
SELECT 'accessor02', ST_isEmpty(the_geom_3dm), ST_isSimple(the_geom_3dm), ST_isClosed(the_geom_3dm), ST_isRing(the_geom_3dm) FROM public.compoundcurve;
SELECT 'accessor03', ST_isEmpty(the_geom_3dz), ST_isSimple(the_geom_3dz), ST_isClosed(the_geom_3dz), ST_isRing(the_geom_3dz) FROM public.compoundcurve;
SELECT 'accessor04', ST_isEmpty(the_geom_4d), ST_isSimple(the_geom_4d), ST_isClosed(the_geom_4d), ST_isRing(the_geom_4d) FROM public.compoundcurve;

SELECT 'envelope01', ST_asText(ST_snapToGrid(ST_envelope(the_geom_2d), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.compoundcurve;
SELECT 'envelope02', ST_asText(ST_snapToGrid(ST_envelope(the_geom_3dm), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.compoundcurve;
SELECT 'envelope03', ST_asText(ST_snapToGrid(ST_envelope(the_geom_3dz), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.compoundcurve;
SELECT 'envelope04', ST_asText(ST_snapToGrid(ST_envelope(the_geom_4d), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.compoundcurve;

SELECT DropGeometryColumn('public', 'compoundcurve', 'the_geom_4d');
SELECT DropGeometryColumn('public', 'compoundcurve', 'the_geom_3dz');
SELECT DropGeometryColumn('public', 'compoundcurve', 'the_geom_3dm');
SELECT DropGeometryColumn('public', 'compoundcurve', 'the_geom_2d');
DROP TABLE public.compoundcurve;

SELECT 'valid wkt compound curve 1', encode(ST_AsBinary(ST_GeomFromEWKT('COMPOUNDCURVE((153.72942375 -27.21757040, 152.29285719 -29.23940482, 154.74034096 -30.51635287),(154.74034096 -30.51635287, 152.39926953 -32.16574411, 155.11278414 -34.08116619, 151.86720784 -35.62414508))'),'ndr'),'hex');
SELECT 'valid wkt compound curve 2', encode(ST_AsBinary(ST_GeomFromEWKT('COMPOUNDCURVE((153.72942375 -27.21757040, 152.29285719 -29.23940482, 154.74034096 -30.51635287, 152.39926953 -32.16574411, 155.11278414 -34.08116619, 151.86720784 -35.62414508))'),'ndr'),'hex');
SELECT 'valid wkt compound curve 3', encode(ST_AsBinary(ST_GeomFromEWKT('COMPOUNDCURVE((151.60117699 -27.32398274, 151.22873381 -35.94338210, 150.74987829 -27.80283826))'),'ndr'),'hex');
SELECT 'valid wkt compound curve 4', encode(ST_AsBinary(ST_GeomFromEWKT('COMPOUNDCURVE((153.72942375 -27.21757040, 152.29285719 -29.23940482, 154.74034096 -30.51635287),CIRCULARSTRING(154.74034096 -30.51635287, 154.74034096 -30.51635287, 152.39926953 -32.16574411, 155.11278414 -34.08116619, 151.86720784 -35.62414508))'),'ndr'),'hex');
SELECT 'valid wkt compound curve 5', encode(ST_AsBinary(ST_GeomFromEWKT('COMPOUNDCURVE(CIRCULARSTRING(157.87950492 -27.59001358, 156.01728901 -28.28169378, 155.59163966 -26.52589021),(155.59163966 -26.52589021, 153.72942375 -27.21757040, 152.29285719 -29.23940482, 154.74034096 -30.51635287),CIRCULARSTRING(154.74034096 -30.51635287, 154.74034096 -30.51635287, 152.39926953 -32.16574411, 155.11278414 -34.08116619, 151.86720784 -35.62414508))'),'ndr'),'hex');
SELECT 'invalid wkt compound curve 1', ST_GeomFromEWKT('COMPOUNDCURVE((153.72942375 -27.21757040, 152.29285719 -29.23940482, 154.74034096 -30.51635287),(152.39926953 -32.16574411, 155.11278414 -34.08116619, 151.86720784 -35.62414508))');
SELECT 'invalid wkt compound curve 2', ST_GeomFromEWKT('COMPOUNDCURVE((153.72942375 -27.21757040, 152.29285719 -29.23940482),CIRCULARSTRING(154.74034096 -30.51635287, 154.74034096 -30.51635287, 152.39926953 -32.16574411, 155.11278414 -34.08116619, 151.86720784 -35.62414508))');
SELECT 'invalid wkt compound curve 3', ST_GeomFromEWKT('COMPOUNDCURVE(CIRCULARSTRING(157.87950492 -27.59001358, 156.01728901 -28.28169378, 155.59163966 -26.52589021, 153.72942375 -27.21757040),(153.72942375 -27.21757040, 152.29285719 -29.23940482),CIRCULARSTRING(154.74034096 -30.51635287, 154.74034096 -30.51635287, 152.39926953 -32.16574411, 155.11278414 -34.08116619, 151.86720784 -35.62414508))');
SELECT 'valid wkb compound curve 1', ST_asEWKT(ST_GeomFromEWKB(decode('0109000000020000000102000000030000009FE5797057376340E09398B1B2373BC05AAE0A165F0963409F6760A2493D3DC0DB6286DFB057634082D8A1B32F843EC0010200000004000000DB6286DFB057634082D8A1B32F843EC075B4E4D0C60C634031FA5D1A371540C0D7197CED9B636340A3CB59A7630A41C050F4A72AC0FB6240974769FCE3CF41C0', 'hex')));
SELECT 'valid wkb compound curve 2', ST_asEWKT(ST_GeomFromEWKB(decode('0109000000010000000102000000060000009FE5797057376340E09398B1B2373BC05AAE0A165F0963409F6760A2493D3DC0DB6286DFB057634082D8A1B32F843EC075B4E4D0C60C634031FA5D1A371540C0D7197CED9B636340A3CB59A7630A41C050F4A72AC0FB6240974769FCE3CF41C0', 'hex')));
SELECT 'valid wkb compound curve 3', ST_asEWKT(ST_GeomFromEWKB(decode('0109000000010000000102000000030000000CE586D73CF36240BBC46888F0523BC0102E91C951E76240DF90A1BEC0F841C0F970C100FFD7624074ADE6CE86CD3BC0', 'hex')));
SELECT 'valid wkb compound curve 4', ST_asEWKT(ST_GeomFromEWKB(decode('0109000000020000000102000000030000009FE5797057376340E09398B1B2373BC05AAE0A165F0963409F6760A2493D3DC0DB6286DFB057634082D8A1B32F843EC0010800000005000000DB6286DFB057634082D8A1B32F843EC0DB6286DFB057634082D8A1B32F843EC075B4E4D0C60C634031FA5D1A371540C0D7197CED9B636340A3CB59A7630A41C050F4A72AC0FB6240974769FCE3CF41C0', 'hex')));
SELECT 'valid wkb compound curve 5', ST_asEWKT(ST_GeomFromEWKB(decode('010900000003000000010800000003000000468280E724BC6340BF4B46210B973BC0F890AEA18D8063402D9664151D483CC0EED64BB6EE726340903CA5BDA0863AC0010200000004000000EED64BB6EE726340903CA5BDA0863AC09FE5797057376340E09398B1B2373BC05AAE0A165F0963409F6760A2493D3DC0DB6286DFB057634082D8A1B32F843EC0010800000005000000DB6286DFB057634082D8A1B32F843EC0DB6286DFB057634082D8A1B32F843EC075B4E4D0C60C634031FA5D1A371540C0D7197CED9B636340A3CB59A7630A41C050F4A72AC0FB6240974769FCE3CF41C0', 'hex')));
SELECT 'null response', ST_NumPoints(ST_GeomFromEWKT('COMPOUNDCURVE(CIRCULARSTRING(0 0,2 0, 2 1, 2 3, 4 3),(4 3, 4 5, 1 4, 0 0))'));
SELECT 'minpoints issues - pass', encode(ST_AsBinary(ST_GeomFromText('COMPOUNDCURVE((0 0,1 1))'),'ndr'),'hex');
SELECT 'minpoints issues - pass', encode(ST_AsBinary(ST_GeomFromText('COMPOUNDCURVE(CIRCULARSTRING(0 0,0 1,1 1))'),'ndr'),'hex');
SELECT 'minpoints issues - fail', ST_GeomFromText('COMPOUNDCURVE(CIRCULARSTRING(0 0,1 1))');
SELECT 'minpoints issues - fail', ST_GeomFromText('COMPOUNDCURVE(CIRCULARSTRING(0 0))');
SELECT 'minpoints issues - fail', ST_GeomFromText('COMPOUNDCURVE((0 0),(0 0,1 1))');
