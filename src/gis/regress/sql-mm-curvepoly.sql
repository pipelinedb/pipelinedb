-- Repeat tests with new function names.
SELECT 'ndims01', ST_ndims(ST_geomfromewkt('CURVEPOLYGON(CIRCULARSTRING(
                -2 0 0 0,
                -1 -1 1 2,
                0 0 2 4,
                1 -1 3 6,
                2 0 4 8,
                0 2 2 4,
                -2 0 0 0),
                (-1 0 1 2,
                0 0.5 2 4,
                1 0 3 6,
                0 1 3 4,
                -1 0 1 2))'));
SELECT 'geometrytype01', geometrytype(ST_geomfromewkt('CURVEPOLYGON(CIRCULARSTRING(
                -2 0 0 0,
                -1 -1 1 2,
                0 0 2 4,
                1 -1 3 6,
                2 0 4 8,
                0 2 2 4,
                -2 0 0 0),
                (-1 0 1 2,
                0 0.5 2 4,
                1 0 3 6,
                0 1 3 4,
                -1 0 1 2))'));
SELECT 'ndims02', ST_ndims(ST_geomfromewkt('CURVEPOLYGON(CIRCULARSTRING(
                -2 0 0,
                -1 -1 1,
                0 0 2,
                1 -1 3,
                2 0 4,
                0 2 2,
                -2 0 0),
                (-1 0 1,
                0 0.5 2,
                1 0 3,
                0 1 3,
                -1 0 1))'));
SELECT 'geometrytype02', geometrytype(ST_geomfromewkt('CURVEPOLYGON(CIRCULARSTRING(
                -2 0 0,
                -1 -1 1,
                0 0 2,
                1 -1 3,
                2 0 4,
                0 2 2,
                -2 0 0),
                (-1 0 1,
                0 0.5 2,
                1 0 3,
                0 1 3,
                -1 0 1))'));
SELECT 'ndims03', ST_ndims(ST_geomfromewkt('CURVEPOLYGONM(CIRCULARSTRING(
                -2 0 0,
                -1 -1 2,
                0 0 4,
                1 -1 6,
                2 0 8,
                0 2 4,
                -2 0 0),
                (-1 0 2,
                0 0.5 4,
                1 0 6,
                0 1 4,
                -1 0 2))'));
SELECT 'geometrytype03', geometrytype(ST_geomfromewkt('CURVEPOLYGONM(CIRCULARSTRING(
                -2 0 0,
                -1 -1 2,
                0 0 4,
                1 -1 6,
                2 0 8,
                0 2 4,
                -2 0 0),
                (-1 0 2,
                0 0.5 4,
                1 0 6,
                0 1 4,
                -1 0 2))'));
SELECT 'ndims04', ST_ndims(ST_geomfromewkt('CURVEPOLYGON(CIRCULARSTRING(
                -2 0,
                -1 -1,
                0 0,
                1 -1,
                2 0,
                0 2,
                -2 0),
                (-1 0,
                0 0.5,
                1 0,
                0 1,
                -1 0))'));
SELECT 'geometrytype04', geometrytype(ST_geomfromewkt('CURVEPOLYGON(CIRCULARSTRING(
                -2 0,
                -1 -1,
                0 0,
                1 -1,
                2 0,
                0 2,
                -2 0),
                (-1 0,
                0 0.5,
                1 0,
                0 1,
                -1 0))'));
                
SELECT 'ndims05', ST_Ndims(ST_geomfromewkt('CURVEPOLYGON(
                COMPOUNDCURVE(
                    (5 5 1 0,5 0 1 1,0 0 1 2,0 5 1 3),
                    CIRCULARSTRING(0 5 1 3,1.5 7.5 1 4,5 5 1 0)),
                (1.5 5 2 0,2.5 6 3 1,3.5 5 2 2,1.5 5 2 0),
                COMPOUNDCURVE(
                    CIRCULARSTRING(1.5 2 2 0,1 2.5 3 1,3.5 2 2 2),
                    (3.5 2 2 2,3.5 4 1 3,1.5 4 1 4,1.5 2 2 0)))'));

CREATE TABLE public.curvepolygon (id INTEGER, description VARCHAR,
the_geom_2d GEOMETRY(CURVEPOLYGON),
the_geom_3dm GEOMETRY(CURVEPOLYGONM),
the_geom_3dz GEOMETRY(CURVEPOLYGONZ),
the_geom_4d GEOMETRY(CURVEPOLYGONZM));

INSERT INTO public.curvepolygon (
                id,
                description
              ) VALUES (
                1, 'curvepolygon');
UPDATE public.curvepolygon
        SET the_geom_4d = ST_Geomfromewkt('CURVEPOLYGON(CIRCULARSTRING(
                -2 0 0 0,
                -1 -1 1 2,
                0 0 2 4,
                1 -1 3 6,
                2 0 4 8,
                0 2 2 4,
                -2 0 0 0),
                (-1 0 1 2,
                0 0.5 2 4,
                1 0 3 6,
                0 1 3 4,
                -1 0 1 2))');
UPDATE public.curvepolygon
        SET the_geom_3dz = ST_Geomfromewkt('CURVEPOLYGON(CIRCULARSTRING(
                -2 0 0,
                -1 -1 1,
                0 0 2,
                1 -1 3,
                2 0 4,
                0 2 2,
                -2 0 0),
                (-1 0 1,
                0 0.5 2,
                1 0 3,
                0 1 3,
                -1 0 1))');
UPDATE public.curvepolygon
        SET the_geom_3dm = ST_Geomfromewkt('CURVEPOLYGONM(CIRCULARSTRING(
                -2 0 0,
                -1 -1 2,
                0 0 4,
                1 -1 6,
                2 0 8,
                0 2 4,
                -2 0 0),
                (-1 0 2,
                0 0.5 4,
                1 0 6,
                0 1 4,
                -1 0 2))');
UPDATE public.curvepolygon
        SET the_geom_2d = ST_Geomfromewkt('CURVEPOLYGON(CIRCULARSTRING(
                -2 0,
                -1 -1,
                0 0,
                1 -1,
                2 0,
                0 2,
                -2 0),
                (-1 0,
                0 0.5,
                1 0,
                0 1,
                -1 0))');

-- These tests will fail on different architectures
-- We need a way to handle multiple byte orderings
--SELECT 'asbinary01', encode(asbinary(the_geom_2d), 'hex') FROM public.curvepolygon;
--SELECT 'asbinary02', encode(asbinary(the_geom_3dm), 'hex') FROM public.curvepolygon;
--SELECT 'asbinary03', encode(asbinary(the_geom_3dz), 'hex') FROM public.curvepolygon;
--SELECT 'asbinary04', encode(asbinary(the_geom_4d), 'hex') FROM public.curvepolygon;
--
--SELECT 'asewkb01', encode(asewkb(the_geom_2d), 'hex') FROM public.curvepolygon;
--SELECT 'asewkb02', encode(asewkb(the_geom_3dm), 'hex') FROM public.curvepolygon;
--SELECT 'asewkb03', encode(asewkb(the_geom_3dz), 'hex') FROM public.curvepolygon;
--SELECT 'asewkb04', encode(asewkb(the_geom_4d), 'hex') FROM public.curvepolygon;

SELECT 'ST_CurveToLine-201',ST_asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_2d, 2), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.curvepolygon;
SELECT 'ST_CurveToLine-202',ST_asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_3dm, 2), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.curvepolygon;
SELECT 'ST_CurveToLine-203',ST_asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_3dz, 2), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.curvepolygon;
SELECT 'ST_CurveToLine-204',ST_asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_4d, 2), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.curvepolygon;

SELECT 'ST_CurveToLine-401',ST_asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_2d, 4), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.curvepolygon;
SELECT 'ST_CurveToLine-402',ST_asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_3dm, 4), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.curvepolygon;
SELECT 'ST_CurveToLine-403',ST_asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_3dz, 4), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.curvepolygon;
SELECT 'ST_CurveToLine-404',ST_asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_4d, 4), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.curvepolygon;

SELECT 'ST_CurveToLine01',ST_asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_2d), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.curvepolygon;
SELECT 'ST_CurveToLine02',ST_asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_3dm), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.curvepolygon;
SELECT 'ST_CurveToLine03',ST_asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_3dz), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.curvepolygon;
SELECT 'ST_CurveToLine04',ST_asewkt(ST_SnapToGrid(ST_CurveToLine(the_geom_4d), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.curvepolygon;

-- Removed due to descrepencies between hardware
--SELECT 'box2d01', box2d(the_geom_2d) FROM public.curvepolygon;
--SELECT 'box2d02', box2d(the_geom_3dm) FROM public.curvepolygon;
--SELECT 'box2d03', box2d(the_geom_3dz) FROM public.curvepolygon;
--SELECT 'box2d04', box2d(the_geom_4d) FROM public.curvepolygon;

--SELECT 'box3d01', box3d(the_geom_2d) FROM public.curvepolygon;
--SELECT 'box3d02', box3d(the_geom_3dm) FROM public.curvepolygon;
--SELECT 'box3d03', box3d(the_geom_3dz) FROM public.curvepolygon;
--SELECT 'box3d04', box3d(the_geom_4d) FROM public.curvepolygon;

-- TODO: ST_SnapToGrid is required to remove platform dependent precision
-- issues.  Until ST_SnapToGrid is updated to work against curves, these
-- tests cannot be run.
--SELECT 'ST_LineToCurve01',ST_asewkt(ST_LineToCurve(ST_CurveToLine(the_geom_2d))) FROM public.curvepolygon;
--SELECT 'ST_LineToCurve02',ST_asewkt(ST_LineToCurve(ST_CurveToLine(the_geom_3dm))) FROM public.curvepolygon;
--SELECT 'ST_LineToCurve03',ST_asewkt(ST_LineToCurve(ST_CurveToLine(the_geom_3dz))) FROM public.curvepolygon;
--SELECT 'ST_LineToCurve04',ST_asewkt(ST_LineToCurve(ST_CurveToLine(the_geom_4d))) FROM public.curvepolygon;

-- Repeat tests with new function names.
SELECT 'astext01', ST_astext(the_geom_2d) FROM public.curvepolygon;
SELECT 'astext02', ST_astext(the_geom_3dm) FROM public.curvepolygon;
SELECT 'astext03', ST_astext(the_geom_3dz) FROM public.curvepolygon;
SELECT 'astext04', ST_astext(the_geom_4d) FROM public.curvepolygon;

SELECT 'asewkt01', ST_asewkt(the_geom_2d) FROM public.curvepolygon;
SELECT 'asewkt02', ST_asewkt(the_geom_3dm) FROM public.curvepolygon;
SELECT 'asewkt03', ST_asewkt(the_geom_3dz) FROM public.curvepolygon;
SELECT 'asewkt04', ST_asewkt(the_geom_4d) FROM public.curvepolygon;

-- These tests will fail on different architectures
-- We need a way to handle multiple byte orderings
--SELECT 'asbinary01', encode(ST_asbinary(the_geom_2d), 'hex') FROM public.curvepolygon;
--SELECT 'asbinary02', encode(ST_asbinary(the_geom_3dm), 'hex') FROM public.curvepolygon;
--SELECT 'asbinary03', encode(ST_asbinary(the_geom_3dz), 'hex') FROM public.curvepolygon;
--SELECT 'asbinary04', encode(ST_asbinary(the_geom_4d), 'hex') FROM public.curvepolygon;
--
--SELECT 'asewkb01', encode(ST_asewkb(the_geom_2d), 'hex') FROM public.curvepolygon;
--SELECT 'asewkb02', encode(ST_asewkb(the_geom_3dm), 'hex') FROM public.curvepolygon;
--SELECT 'asewkb03', encode(ST_asewkb(the_geom_3dz), 'hex') FROM public.curvepolygon;
--SELECT 'asewkb04', encode(ST_asewkb(the_geom_4d), 'hex') FROM public.curvepolygon;

-- Removed due to descrepencies between hardware
--SELECT 'box2d01', ST_box2d(the_geom_2d) FROM public.curvepolygon;
--SELECT 'box2d02', ST_box2d(the_geom_3dm) FROM public.curvepolygon;
--SELECT 'box2d03', ST_box2d(the_geom_3dz) FROM public.curvepolygon;
--SELECT 'box2d04', ST_box2d(the_geom_4d) FROM public.curvepolygon;

--SELECT 'box3d01', ST_box3d(the_geom_2d) FROM public.curvepolygon;
--SELECT 'box3d02', ST_box3d(the_geom_3dm) FROM public.curvepolygon;
--SELECT 'box3d03', ST_box3d(the_geom_3dz) FROM public.curvepolygon;
--SELECT 'box3d04', ST_box3d(the_geom_4d) FROM public.curvepolygon;

SELECT 'isValid01', ST_isValid(the_geom_2d) FROM public.curvepolygon;
SELECT 'isValid02', ST_isValid(the_geom_3dm) FROM public.curvepolygon;
SELECT 'isValid03', ST_isValid(the_geom_3dz) FROM public.curvepolygon;
SELECT 'isValid04', ST_isValid(the_geom_4d) FROM public.curvepolygon;

SELECT 'dimension01', ST_dimension(the_geom_2d) FROM public.curvepolygon;
SELECT 'dimension02', ST_dimension(the_geom_3dm) FROM public.curvepolygon;
SELECT 'dimension03', ST_dimension(the_geom_3dz) FROM public.curvepolygon;
SELECT 'dimension04', ST_dimension(the_geom_4d) FROM public.curvepolygon;

SELECT 'SRID01', ST_SRID(the_geom_2d) FROM public.curvepolygon;
SELECT 'SRID02', ST_SRID(the_geom_3dm) FROM public.curvepolygon;
SELECT 'SRID03', ST_SRID(the_geom_3dz) FROM public.curvepolygon;
SELECT 'SRID04', ST_SRID(the_geom_4d) FROM public.curvepolygon;

SELECT 'envelope01', ST_asText(ST_snapToGrid(ST_envelope(the_geom_2d), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.curvepolygon;
SELECT 'envelope02', ST_asText(ST_snapToGrid(ST_envelope(the_geom_3dm), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.curvepolygon;
SELECT 'envelope03', ST_asText(ST_snapToGrid(ST_envelope(the_geom_3dz), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.curvepolygon;
SELECT 'envelope04', ST_asText(ST_snapToGrid(ST_envelope(the_geom_4d), 'POINT(0 0 0 0)'::geometry, 1e-8, 1e-8, 1e-8, 1e-8)) FROM public.curvepolygon;

SELECT 'startPoint01', (ST_startPoint(the_geom_2d) is null) FROM public.curvepolygon;
SELECT 'startPoint02', (ST_startPoint(the_geom_3dm) is null) FROM public.curvepolygon;
SELECT 'startPoint03', (ST_startPoint(the_geom_3dz) is null) FROM public.curvepolygon;
SELECT 'startPoint04', (ST_startPoint(the_geom_4d) is null) FROM public.curvepolygon;

SELECT 'endPoint01', (ST_endPoint(the_geom_2d) is null) FROM public.curvepolygon;
SELECT 'endPoint02', (ST_endPoint(the_geom_3dm) is null) FROM public.curvepolygon;
SELECT 'endPoint03', (ST_endPoint(the_geom_3dz) is null) FROM public.curvepolygon;
SELECT 'endPoint04', (ST_endPoint(the_geom_4d) is null) FROM public.curvepolygon;

SELECT 'exteriorRing01', ST_asEWKT(ST_exteriorRing(the_geom_2d)) FROM public.curvepolygon;
SELECT 'exteriorRing02', ST_asEWKT(ST_exteriorRing(the_geom_3dm)) FROM public.curvepolygon;
SELECT 'exteriorRing03', ST_asEWKT(ST_exteriorRing(the_geom_3dz)) FROM public.curvepolygon;
SELECT 'exteriorRing04', ST_asEWKT(ST_exteriorRing(the_geom_4d)) FROM public.curvepolygon;

SELECT 'numInteriorRings01', ST_numInteriorRings(the_geom_2d) FROM public.curvepolygon;
SELECT 'numInteriorRings02', ST_numInteriorRings(the_geom_3dm) FROM public.curvepolygon;
SELECT 'numInteriorRings03', ST_numInteriorRings(the_geom_3dz) FROM public.curvepolygon;
SELECT 'numInteriorRings04', ST_numInteriorRings(the_geom_4d) FROM public.curvepolygon;

SELECT 'interiorRingN-101', ST_asEWKT(ST_interiorRingN(the_geom_2d, 1)) FROM public.curvepolygon;
SELECT 'interiorRingN-102', ST_asEWKT(ST_interiorRingN(the_geom_3dm, 1)) FROM public.curvepolygon;
SELECT 'interiorRingN-103', ST_asEWKT(ST_interiorRingN(the_geom_3dz, 1)) FROM public.curvepolygon;
SELECT 'interiorRingN-104', ST_asEWKT(ST_interiorRingN(the_geom_4d, 1)) FROM public.curvepolygon;

SELECT 'interiorRingN-201', ST_asEWKT(ST_interiorRingN(the_geom_2d, 2)) FROM public.curvepolygon;
SELECT 'interiorRingN-202', ST_asEWKT(ST_interiorRingN(the_geom_3dm, 2)) FROM public.curvepolygon;
SELECT 'interiorRingN-203', ST_asEWKT(ST_interiorRingN(the_geom_3dz, 2)) FROM public.curvepolygon;
SELECT 'interiorRingN-204', ST_asEWKT(ST_interiorRingN(the_geom_4d, 2)) FROM public.curvepolygon;

SELECT DropGeometryColumn('public', 'curvepolygon', 'the_geom_2d');
SELECT DropGeometryColumn('public', 'curvepolygon', 'the_geom_3dm');
SELECT DropGeometryColumn('public', 'curvepolygon', 'the_geom_3dz');
SELECT DropGeometryColumn('public', 'curvepolygon', 'the_geom_4d');
DROP TABLE public.curvepolygon;

SELECT 'valid wkt curve polygon 1', encode(ST_AsBinary(ST_GeomFromEWKT('CURVEPOLYGON((143.62025166838282 -30.037497356076827, 142.92857147299705 -32.75101196874403, 145.96132309891922 -34.985671061528784, 149.57565307617188 -33.41153335571289, 149.41972407584802 -29.824672680573517, 146.1209416055467 -30.19711586270431, 143.62025166838282 -30.037497356076827))'),'ndr'),'hex');
SELECT 'valid wkt curve polygon 2', encode(ST_AsBinary(ST_GeomFromEWKT('CURVEPOLYGON((143.62025166838282 -30.037497356076827, 142.92857147299705 -32.75101196874403, 145.96132309891922 -34.985671061528784, 149.57565307617188 -33.41153335571289, 149.41972407584802 -29.824672680573517, 146.1209416055467 -30.19711586270431, 143.62025166838282 -30.037497356076827),(144.84399355252685 -31.26123924022086, 144.20551952601693 -32.27215644886158, 145.55230712890625 -33.49203872680664, 147.97080993652344 -32.03618621826172, 146.38697244992585 -31.47406391572417, 144.84399355252685 -31.26123924022086))'),'ndr'),'hex');
SELECT 'valid wkt curve polygon 3', encode(ST_AsBinary(ST_GeomFromEWKT('CURVEPOLYGON(CIRCULARSTRING(143.62025166838282 -30.037497356076827, 142.92857147299705 -32.75101196874403, 145.96132309891922 -34.985671061528784, 149.57565307617188 -33.41153335571289, 149.41972407584802 -29.824672680573517, 146.1209416055467 -30.19711586270431, 143.62025166838282 -30.037497356076827))'),'ndr'),'hex');
SELECT 'valid wkt curve polygon 4',
  encode(ST_AsBinary(ST_GeomFromEWKT('CURVEPOLYGON(CIRCULARSTRING(143.62025166838282 -30.037497356076827, 142.92857147299705 -32.75101196874403, 145.96132309891922 -34.985671061528784, 149.57565307617188 -33.41153335571289, 149.41972407584802 -29.824672680573517, 146.1209416055467 -30.19711586270431, 143.62025166838282 -30.037497356076827),(144.84399355252685 -31.26123924022086, 144.20551952601693 -32.27215644886158, 145.55230712890625 -33.49203872680664, 147.97080993652344 -32.03618621826172, 146.38697244992585 -31.47406391572417, 144.84399355252685 -31.26123924022086))'),'ndr'),'hex');
SELECT 'valid wkt curve polygon 5', encode(ST_AsBinary(ST_GeomFromEWKT('CURVEPOLYGON((143.62025166838282 -30.037497356076827, 142.92857147299705 -32.75101196874403, 145.96132309891922 -34.985671061528784, 149.57565307617188 -33.41153335571289, 149.41972407584802 -29.824672680573517, 146.1209416055467 -30.19711586270431, 143.62025166838282 -30.037497356076827),COMPOUNDCURVE(CIRCULARSTRING(144.84399355252685 -31.26123924022086, 144.20551952601693 -32.27215644886158, 145.55230712890625 -33.49203872680664), (145.55230712890625 -33.49203872680664, 147.97080993652344 -32.03618621826172),CIRCULARSTRING(147.97080993652344 -32.03618621826172, 146.38697244992585 -31.47406391572417, 144.84399355252685 -31.26123924022086)))'),'ndr'),'hex');
SELECT 'invalid wkt curve polygon 4', ST_GeomFromEWKT('CURVEPOLYGON((143.62025166838282 -30.037497356076827, 142.92857147299705 -32.75101196874403, 145.96132309891922 -34.985671061528784, 149.57565307617188 -33.41153335571289, 149.41972407584802 -29.824672680573517, 146.1209416055467 -30.19711586270431, 143.62025166838282 -30.037497356076))');
SELECT 'invalid wkt curve polygon 5', ST_GeomFromEWKT('CURVEPOLYGON((143.62025166838282 -30.037497356076827, 142.92857147299705 -32.75101196874403, 145.96132309891922 -34.985671061528784, 149.57565307617188 -33.41153335571289, 149.41972407584802 -29.824672680573517, 146.1209416055467 -30.19711586270431, 143.62025166838282 -30.037497356076827),(144.84399355252685 -31.26123924022086, 144.20551952601693 -32.27215644886158, 145.55230712890625 -33.49203872680664, 147.97080993652344 -32.03618621826172, 146.38697244992585 -31.47406391572417))');
SELECT 'invalid wkt curve polygon 6', ST_GeomFromEWKT('CURVEPOLYGON((143.62025166838282 -30.037497356076827, 142.92857147299705 -32.75101196874403, 145.96132309891922 -34.985671061528784, 149.57565307617188 -33.41153335571289, 149.41972407584802 -29.824672680573517, 146.1209416055467 -30.19711586270431),(144.84399355252685 -31.26123924022086, 144.20551952601693 -32.27215644886158, 145.55230712890625 -33.49203872680664, 147.97080993652344 -32.03618621826172, 146.38697244992585 -31.47406391572417, 144.84399355252685 -31.26123924022086))');
SELECT 'invalid wkt curve polygon 7', ST_GeomFromEWKT('CURVEPOLYGON(CIRCULARSTRING(143.62025166838282 -30.037497356076827, 142.92857147299705 -32.75101196874403, 145.96132309891922 -34.985671061528784, 149.57565307617188 -33.41153335571289, 149.41972407584802 -29.824672680573517, 143.62025166838282 -30.037497356076827))');
SELECT 'invalid wkt curve polygon 8', ST_GeomFromEWKT('CURVEPOLYGON(CIRCULARSTRING(143.62025166838282 -30.037497356076827, 142.92857147299705 -32.75101196874403, 145.96132309891922 -34.985671061528784, 149.57565307617188 -33.41153335571289, 149.41972407584802 -29.824672680573517, 146.1209416055467 -30.19711586270431),(144.84399355252685 -31.26123924022086, 144.20551952601693 -32.27215644886158, 145.55230712890625 -33.49203872680664, 147.97080993652344 -32.03618621826172, 146.38697244992585 -31.47406391572417, 144.84399355252685 -31.26123924022086))');
SELECT 'invalid wkt curve polygon 9', ST_GeomFromEWKT('CURVEPOLYGON((143.62025166838282 -30.037497356076827, 142.92857147299705 -32.75101196874403, 145.96132309891922 -34.985671061528784, 149.57565307617188 -33.41153335571289, 149.41972407584802 -29.824672680573517, 146.1209416055467 -30.19711586270431, 143.62025166838282 -30.037497356076827),COMPOUNDCURVE(CIRCULARSTRING(144.84399355252685 -31.26123924022086, 144.20551952601693 -32.27215644886158, 145.55230712890625 -33.49203872680664),CIRCULARSTRING(147.97080993652344 -32.03618621826172, 146.38697244992585 -31.47406391572417, 144.84399355252685 -31.26123924022086))');
SELECT 'invalid wkt curve polygon a', ST_GeomFromEWKT('CURVEPOLYGON((143.62025166838282 -30.037497356076827, 142.92857147299705 -32.75101196874403, 145.96132309891922 -34.985671061528784, 149.57565307617188 -33.41153335571289, 149.41972407584802 -29.824672680573517, 146.1209416055467 -30.19711586270431, 143.62025166838282 -30.037497356076827),COMPOUNDCURVE(CIRCULARSTRING(144.84399355252685 -31.26123924022086, 144.20551952601693 -32.27215644886158, 145.55230712890625 -33.49203872680664),(145.55230712890625 -33.49203872680664, 147.97080993652344 -32.03618621826172),CIRCULARSTRING(147.97080993652344 -32.03618621826172, 146.38697244992585 -31.47406391572417, 144.84399355252685 -30.76123924022086))');
SELECT 'invalid wkt curve polygon b', ST_GeomFromEWKT('CURVEPOLYGON((143.62025166838282 -30.037497356076827, 142.92857147299705 -32.75101196874403, 145.96132309891922 -34.985671061528784, 149.57565307617188 -33.41153335571289, 149.41972407584802 -29.824672680573517, 146.1209416055467 -30.19711586270431, 143.62025166838282 -30.037497356076827),COMPOUNDCURVE(CIRCULARSTRING(144.84399355252685 -31.26123924022086, 144.20551952601693 -32.27215644886158, 145.55230712890625 -33.49203872680664),(145.55230712890625 -33.49203872680664),CIRCULARSTRING(147.97080993652344 -32.03618621826172, 146.38697244992585 -31.47406391572417, 144.84399355252685 -31.26123924022086))');
SELECT 'valid ewkb curve polygon 1', ST_asEWKT(ST_GeomFromEWKB(decode('010a00000001000000010200000007000000ccdf061ad9f3614054093e6d99093ec0ab9085dbb6dd614081540229216040c0ebd7a828c33e62409bf026782a7e41c0000000c06bb2624000000020adb440c08e632f616ead6240c9f7b0bf1dd33dc09011eec0de4362407dd6672f76323ec0ccdf061ad9f3614054093e6d99093ec0', 'hex')));
SELECT 'valid ewkb curve polygon 2', ST_asEWKT(ST_GeomFromEWKB(decode('010a00000002000000010200000007000000ccdf061ad9f3614054093e6d99093ec0ab9085dbb6dd614081540229216040c0ebd7a828c33e62409bf026782a7e41c0000000c06bb2624000000020adb440c08e632f616ead6240c9f7b0bf1dd33dc09011eec0de4362407dd6672f76323ec0ccdf061ad9f3614054093e6d99093ec00102000000060000006844c4fe011b6240342e2993e0423fc0d45daf9d93066240c4a0c305d62240c000000080ac31624000000020fbbe40c0000000e0107f6240000000c0a10440c04e1c0c14624c6240bf3fb6405c793fc06844c4fe011b6240342e2993e0423fc0', 'hex')));
SELECT 'valid ewkb curve polygon 3', ST_asEWKT(ST_GeomFromEWKB(decode('010a00000001000000010800000007000000ccdf061ad9f3614054093e6d99093ec0ab9085dbb6dd614081540229216040c0ebd7a828c33e62409bf026782a7e41c0000000c06bb2624000000020adb440c08e632f616ead6240c9f7b0bf1dd33dc09011eec0de4362407dd6672f76323ec0ccdf061ad9f3614054093e6d99093ec0', 'hex')));
SELECT 'valid ewkb curve polygon 4', ST_asEWKT(ST_GeomFromEWKB(decode('010a00000002000000010800000007000000ccdf061ad9f3614054093e6d99093ec0ab9085dbb6dd614081540229216040c0ebd7a828c33e62409bf026782a7e41c0000000c06bb2624000000020adb440c08e632f616ead6240c9f7b0bf1dd33dc09011eec0de4362407dd6672f76323ec0ccdf061ad9f3614054093e6d99093ec00102000000060000006844c4fe011b6240342e2993e0423fc0d45daf9d93066240c4a0c305d62240c000000080ac31624000000020fbbe40c0000000e0107f6240000000c0a10440c04e1c0c14624c6240bf3fb6405c793fc06844c4fe011b6240342e2993e0423fc0', 'hex')));
SELECT 'valid ewkb curve polygon 5', ST_asEWKT(ST_GeomFromEWKB(decode('010a00000002000000010200000007000000ccdf061ad9f3614054093e6d99093ec0ab9085dbb6dd614081540229216040c0ebd7a828c33e62409bf026782a7e41c0000000c06bb2624000000020adb440c08e632f616ead6240c9f7b0bf1dd33dc09011eec0de4362407dd6672f76323ec0ccdf061ad9f3614054093e6d99093ec00109000000030000000108000000030000006844c4fe011b6240342e2993e0423fc0d45daf9d93066240c4a0c305d62240c000000080ac31624000000020fbbe40c001020000000200000000000080ac31624000000020fbbe40c0000000e0107f6240000000c0a10440c0010800000003000000000000e0107f6240000000c0a10440c04e1c0c14624c6240bf3fb6405c793fc06844c4fe011b6240342e2993e0423fc0', 'hex')));
SELECT 'valid curve 6', encode(ST_AsBinary(ST_GeomFromText('CURVEPOLYGON(COMPOUNDCURVE(CIRCULARSTRING(0 0,2 0, 2 1, 2 3, 4 3),(4 3, 4 5, 1 4, 0 0)), CIRCULARSTRING(1.7 1, 1.4 0.4, 1.7 1) )'),'ndr'),'hex');
SELECT 'valid curve 7', encode(ST_AsBinary(ST_GeomFromText('CURVEPOLYGON(COMPOUNDCURVE(CIRCULARSTRING(0 0,2 0, 2 1, 2 3, 4 3),(4 3, 4 5, 1 4, 0 0)), (1.7 1, 1.4 0.4, 1.7 1) )'),'ndr'),'hex');
SELECT 'valid curve 8', encode(ST_AsBinary(ST_GeomFromText('CURVEPOLYGON(COMPOUNDCURVE(CIRCULARSTRING(0 0,2 0, 2 1, 2 3, 4 3),(4 3, 0 0)), CIRCULARSTRING(1.7 1, 1.4 0.4, 1.7 1) )'),'ndr'),'hex');
SELECT 'null response', ST_NumPoints(ST_GeomFromEWKT('CURVEPOLYGON(COMPOUNDCURVE(CIRCULARSTRING(0 0,2 0, 2 1, 2 3, 4 3),(4 3, 4 5, 1 4, 0 0)), CIRCULARSTRING(1.7 1, 1.4 0.4, 1.7 1) )'));
