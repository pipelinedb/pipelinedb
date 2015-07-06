SELECT 'flip1', ST_AsText(ST_FlipCoordinates('POINT(0 1)'));
SELECT 'flip2', ST_AsText(ST_FlipCoordinates('GEOMETRYCOLLECTION(POINT(1 2),MULTIPOLYGON(((0 1,1 2,2 1,0 1)),((10 0,20 0,20 10,10 0),(2 2,4 2,4 4,2 2))),LINESTRING(0 1,1 0))'));

-- Bogus calls (swapping unavailable ordinate)
SELECT ST_AsText(ST_SwapOrdinates('POINTZ(0 1 2)','xm'));
SELECT ST_AsText(ST_SwapOrdinates('POINTM(0 1 2)','zy'));
-- Bogus calls (short spec)
SELECT ST_AsText(ST_SwapOrdinates('POINTZ(0 1 2)','x'));
-- Bogus calls (invalid ordinate names)
SELECT ST_AsText(ST_SwapOrdinates('POINTZ(0 1 2)','pq'));

SELECT 'swap1', ST_AsText(ST_SwapOrdinates('POINTZ(0 1 2)','xz'));
SELECT 'swap2', ST_AsText(ST_SwapOrdinates('POINTM(0 1 2)','my'));
SELECT 'swap3', ST_AsText(ST_SwapOrdinates('POINTZM(0 1 2 3)','mz'));
SELECT 'swap4', ST_AsText(ST_SwapOrdinates('MULTICURVE ZM ((5 5 1 3, 3 5 2 2, 3 3 3 1, 0 3 1 1), CIRCULARSTRING ZM (0 0 0 0, 0.2 1 3 -2, 0.5 1.4 1 2), COMPOUNDCURVE ZM (CIRCULARSTRING ZM (0 0 0 0,1 1 1 2,1 0 0 1),(1 0 0 1,0 1 5 4)))','my'));
