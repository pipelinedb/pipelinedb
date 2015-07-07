CREATE TABLE rt_io_test (
        id numeric,
        name text,
        hexwkb_ndr text,
        hexwkb_xdr text,
        rast raster
    );

-- 1x1, no bands,  no transform, scale 1:1
INSERT INTO rt_io_test (id, name, hexwkb_ndr, hexwkb_xdr)
VALUES ( 0, '1x1 no bands, no transform',
(
'01' -- little endian (uint8 ndr)
|| 
'0000' -- version (uint16 0)
||
'0000' -- nBands (uint16 0)
||
'000000000000F03F' -- scaleX (float64 1)
||
'000000000000F03F' -- scaleY (float64 1)
||
'0000000000000000' -- ipX (float64 0)
||
'0000000000000000' -- ipY (float64 0)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0A000000' -- SRID (int32 10)
||
'0100' -- width (uint16 1)
||
'0100' -- height (uint16 1)
)
,
(
'00' -- big endian (uint8 xdr)
|| 
'0000' -- version (uint16 0)
||
'0000' -- nBands (uint16 0)
||
'3FF0000000000000' -- scaleX (float64 1)
||
'3FF0000000000000' -- scaleY (float64 1)
||
'0000000000000000' -- ipX (float64 0)
||
'0000000000000000' -- ipY (float64 0)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0000000A' -- SRID (int32 10)
||
'0001' -- width (uint16 1)
||
'0001' -- height (uint16 1)
)
);

-- 1x1, single band of type 1BB, no transform, scale 1:1
INSERT INTO rt_io_test (id, name, hexwkb_ndr, hexwkb_xdr)
VALUES ( 1, '1x1 single band (1BB) no transform',
(
'01' -- little endian (uint8 ndr)
|| 
'0000' -- version (uint16 0)
||
'0100' -- nBands (uint16 1)
||
'000000000000F03F' -- scaleX (float64 1)
||
'000000000000F03F' -- scaleY (float64 1)
||
'0000000000000000' -- ipX (float64 0)
||
'0000000000000000' -- ipY (float64 0)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0A000000' -- SRID (int32 10)
||
'0100' -- width (uint16 1)
||
'0100' -- height (uint16 1)
||
'00' -- first band type (1BB)
||
'00' -- novalue==0
||
'01' -- pixel(0,0)==1
)
,
(
'00' -- big endian (uint8 xdr)
|| 
'0000' -- version (uint16 0)
||
'0001' -- nBands (uint16 1)
||
'3FF0000000000000' -- scaleX (float64 1)
||
'3FF0000000000000' -- scaleY (float64 1)
||
'0000000000000000' -- ipX (float64 0)
||
'0000000000000000' -- ipY (float64 0)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0000000A' -- SRID (int32 10)
||
'0001' -- width (uint16 1)
||
'0001' -- height (uint16 1)
||
'00' -- first band type (1BB)
||
'00' -- novalue==0
||
'01' -- pixel(0,0)==1
)
);

-- 1x1, single band of type 2BUI, no transform, scale 1:1
INSERT INTO rt_io_test (id, name, hexwkb_ndr, hexwkb_xdr)
VALUES ( 2, '1x1 single band (2BUI) no transform',
(
'01' -- little endian (uint8 ndr)
|| 
'0000' -- version (uint16 0)
||
'0100' -- nBands (uint16 1)
||
'000000000000F03F' -- scaleX (float64 1)
||
'000000000000F03F' -- scaleY (float64 1)
||
'0000000000000000' -- ipX (float64 0)
||
'0000000000000000' -- ipY (float64 0)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0A000000' -- SRID (int32 10)
|| 
'0100' -- width (uint16 1)
||
'0100' -- height (uint16 1)
||
'01' -- first band type (2BUI)
||
'00' -- novalue==0
||
'03' -- pixel(0,0)==3
),
(
'00' -- big endian (uint8 xdr)
|| 
'0000' -- version (uint16 0)
||
'0001' -- nBands (uint16 1)
||
'3FF0000000000000' -- scaleX (float64 1)
||
'3FF0000000000000' -- scaleY (float64 1)
||
'0000000000000000' -- ipX (float64 0)
||
'0000000000000000' -- ipY (float64 0)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0000000A' -- SRID (int32 10)
||
'0001' -- width (uint16 1)
||
'0001' -- height (uint16 1)
||
'01' -- first band type (2BUI)
||
'00' -- novalue==0
||
'03' -- pixel(0,0)==3
) );

-- 1x1, single band of type 4BUI, no transform, scale 1:1
INSERT INTO rt_io_test (id, name, hexwkb_ndr, hexwkb_xdr)
VALUES ( 3, '1x1 single band (4BUI) no transform',
(
'01' -- little endian (uint8 ndr)
|| 
'0000' -- version (uint16 0)
||
'0100' -- nBands (uint16 1)
||
'000000000000F03F' -- scaleX (float64 1)
||
'000000000000F03F' -- scaleY (float64 1)
||
'0000000000000000' -- ipX (float64 0)
||
'0000000000000000' -- ipY (float64 0)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0A000000' -- SRID (int32 10)
|| 
'0100' -- width (uint16 1)
||
'0100' -- height (uint16 1)
||
'02' -- first band type (4BUI)
||
'00' -- novalue==0
||
'0F' -- pixel(0,0)==15
),
(
'00' -- big endian (uint8 xdr)
|| 
'0000' -- version (uint16 0)
||
'0001' -- nBands (uint16 1)
||
'3FF0000000000000' -- scaleX (float64 1)
||
'3FF0000000000000' -- scaleY (float64 1)
||
'0000000000000000' -- ipX (float64 0)
||
'0000000000000000' -- ipY (float64 0)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0000000A' -- SRID (int32 10)
||
'0001' -- width (uint16 1)
||
'0001' -- height (uint16 1)
||
'02' -- first band type (4BUI)
||
'00' -- novalue==0
||
'0F' -- pixel(0,0)==15
) );

-- 1x1, single band of type 8BSI, no transform, scale 1:1
INSERT INTO rt_io_test (id, name, hexwkb_ndr, hexwkb_xdr)
VALUES ( 4, '1x1 single band (8BSI) no transform',
(
'01' -- little endian (uint8 ndr)
|| 
'0000' -- version (uint16 0)
||
'0100' -- nBands (uint16 1)
||
'000000000000F03F' -- scaleX (float64 1)
||
'000000000000F03F' -- scaleY (float64 1)
||
'0000000000000000' -- ipX (float64 0)
||
'0000000000000000' -- ipY (float64 0)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0A000000' -- SRID (int32 10)
|| 
'0100' -- width (uint16 1)
||
'0100' -- height (uint16 1)
||
'03' -- first band type (8BSI)
||
'00' -- novalue==0
||
'FF' -- pixel(0,0)==-1
),
(
'00' -- big endian (uint8 xdr)
|| 
'0000' -- version (uint16 0)
||
'0001' -- nBands (uint16 1)
||
'3FF0000000000000' -- scaleX (float64 1)
||
'3FF0000000000000' -- scaleY (float64 1)
||
'0000000000000000' -- ipX (float64 0)
||
'0000000000000000' -- ipY (float64 0)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0000000A' -- SRID (int32 10)
||
'0001' -- width (uint16 1)
||
'0001' -- height (uint16 1)
||
'03' -- first band type (8BSI)
||
'00' -- novalue==0
||
'FF' -- pixel(0,0)==-1
) );

-- 1x1, single band of type 8BUI, no transform, scale 1:1
INSERT INTO rt_io_test (id, name, hexwkb_ndr, hexwkb_xdr)
VALUES ( 5, '1x1 single band (8BUI) no transform',
(
'01' -- little endian (uint8 ndr)
|| 
'0000' -- version (uint16 0)
||
'0100' -- nBands (uint16 1)
||
'000000000000F03F' -- scaleX (float64 1)
||
'000000000000F03F' -- scaleY (float64 1)
||
'0000000000000000' -- ipX (float64 0)
||
'0000000000000000' -- ipY (float64 0)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0A000000' -- SRID (int32 10)
|| 
'0100' -- width (uint16 1)
||
'0100' -- height (uint16 1)
||
'04' -- first band type (8BUI)
||
'00' -- novalue==0
||
'FF' -- pixel(0,0)==255
),
(
'00' -- big endian (uint8 xdr)
|| 
'0000' -- version (uint16 0)
||
'0001' -- nBands (uint16 1)
||
'3FF0000000000000' -- scaleX (float64 1)
||
'3FF0000000000000' -- scaleY (float64 1)
||
'0000000000000000' -- ipX (float64 0)
||
'0000000000000000' -- ipY (float64 0)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0000000A' -- SRID (int32 10)
||
'0001' -- width (uint16 1)
||
'0001' -- height (uint16 1)
||
'04' -- first band type (8BUI)
||
'00' -- novalue==0
||
'FF' -- pixel(0,0)==255
) );

-- 1x1, single band of type 16BSI, no transform, scale 1:1
INSERT INTO rt_io_test (id, name, hexwkb_ndr, hexwkb_xdr)
VALUES ( 6, '1x1 single band (16BSI) no transform',
(
'01' -- little endian (uint8 ndr)
|| 
'0000' -- version (uint16 0)
||
'0100' -- nBands (uint16 1)
||
'000000000000F03F' -- scaleX (float64 1)
||
'000000000000F03F' -- scaleY (float64 1)
||
'0000000000000000' -- ipX (float64 0)
||
'0000000000000000' -- ipY (float64 0)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0A000000' -- SRID (int32 10)
|| 
'0100' -- width (uint16 1)
||
'0100' -- height (uint16 1)
||
'05' -- first band type (16BSI)
||
'0000' -- novalue==0
||
'FFFF' -- pixel(0,0)==-1
),
(
'00' -- big endian (uint8 xdr)
|| 
'0000' -- version (uint16 0)
||
'0001' -- nBands (uint16 1)
||
'3FF0000000000000' -- scaleX (float64 1)
||
'3FF0000000000000' -- scaleY (float64 1)
||
'0000000000000000' -- ipX (float64 0)
||
'0000000000000000' -- ipY (float64 0)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0000000A' -- SRID (int32 10)
||
'0001' -- width (uint16 1)
||
'0001' -- height (uint16 1)
||
'05' -- first band type (16BSI)
||
'0000' -- novalue==0
||
'FFFF' -- pixel(0,0)==-1
) );

-- 1x1, single band of type 16BUI, no transform, scale 1:1
INSERT INTO rt_io_test (id, name, hexwkb_ndr, hexwkb_xdr)
VALUES ( 7, '1x1 single band (16BUI) no transform',
(
'01' -- little endian (uint8 ndr)
|| 
'0000' -- version (uint16 0)
||
'0100' -- nBands (uint16 1)
||
'000000000000F03F' -- scaleX (float64 1)
||
'000000000000F03F' -- scaleY (float64 1)
||
'0000000000000000' -- ipX (float64 0)
||
'0000000000000000' -- ipY (float64 0)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0A000000' -- SRID (int32 10)
|| 
'0100' -- width (uint16 1)
||
'0100' -- height (uint16 1)
||
'06' -- first band type (16BUI)
||
'0000' -- novalue==0
||
'FFFF' -- pixel(0,0)==65535
),
(
'00' -- big endian (uint8 xdr)
|| 
'0000' -- version (uint16 0)
||
'0001' -- nBands (uint16 1)
||
'3FF0000000000000' -- scaleX (float64 1)
||
'3FF0000000000000' -- scaleY (float64 1)
||
'0000000000000000' -- ipX (float64 0)
||
'0000000000000000' -- ipY (float64 0)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0000000A' -- SRID (int32 10)
||
'0001' -- width (uint16 1)
||
'0001' -- height (uint16 1)
||
'06' -- first band type (16BUI)
||
'0000' -- novalue==0
||
'FFFF' -- pixel(0,0)==65535
) );

-- 1x1, single band of type 32BSI, no transform, scale 1:1
INSERT INTO rt_io_test (id, name, hexwkb_ndr, hexwkb_xdr)
VALUES ( 8, '1x1 single band (32BSI) no transform',
(
'01' -- little endian (uint8 ndr)
|| 
'0000' -- version (uint16 0)
||
'0100' -- nBands (uint16 1)
||
'000000000000F03F' -- scaleX (float64 1)
||
'000000000000F03F' -- scaleY (float64 1)
||
'0000000000000000' -- ipX (float64 0)
||
'0000000000000000' -- ipY (float64 0)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0A000000' -- SRID (int32 10)
|| 
'0100' -- width (uint16 1)
||
'0100' -- height (uint16 1)
||
'07' -- first band type (32BSI)
||
'00000000' -- novalue==0
||
'FFFFFFFF' -- pixel(0,0)==-1 ?
),
(
'00' -- big endian (uint8 xdr)
|| 
'0000' -- version (uint16 0)
||
'0001' -- nBands (uint16 1)
||
'3FF0000000000000' -- scaleX (float64 1)
||
'3FF0000000000000' -- scaleY (float64 1)
||
'0000000000000000' -- ipX (float64 0)
||
'0000000000000000' -- ipY (float64 0)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0000000A' -- SRID (int32 10)
||
'0001' -- width (uint16 1)
||
'0001' -- height (uint16 1)
||
'07' -- first band type (32BSI)
||
'00000000' -- novalue==0
||
'FFFFFFFF' -- pixel(0,0)==-1 ? 
) );

-- 1x1, single band of type 32BUI, no transform, scale 1:1
INSERT INTO rt_io_test (id, name, hexwkb_ndr, hexwkb_xdr)
VALUES ( 9, '1x1 single band (32BUI) no transform',
(
'01' -- little endian (uint8 ndr)
|| 
'0000' -- version (uint16 0)
||
'0100' -- nBands (uint16 1)
||
'000000000000F03F' -- scaleX (float64 1)
||
'000000000000F03F' -- scaleY (float64 1)
||
'0000000000000000' -- ipX (float64 0)
||
'0000000000000000' -- ipY (float64 0)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0A000000' -- SRID (int32 10)
|| 
'0100' -- width (uint16 1)
||
'0100' -- height (uint16 1)
||
'08' -- first band type (32BUI)
||
'00000000' -- novalue==0
||
'FFFFFFFF' -- pixel(0,0)=4294967295
),
(
'00' -- big endian (uint8 xdr)
|| 
'0000' -- version (uint16 0)
||
'0001' -- nBands (uint16 1)
||
'3FF0000000000000' -- scaleX (float64 1)
||
'3FF0000000000000' -- scaleY (float64 1)
||
'0000000000000000' -- ipX (float64 0)
||
'0000000000000000' -- ipY (float64 0)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0000000A' -- SRID (int32 10)
||
'0001' -- width (uint16 1)
||
'0001' -- height (uint16 1)
||
'08' -- first band type (32BUI)
||
'00000000' -- novalue==0
||
'FFFFFFFF' -- pixel(0,0)=4294967295
) );

-- 1x1, single band of type 32BF, no transform, scale 1:1
INSERT INTO rt_io_test (id, name, hexwkb_ndr, hexwkb_xdr)
VALUES ( 11, '1x1 single band (32BF) no transform',
(
'01' -- little endian (uint8 ndr)
|| 
'0000' -- version (uint16 0)
||
'0100' -- nBands (uint16 1)
||
'000000000000F03F' -- scaleX (float64 1)
||
'000000000000F03F' -- scaleY (float64 1)
||
'0000000000000000' -- ipX (float64 0)
||
'0000000000000000' -- ipY (float64 0)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0A000000' -- SRID (int32 10)
|| 
'0100' -- width (uint16 1)
||
'0100' -- height (uint16 1)
||
'0A' -- first band type (32BF)
||
'00000000' -- novalue==0
||
'CDCC8C3F' -- pixel(0,0)=1.1
),
(
'00' -- big endian (uint8 xdr)
|| 
'0000' -- version (uint16 0)
||
'0001' -- nBands (uint16 1)
||
'3FF0000000000000' -- scaleX (float64 1)
||
'3FF0000000000000' -- scaleY (float64 1)
||
'0000000000000000' -- ipX (float64 0)
||
'0000000000000000' -- ipY (float64 0)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0000000A' -- SRID (int32 10)
||
'0001' -- width (uint16 1)
||
'0001' -- height (uint16 1)
||
'0A' -- first band type (32BF)
||
'00000000' -- novalue==0
||
'3F8CCCCD' -- pixel(0,0)=1.1
) );

-- 1x1, single band of type 64BF, no transform, scale 1:1
INSERT INTO rt_io_test (id, name, hexwkb_ndr, hexwkb_xdr)
VALUES ( 11, '1x1 single band (64BF) no transform',
(
'01' -- little endian (uint8 ndr)
|| 
'0000' -- version (uint16 0)
||
'0100' -- nBands (uint16 1)
||
'000000000000F03F' -- scaleX (float64 1)
||
'000000000000F03F' -- scaleY (float64 1)
||
'0000000000000000' -- ipX (float64 0)
||
'0000000000000000' -- ipY (float64 0)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0A000000' -- SRID (int32 10)
|| 
'0100' -- width (uint16 1)
||
'0100' -- height (uint16 1)
||
'0B' -- first band type (64BF)
||
'0000000000000000' -- novalue==0
||
'AE47E17A14AE1540' -- pixel(0,0)=5.42
),
(
'00' -- big endian (uint8 xdr)
|| 
'0000' -- version (uint16 0)
||
'0001' -- nBands (uint16 1)
||
'3FF0000000000000' -- scaleX (float64 1)
||
'3FF0000000000000' -- scaleY (float64 1)
||
'0000000000000000' -- ipX (float64 0)
||
'0000000000000000' -- ipY (float64 0)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0000000A' -- SRID (int32 10)
||
'0001' -- width (uint16 1)
||
'0001' -- height (uint16 1)
||
'0B' -- first band type (64BF)
||
'0000000000000000' -- novalue==0
||
'4015AE147AE147AE' -- pixel(0,0)=5.42
) );

-- 1x1, single band of type 64BF (external: 3:/tmp/t.tif),
-- no transform, scale 1:1
--
INSERT INTO rt_io_test (id, name, hexwkb_ndr, hexwkb_xdr)
VALUES ( 12, '1x1 single band (64BF external) no transform',
(
'01' -- little endian (uint8 ndr)
|| 
'0000' -- version (uint16 0)
||
'0100' -- nBands (uint16 1)
||
'000000000000F03F' -- scaleX (float64 1)
||
'000000000000F03F' -- scaleY (float64 1)
||
'0000000000000000' -- ipX (float64 0)
||
'0000000000000000' -- ipY (float64 0)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0A000000' -- SRID (int32 10)
|| 
'0100' -- width (uint16 1)
||
'0100' -- height (uint16 1)
||
'8B' -- first band type (64BF + ext flag)
||
'0000000000000000' -- novalue==0
||
'03' -- ext band num == 3
||
'2F746D702F742E74696600' -- "/tmp/t.tif"
),
(
'00' -- big endian (uint8 xdr)
|| 
'0000' -- version (uint16 0)
||
'0001' -- nBands (uint16 1)
||
'3FF0000000000000' -- scaleX (float64 1)
||
'3FF0000000000000' -- scaleY (float64 1)
||
'0000000000000000' -- ipX (float64 0)
||
'0000000000000000' -- ipY (float64 0)
||
'0000000000000000' -- skewX (float64 0)
||
'0000000000000000' -- skewY (float64 0)
||
'0000000A' -- SRID (int32 10)
||
'0001' -- width (uint16 1)
||
'0001' -- height (uint16 1)
||
'8B' -- first band type (64BF)
||
'0000000000000000' -- novalue==0
||
'03' -- ext band num == 3
||
'2F746D702F742E74696600' -- "/tmp/t.tif"
) );

SELECT name,
    hexwkb_ndr::raster::text = hexwkb_ndr or hexwkb_ndr::raster::text = hexwkb_xdr as ndr_io,
    hexwkb_xdr::raster::text = hexwkb_ndr or hexwkb_xdr::raster::text = hexwkb_xdr as xdr_io
FROM rt_io_test;

-- Out of range value for 1BB pixeltype
SELECT (
        '01' || -- little endian (uint8 ndr)
        '0000' || -- version (uint16 0)
        '0100' || -- nBands (uint16 1)
        '000000000000F03F' || -- scaleX (float64 1)
        '000000000000F03F' || -- scaleY (float64 1)
        '0000000000000000' || -- ipX (float64 0)
        '0000000000000000' || -- ipY (float64 0)
        '0000000000000000' || -- skewX (float64 0)
        '0000000000000000' || -- skewY (float64 0)
        '0A000000' || -- SRID (int32 10)
        '0100' || -- width (uint16 1)
        '0100' || -- height (uint16 1)
        '00' || -- first band type (1BB)
        '00' ||-- novalue==0
        '02' -- pixel(0,0)==2 (out of 0..1 range)
)::raster;

-- Out of range value for 2BUI pixeltype
SELECT (
        '01' || -- little endian (uint8 ndr)
        '0000' || -- version (uint16 0)
        '0100' || -- nBands (uint16 1)
        '000000000000F03F' || -- scaleX (float64 1)
        '000000000000F03F' || -- scaleY (float64 1)
        '0000000000000000' || -- ipX (float64 0)
        '0000000000000000' || -- ipY (float64 0)
        '0000000000000000' || -- skewX (float64 0)
        '0000000000000000' || -- skewY (float64 0)
        '0A000000' || -- SRID (int32 10)
        '0100' || -- width (uint16 1)
        '0100' || -- height (uint16 1)
        '01' || -- first band type (2BUI)
        '00' ||-- novalue==0
        '04' -- pixel(0,0)==4 (out of 0..3 range)
)::raster;

-- Out of range value for 4BUI pixeltype
SELECT (
        '01' || -- little endian (uint8 ndr)
        '0000' || -- version (uint16 0)
        '0100' || -- nBands (uint16 1)
        '000000000000F03F' || -- scaleX (float64 1)
        '000000000000F03F' || -- scaleY (float64 1)
        '0000000000000000' || -- ipX (float64 0)
        '0000000000000000' || -- ipY (float64 0)
        '0000000000000000' || -- skewX (float64 0)
        '0000000000000000' || -- skewY (float64 0)
        '0A000000' || -- SRID (int32 10)
        '0100' || -- width (uint16 1)
        '0100' || -- height (uint16 1)
        '02' || -- first band type (4BUI)
        '00' ||-- novalue==0
        '10' -- pixel(0,0)==16 (out of 0..15 range)
)::raster;

DROP TABLE rt_io_test;
