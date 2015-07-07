select 1, valid, reason, st_astext(location) FROM (
  SELECT (ST_IsValidDetail('LINESTRING (70 250, 190 340)')).*
) foo;

select 2, valid, reason, st_astext(location) FROM (
  SELECT (ST_IsValidDetail('LINESTRING (70 250, 70 250)')).*
) foo;

-- Twisted polygon 
select 3, valid, reason, st_astext(location) FROM (
  SELECT (ST_IsValidDetail(
'POLYGON ((70 250, 70 500, 80 400, 40 400, 70 250))'
  )).*
) foo;

-- Twisted polygon is also invalid for ESRI
select 4, valid, reason, st_astext(location) FROM (
  SELECT (ST_IsValidDetail(
'POLYGON ((70 250, 70 500, 80 400, 40 400, 70 250))'
  ,
  1 -- ESRI flag
  )).*
) foo;

-- Self-touching ring forming hole
select 5, valid, reason, st_astext(location) FROM (
  SELECT (ST_IsValidDetail(
'POLYGON ((70 250, 40 500, 100 400, 70 250, 80 350, 60 350, 70 250))'
  ,
  0 -- No flags
  )).*
) foo;
select '5s', ST_IsValid(
'POLYGON ((70 250, 40 500, 100 400, 70 250, 80 350, 60 350, 70 250))' , 0);
select '5r', ST_IsValidReason(
'POLYGON ((70 250, 40 500, 100 400, 70 250, 80 350, 60 350, 70 250))' , 0);

-- Self-touching ring forming hole with ESRI flag
select 6, valid, reason, st_astext(location) FROM (
  SELECT (ST_IsValidDetail(
'POLYGON ((70 250, 40 500, 100 400, 70 250, 80 350, 60 350, 70 250))'
  ,
  1 -- ESRI flag
  )).*
) foo;
select '6s', ST_IsValid(
'POLYGON ((70 250, 40 500, 100 400, 70 250, 80 350, 60 350, 70 250))' , 1);
select '5r', ST_IsValidReason(
'POLYGON ((70 250, 40 500, 100 400, 70 250, 80 350, 60 350, 70 250))' , 1);


