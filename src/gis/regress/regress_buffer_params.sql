---
--- Tests for ST_Buffer with parameters (needs GEOS-3.2 or higher)
---
---

-- Ouput is snapped to grid to account for small floating numbers
-- differences between architectures
SELECT 'point quadsegs=2', ST_AsText(ST_SnapToGrid(st_buffer('POINT(0 0)', 1, 'quad_segs=2'), 1.0e-6));
SELECT 'line quadsegs=2', ST_AsText(ST_SnapToGrid(st_buffer('LINESTRING(0 0, 10 0)', 2, 'quad_segs=2'), 1.0e-6));
SELECT 'line quadsegs=2 endcap=flat', ST_AsText(ST_SnapToGrid(st_buffer('LINESTRING(0 0, 10 0)', 2, 'quad_segs=2 endcap=flat'), 1.0e-6));
SELECT 'line quadsegs=2 endcap=butt', ST_AsText(ST_SnapToGrid(st_buffer('LINESTRING(0 0, 10 0)', 2, 'quad_segs=2 endcap=butt'), 1.0e-6));
SELECT 'line quadsegs=2 endcap=square', ST_AsText(ST_SnapToGrid(st_buffer('LINESTRING(0 0, 10 0)', 2, 'quad_segs=2 endcap=square'), 1.0e-6));
SELECT 'poly quadsegs=2 join=round', ST_AsText(ST_SnapToGrid(st_buffer('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))', 2, 'quad_segs=2 join=round'), 1.0e-6));
SELECT 'poly quadsegs=2 join=bevel', ST_AsText(ST_SnapToGrid(st_buffer('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))', 2, 'quad_segs=2 join=bevel'), 1.0e-6));
SELECT 'poly quadsegs=2 join=mitre', ST_AsText(ST_SnapToGrid(st_buffer('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))', 2, 'quad_segs=2 join=mitre'), 1.0e-6));
SELECT 'poly quadsegs=2 join=mitre mitre_limit=1', ST_AsText(ST_SnapToGrid(st_buffer('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))', 2, 'quad_segs=2 join=mitre mitre_limit=1'), 1.0e-6));
SELECT 'poly quadsegs=2 join=miter miter_limit=1', ST_AsText(ST_SnapToGrid(st_buffer('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))', 2, 'quad_segs=2 join=miter miter_limit=1'), 1.0e-6));

