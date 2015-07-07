--
-- From examples in chapter 1.12.1 of
-- "Spatial Topology and Network Data Models" (Oracle manual)
--
-- Modified to use postgis-based topology model.
-- Loads the whole topology represented in Figure 1-1 of the
-- manual, creates TopoGeometry objects and associations.
--

--ORA--------------------------------
--ORA---- Main steps for using the topology data model with a topology
--ORA---- built from edge, node, and face data
--ORA--------------------------------
--ORA---- ...
--ORA---- 7. Query the data.
--ORA---- 8. Optionally, edit data using the PL/SQL or Java API.


BEGIN;

-- 7. Query the data.
SELECT a.feature_name, id(a.feature) as tg_id,
	ST_AsText(topology.Geometry(a.feature)) as geom
FROM features.land_parcels a;

-- Query not in original example --strk;
SELECT a.feature_name, id(a.feature) as tg_id,
	ST_AsText(topology.Geometry(a.feature)) as geom
FROM features.traffic_signs a;

-- Query not in original example --strk;
SELECT a.feature_name, id(a.feature) as tg_id,
	ST_AsText(topology.Geometry(a.feature)) as geom
FROM features.city_streets a;

-- Query hierarchical feautures
SELECT feature_name, ST_AsText(topology.geometry(feature))
FROM features.big_signs;

SELECT feature_name,ST_AsText(topology.geometry(feature))
FROM features.big_streets;

SELECT feature_name,ST_AsText(topology.geometry(feature))
FROM features.big_parcels;


--NOTYET--
--NOTYET--/* Window is city_streets */
--NOTYET--SELECT a.feature_name, b.feature_name
--NOTYET--  FROM city_streets b,
--NOTYET--     land_parcels a
--NOTYET--  WHERE b.feature_name like 'R%' AND
--NOTYET--     sdo_anyinteract(a.feature, b.feature) = 'TRUE'
--NOTYET--  ORDER BY b.feature_name, a.feature_name;
--NOTYET--
--NOTYET---- Find all streets that have any interaction with land parcel P3.
--NOTYET---- (Should return only R1.)
--NOTYET--SELECT c.feature_name FROM city_streets c, land_parcels l
--NOTYET--  WHERE l.feature_name = 'P3' AND
--NOTYET--   SDO_ANYINTERACT (c.feature, l.feature) = 'TRUE';
--NOTYET--
--NOTYET---- Find all land parcels that have any interaction with traffic sign S1.
--NOTYET---- (Should return P1 and P2.)
--NOTYET--SELECT l.feature_name FROM land_parcels l, traffic_signs t
--NOTYET--  WHERE t.feature_name = 'S1' AND
--NOTYET--   SDO_ANYINTERACT (l.feature, t.feature) = 'TRUE';
--NOTYET--
--NOTYET---- Get the geometry for land parcel P1.
--NOTYET--SELECT l.feature_name, l.feature.get_geometry()
--NOTYET--  FROM land_parcels l WHERE l.feature_name = 'P1';
--NOTYET--
--NOTYET---- Get the boundary of face with face_id 3.
--NOTYET--SELECT topology.GET_FACE_BOUNDARY('CITY_DATA', 3) FROM DUAL;
--NOTYET--
--NOTYET---- Get the topological elements for land parcel P2.
--NOTYET---- CITY_DATA layer, land parcels (tg_ layer_id = 1), parcel P2 (tg_id = 2)
--NOTYET--SELECT topology.GET_TOPO_OBJECTS('CITY_DATA', 1, 2) FROM DUAL;
--NOTYET--
--NOTYET--

END;

