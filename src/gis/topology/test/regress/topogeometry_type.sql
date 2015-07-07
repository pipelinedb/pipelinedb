set client_min_messages to WARNING;

\i load_topology.sql
\i load_features.sql
\i more_features.sql
\i hierarchy.sql

SELECT DISTINCT 'GeometryType(traffic_signs)',
	geometrytype(feature) FROM features.traffic_signs;
SELECT DISTINCT 'ST_GeometryType(traffic_signs)',
	st_geometrytype(feature) FROM features.traffic_signs;

SELECT DISTINCT 'GeometryType(city_streets)',
	geometrytype(feature) FROM features.city_streets;
SELECT DISTINCT 'ST_GeometryType(city_streets)',
	st_geometrytype(feature) FROM features.city_streets;

SELECT DISTINCT 'GeometryType(land_parcels)',
	geometrytype(feature) FROM features.land_parcels;
SELECT DISTINCT 'ST_GeometryType(land_parcels)',
	st_geometrytype(feature) FROM features.land_parcels;

SELECT DISTINCT 'GeometryType(big_signs)',
	geometrytype(feature) FROM features.big_signs;
SELECT DISTINCT 'ST_GeometryType(big_signs)',
	st_geometrytype(feature) FROM features.big_signs;

SELECT DISTINCT 'GeometryType(big_streets)',
	geometrytype(feature) FROM features.big_streets;
SELECT DISTINCT 'ST_GeometryType(big_streets)',
	st_geometrytype(feature) FROM features.big_streets;

SELECT DISTINCT 'GeometryType(big_parcels)',
	geometrytype(feature) FROM features.big_parcels;
SELECT DISTINCT 'ST_GeometryType(big_parcels)',
	st_geometrytype(feature) FROM features.big_parcels;

SELECT topology.DropTopology('city_data');
DROP SCHEMA features CASCADE;
