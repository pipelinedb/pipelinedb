\set VERBOSITY terse
set client_min_messages to ERROR;

\i load_topology.sql
\i load_features.sql

SELECT 'relation before', count(distinct topogeo_id)
 FROM city_data.relation
 WHERE layer_id = 1;

SELECT 'feature before',
    feature_name,
    ST_IsEmpty(feature)
  FROM features.land_parcels
  WHERE feature_name = 'P1'
  ORDER BY feature_name;

SELECT 'feature during',
    feature_name,
    ST_IsEmpty(clearTopoGeom(feature))
  FROM features.land_parcels
  WHERE feature_name = 'P1'
  ORDER BY feature_name;

SELECT 'feature after',
    feature_name,
    ST_IsEmpty(feature)
  FROM features.land_parcels
  WHERE feature_name = 'P1'
  ORDER BY feature_name;

SELECT 'relation after', count(distinct topogeo_id)
 FROM city_data.relation
 WHERE layer_id = 1;

select droptopology('city_data');
