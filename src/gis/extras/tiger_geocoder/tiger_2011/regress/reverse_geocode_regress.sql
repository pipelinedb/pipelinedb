\timing
SELECT pprint_addy(addy[1]), addy FROM reverse_geocode(ST_Point(-71.27593,42.33891));  -- I 90, Weston MA
SELECT pprint_addy(addy[1]), addy FROM reverse_geocode(ST_Point(-71.85335,42.19262));  -- I 90, Auburn, MA 01501
SELECT pprint_addy(addy[1]), addy FROM reverse_geocode(ST_Point(-71.057811,42.358274)); -- 1 Devonshire Place (washington st area)
SELECT pprint_addy(addy[1]), addy FROM reverse_geocode(ST_Point(-71.123848,42.41115)); -- 30 capen, Medford, MA 02155
SELECT pprint_addy(addy[1]), addy FROM reverse_geocode(ST_Point(-71.09436,42.35981)); -- 58 Massachusetts Ave, Cambridge, MA 02139 (this gives me different answer but might be tiger change)
SELECT '#1913' As ticket, pprint_addy(addy[1]) FROM reverse_geocode(ST_Point(-71.2248416, 42.30344833)); -- I- 95, Needham, MA 02494
SELECT '#2927', pprint_addy(addy[1]) FROM reverse_geocode(ST_Point(-71.058246,42.36514)); -- 77 N Washington St, Boston, MA 02114
\timing