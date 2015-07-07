\a
--SET seq_page_cost='1000';
SELECT set_geocode_setting('debug_reverse_geocode', 'false');
SELECT set_geocode_setting('debug_geocode_address', 'false');
SELECT set_geocode_setting('debug_normalize_address', 'false');
SELECT set_geocode_setting('debug_geocode_intersection', 'false');
SELECT set_geocode_setting('reverse_geocode_numbered_roads', '1'); -- prefer numbered highway name
\o normalize_address_regress.out
\i normalize_address_regress.sql
\o pagc_normalize_address_regress.out
\i pagc_normalize_address_regress.sql
\o geocode_regress.out
\i geocode_regress.sql
\o reverse_geocode_regress.out
\i reverse_geocode_regress.sql