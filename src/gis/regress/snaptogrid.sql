
-- postgis-users/2006-January/010870.html
CREATE TEMP TABLE tmp (orig geometry);
INSERT INTO tmp (orig) VALUES ('01020000207D18000007000000D7A3701D641B3FC18B6CE7FB5A721841FA7E6A9C5E1B3FC191ED7C3F9872184139B4C816591B3FC1E3A59BC4D472184104560E4D891A3FC177BE9F1ABF7118417B14AEA7961A3FC18716D94E0C711841022B8716671B3FC1C74B370939721841D7A3701D641B3FC18B6CE7FB5A721841');

-- Repeat tests with new function names.
SELECT ST_snaptogrid(orig, 0.001) ~= ST_snaptogrid(ST_snaptogrid(orig, 0.001), 0.001) FROM tmp;
SELECT ST_snaptogrid(orig, 0.005) ~= ST_snaptogrid(ST_snaptogrid(orig, 0.005), 0.005) FROM tmp;
SELECT ST_snaptogrid(orig, 0.002) ~= ST_snaptogrid(ST_snaptogrid(orig, 0.002), 0.002) FROM tmp;
SELECT ST_snaptogrid(orig, 0.003) ~= ST_snaptogrid(ST_snaptogrid(orig, 0.003), 0.003) FROM tmp;
SELECT ST_snaptogrid(orig, 0.0002) ~= ST_snaptogrid(ST_snaptogrid(orig, 0.0002), 0.0002) FROM tmp;
DROP TABLE tmp;
