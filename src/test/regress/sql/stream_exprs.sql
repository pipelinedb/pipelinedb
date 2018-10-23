CREATE FOREIGN TABLE test_exprs_stream (b boolean, t text, n numeric) SERVER pipelinedb;
CREATE VIEW test_stream_exprs AS SELECT b::boolean, t::text, n::numeric FROM test_exprs_stream;

INSERT INTO test_exprs_stream (b, t, n) VALUES (true and true, substring('string!', 1, 3), 1.2 + 100.0001);
INSERT INTO test_exprs_stream (b, t, n) VALUES (1 < 2, 'first' || 'second', 100 % 2 * log(2, 3));
INSERT INTO test_exprs_stream (b, t, n) VALUES (1 is null, lower('UPPER'), pow(6!::float8 + 4, 3));
INSERT INTO test_exprs_stream (b, t, n) VALUES (true and true and true and false, trim(both 'x' from 'xTomxx'), pi() * mod(1 + 3, 4 * 8));
INSERT INTO test_exprs_stream (b, t, n) VALUES ('t', overlay('Txxxxas' placing 'hom' from 2 for 4), acos(1) * trunc(4.003));
INSERT INTO test_exprs_stream (b, t, n) VALUES ('on' and true, substring(upper('lower'), 1, 2), 42.82 - @(-12));
INSERT INTO test_exprs_stream (b, t, n) VALUES ('on' or 'off', btrim('xyxtrimyyx', 'xy'), 2^32);
INSERT INTO test_exprs_stream (b, t, n) VALUES (1 * 2 > 2 * 3, md5(md5(md5('md5 me three times!'))), |/(64));
INSERT INTO test_exprs_stream (b, t, n) VALUES (1 + 4 * 3 - 14 < 1.2, 'true', pow(2::float8, 32::numeric));
INSERT INTO test_exprs_stream (b, t, n) VALUES (true and not false or true, substring('left', 1, 2) || substring('right', 1, 2), 1 << 4 << 4);
INSERT INTO test_exprs_stream (b, t, n) VALUES (null and true, substring('x' || 'blah' || 'x', 2, 4), (100 + 24) * (16 * 3 + 4) - (16 << 2 >> 2));

-- Nonexistent fields should default to null
INSERT INTO test_exprs_stream (b) VALUES (false);
INSERT INTO test_exprs_stream (t) VALUES ('text!');
INSERT INTO test_exprs_stream (n) VALUES (1 - 1);

SELECT * FROM test_stream_exprs ORDER BY b, t, n;

DROP FOREIGN TABLE test_exprs_stream CASCADE;
