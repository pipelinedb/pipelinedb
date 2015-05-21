-- bit_and, bit_or
CREATE CONTINUOUS VIEW test_sw_bit_and AS SELECT k::text, bit_and(b::bit) FROM bit_test_sw_sw_stream GROUP BY k;
CREATE CONTINUOUS VIEW test_sw_bit_or AS SELECT k::text, bit_or(b::bit) FROM bit_test_sw_sw_stream WHERE arrival_timestamp > clock_timestamp() - interval '60 second'  GROUP BY k;

INSERT INTO bit_test_sw_sw_stream (k, b) VALUES ('x', 1::bit), ('x', '1'::bit), ('x', 1::bit), ('x', 1::bit), ('x', 1::bit), ('x', 1::bit), ('x', 1::bit);
INSERT INTO bit_test_sw_sw_stream (k, b) VALUES ('y', 0::bit), ('y', 1::bit), ('y', 0::bit), ('x', 1::bit), ('x', 1::bit), ('x', 1::bit), ('x', 1::bit);

SELECT * FROM test_sw_bit_and ORDER BY k DESC;
SELECT * FROM test_sw_bit_or ORDER BY k DESC;

SELECT pg_sleep(1);

INSERT INTO bit_test_sw_sw_stream (k, b) VALUES ('x', 1::bit), ('x', 0::bit);
INSERT INTO bit_test_sw_sw_stream (k, b) VALUES ('y', 0::bit);

SELECT * FROM test_sw_bit_and ORDER BY k DESC;
SELECT * FROM test_sw_bit_or ORDER BY k DESC;

-- bool_and, bool_or, every
CREATE CONTINUOUS VIEW test_sw_bool_and AS SELECT k::text, bool_and(b::boolean) FROM bool_test_sw_sw_stream WHERE arrival_timestamp > clock_timestamp() - interval '60 second'  GROUP BY k;
CREATE CONTINUOUS VIEW test_sw_bool_or AS SELECT k::text, bool_or(b::boolean) FROM bool_test_sw_sw_stream WHERE arrival_timestamp > clock_timestamp() - interval '60 second'  GROUP BY k;
CREATE CONTINUOUS VIEW test_sw_every AS SELECT k::text, every(b::boolean) FROM bool_test_sw_sw_stream WHERE arrival_timestamp > clock_timestamp() - interval '60 second'  GROUP BY k;

INSERT INTO bool_test_sw_sw_stream (k, b) VALUES ('x', 't'), ('x', 't'), ('x', true), ('x', 't'), ('x', 't'), ('x', true), ('x', true);
INSERT INTO bool_test_sw_sw_stream (k, b) VALUES ('y', false), ('y', 'f'), ('y', 'f'), ('x', false), ('x', true), ('x', true), ('x', 't');

SELECT * FROM test_sw_bool_and ORDER BY k DESC;
SELECT * FROM test_sw_bool_or ORDER BY k DESC;
SELECT * FROM test_sw_every ORDER BY k DESC;

SELECT pg_sleep(1);

INSERT INTO bool_test_sw_sw_stream (k, b) VALUES ('x', 't'), ('x', 'f');
INSERT INTO bool_test_sw_sw_stream (k, b) VALUES ('y', 'f');

SELECT * FROM test_sw_bool_and ORDER BY k DESC;
SELECT * FROM test_sw_bool_or ORDER BY k DESC;
SELECT * FROM test_sw_every ORDER BY k DESC;

DROP CONTINUOUS VIEW test_sw_bit_and;
DROP CONTINUOUS VIEW test_sw_bit_or;
DROP CONTINUOUS VIEW test_sw_bool_and;
DROP CONTINUOUS VIEW test_sw_bool_or;
DROP CONTINUOUS VIEW test_sw_every;
