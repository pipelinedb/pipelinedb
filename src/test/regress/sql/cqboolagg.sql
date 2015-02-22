SET debug_sync_stream_insert = 'on';

-- bit_and, bit_or
CREATE CONTINUOUS VIEW test_bit_and AS SELECT k::text, bit_and(b::bit) FROM bit_stream_cqboolagg GROUP BY k;
CREATE CONTINUOUS VIEW test_bit_or AS SELECT k::text, bit_or(b::bit) FROM bit_stream_cqboolagg GROUP BY k;

ACTIVATE test_bit_and, test_bit_or;

INSERT INTO bit_stream_cqboolagg (k, b) VALUES ('x', 1::bit), ('x', '1'::bit), ('x', 1::bit), ('x', 1::bit), ('x', 1::bit), ('x', 1::bit), ('x', 1::bit);
INSERT INTO bit_stream_cqboolagg (k, b) VALUES ('y', 0::bit), ('y', 1::bit), ('y', 0::bit), ('x', 1::bit), ('x', 1::bit), ('x', 1::bit), ('x', 1::bit);

DEACTIVATE test_bit_and, test_bit_or;

SELECT * FROM test_bit_and ORDER BY k DESC;
SELECT * FROM test_bit_or ORDER BY k DESC;

ACTIVATE test_bit_and, test_bit_or;

INSERT INTO bit_stream_cqboolagg (k, b) VALUES ('x', 1::bit), ('x', 0::bit);
INSERT INTO bit_stream_cqboolagg (k, b) VALUES ('y', 0::bit);

DEACTIVATE test_bit_and, test_bit_or;

SELECT * FROM test_bit_and ORDER BY k DESC;
SELECT * FROM test_bit_or ORDER BY k DESC;

-- bool_and, bool_or, every
CREATE CONTINUOUS VIEW test_bool_and AS SELECT k::text, bool_and(b::boolean) FROM bool_stream_cqboolagg GROUP BY k;
CREATE CONTINUOUS VIEW test_bool_or AS SELECT k::text, bool_or(b::boolean) FROM bool_stream_cqboolagg GROUP BY k;
CREATE CONTINUOUS VIEW test_every AS SELECT k::text, every(b::boolean) FROM bool_stream_cqboolagg GROUP BY k;

ACTIVATE test_bool_and, test_bool_or, test_every;

INSERT INTO bool_stream_cqboolagg (k, b) VALUES ('x', 't'), ('x', 't'), ('x', true), ('x', 't'), ('x', 't'), ('x', true), ('x', true);
INSERT INTO bool_stream_cqboolagg (k, b) VALUES ('y', false), ('y', 'f'), ('y', 'f'), ('x', false), ('x', true), ('x', 't'), ('x', 't');

DEACTIVATE test_bool_and, test_bool_or, test_every;

SELECT * FROM test_bool_and ORDER BY k DESC;
SELECT * FROM test_bool_or ORDER BY k DESC;
SELECT * FROM test_every ORDER BY k DESC;

ACTIVATE test_bool_and, test_bool_or, test_every;

INSERT INTO bool_stream_cqboolagg (k, b) VALUES ('x', 't'), ('x', 'f');
INSERT INTO bool_stream_cqboolagg (k, b) VALUES ('y', 'f');

DEACTIVATE test_bool_and, test_bool_or, test_every;

SELECT * FROM test_bool_and ORDER BY k DESC;
SELECT * FROM test_bool_or ORDER BY k DESC;
SELECT * FROM test_every ORDER BY k DESC;

DROP CONTINUOUS VIEW test_bit_and;
DROP CONTINUOUS VIEW test_bit_or;
DROP CONTINUOUS VIEW test_bool_and;
DROP CONTINUOUS VIEW test_bool_or;
DROP CONTINUOUS VIEW test_every;
