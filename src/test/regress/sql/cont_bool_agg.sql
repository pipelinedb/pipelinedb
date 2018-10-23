-- bit_and, bit_or
CREATE FOREIGN TABLE bit_stream_cqboolagg (k text, b bit) SERVER pipelinedb;

CREATE VIEW test_bit_and AS SELECT k::text, bit_and(b::bit) FROM bit_stream_cqboolagg GROUP BY k;
CREATE VIEW test_bit_or AS SELECT k::text, bit_or(b::bit) FROM bit_stream_cqboolagg GROUP BY k;

INSERT INTO bit_stream_cqboolagg (k, b) VALUES ('x', 1::bit), ('x', '1'::bit), ('x', 1::bit), ('x', 1::bit), ('x', 1::bit), ('x', 1::bit), ('x', 1::bit);
INSERT INTO bit_stream_cqboolagg (k, b) VALUES ('y', 0::bit), ('y', 1::bit), ('y', 0::bit), ('x', 1::bit), ('x', 1::bit), ('x', 1::bit), ('x', 1::bit);

SELECT * FROM test_bit_and ORDER BY k DESC;
SELECT * FROM test_bit_or ORDER BY k DESC;

INSERT INTO bit_stream_cqboolagg (k, b) VALUES ('x', 1::bit), ('x', 0::bit);
INSERT INTO bit_stream_cqboolagg (k, b) VALUES ('y', 0::bit);

SELECT * FROM test_bit_and ORDER BY k DESC;
SELECT * FROM test_bit_or ORDER BY k DESC;

DROP FOREIGN TABLE bit_stream_cqboolagg CASCADE;

-- bool_and, bool_or, every
CREATE FOREIGN TABLE bool_stream_cqboolagg (k text, b boolean) SERVER pipelinedb;

CREATE VIEW test_bool_and AS SELECT k::text, bool_and(b::boolean) FROM bool_stream_cqboolagg GROUP BY k;
CREATE VIEW test_bool_or AS SELECT k::text, bool_or(b::boolean) FROM bool_stream_cqboolagg GROUP BY k;
CREATE VIEW test_every AS SELECT k::text, every(b::boolean) FROM bool_stream_cqboolagg GROUP BY k;

INSERT INTO bool_stream_cqboolagg (k, b) VALUES ('x', 't'), ('x', 't'), ('x', true), ('x', 't'), ('x', 't'), ('x', true), ('x', true);
INSERT INTO bool_stream_cqboolagg (k, b) VALUES ('y', false), ('y', 'f'), ('y', 'f'), ('x', false), ('x', true), ('x', 't'), ('x', 't');

SELECT * FROM test_bool_and ORDER BY k DESC;
SELECT * FROM test_bool_or ORDER BY k DESC;
SELECT * FROM test_every ORDER BY k DESC;

INSERT INTO bool_stream_cqboolagg (k, b) VALUES ('x', 't'), ('x', 'f');
INSERT INTO bool_stream_cqboolagg (k, b) VALUES ('y', 'f');

SELECT * FROM test_bool_and ORDER BY k DESC;
SELECT * FROM test_bool_or ORDER BY k DESC;
SELECT * FROM test_every ORDER BY k DESC;

DROP FOREIGN TABLE bool_stream_cqboolagg CASCADE;
