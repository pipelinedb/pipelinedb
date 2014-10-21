-- Decode all boolean literal types.

SET debug_sync_stream_insert = 'on';

CREATE CONTINUOUS VIEW test_bool AS SELECT b::boolean FROM stream;

ACTIVATE test_bool;

INSERT INTO stream (b) VALUES (TRUE), (true), ('true'), ('t'), ('y'), ('on'), ('1'), (1);
INSERT INTO stream (b) VALUES (FALSE), (false), ('false'), ('f'), ('n'), ('off'), ('0'), (0);

DEACTIVATE;

SELECT b, COUNT(*) FROM test_bool GROUP BY b;

DROP CONTINUOUS VIEW test_bool;
