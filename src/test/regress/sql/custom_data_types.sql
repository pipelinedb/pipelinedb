CREATE TABLE custom_dt_hll (x text, y hll);

INSERT INTO custom_dt_hll VALUES ('a', hll_empty()), ('b', hll_empty(5)), ('c', NULL);

SELECT hll_print(y) FROM custom_dt_hll;

UPDATE custom_dt_hll SET y = hll_add(y, 'hello');
UPDATE custom_dt_hll SET y = hll_add(y, 'world');

SELECT hll_print(y) FROM custom_dt_hll;
SELECT hll_print(hll_agg(x)) FROM custom_dt_hll;
SELECT hll_print(hll_agg(x, 10)) FROM custom_dt_hll;

DROP TABLE custom_dt_hll;

CREATE TABLE custom_dt_bloom (x text, y bloom);

INSERT INTO custom_dt_bloom VALUES ('a', bloom_empty()), ('b', bloom_empty(0.05, 1000)), ('c', NULL);

SELECT bloom_print(y) FROM custom_dt_bloom;

UPDATE custom_dt_bloom SET y = bloom_add(y, 'hello');
UPDATE custom_dt_bloom SET y = bloom_add(y, 'world');

SELECT bloom_print(y) FROM custom_dt_bloom;
SELECT bloom_print(bloom_agg(x)) FROM custom_dt_bloom;
SELECT bloom_print(bloom_agg(x, 0.05, 1000)) FROM custom_dt_bloom;

DROP TABLE custom_dt_bloom;

CREATE TABLE custom_dt_cmsketch (x text, y cmsketch);

INSERT INTO custom_dt_cmsketch VALUES ('a', cmsketch_empty()), ('b', cmsketch_empty(0.05, 0.80)), ('c', NULL);

SELECT cmsketch_print(y) FROM custom_dt_cmsketch;

UPDATE custom_dt_cmsketch SET y = cmsketch_add(y, 'hello');
UPDATE custom_dt_cmsketch SET y = cmsketch_add(y, 'world', 5);

SELECT cmsketch_print(y) FROM custom_dt_cmsketch;
SELECT cmsketch_print(cmsketch_agg(x)) FROM custom_dt_cmsketch;
SELECT cmsketch_print(cmsketch_agg(x, 0.05, 0.80)) FROM custom_dt_cmsketch;

DROP TABLE custom_dt_cmsketch;

CREATE TABLE custom_dt_tdigest (x text, y tdigest);

INSERT INTO custom_dt_tdigest VALUES ('a', tdigest_empty()), ('b', tdigest_empty(25)), ('c', NULL);

SELECT tdigest_print(y) FROM custom_dt_tdigest;

UPDATE custom_dt_tdigest SET y = tdigest_add(y, 10);
UPDATE custom_dt_tdigest SET y = tdigest_add(y, 20, 2);

SELECT tdigest_print(y) FROM custom_dt_tdigest;
SELECT tdigest_print(tdigest_agg(x)) FROM custom_dt_tdigest;
SELECT tdigest_print(tdigest_agg(x, 25)) FROM custom_dt_tdigest;

DROP TABLE custom_dt_tdigest;
