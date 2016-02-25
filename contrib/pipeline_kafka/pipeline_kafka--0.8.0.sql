/* contrib/pipeline_kafka/pipeline_kafka--0.8.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pipeline_kafka" to load this file. \quit

-- Consumers added with kafka_consume_begin
CREATE TABLE pipeline_kafka_consumers (
	relation text,
	topic text,
	batchsize integer,
	parallelism integer,
	format text,
	delimiter text,
	quote text
) WITH OIDS;

CREATE TABLE pipeline_kafka_offsets (
	consumer_id oid,
	partition integer,
	"offset" bigint
);
-- Brokers added with kafka_add_broker
CREATE TABLE pipeline_kafka_brokers (
	host text PRIMARY KEY
) WITH OIDS;


CREATE FUNCTION kafka_consume_begin(topic text, relation text,
	format text DEFAULT 'text', delimiter text DEFAULT '\t', batchsize integer DEFAULT 1000, parallelism integer DEFAULT 1,	quote text DEFAULT '"')

RETURNS text
AS 'MODULE_PATHNAME', 'kafka_consume_begin_tr'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION kafka_consume_end(topic text, relation text)
RETURNS text
AS 'MODULE_PATHNAME', 'kafka_consume_end_tr'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION kafka_consume_begin()
RETURNS text
AS 'MODULE_PATHNAME', 'kafka_consume_begin_all'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION kafka_consume_end()
RETURNS text
AS 'MODULE_PATHNAME', 'kafka_consume_end_all'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION kafka_add_broker(host text)
RETURNS text
AS 'MODULE_PATHNAME', 'kafka_add_broker'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION kafka_remove_broker(host text)
RETURNS text
AS 'MODULE_PATHNAME', 'kafka_remove_broker'
LANGUAGE C IMMUTABLE;
