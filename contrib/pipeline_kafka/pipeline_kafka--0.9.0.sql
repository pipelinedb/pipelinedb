/* contrib/pipeline_kafka/pipeline_kafka--0.9.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pipeline_kafka" to load this file. \quit

-- Consumers added with kafka_consume_begin
CREATE TABLE pipeline_kafka_consumers (
  relation    text    NOT NULL,
  topic       text    NOT NULL,
  format      text    NOT NULL,
  delimiter   text,
  quote       text,
  escape      text,
  batchsize   integer NOT NULL,
  parallelism integer NOT NULL
) WITH OIDS;

CREATE TABLE pipeline_kafka_offsets (
	consumer_id oid     NOT NULL,
	partition   integer NOT NULL,
	"offset"    bigint  NOT NULL
);
-- Brokers added with kafka_add_broker
CREATE TABLE pipeline_kafka_brokers (
	host text PRIMARY KEY
) WITH OIDS;

CREATE FUNCTION kafka_consume_begin (
  topic        text,
  relation     text,
  format       text    DEFAULT 'text',
  delimiter    text    DEFAULT E'\t',
  quote        text    DEFAULT NULL,
  escape       text    DEFAULT NULL,
  batchsize    integer DEFAULT 1000,
  parallelism  integer DEFAULT 1,
  start_offset bigint  DEFAULT NULL
)
RETURNS text
AS 'MODULE_PATHNAME', 'kafka_consume_begin_tr'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION kafka_consume_end (
  topic    text,
  relation text
)
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

CREATE FUNCTION kafka_add_broker (
  host text
)
RETURNS text
AS 'MODULE_PATHNAME', 'kafka_add_broker'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION kafka_remove_broker (
  host text
)
RETURNS text
AS 'MODULE_PATHNAME', 'kafka_remove_broker'
LANGUAGE C IMMUTABLE;
