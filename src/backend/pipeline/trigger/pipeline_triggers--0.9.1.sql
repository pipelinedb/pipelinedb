-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pipeline_triggers" to load this file. \quit

CREATE SCHEMA pipeline_triggers;

-- Continuous triggers
CREATE FUNCTION pipeline_triggers.send_alert_new_row()
RETURNS trigger
AS '$libdir/pipeline_triggers', 'send_alert_new_row'
LANGUAGE C IMMUTABLE;
