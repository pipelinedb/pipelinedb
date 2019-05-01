
-- OIDs are finicky and PG is moving away from them
ALTER TABLE pipelinedb.cont_query SET WITHOUT OIDS;

-- ALTER TABLE pipelinedb.cont_query ADD partition_duration int4;
-- ALTER TABLE pipelinedb.cont_query ADD partition_key_attno int2;
