CREATE CONTINUOUS VIEW cont_trigger AS SELECT count(*) FROM stream;

-- Invalid triggers
CREATE TRIGGER cont_tg BEFORE INSERT ON cont_trigger FOR EACH ROW EXECUTE PROCEDURE pipeline_stream_insert();
CREATE TRIGGER cont_tg AFTER DELETE ON cont_trigger FOR EACH ROW EXECUTE PROCEDURE pipeline_stream_insert();
CREATE TRIGGER cont_tg AFTER INSERT ON cont_trigger FOR EACH STATEMENT EXECUTE PROCEDURE pipeline_stream_insert();
CREATE TRIGGER cont_tg AFTER TRUNCATE ON cont_trigger FOR EACH ROW EXECUTE PROCEDURE pipeline_stream_insert();
