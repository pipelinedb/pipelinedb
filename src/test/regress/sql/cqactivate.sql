CREATE CONTINUOUS VIEW cqactivate1 AS SELECT id::integer FROM stream_sync_cq WHERE id=9099;
ACTIVATE cqactivate1;
CREATE CONTINUOUS VIEW cqactivate2 AS SELECT id::integer FROM stream_sync_cq WHERE id=9099;
ACTIVATE cqactivate2;
SELECT name, query FROM pipeline_queries WHERE state='a' AND name='cqactivate1';
SELECT name, query FROM pipeline_queries WHERE state='a' AND name='cqactivate2';
DEACTIVATE cqactivate1;
DEACTIVATE cqactivate2;
SELECT name, query FROM pipeline_queries WHERE state='i' AND name='cqactivate1';
SELECT name, query FROM pipeline_queries WHERE state='i' AND name='cqactivate2';
DEACTIVATE cqactivate1;
DEACTIVATE cqactivate2;

DROP CONTINUOUS VIEW cqactivate1;
DROP CONTINUOUS VIEW cqactivate2;
