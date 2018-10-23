from base import pipeline, clean_db
import random
import time


def test_cq_stats(pipeline, clean_db):
  """
  Verify that CQ statistics collection works
  """
  num_combiners = int(pipeline.execute('SHOW pipelinedb.num_combiners')[0]['pipelinedb.num_combiners'])
  num_workers = int(pipeline.execute('SHOW pipelinedb.num_workers')[0]['pipelinedb.num_workers'])

  pipeline.create_stream('stream0', x='int')

  # 10 rows
  q = 'SELECT x::integer % 10 AS g, COUNT(*) FROM stream0 GROUP BY g'
  pipeline.create_cv('test_10_groups', q)

  # 1 row
  q = 'SELECT COUNT(*) FROM stream0'
  pipeline.create_cv('test_1_group', q)

  values = [(random.randint(1, 1024),) for n in range(1000)]

  pipeline.insert('stream0', ('x',), values)
  pipeline.insert('stream0', ('x',), values)
  # Sleep a little so that the next time we insert, we force the stats collector.
  # Must be >= 1s since that's the force interval.
  time.sleep(1)
  pipeline.insert('stream0', ('x',), values)
  pipeline.insert('stream0', ('x',), values)

  # Sleep a little so the stats collector flushes all the stats.
  time.sleep(1)

  proc_result = pipeline.execute('SELECT * FROM pipelinedb.proc_stats')
  cq_result = pipeline.execute('SELECT * FROM pipelinedb.query_stats')

  proc_rows = len(proc_result)
  cq_rows = len(cq_result)

  # We are guaranteed to send data to all combiners but only at least 1 worker
  # since we randomly select which worker to send the data to.
  assert proc_rows >= num_combiners + 1
  assert proc_rows <= num_combiners + num_workers
  assert cq_rows == 4

  # We get 2000 in case the first two microbatches go to the same worker
  # and the second two go to a different one. In this case, both will flush
  # the first microbatch they see, so 1000 + 1000.
  result = pipeline.execute("SELECT * FROM pipelinedb.query_stats WHERE continuous_query = 'test_10_groups' AND type = 'worker'")[0]
  assert result['input_rows'] in [2000, 3000, 4000]

  result = pipeline.execute("SELECT * FROM pipelinedb.query_stats WHERE continuous_query = 'test_10_groups' AND type = 'combiner'")[0]
  assert result['output_rows'] == 10

  result = pipeline.execute("SELECT * FROM pipelinedb.query_stats WHERE continuous_query = 'test_1_group' AND type = 'worker'")[0]
  assert result['input_rows'] in [2000, 3000, 4000]

  result = pipeline.execute("SELECT * FROM pipelinedb.query_stats WHERE continuous_query = 'test_1_group' AND type = 'combiner'")[0]
  assert result['output_rows'] == 1

  # Update stats should be zero for worker stats
  result = pipeline.execute("SELECT sum(updated_rows), sum(updated_bytes) FROM pipelinedb.proc_query_stats WHERE type = 'worker'")[0]
  assert result[0] == 0
  assert result[1] == 0

def test_stream_stats(pipeline, clean_db):
  """
  Verify that stream-level statistics collection works
  """
  # create a few streams
  for n in range(8):
    sname = 's%d' % n
    pipeline.create_stream(sname, x='int')
    cvname = 'cv%d' % n
    pipeline.create_cv(cvname, 'SELECT count(*) FROM %s' % sname)

  for n in range(8):
    sname = 's%d' % n
    x = n + 1
    values = [(v,) for v in range(1000 * x)]
    pipeline.insert(sname, ('x',), values)

  time.sleep(2)

  for n in range(8):
    sname = 's%d' % n
    row = pipeline.execute("SELECT stream, input_rows, input_batches, input_bytes FROM pipelinedb.stream_stats WHERE stream = '%s'" % sname)[0]
    x = n + 1
    assert row['input_rows'] == 1000 * x
