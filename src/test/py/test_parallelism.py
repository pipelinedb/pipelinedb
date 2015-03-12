from base import pipeline, clean_db
import time


def test_parallelism(pipeline, clean_db):
  """
  Run CQs where workers sleep on each tuple and ensure that multiple workers
  process tuples faster.
  """
  pipeline.create_cv('test_parallelism',
                     'SELECT x::int, pg_sleep(0.01) FROM stream')

  values = [(i, ) for i in xrange(100)]
  num_batches = 10

  pipeline.activate('test_parallelism', parallelism=4)
  pipeline.set_sync_insert(False)
  start = time.time()
  for i in xrange(num_batches):
    pipeline.insert('stream', ('x', ), values)
  pipeline.set_sync_insert(True)
  pipeline.insert('stream', ('x', ), values[:4])
  t1 = time.time() - start
  pipeline.deactivate()

  q = 'SELECT COUNT(*) FROM test_parallelism'
  assert pipeline.execute(q).first()['count'] == num_batches * len(values) + 4

  pipeline.activate('test_parallelism', parallelism=1)
  pipeline.set_sync_insert(False)
  start = time.time()
  for i in xrange(num_batches):
    pipeline.insert('stream', ('x', ), values)
  # The following ensures that all previous events have been consumed.
  pipeline.set_sync_insert(True)
  pipeline.insert('stream', ('x', ), values[:1])
  t2 = time.time() - start
  pipeline.deactivate()

  assert pipeline.execute(q).first()[0] == num_batches * len(values) * 2 + 5
  assert t1 < t2 / 2
