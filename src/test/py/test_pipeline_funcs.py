from base import async_insert, pipeline, clean_db
import getpass
import psycopg2
import threading
import time


def test_combine_table(pipeline, clean_db):
  pipeline.create_stream('s', x='int')
  pipeline.create_cv('combine_table',
                     'SELECT x::int, COUNT(*) FROM s GROUP BY x')

  values = [(i,) for i in xrange(1000)]
  pipeline.insert('s', ('x',), values)

  pipeline.execute('SELECT * INTO tmprel FROM combine_table_mrel')

  stop = False
  ninserts = [0]

  def insert():
    while not stop:
      pipeline.insert('s', ('x',), values)
      ninserts[0] += 1
      time.sleep(0.01)

  t = threading.Thread(target=insert)
  t.start()

  time.sleep(2)

  conn = psycopg2.connect('dbname=postgres user=%s host=localhost port=%s' %
                          (getpass.getuser(), pipeline.port))
  cur = conn.cursor()
  cur.execute("SELECT pipelinedb.combine_table('combine_table', 'tmprel')")
  conn.commit()
  conn.close()

  stop = True
  t.join()

  assert ninserts[0] > 0

  rows = list(pipeline.execute('SELECT count FROM combine_table'))
  assert len(rows) == 1000
  for row in rows:
    assert row[0] == ninserts[0] + 2

  pipeline.execute('DROP TABLE tmprel')


def test_combine_table_no_groups(pipeline, clean_db):
  pipeline.create_stream('s', x='int')
  pipeline.create_cv('no_groups', 'SELECT COUNT(*) FROM s')
  values = [(i,) for i in xrange(1000)]
  pipeline.insert('s', ('x',), values)

  pipeline.execute('SELECT * INTO tmprel FROM no_groups_mrel')
  pipeline.execute("SELECT pipelinedb.combine_table('no_groups', 'tmprel')")

  rows = pipeline.execute('SELECT count FROM no_groups')
  assert len(rows) == 1
  assert len(rows[0]) == 2
  assert rows[0][0] == 2000


def test_pipeline_flush(pipeline, clean_db):
  pipeline.execute('SET pipelinedb.stream_insert_level=async')
  pipeline.create_stream('s', x='int')

  pipeline.create_cv('flush', 'SELECT x, cq_sleep(0.01) FROM s')

  values = [(i,) for i in xrange(1000)]
  start = time.time()

  # This will take 0.01 * 1000 = 10s to process but return immediately since
  # inserts are async and values will fit in one batch.
  pipeline.insert('s', ('x',), values)
  insert_end = time.time()

  pipeline.execute('SELECT pipelinedb.flush()')
  flush_end = time.time()

  assert insert_end - start < 0.1
  assert flush_end - start > 10

  row = list(pipeline.execute('SELECT count(*) FROM flush'))[0]
  assert row[0] == 1000

  pipeline.execute('SET pipelinedb.stream_insert_level=sync_commit')
