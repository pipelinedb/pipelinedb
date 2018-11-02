from base import async_insert, pipeline, clean_db
import getpass
import os
import psycopg2
import pytest
import tempfile
import threading
import time


def test_create_drop_continuous_view(pipeline, clean_db):
  """
  Basic sanity check
  """
  pipeline.create_stream('stream0', id='int')
  pipeline.create_cv('cv0', 'SELECT id::integer FROM stream0')
  pipeline.create_cv('cv1', 'SELECT id::integer FROM stream0')
  pipeline.create_cv('cv2', 'SELECT id::integer FROM stream0')

  result = pipeline.execute('SELECT name FROM pipelinedb.get_views()')
  names = [r['name'] for r in result]

  assert sorted(names) == ['cv0', 'cv1', 'cv2']

  pipeline.drop_cv('cv0')
  pipeline.drop_cv('cv1')
  pipeline.drop_cv('cv2')

  result = pipeline.execute('SELECT name FROM pipelinedb.get_views()')
  names = [r['name'] for r in result]

  assert len(names) == 0


def test_simple_insert(pipeline, clean_db):
  """
  Verify that we can insert some rows and count some stuff
  """
  pipeline.create_stream('stream0', key='int')
  pipeline.create_cv('cv',
             'SELECT key::integer, COUNT(*) FROM stream0 GROUP BY key')

  rows = [(n % 10,) for n in range(1000)]

  pipeline.insert('stream0', ('key',), rows)

  result = list(pipeline.execute('SELECT key, count FROM cv ORDER BY key'))

  assert len(result) == 10
  for i, row in enumerate(result):
    assert row['key'] == i
    assert row['count'] == 100


def test_multiple(pipeline, clean_db):
  """
  Verify that multiple continuous views work together properly
  """
  pipeline.create_stream('stream0', n='numeric', s='text', unused='int')
  pipeline.create_cv('cv0', 'SELECT n::numeric FROM stream0 WHERE n > 10.00001')
  pipeline.create_cv('cv1',
             'SELECT s::text FROM stream0 WHERE s LIKE \'%%this%%\'')

  rows = [(float(n + 10), 'this', 100) for n in range(1000)]
  for n in range(10):
    rows.append((float(n), 'not a match', -n))

  pipeline.insert('stream0', ('n', 's', 'unused'), rows)

  result = list(pipeline.execute('SELECT * FROM cv0'))
  assert len(result) == 999

  result = list(pipeline.execute('SELECT * FROM cv1'))
  assert len(result) == 1000


def test_combine(pipeline, clean_db):
  """
  Verify that partial tuples are combined with on-disk tuples
  """
  pipeline.create_stream('stream0', key='text', unused='int')
  pipeline.create_cv('combine',
             'SELECT key::text, COUNT(*) FROM stream0 GROUP BY key')

  rows = []
  for n in range(100):
    for m in range(100):
      key = '%d%d' % (n % 10, m)
      rows.append((key, 0))

  pipeline.insert('stream0', ('key', 'unused'), rows)

  total = 0
  result = pipeline.execute('SELECT count FROM combine')
  for row in result:
    total += row['count']

  assert total == 10000


def test_multiple_stmts(pipeline, clean_db):
  pipeline.create_stream('stream0', unused='int')
  conn = psycopg2.connect('dbname=postgres user=%s host=localhost port=%s'
              % (getpass.getuser(), pipeline.port))
  db = conn.cursor()
  db.execute('CREATE VIEW test_multiple AS '
         'SELECT COUNT(*) FROM stream0; SELECT 1;')
  conn.commit()
  conn.close()

  pipeline.insert('stream0', ('unused',), [(1,)] * 100)

  result = list(pipeline.execute('SELECT count FROM test_multiple'))
  assert len(result) == 1
  assert result[0]['count'] == 100


def test_uniqueness(pipeline, clean_db):
  pipeline.create_stream('stream0', x='int')
  pipeline.create_cv('uniqueness',
             'SELECT x::int, count(*) FROM stream0 GROUP BY x')

  for i in range(10):
    rows = [((10000 * i) + j,) for j in xrange(10000)]
    pipeline.insert('stream0', ('x',), rows)

  count = pipeline.execute('SELECT count(*) FROM uniqueness')[0][0]
  distinct_count = pipeline.execute('SELECT count(DISTINCT x) FROM uniqueness')[0][0]

  assert count == distinct_count

@async_insert
def test_concurrent_inserts(pipeline, clean_db):
  pipeline.create_stream('stream0', x='int')
  pipeline.create_cv('concurrent_inserts0',
             'SELECT x::int, count(*) FROM stream0 GROUP BY x')
  pipeline.create_cv('concurrent_inserts1', 'SELECT count(*) FROM stream0')

  num_threads = 4
  stop = False
  inserted = [0] * num_threads

  def insert(i):
    conn = psycopg2.connect('dbname=postgres user=%s host=localhost port=%s'
                % (getpass.getuser(), pipeline.port))
    cur = conn.cursor()
    while not stop:
      cur.execute('INSERT INTO stream0 (x) '
            'SELECT x % 100 FROM generate_series(1, 2000) AS x')
      conn.commit()
      inserted[i] += 2000
    conn.close()

  threads = [threading.Thread(target=insert, args=(i,))
         for i in range(num_threads)]
  map(lambda t: t.start(), threads)

  time.sleep(60)

  stop = True
  map(lambda t: t.join(), threads)

  time.sleep(5)

  total = pipeline.execute('SELECT sum(count) FROM concurrent_inserts0')[0]['sum']
  assert total == sum(inserted)

  total = pipeline.execute('SELECT count FROM concurrent_inserts1')[0]['count']
  assert total == sum(inserted)

@async_insert
def test_concurrent_copy(pipeline, clean_db):
  pipeline.create_stream('stream0', x='int')
  pipeline.create_cv('concurrent_copy0',
             'SELECT x::int, count(*) FROM stream0 GROUP BY x')
  pipeline.create_cv('concurrent_copy1', 'SELECT count(*) FROM stream0')

  tmp_file = os.path.join(tempfile.gettempdir(), 'tmp.copy')
  query = 'SELECT generate_series(1, 2000) AS x'
  pipeline.execute("COPY (%s) TO '%s'" % (query, tmp_file))

  num_threads = 4
  stop = False
  inserted = [0] * num_threads

  def insert(i):
    conn = psycopg2.connect('dbname=postgres user=%s host=localhost port=%s'
                % (getpass.getuser(), pipeline.port))
    cur = conn.cursor()
    while not stop:
      cur.execute("COPY stream0 (x) FROM '%s'" % tmp_file)
      conn.commit()
      inserted[i] += 2000
    conn.close()

  threads = [threading.Thread(target=insert, args=(i,))
         for i in range(num_threads)]
  map(lambda t: t.start(), threads)

  time.sleep(60)

  stop = True
  map(lambda t: t.join(), threads)

  time.sleep(5)

  total = pipeline.execute('SELECT sum(count) FROM concurrent_copy0')[0][0]
  assert total == sum(inserted)

  total = pipeline.execute('SELECT count FROM concurrent_copy1')[0][0]
  assert total == sum(inserted)

def test_drop_mrel_column(pipeline, clean_db):
  """
  Verify that we can't drop matrel columns
  """
  pipeline.create_stream('mrel_drop_s', x='integer')
  q = """
  SELECT x, sum(x), avg(x), count(*) FROM mrel_drop_s GROUP BY x
  """
  pipeline.create_cv('mrel_drop_cv', q)

  for col in ('x', 'sum', 'avg', 'count'):
    with pytest.raises(psycopg2.InternalError):
      pipeline.execute('ALTER TABLE mrel_drop_cv_mrel DROP COLUMN %s' % col)
