from base import pipeline, clean_db
import os
import random


def _generate_csv(path, rows, desc=None, delimiter=','):
  csv = open(path, 'wa')
  if desc:
    rows = [desc] + rows
  for row in rows:
    line = delimiter.join(str(v) for v in row) + '\n'
    csv.write(line)
  csv.close()

def test_copy_to_stream(pipeline, clean_db):
  """
  Verify that copying data from a file into a stream works
  """
  pipeline.create_stream('stream0', x='int', y='float8', z='numeric')
  q = 'SELECT sum(x::integer) AS s0, sum(y::float8) AS s1, avg(z::numeric) FROM stream0'
  pipeline.create_cv('test_copy_to_stream', q)
  pipeline.create_table('test_copy_to_stream_t', x='integer', y='float8', z='numeric')

  path = os.path.abspath(os.path.join(pipeline.data_dir, 'test_copy.csv'))

  rows = []
  for n in range(10000):
    row = random.randint(1, 1024), random.randint(1, 1024), random.random()
    rows.append(row)

  _generate_csv(path, rows, desc=('x', 'y', 'z'))

  pipeline.execute('COPY test_copy_to_stream_t (x, y, z) FROM \'%s\' HEADER CSV' % path)

  pipeline.execute('COPY stream0 (x, y, z) FROM \'%s\' HEADER CSV' % path)

  expected = pipeline.execute('SELECT sum(x::integer) AS s0, sum(y::float8) AS s1, avg(z::numeric) FROM test_copy_to_stream_t')[0]
  result = pipeline.execute('SELECT * FROM test_copy_to_stream')

  assert len(result) == 1

  result = result[0]

  assert result[0] == expected[0]
  assert result[1] == expected[1]
  assert result[2] == expected[2]


def test_colums_subset(pipeline, clean_db):
  """
  Verify that copying data from a file into a stream works when the file's input
  columns are a subset of the stream0's columns
  """
  pipeline.create_stream('stream0', x='int', y='float8', z='numeric', m='int')
  q = 'SELECT sum(x::integer) AS s0, sum(y::float8) AS s1, avg(z::numeric), max(m::integer) FROM stream0'
  pipeline.create_cv('test_copy_subset', q)
  pipeline.create_table('test_copy_subset_t', x='integer', y='float8', z='numeric')

  path = os.path.abspath(os.path.join(pipeline.data_dir, 'test_copy.csv'))

  rows = []
  for n in range(10000):
    row = random.randint(1, 1024), random.randint(1, 1024), random.random()
    rows.append(row)

  _generate_csv(path, rows, desc=('x', 'y', 'z'))

  pipeline.execute('COPY test_copy_subset_t (x, y, z) FROM \'%s\' HEADER CSV' % path)

  pipeline.execute('COPY stream0 (x, y, z) FROM \'%s\' HEADER CSV' % path)

  expected = pipeline.execute('SELECT sum(x::integer) AS s0, sum(y::float8) AS s1, avg(z::numeric) FROM test_copy_subset_t')[0]
  result = pipeline.execute('SELECT s0, s1, avg FROM test_copy_subset')

  assert len(result) == 1

  result = result[0]

  assert result[0] == expected[0]
  assert result[1] == expected[1]
  assert result[2] == expected[2]


def test_column_ordering(pipeline, clean_db):
  """
  Verify that we can COPY data having columns ordered differently from the
  target stream's definition
  """
  pipeline.create_stream('test_copy_s', x='int', y='float8', z='text')
  q = 'SELECT x, y, z, count(*) FROM test_copy_s GROUP BY x, y, z'
  pipeline.create_cv('test_ordering', q)

  path = os.path.abspath(os.path.join(pipeline.data_dir, 'test_ordering.csv'))
  q = "SELECT x::text || '!' AS z, x AS y, -x AS x FROM generate_series(1, 10000) x"
  pipeline.execute("COPY (%s) TO '%s'" % (q, path))

  pipeline.execute("COPY test_copy_s (z, y, x) FROM '%s'" % path)
  pipeline.execute("COPY test_copy_s (z, y, x) FROM '%s'" % path)
  rows = pipeline.execute('SELECT * FROM test_ordering')

  assert len(rows) == 10000
  for row in rows:
    assert row['x'] < 0
    assert row['y'] > 0
    assert '!' in row['z']
    assert row['count'] == 2


def test_arrival_timestamp(pipeline, clean_db):
  """
  Verify that we can include arrival_timestamp in COPY data
  """
  pipeline.create_stream('test_ats_s', x='int', y='float8', z='text', order=('x', 'y', 'z'))
  q = 'SELECT arrival_timestamp, x, y, z, count(*) FROM test_ats_s GROUP BY arrival_timestamp, x, y, z'
  pipeline.create_cv('test_ats', q)

  path = os.path.abspath(os.path.join(pipeline.data_dir, 'test_ats.csv'))
  q = "SELECT -x, x::float8, x::text || '!', '2018-09-12 00:00:00' AS arrival_timestamp FROM generate_series(1, 10000) x"
  pipeline.execute("COPY (%s) TO '%s'" % (q, path))

  pipeline.execute("COPY test_ats_s FROM '%s'" % path)
  pipeline.execute("COPY test_ats_s FROM '%s'" % path)
  rows = pipeline.execute('SELECT * FROM test_ats')

  assert len(rows) == 10000
  for row in rows:
    assert row['x'] < 0
    assert row['y'] > 0
    assert '!' in row['z']
    assert row['count'] == 2


def test_regression(pipeline, clean_db):
  path = os.path.abspath(os.path.join(pipeline.data_dir, 'test_copy.csv'))
  _generate_csv(path, [['2015-06-01 00:00:00', 'De', 'Adam_Babareka', '1', '37433']], desc=('day', 'project', 'title', 'count', 'size'))

  pipeline.create_stream('copy_regression_stream', count='int', day='timestamp', project='text', title='text', size='int')
  pipeline.create_cv('test_copy_regression', 'SELECT sum(count) FROM copy_regression_stream')

  pipeline.execute("COPY copy_regression_stream (day, project, title, count, size) FROM '%s' CSV HEADER" % path)
