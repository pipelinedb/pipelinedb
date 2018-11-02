from base import pipeline, clean_db
import psycopg2
import pytest
import random
import time


def _match(r0, r1, cols):
  return tuple(r0[i] for i in cols) == tuple(r1[i] for i in cols)

def _join(left, right, cols):
  result = []
  for l in left:
    for r in right:
      if _match(l, r, cols):
        result.append(l + r)
  return result

def _insert(pipeline, table, rows, sleep=0):
  cols = ['col%d' % c for c in range(len(rows[0]))]
  pipeline.insert(table, cols, rows)
  if sleep:
    time.sleep(sleep)

def _generate_row(n):
  return tuple(random.choice([1, 0]) for n in range(n))

def _generate_rows(num_cols, max_rows):
  return [_generate_row(num_cols) for n in range(1, max_rows)]


def test_join_with_aggs(pipeline, clean_db):
  """
  Verify that joins involving aggregates referencing columns from
  multiple tables work
  """
  num_cols = 4
  join_cols = [1]
  q = """
  SELECT
  sum(s.col0::integer) AS s0,
  sum(a0.col0::integer) AS s1,
  sum(a1.col0::integer) AS s2
  FROM a1 JOIN a0 ON a1.col1 = a0.col1
  JOIN stream0 s ON s.col1::integer = a0.col1
  """
  a0_cols = dict([('col%d' % n, 'integer') for n in range(num_cols)])
  a1_cols = dict([('col%d' % n, 'integer') for n in range(num_cols)])

  pipeline.create_table('a0', **a0_cols)
  pipeline.create_table('a1', **a1_cols)

  a0 = _generate_rows(num_cols, 64)
  a1 = _generate_rows(num_cols, 64)
  s = _generate_rows(num_cols, 64)

  _insert(pipeline, 'a0', a0, 0.1)
  _insert(pipeline, 'a1', a1, 0.1)

  pipeline.create_stream('stream0', **a0_cols)
  pipeline.create_cv('test_agg_join', q)
  _insert(pipeline, 'stream0', s)

  expected = _join(a1, _join(a0, s, join_cols), join_cols)
  result = pipeline.execute('SELECT * FROM test_agg_join')[0]

  # sum of col0 from stream
  s0_expected = sum([r[num_cols * 2] for r in expected])

  # sum of col0 from a0
  s1_expected = sum([r[num_cols * 1] for r in expected])

  # sum of col0 from a1
  s2_expected = sum([r[num_cols * 0] for r in expected])

  assert s0_expected == result['s0']
  assert s1_expected == result['s1']
  assert s2_expected == result['s2']

def test_join_with_where(pipeline, clean_db):
  """
  Verify that stream-table joins using a WHERE clause work properly
  """
  num_cols = 4
  q = """
  SELECT s.col0::integer FROM stream0 s, wt WHERE s.col0 = 1 AND wt.col0 = 1
  """
  wt_cols = dict([('col%d' % n, 'integer') for n in range(num_cols)])

  pipeline.create_table('wt', **wt_cols)
  pipeline.create_table('wt_s', **wt_cols)

  wt = _generate_rows(num_cols, 64)
  s = _generate_rows(num_cols, 64)

  _insert(pipeline, 'wt', wt, 0.1)
  _insert(pipeline, 'wt_s', s, 0.1)

  pipeline.create_stream('stream0', **wt_cols)
  pipeline.create_cv('test_join_where', q)
  _insert(pipeline, 'stream0', s)

  expected = pipeline.execute('SELECT COUNT(*) FROM wt_s s, wt WHERE s.col0 = 1 AND wt.col0 = 1')[0]
  result = pipeline.execute('SELECT COUNT(*) FROM test_join_where')[0]

  assert result['count'] == expected['count']

def test_join_ordering(pipeline, clean_db):
  """
  Verify that the correct plan is generated regardless of the ordering of
  streams and tables.
  """
  num_cols = 8
  join_cols = [0]
  ordering0_cols = dict([('col%d' % n, 'integer') for n in range(num_cols)])
  ordering1_cols = dict([('col%d' % n, 'integer') for n in range(num_cols)])

  pipeline.create_table('ordering0', **ordering0_cols)
  pipeline.create_table('ordering1', **ordering1_cols)

  ordering0 = _generate_rows(num_cols, 64)
  ordering1 = _generate_rows(num_cols, 64)
  _insert(pipeline, 'ordering0', ordering0, 0.1)
  _insert(pipeline, 'ordering1', ordering1, 0.1)

  pipeline.create_stream('stream0', **ordering0_cols)

  # stream, table, table
  q0 = """
  SELECT s.col0::integer, ordering0.col3, ordering1.col4 FROM
  stream0 s JOIN ordering0 ON s.col0 = ordering0.col0
  JOIN ordering1 ON ordering0.col0 = ordering1.col0
  """
  pipeline.create_cv('test_ordering0', q0)

  # table, stream, table
  q1 = """
  SELECT s.col0::integer, ordering0.col3, ordering1.col4 FROM
  ordering0 JOIN stream0 s ON s.col0 = ordering0.col0
  JOIN ordering1 ON ordering0.col0 = ordering1.col0
  """
  pipeline.create_cv('test_ordering1', q1)

  # table, table, stream
  q2 = """
  SELECT s.col0::integer, ordering0.col3, ordering1.col4 FROM
  ordering0 JOIN ordering1 ON ordering0.col0 = ordering1.col0
  JOIN stream0 s ON s.col0 = ordering0.col0
  """
  pipeline.create_cv('test_ordering2', q2)

  s = _generate_rows(num_cols, 64)
  _insert(pipeline, 'stream0', s)

  expected = _join(ordering0, _join(ordering1, s, join_cols), join_cols)

  result0 = pipeline.execute('SELECT COUNT(*) FROM test_ordering0')[0]
  result1 = pipeline.execute('SELECT COUNT(*) FROM test_ordering1')[0]
  result2 = pipeline.execute('SELECT COUNT(*) FROM test_ordering2')[0]

  assert result0['count'] == len(expected)
  assert result1['count'] == len(expected)
  assert result2['count'] == len(expected)

def test_join_across_batches(pipeline, clean_db):
  """
  Verify that stream-table joins are properly built when they
  span across multiple input batches
  """
  num_cols = 4
  join_cols = [0]
  t_cols = dict([('col%d' % n, 'integer') for n in range(num_cols)])
  pipeline.create_table('batch', **t_cols)
  pipeline.create_stream('stream0', **t_cols)

  q = """
  SELECT s.col0::integer FROM batch JOIN stream0 s ON batch.col0 = s.col0
  """

  t = _generate_rows(num_cols, 64)
  _insert(pipeline, 'batch', t, 0.1)

  s = _generate_rows(num_cols, 64)
  pipeline.create_cv('test_batched_join', q)
  _insert(pipeline, 'stream0', s)

  expected = _join(t, s, join_cols)
  result = pipeline.execute('SELECT COUNT(*) FROM test_batched_join')[0]

  assert result['count'] == len(expected)

def test_incremental_join(pipeline, clean_db):
  """
  Verify that join results increase appropriately as we incrementally
  add stream events to the input
  """
  num_cols = 4
  join_cols = [0, 1]
  t_cols = dict([('col%d' % n, 'integer') for n in range(num_cols)])
  pipeline.create_table('inc', **t_cols)
  pipeline.create_stream('stream0', **t_cols)

  q = """
  SELECT s.col0::integer FROM inc JOIN stream0 s ON inc.col0 = s.col0
  AND inc.col1 = s.col1::integer
  """
  t = _generate_rows(num_cols, 64)
  _insert(pipeline, 'inc', t, 0.1)

  pipeline.create_cv('test_join', q)
  s = []
  for n in range(2):
    row = _generate_row(num_cols)
    _insert(pipeline, 'stream0', [row])
    s.append(row)

  expected = _join(t, s, join_cols)
  result = pipeline.execute('SELECT COUNT(*) FROM test_join')[0]

  assert result['count'] == len(expected)

def test_join_multiple_tables(pipeline, clean_db):
  """
  Verify that stream-table joins involving multiple tables work
  """
  num_cols = 8
  join_cols = [0]
  t0_cols = dict([('col%d' % n, 'integer') for n in range(num_cols)])
  t1_cols = dict([('col%d' % n, 'integer') for n in range(num_cols)])

  pipeline.create_table('t0', **t0_cols)
  pipeline.create_table('t1', **t1_cols)
  pipeline.create_stream('stream0', **t0_cols)
  q = """
  SELECT s.col0::integer FROM t0 JOIN t1 ON t0.col0 = t1.col0
  JOIN stream0 s ON t1.col0 = s.col0
  """

  t0 = _generate_rows(num_cols, 64)
  t1 = _generate_rows(num_cols, 64)
  s = _generate_rows(num_cols, 64)

  _insert(pipeline, 't1', t1, 0.1)
  _insert(pipeline, 't0', t0, 0.1)

  pipeline.create_cv('test_join_multi', q)
  _insert(pipeline, 'stream0', s)

  expected = _join(t0, _join(s, t1, join_cols), join_cols)
  result = pipeline.execute('SELECT COUNT(*) FROM test_join_multi')[0]

  assert result['count'] == len(expected)

def test_indexed(pipeline, clean_db):
  """
  Verify that stream-table joins involving indexed tables work
  """
  pipeline.create_stream('stream0', x='int', y='int')
  q = """
  SELECT stream0.x::integer, count(*) FROM stream0
  JOIN test_indexed_t t ON stream0.x = t.x GROUP BY stream0.x
  """
  pipeline.create_table('test_indexed_t', x='integer', y='integer')
  pipeline.execute('CREATE INDEX idx ON test_indexed_t(x)')

  t = _generate_rows(2, 1000)
  s = _generate_rows(2, 1000)

  pipeline.insert('test_indexed_t', ('x', 'y'), t)
  time.sleep(0.1)

  pipeline.create_cv('test_indexed', q)
  pipeline.insert('stream0', ('x', 'y'), s)

  expected = _join(s, t, [0])
  result = pipeline.execute('SELECT sum(count) FROM test_indexed')[0]

  assert result['sum'] == len(expected)

def test_drop_columns(pipeline, clean_db):
  """
  Verify that columns on the table-side of a stream-table join can't be dropped
  """
  pipeline.create_table('drop_t', x='integer', y='integer')
  pipeline.create_stream('drop_s', x='integer')
  q = """
  SELECT s.x, count(*) FROM drop_s s JOIN drop_t t ON s.x = t.x GROUP BY s.x
  """
  pipeline.create_cv('stj_drop_cv', q)

  with pytest.raises(psycopg2.InternalError):
    pipeline.execute('ALTER TABLE drop_t DROP COLUMN x')

  # Columns not being joined on can be dropped though
  pipeline.execute('ALTER TABLE drop_t DROP COLUMN y')
