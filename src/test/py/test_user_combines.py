from base import pipeline, clean_db
import random


def test_simple_aggs(pipeline, clean_db):
  """
  Verify that combines work properly on simple aggs
  """
  q = """
  SELECT x::integer % 10 AS k,
  avg(x), sum(y::float8), count(*) FROM stream0 GROUP BY k;
  """
  desc = ('x', 'y')
  pipeline.create_stream('stream0', x='int', y='float8')
  pipeline.create_cv('test_simple_aggs', q)
  pipeline.create_table('test_simple_aggs_t', x='integer', y='float8')

  rows = []
  for n in range(10000):
    row = (random.randint(0, 1000), random.random())
    rows.append(row)

  pipeline.insert('stream0', desc, rows)
  pipeline.insert('test_simple_aggs_t', desc, rows)

  table_result = pipeline.execute('SELECT avg(x), sum(y::float8), count(*) FROM test_simple_aggs_t')
  cv_result = pipeline.execute('SELECT combine(avg), combine(sum), combine(count) FROM test_simple_aggs')

  assert len(table_result) == len(cv_result)

  for tr, cr in zip(table_result, cv_result):
    assert abs(tr[0] - cr[0]) < 0.00001
    assert abs(tr[1] - cr[1]) < 0.00001
    assert abs(tr[2] - cr[2]) < 0.00001

def test_object_aggs(pipeline, clean_db):
  """
  Verify that combines work properly on object aggs
  """
  q = """
  SELECT x::integer % 10 AS k,
  json_agg(x), json_object_agg(x, y::float8), string_agg(s::text, \' :: \')FROM stream0 GROUP BY k;
  """
  desc = ('x', 'y', 's')
  pipeline.create_stream('stream0', x='int', y='float8', s='text')
  pipeline.create_cv('test_object_aggs', q)
  pipeline.create_table('test_object_aggs_t', x='integer', y='float8', s='text')

  rows = []
  for n in range(10000):
    row = (random.randint(0, 1000), random.random(), str(n) * random.randint(1, 8))
    rows.append(row)

  pipeline.insert('stream0', desc, rows)
  pipeline.insert('test_object_aggs_t', desc, rows)

  tq = """
  SELECT json_agg(x), json_object_agg(x, y::float8), string_agg(s::text, \' :: \') FROM test_object_aggs_t
  """
  table_result = pipeline.execute(tq)

  cq = """
  SELECT combine(json_agg), combine(json_object_agg), combine(string_agg) FROM test_object_aggs
  """
  cv_result = pipeline.execute(cq)

  assert len(table_result) == len(cv_result)

  for tr, cr in zip(table_result, cv_result):
    assert sorted(tr[0]) == sorted(cr[0])
    assert sorted(tr[1]) == sorted(cr[1])
    assert sorted(tr[2]) == sorted(cr[2])

def test_stats_aggs(pipeline, clean_db):
  """
  Verify that combines work on stats aggs
  """
  q = """
  SELECT x::integer % 10 AS k,
  regr_sxx(x, y::float8), stddev(x) FROM stream0 GROUP BY k;
  """
  desc = ('x', 'y')
  pipeline.create_stream('stream0', x='int', y='float8')
  pipeline.create_cv('test_stats_aggs', q)
  pipeline.create_table('test_stats_aggs_t', x='integer', y='float8')

  rows = []
  for n in range(10000):
    row = (random.randint(0, 1000), random.random())
    rows.append(row)

  pipeline.insert('stream0', desc, rows)
  pipeline.insert('test_stats_aggs_t', desc, rows)

  tq = """
  SELECT regr_sxx(x, y::float8), stddev(x) FROM test_stats_aggs_t
  """
  table_result = pipeline.execute(tq)

  cq = """
  SELECT combine(regr_sxx), combine(stddev) FROM test_stats_aggs
  """
  cv_result = pipeline.execute(cq)

  assert len(table_result) == len(cv_result)

  for tr, cr in zip(table_result, cv_result):
    assert abs(tr[0] - cr[0]) < 0.00001
    assert abs(tr[1] - cr[1]) < 0.00001

def test_hypothetical_set_aggs(pipeline, clean_db):
  """
  Verify that combines work properly on HS aggs
  """
  q = """
  SELECT x::integer % 10 AS k,
  rank(256) WITHIN GROUP (ORDER BY x),
  dense_rank(256) WITHIN GROUP (ORDER BY x)
  FROM stream0 GROUP BY k
  """
  desc = ('x', 'y')
  pipeline.create_stream('stream0', x='int', y='float8')
  pipeline.create_cv('test_hs_aggs', q)
  pipeline.create_table('test_hs_aggs_t', x='integer', y='float8')

  rows = []
  for n in range(10000):
    row = (random.randint(0, 1000), random.random())
    rows.append(row)

  pipeline.insert('stream0', desc, rows)
  pipeline.insert('test_hs_aggs_t', desc, rows)

  # Note that the CQ will use the combinable variant of dense_rank,
  # so use that on the table too
  tq = """
  SELECT rank(256) WITHIN GROUP (ORDER BY x), combinable_dense_rank(256, x)
  FROM test_hs_aggs_t
  """
  table_result = pipeline.execute(tq)

  cq = """
  SELECT combine(rank), combine(dense_rank) FROM test_hs_aggs
  """
  cv_result = pipeline.execute(cq)

  assert len(table_result) == len(cv_result)

  for tr, cr in zip(table_result, cv_result):
    assert tr[0] == cr[0]
    assert tr[1] == cr[1]

def test_hll_distinct(pipeline, clean_db):
  """
  Verify that combines work on HLL COUNT DISTINCT queries
  """
  q = """
  SELECT x::integer % 10 AS k, COUNT(DISTINCT x) AS count FROM stream0 GROUP BY k
  """
  desc = ('x', 'y')
  pipeline.create_stream('stream0', x='int', y='float8')
  pipeline.create_cv('test_hll_distinct', q)
  pipeline.create_table('test_hll_distinct_t', x='integer', y='float8')

  rows = []
  for n in range(10000):
    row = (random.randint(0, 1000), random.random())
    rows.append(row)

  pipeline.insert('stream0', desc, rows)
  pipeline.insert('test_hll_distinct_t', desc, rows)

  # Note that the CQ will use the HLL variant of COUNT DISTINCT,
  # so use hll_count_distinct on the table too
  tq = """
  SELECT hll_count_distinct(x) FROM test_hll_distinct_t
  """
  table_result = pipeline.execute(tq)

  cq = """
  SELECT combine(count) FROM test_hll_distinct
  """
  cv_result = pipeline.execute(cq)

  assert len(table_result) == len(cv_result)

  for tr, cr in zip(table_result, cv_result):
    assert tr[0] == cr[0]

def test_windowed_combine(pipeline, clean_db):
  """
  Verify that windowed queries with combines work
  """
  q = """
  SELECT x::integer, avg(y::integer) FROM stream0 GROUP BY x
  """
  desc = ('x', 'y')
  pipeline.create_stream('stream0', x='int', y='float8')
  pipeline.create_cv('test_windowed_combine', q)
  pipeline.create_table('test_windowed_combine_t', x='integer', y='integer')

  rows = []
  for n in range(10000):
    row = (n, n)
    rows.append(row)

  pipeline.insert('stream0', desc, rows)
  pipeline.insert('test_windowed_combine_t', desc, rows)

  table = """
  SELECT first_value(x) OVER w, avg(y) OVER w
  FROM test_windowed_combine_t
  WINDOW w AS (ORDER BY x ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING)
  ORDER BY first_value
  """
  expected = pipeline.execute(table)
  combine = """
  SELECT first_value(x) OVER w, avg(avg) OVER w
  FROM test_windowed_combine
  WINDOW w AS (ORDER BY x ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING)
  ORDER BY first_value
  """
  actual = pipeline.execute(combine)

  for e, a in zip(expected, actual):
    assert e[0] == a[0]
    assert e[1] == a[1]

def test_combine_in_view(pipeline, clean_db):
  """
  Verify that combines in views on top of continuous views work
  """
  q = """
  SELECT x::integer, avg(y::integer) FROM stream0 GROUP BY x
  """
  desc = ('x', 'y')
  pipeline.create_stream('stream0', x='int', y='float8')
  pipeline.create_cv('test_combine_view', q)
  pipeline.execute('CREATE VIEW v AS SELECT combine(avg) FROM test_combine_view')

  rows = []
  for n in range(10000):
    rows.append((random.randint(1, 256), random.randint(1, 1024)))

  pipeline.insert('stream0', desc, rows)

  view = pipeline.execute('SELECT * FROM v')

  assert len(view) == 1

  expected = sum(r[1] for r in rows) / float(len(rows))

  assert abs(float(view[0][0]) - expected) < 0.00001

  pipeline.execute('DROP VIEW v')
