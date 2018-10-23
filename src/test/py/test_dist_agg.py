from base import pipeline, clean_db


def test_dist_agg(pipeline, clean_db):
  """
  Test dist_agg, dist_merge_agg, dist_cdf, dist_quantile
  """
  q = """
  SELECT k::integer, dist_agg(x::int) AS t FROM test_tdigest_stream
  GROUP BY k
  """
  desc = ('k', 'x')
  pipeline.create_stream('test_tdigest_stream', k='int', x='int')
  pipeline.create_cv('test_tdigest_agg', q)

  rows = []
  for _ in range(10):
    for n in range(1000):
      rows.append((0, n))
      rows.append((1, n + 500))

  pipeline.insert('test_tdigest_stream', desc, rows)

  result = pipeline.execute('SELECT dist_quantile(t, 0.1) FROM test_tdigest_agg ORDER BY k')
  assert len(result) == 2
  assert abs(int(result[0]['dist_quantile']) - 99) <= 1
  assert abs(int(result[1]['dist_quantile']) - 599) <= 1

  result = pipeline.execute('SELECT dist_quantile(combine(t), 0.1) FROM test_tdigest_agg')
  assert len(result) == 1
  assert abs(int(result[0]['dist_quantile']) - 200) <= 4

  result = pipeline.execute('SELECT dist_cdf(t, 600) FROM test_tdigest_agg ORDER BY k')
  assert len(result) == 2
  assert round(result[0]['dist_cdf'], 2) == 0.6
  assert round(result[1]['dist_cdf'], 2) == 0.1

  result = pipeline.execute('SELECT dist_cdf(combine(t), 600) FROM test_tdigest_agg')
  assert len(result) == 1
  assert round(result[0]['dist_cdf'], 2) == 0.35

def test_tdigest_type(pipeline, clean_db):
  pipeline.create_table('test_tdigest_type', x='int', y='tdigest')
  pipeline.execute('INSERT INTO test_tdigest_type (x, y) VALUES '
       '(1, tdigest_empty()), (2, tdigest_empty())')

  for i in xrange(1000):
    pipeline.execute('UPDATE test_tdigest_type SET y = dist_add(y, %d %% (x * 500))' % i)

  result = list(pipeline.execute('SELECT dist_cdf(y, 400), '
           'dist_quantile(y, 0.9)'
           'FROM test_tdigest_type ORDER BY x'))
  assert map(lambda x: round(x, 1), (result[0]['dist_cdf'], result[0]['dist_quantile'])) == [0.8, 449.5]
  assert map(lambda x: round(x, 1), (result[1]['dist_cdf'], result[1]['dist_quantile'])) == [0.4, 899.5]
