from base import pipeline, clean_db


def test_filter_clause(pipeline, clean_db):
  """
  Verify that FILTER clauses work on aggregates and sliding window aggregates
  """
  pipeline.create_stream('test_filter_stream', x='int')
  q = """
  SELECT SUM(x::int) FILTER (WHERE mod(x, 2) = 0) AS sum2, SUM(x::int) FILTER (WHERE mod(x, 3) = 0) AS sum3 FROM test_filter_stream
  """
  sw = """
  WHERE arrival_timestamp > clock_timestamp() - interval '30 second'
  """
  pipeline.create_cv('test_filter', q)
  pipeline.create_cv('test_filter_sw', '%s %s' % (q, sw))

  desc = ('x', )
  rows = []
  for n in range(1000):
    rows.append((n, ))

  pipeline.insert('test_filter_stream', desc, rows)

  sum2 = sum(filter(lambda x: x % 2 == 0, map(lambda x: x[0], rows)))
  sum3 = sum(filter(lambda x: x % 3 == 0, map(lambda x: x[0], rows)))

  result1 = pipeline.execute('SELECT * FROM test_filter')[0]
  result2 = pipeline.execute('SELECT * FROM test_filter_sw')[0]

  assert result1['sum2'] == result2['sum2'] == sum2
  assert result1['sum3'] == result2['sum3'] == sum3
