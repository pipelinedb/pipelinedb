from base import pipeline, clean_db


def test_freq_agg(pipeline, clean_db):
  """
  Test freq_agg, freq_merge_agg
  """
  pipeline.create_stream('test_cmsketch_stream', k='int', x='int')

  q = """
  SELECT k::integer, freq_agg(x::int) AS c FROM test_cmsketch_stream
  GROUP BY k
  """
  desc = ('k', 'x')
  pipeline.create_cv('test_cmsketch_agg', q)

  rows = []
  for n in range(1000):
    rows.append((0, n % 20))
    rows.append((1, n % 50))

  pipeline.insert('test_cmsketch_stream', desc, rows)

  result = pipeline.execute('SELECT freq(c, 10) AS x, freq(c, 40) AS y, freq(c, 60) FROM test_cmsketch_agg ORDER BY k')
  assert len(result) == 2
  assert (result[0][0], result[0][1], result[0][2]) == (50, 0, 0)
  assert (result[1][0], result[1][1], result[1][2]) == (20, 20, 0)

  result = pipeline.execute('SELECT freq(combine(c), 10) AS x, freq(combine(c), 40) AS y, freq(combine(c), 60) FROM test_cmsketch_agg')
  assert len(result) == 1
  (result[0][0], result[0][1], result[0][2]) == (70, 20, 0)

def test_cmsketch_type(pipeline, clean_db):
  pipeline.create_table('test_cmsketch_type', x='int', y='cmsketch')
  pipeline.execute('INSERT INTO test_cmsketch_type (x, y) VALUES '
           '(1, cmsketch_empty()), (2, cmsketch_empty())')

  for i in range(1000):
    pipeline.execute('UPDATE test_cmsketch_type SET y = freq_add(y, %d %% x)' % i)

  result = list(pipeline.execute('SELECT freq(y, 0), '
                   'freq(y, 1) '
                   'FROM test_cmsketch_type ORDER BY x'))
  assert (result[0][0], result[0][1]) == (1000, 0)
  assert (result[1][0], result[1][1]) == (500, 500)

def test_cksketch_frequency(pipeline, clean_db):
  pipeline.create_stream('test_cmsketch_stream', k='int', x='int')

  q = """
  SELECT k::integer, freq_agg(x::int) AS c FROM test_cmsketch_stream
  GROUP BY k
  """
  desc = ('k', 'x')
  pipeline.create_cv('test_cmsketch_frequency', q)

  rows = [(n, None) for n in range(100)]
  pipeline.insert('test_cmsketch_stream', desc, rows)

  result = pipeline.execute('SELECT freq(c, null) AS x FROM test_cmsketch_frequency ORDER BY k')
  assert len(result) == 100
  for row in result:
    assert row[0] == 0
