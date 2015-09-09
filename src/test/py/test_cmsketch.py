from base import pipeline, clean_db


def test_cmsketch_agg(pipeline, clean_db):
    """
    Test cmsketch_agg, cmsketch_merge_agg, cmsketch_cdf, cmsketch_quantile
    """
    q = """
    SELECT k::integer, cmsketch_agg(x::int) AS c FROM test_cmsketch_stream
    GROUP BY k
    """
    desc = ('k', 'x')
    pipeline.create_cv('test_cmsketch_agg', q)

    rows = []
    for n in range(1000):
        rows.append((0, n % 20))
        rows.append((1, n % 50))

    pipeline.insert('test_cmsketch_stream', desc, rows)

    result = list(pipeline.execute(
      'SELECT cmsketch_frequency(c, 10) AS x, cmsketch_frequency(c, 40) AS y, '
      'cmsketch_frequency(c, 60) FROM test_cmsketch_agg ORDER BY k').fetchall())
    assert len(result) == 2
    assert tuple(result[0]) == (50, 0, 0)
    assert tuple(result[1]) == (20, 20, 0)

    result = list(pipeline.execute(
      'SELECT cmsketch_frequency(combine(c), 10) AS x, '
      'cmsketch_frequency(combine(c), 40) AS y, cmsketch_frequency(combine(c), 60) '
      'FROM test_cmsketch_agg').fetchall())
    assert len(result) == 1
    assert tuple(result[0]) == (70, 20, 0)

def test_cmsketch_type(pipeline, clean_db):
  pipeline.create_table('test_cmsketch_type', x='int', y='cmsketch')
  pipeline.execute('INSERT INTO test_cmsketch_type (x, y) VALUES '
                   '(1, cmsketch_empty()), (2, cmsketch_empty())')

  for i in xrange(1000):
    pipeline.execute('UPDATE test_cmsketch_type '
                     'SET y = cmsketch_add(y, {} %% x)'.format(i))

  result = list(pipeline.execute('SELECT cmsketch_frequency(y, 0), '
                                 'cmsketch_frequency(y, 1) '
                                 'FROM test_cmsketch_type ORDER BY x'))
  assert result[0] == (1000, 0)
  assert result[1] == (500, 500)
