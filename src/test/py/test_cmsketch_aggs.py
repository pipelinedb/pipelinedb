from base import pipeline, clean_db


def test_cmsketch_type(pipeline, clean_db):
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
      'SELECT cmsketch_count(c, 10) AS x, cmsketch_count(c, 40) AS y, cmsketch_count(c, 60) FROM test_cmsketch_agg ORDER BY k').fetchall())
    assert len(result) == 2
    assert tuple(result[0]) == (50, 0, 0)
    assert tuple(result[1]) == (20, 20, 0)

    result = list(pipeline.execute(
      'SELECT cmsketch_count(combine(c), 10) AS x, cmsketch_count(combine(c), 40) AS y, cmsketch_count(combine(c), 60) FROM test_cmsketch_agg').fetchall())
    assert len(result) == 1
    assert tuple(result[0]) == (70, 20, 0)
