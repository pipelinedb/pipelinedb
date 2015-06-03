from base import pipeline, clean_db


def test_tdigest_agg(pipeline, clean_db):
    """
    Test tdigest_agg, tdigest_merge_agg, tdigest_cdf, tdigest_quantile
    """
    q = """
    SELECT k::integer, tdigest_agg(x::int) AS t FROM test_tdigest_stream
    GROUP BY k
    """
    desc = ('k', 'x')
    pipeline.create_cv('test_tdigest_agg', q)

    rows = []
    for _ in range(10):
      for n in range(1000):
        rows.append((0, n))
        rows.append((1, n + 500))

    pipeline.insert('test_tdigest_stream', desc, rows)

    result = list(pipeline.execute(
      'SELECT tdigest_quantile(t, 0.1) FROM test_tdigest_agg ORDER BY k')
                  .fetchall())
    assert len(result) == 2
    assert abs(int(result[0]['tdigest_quantile']) - 99) <= 1
    assert abs(int(result[1]['tdigest_quantile']) - 599) <= 1

    result = list(pipeline.execute(
      'SELECT tdigest_quantile(combine(t), 0.1) FROM test_tdigest_agg')
                  .fetchall())
    assert len(result) == 1
    assert abs(int(result[0]['tdigest_quantile']) - 200) <= 4

    result = list(pipeline.execute(
      'SELECT tdigest_cdf(t, 600) FROM test_tdigest_agg ORDER BY k')
                  .fetchall())
    assert len(result) == 2
    assert round(result[0]['tdigest_cdf'], 2) == 0.6
    assert round(result[1]['tdigest_cdf'], 2) == 0.1

    result = list(pipeline.execute(
      'SELECT tdigest_cdf(combine(t), 600) FROM test_tdigest_agg').fetchall())
    assert len(result) == 1
    assert round(result[0]['tdigest_cdf'], 2) == 0.35
