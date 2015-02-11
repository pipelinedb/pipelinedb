from base import pipeline, clean_db


def test_tdigest_type(pipeline, clean_db):
    """
    Test tdigest_agg, tdigest_merge_agg, tdigest_cdf, tdigest_quantile
    """
    q = """
    SELECT k::integer, tdigest_agg(x::integer) AS t FROM test_tdigest_stream
    GROUP BY k
    """
    desc = ('k', 'x')
    pipeline.create_cv('test_tdigest_agg', q)

    rows = []
    for n in range(1000):
        rows.append((0, n))
        rows.append((1, n + 500))

    pipeline.activate()
    pipeline.insert('test_tdigest_stream', desc, rows)
    pipeline.deactivate()

    result = pipeline.execute(
      'SELECT tdigest_quantile(t, 0.1) FROM test_tdigest_agg')
    assert len(result) == 2
    assert result[0]['tdigest_quantile'] == 100
    assert result[1]['tdigest_quantile'] == 600

    result = pipeline.execute(
      'SELECT tdigest_quantile(combine(t), 1) FROM test_tdigest_agg')
    assert len(result) == 1
    assert result[0]['tdigest_quantile'] == 200

    result = pipeline.execute(
      'SELECT tdigest_cdf(t, 600) FROM test_tdigest_agg')
    assert len(result) == 2
    assert result[0]['tdigest_cdf'] == 0.6
    assert result[1]['tdigest_cdf'] == 0.1

    result = pipeline.execute(
      'SELECT tdigest_cdf(combine(t), 600) FROM test_tdigest_agg')
    assert len(result) == 1
    assert result[0]['tdigest_cdf'] == 200
