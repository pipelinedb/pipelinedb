from base import pipeline, clean_db


def test_cms_type(pipeline, clean_db):
    """
    Test cms_agg, cms_merge_agg, cms_cdf, cms_quantile
    """
    q = """
    SELECT k::integer, cms_agg(x::int) AS c FROM test_cms_stream
    GROUP BY k
    """
    desc = ('k', 'x')
    pipeline.create_cv('test_cms_agg', q)

    rows = []
    for n in range(1000):
        rows.append((0, n % 20))
        rows.append((1, n % 50))

    pipeline.activate()
    pipeline.insert('test_cms_stream', desc, rows)
    pipeline.deactivate()

    result = list(pipeline.execute(
      'SELECT cms_count(c, 10) AS x, cms_count(c, 40) AS y, cms_count(c, 60) FROM test_cms_agg').fetchall())
    assert len(result) == 2
    assert tuple(result[0]) == (50, 0, 0)
    assert tuple(result[1]) == (20, 20, 0)

    result = list(pipeline.execute(
      'SELECT cms_count(combine(c), 10) AS x, cms_count(combine(c), 40) AS y, cms_count(combine(c), 60) FROM test_cms_agg').fetchall())
    assert len(result) == 1
    assert tuple(result[0]) == (70, 20, 0)
