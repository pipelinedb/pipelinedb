from base import pipeline, clean_db
from datetime import datetime, timedelta
import random


def _test_agg(pipeline, agg, check_fn=None):
    name = agg[:agg.find('(')]
    q = 'SELECT g::integer, %s OVER (PARTITION BY g ORDER BY ts::timestamp) FROM %s'
    cv_name = 'test_%s' % name
    table_name = 'test_%s_t' % name
    desc = ('ts', 'g', 'x', 'y', 'z')

    pipeline.create_cv(cv_name, q % (agg, 'stream'))
    pipeline.create_table(table_name, ts='timestamp', x='integer', y='integer', z='integer', g='integer')

    rows = []
    for i, n in enumerate(range(1000)):
        ts = str(datetime.utcnow() + timedelta(seconds=i))
        row = ts, n % 10, random.randint(1, 256), random.randint(1, 256), random.randint(1, 256)
        rows.append(row)

    pipeline.insert('stream', desc, rows)
    pipeline.insert(table_name, desc, rows)

    if check_fn:
        return check_fn(pipeline)

    expected = list(pipeline.execute(q % (agg, table_name) + ' ORDER BY g'))
    result = list(pipeline.execute('SELECT * FROM %s ORDER BY g' % cv_name))

    assert len(expected) == len(result)

    for e, r in zip(expected, result):
        assert e == r

    pipeline.drop_cv(cv_name)
    pipeline.drop_table(table_name)


def hll_check(pdb):
    """
    The HLLs generated by querying the table may be in sparse
    format, whereas CV HLLs will always be in dense form, so we
    need to just compare the cardinalities
    """
    q = """
    SELECT g, hll_cardinality(hll_agg) FROM
        (SELECT g, hll_agg(x) OVER (PARTITION BY g ORDER BY ts) FROM test_hll_agg_t) _
    ORDER BY g
    """
    expected = list(pdb.execute(q))
    q = """
    SELECT g, hll_cardinality(hll_agg) FROM test_hll_agg ORDER BY g
    """
    result = list(pdb.execute(q))

    assert len(expected) == len(result)

    for e, r in zip(expected, result):
        assert e == r

def tdigest_check(pdb):
  """
  t-digests may compress differently, so only check that count (total weight)
  if identical
  """
  def get_count(s):
    # s looks like: { count = 2, k = 200, centroids: 1 }
    return int(s.split('=')[1].strip().split(',')[0])

  q = """
  SELECT g, tdigest_agg(x) OVER (PARTITION BY g ORDER BY ts)
  FROM test_tdigest_agg_t ORDER BY g
  """
  expected = list(pdb.execute(q))
  q = """
  SELECT g, tdigest_agg FROM test_tdigest_agg ORDER BY g
  """
  result = list(pdb.execute(q))

  assert len(expected) == len(result)

  for e, r in zip(expected, result):
    assert e['g'] == r['g']
    assert get_count(e['tdigest_agg']) == get_count(r['tdigest_agg'])

def test_aggs(pipeline, clean_db):
    """
    Verify that aggregates work properly when windowed in continuous views
    """
    _test_agg(pipeline, 'sum(x::integer)')
    _test_agg(pipeline, 'avg(x::integer)')
    _test_agg(pipeline, 'count(*)')
    _test_agg(pipeline, 'regr_sxx(x::float8, y::float8)')
    _test_agg(pipeline, 'covar_pop(x::integer, y::integer)')
    _test_agg(pipeline, 'covar_samp(x::integer, y::integer)')
    _test_agg(pipeline, 'regr_avgx(x::integer, y::integer)')
    _test_agg(pipeline, 'regr_avgy(x::integer, y::integer)')
    _test_agg(pipeline, 'regr_count(x::integer, y::integer)')
    _test_agg(pipeline, 'regr_intercept(x::integer, y::integer)')
    _test_agg(pipeline, 'regr_r2(x::integer, y::integer)')
    _test_agg(pipeline, 'regr_slope(x::integer, y::integer)')
    _test_agg(pipeline, 'regr_sxy(x::integer, y::integer)')
    _test_agg(pipeline, 'regr_syy(x::integer, y::integer)')
    _test_agg(pipeline, 'stddev(x::integer)')
    _test_agg(pipeline, 'stddev_pop(x::integer)')
    _test_agg(pipeline, 'variance(x::integer)')
    _test_agg(pipeline, 'var_pop(x::integer)')
    _test_agg(pipeline, 'bloom_agg(x::integer)')
    _test_agg(pipeline, 'cmsketch_agg(x::integer)')
    _test_agg(pipeline, 'hll_agg(x::integer)', check_fn=hll_check)
    _test_agg(pipeline, 'tdigest_agg(x::integer)', check_fn=tdigest_check)
