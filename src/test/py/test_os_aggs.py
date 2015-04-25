from base import pipeline, clean_db
import random

def test_percentile_cont_agg(pipeline, clean_db):
  range_top = 100000
  q = [0.0, 0.01, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.99, 1.0]

  batches = []
  for _ in xrange(10):
    b = [(random.randint(0, range_top),) for _ in xrange(5000)]
    batches.append(b)

  query = '''SELECT
  percentile_cont(ARRAY[%s])
  WITHIN GROUP (ORDER BY x::integer) FROM %s
  ''' % (', '.join(map(lambda f: str(f), q)), '%s')

  pipeline.create_cv('test_cq_percentile_cont', query % 'test_stream')
  pipeline.create_table('test_percentile_cont', x='integer')

  pipeline.activate()

  for b in batches:
    pipeline.insert('test_stream', ('x',), b)
    pipeline.insert('test_percentile_cont', ('x',), b)

  pipeline.deactivate()

  actual = pipeline.execute(query % 'test_percentile_cont')
  result = pipeline.execute('SELECT * FROM test_cq_percentile_cont')

  actual = actual.first()['percentile_cont']
  result = result.first()['percentile_cont']

  assert len(actual) == len(result)
  assert result == sorted(result)
  diff = [abs(actual[i] - result[i]) for i in xrange(len(actual))]

  # 0th and 100th percentile should be accurate.
  assert diff[0] == 0
  assert int(diff[-1]) == 0

  # 1st and 99th percentile should be within 0.01%.
  assert diff[1] <= 0.001 * range_top
  assert diff[-2] <= 0.001 * range_top

  # All percentiles should be within 0.3%.
  assert all(x <= 0.005 * range_top for x in diff)
