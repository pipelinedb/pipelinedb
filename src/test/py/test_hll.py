from base import pipeline, clean_db
import random


def test_user_low_and_high_card(pipeline, clean_db):
  """
  Verify that HLL's with low and high cardinalities are correcly combined
  """
  pipeline.create_stream('test_hll_stream', x='int', k='integer')
  q = """
  SELECT k::integer, hll_agg(x::integer) FROM test_hll_stream GROUP BY k
  """
  desc = ('k', 'x')
  pipeline.create_cv('test_hll_agg', q)

  # Low cardinalities
  rows = []
  for n in range(1000):
    rows.append((0, random.choice((-1, -2))))
    rows.append((1, random.choice((-3, -4))))

  # High cardinalities
  for n in range(10000):
    rows.append((2, n))
    rows.append((3, n))

  pipeline.insert('test_hll_stream', desc, rows)

  result = pipeline.execute('SELECT hll_cardinality(combine(hll_agg)) FROM test_hll_agg WHERE k in (0, 1)')[0]
  assert result[0] == 4

  result = pipeline.execute('SELECT hll_cardinality(combine(hll_agg)) FROM test_hll_agg WHERE k in (2, 3)')[0]
  assert result[0] == 9976

  result = pipeline.execute('SELECT hll_cardinality(combine(hll_agg)) FROM test_hll_agg')[0]
  assert result[0] == 9983


def test_hll_agg_hashing(pipeline, clean_db):
  """
  Verify that hll_agg correctly hashes different input types
  """
  pipeline.create_stream('test_hll_stream', x='int', y='text', z='float8')
  q = """
  SELECT hll_agg(x::integer) AS i,
  hll_agg(y::text) AS t,
  hll_agg(z::float8) AS f FROM test_hll_stream
  """
  desc = ('x', 'y', 'z')
  pipeline.create_cv('test_hll_hashing', q)

  rows = []
  for n in range(10000):
    rows.append((n, '%d' % n, float(n)))
    rows.append((n, '%05d' % n, float(n)))

  pipeline.insert('test_hll_stream', desc, rows)

  cvq = """
  SELECT hll_cardinality(i),
  hll_cardinality(t), hll_cardinality(f) FROM test_hll_hashing
  """
  result = pipeline.execute(cvq)

  assert len(result) == 1

  result = result[0]

  assert result[0] == 9976
  assert result[1] == 19951
  assert result[2] == 10062

def test_hll_type(pipeline, clean_db):
  pipeline.create_table('test_hll_type', x='int', y='hyperloglog')
  pipeline.execute('INSERT INTO test_hll_type (x, y) VALUES '
           '(1, hll_empty()), (2, hll_empty())')

  for i in range(1000):
    pipeline.execute('UPDATE test_hll_type SET y = hll_add(y, %d / x)' % i)

  result = pipeline.execute('SELECT hll_cardinality(y) FROM test_hll_type ORDER BY x')
  assert result[0][0] == 995
  assert result[1][0] == 497
