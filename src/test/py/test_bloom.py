from base import pipeline, clean_db
import random


def test_user_low_and_high_card(pipeline, clean_db):
  """
  Verify that Bloom filters's with low and high cardinalities are correcly
  unioned
  """
  pipeline.create_stream('test_bloom_stream', x='int', k='int')

  q = """
  SELECT k::integer, bloom_agg(x::integer) FROM test_bloom_stream GROUP BY k
  """
  desc = ('k', 'x')
  pipeline.create_cv('test_bloom_agg', q)

  # Low cardinalities
  rows = []
  for n in range(1000):
    rows.append((0, random.choice((-1, -2))))
    rows.append((1, random.choice((-3, -4))))

  # High cardinalities
  for n in range(10000):
    rows.append((2, n))
    rows.append((3, n))

  pipeline.insert('test_bloom_stream', desc, rows)

  result = pipeline.execute('SELECT bloom_cardinality(combine(bloom_agg)) '
                'FROM test_bloom_agg WHERE k in (0, 1)')[0]
  assert result[0] == 4

  result = pipeline.execute('SELECT bloom_cardinality(combine(bloom_agg)) '
                'FROM test_bloom_agg WHERE k in (2, 3)')[0]
  assert result[0] == 8879

  result = pipeline.execute('SELECT bloom_cardinality(combine(bloom_agg)) '
                'FROM test_bloom_agg')[0]
  assert result[0] == 8881


def test_bloom_agg_hashing(pipeline, clean_db):
  """
  Verify that bloom_agg correctly hashes different input types
  """
  pipeline.create_stream('test_bloom_stream', x='int', y='text', z='float8')

  q = """
  SELECT bloom_agg(x::integer) AS i,
  bloom_agg(y::text) AS t,
  bloom_agg(z::float8) AS f FROM test_bloom_stream
  """
  desc = ('x', 'y', 'z')
  pipeline.create_cv('test_bloom_hashing', q)

  rows = []
  for n in range(10000):
    rows.append((n, '%d' % n, float(n)))
    rows.append((n, '%05d' % n, float(n)))

  pipeline.insert('test_bloom_stream', desc, rows)

  cvq = """
  SELECT bloom_cardinality(i),
  bloom_cardinality(t), bloom_cardinality(f) FROM test_bloom_hashing
  """
  result = pipeline.execute(cvq)

  assert len(result) == 1

  result = result[0]

  assert result[0] == 8879
  assert result[1] == 15614
  assert result[2] == 8855


def test_bloom_intersection(pipeline, clean_db):
  """
  Verify that bloom_intersection works
  """
  pipeline.create_stream('test_bloom_stream', x='int', k='int')

  q = """
  SELECT k::int, bloom_agg(x::integer) FROM test_bloom_stream GROUP BY k
  """

  desc = ('k', 'x')
  pipeline.create_cv('test_bloom_intersection', q)

  rows = []
  for i in range(10000):
    rows.append((0, 2 * i))
    rows.append((1, i))

  pipeline.insert('test_bloom_stream', desc, rows)

  cvq = """
  SELECT bloom_cardinality(bloom_intersection_agg(bloom_agg))
  FROM test_bloom_intersection
  """

  result = pipeline.execute(cvq)

  assert len(result) == 1

  result = result[0]

  assert result[0] == 5530


def test_bloom_contains(pipeline, clean_db):
  """
  Verify that bloom_contains works
  """
  pipeline.create_stream('test_bloom_stream', x='int')

  q = """
  SELECT bloom_agg(x::integer) FROM test_bloom_stream
  """

  desc = ('x')
  pipeline.create_cv('test_bloom_contains', q)

  rows = []
  for i in range(10000):
    rows.append((2 * i, ))

  pipeline.insert('test_bloom_stream', desc, rows)

  cvq = """
  SELECT bloom_contains(bloom_agg, 0), bloom_contains(bloom_agg, 5000),
  bloom_contains(bloom_agg, 1), bloom_contains(bloom_agg, 5001)
  FROM test_bloom_contains
  """

  result = pipeline.execute(cvq)

  assert len(result) == 1
  result = result[0]
  assert result[0] == True
  assert result[1] == True
  assert result[2] == False
  assert result[3] == False


def test_bloom_type(pipeline, clean_db):
  pipeline.create_table('test_bloom_type', x='int', y='bloom')
  pipeline.execute('INSERT INTO test_bloom_type (x, y) VALUES '
           '(1, bloom_empty()), (2, bloom_empty())')

  for i in xrange(1000):
    pipeline.execute('UPDATE test_bloom_type SET y = bloom_add(y, %d / x)' % i)

  result = pipeline.execute('SELECT bloom_cardinality(y) FROM test_bloom_type ORDER BY x')
  assert result[0][0] == 986
  assert result[1][0] == 495
