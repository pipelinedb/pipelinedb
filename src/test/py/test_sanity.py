from base import pipeline, clean_db
import getpass
import psycopg2


def test_create_drop_continuous_view(pipeline, clean_db):
    """
    Basic sanity check
    """
    pipeline.create_cv('cv0', 'SELECT id::integer FROM stream')
    pipeline.create_cv('cv1', 'SELECT id::integer FROM stream')
    pipeline.create_cv('cv2', 'SELECT id::integer FROM stream')

    result = pipeline.execute('SELECT * FROM pipeline_query')
    names = [r['name'] for r in result]

    assert sorted(names) == ['cv0', 'cv1', 'cv2']

    pipeline.drop_cv('cv0')
    pipeline.drop_cv('cv1')
    pipeline.drop_cv('cv2')

    result = pipeline.execute('SELECT * FROM pipeline_query')
    names = [r['name'] for r in result]

    assert len(names) == 0

def test_simple_insert(pipeline, clean_db):
    """
    Verify that we can insert some rows and count some stuff
    """
    pipeline.create_cv('cv', 'SELECT key::integer, COUNT(*) FROM stream GROUP BY key')

    rows = [(n % 10,) for n in range(1000)]

    pipeline.insert('stream', ('key',), rows)

    result = list(pipeline.execute('SELECT * FROM cv ORDER BY key'))

    assert len(result) == 10
    for i, row in enumerate(result):
        assert row['key'] == i
        assert row['count'] == 100

def test_multiple(pipeline, clean_db):
    """
    Verify that multiple continuous views work together properly
    """
    pipeline.create_cv('cv0', 'SELECT n::numeric FROM stream WHERE n > 10.00001')
    pipeline.create_cv('cv1', 'SELECT s::text FROM stream WHERE s LIKE \'%%this%%\'')

    rows = [(float(n + 10), 'this', 100) for n in range(1000)]
    for n in range(10):
        rows.append((float(n), 'not a match', -n))

    pipeline.insert('stream', ('n', 's', 'unused'), rows)

    result = list(pipeline.execute('SELECT * FROM cv0'))
    assert len(result) == 999

    result = list(pipeline.execute('SELECT * FROM cv1'))
    assert len(result) == 1000

def test_combine(pipeline, clean_db):
    """
    Verify that partial tuples are combined with on-disk tuples
    """
    pipeline.create_cv('combine', 'SELECT key::text, COUNT(*) FROM stream GROUP BY key')

    rows = []
    for n in range(100):
        values = []
        for m in range(100):
            key = '%d%d' % (n % 10, m)
            rows.append((key, 0))

    pipeline.insert('stream', ('key', 'unused'), rows)

    total = 0
    result = pipeline.execute('SELECT * FROM combine')
    for row in result:
        total += row['count']

    assert total == 10000

def test_multiple_stmts(pipeline, clean_db):
  conn = psycopg2.connect('dbname=pipeline user=%s host=localhost port=%s' % (getpass.getuser(), pipeline.port))
  db = conn.cursor()
  db.execute('CREATE CONTINUOUS VIEW test_multiple AS SELECT COUNT(*) FROM stream; SELECT 1;')
  conn.commit()
  conn.close()

  pipeline.insert('stream', ('unused', ), [(1, )] * 100)

  result = list(pipeline.execute('SELECT * FROM test_multiple'))
  assert len(result) == 1
  assert result[0]['count'] == 100

def test_uniqueness(pipeline, clean_db):
  pipeline.create_cv('uniqueness', 'SELECT x::int, count(*) FROM stream GROUP BY x')

  for i in range(10):
    rows = [((10000 * i) + j, ) for j in xrange(10000)]
    pipeline.insert('stream', ('x', ), rows)

  count = pipeline.execute('SELECT count(*) FROM uniqueness').first()['count']
  distinct_count = pipeline.execute(
    'SELECT count(DISTINCT x) FROM uniqueness').first()['count']

  assert count == distinct_count
