from base import pipeline, clean_db


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
