from base import pipeline, clean_db
import random


def test_hll_count_distinct(pipeline, clean_db):
    """
    Verify that streaming COUNT(DISTINCT) works
    """
    q = 'SELECT COUNT(DISTINCT x::integer) FROM stream'
    pipeline.create_cv('test_count_distinct', q)

    desc = ('x',)
    values = [(random.randint(1, 1024),) for n in range(1000)]

    pipeline.insert('stream', desc, values)

    expected = len(set(values))
    result = pipeline.execute('SELECT count FROM test_count_distinct').first()

    # Error rate should be well below %2
    delta = abs(expected - result['count'])

    assert delta / float(expected) <= 0.02
