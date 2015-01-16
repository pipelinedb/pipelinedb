from base import pipeline, clean_db
import random


def test_percentile_cont_agg(pipeline, clean_db):
    values = [random.randint(-1000, 1000) for _ in xrange(5000)]

    query = 'SELECT percentile_cont(ARRAY[0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]) WITHIN GROUP (ORDER BY x::integer) FROM %s'

    pipeline.create_cv('test_cq_percentile_cont', query % 'test_stream')
    pipeline.create_table('test_percentile_cont', x='integer')

    pipeline.activate()

    for v in values:
        pipeline.execute('INSERT INTO test_stream (x) VALUES (%d)' % v)
        pipeline.execute('INSERT INTO test_percentile_cont (x) VALUES (%d)' % v)

    pipeline.deactivate()

    actual = pipeline.execute(query % 'test_percentile_cont')
    result = pipeline.execute('SELECT * FROM test_cq_percentile_cont')

    actual = actual.first()['percentile_cont']
    result = result.first()['percentile_cont']

    # TODO(usmanm): Improve accuracy of t-digest using tdunning's implementation
    # and complete test.
