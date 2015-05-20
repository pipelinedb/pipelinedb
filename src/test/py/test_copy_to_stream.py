from base import pipeline, clean_db
import os
import random
import time


def _generate_csv(path, rows, desc=None, delimiter=','):
    csv = open(path, 'wa')
    if desc:
        rows = [desc] + rows
    for row in rows:
        line = delimiter.join(str(v) for v in row) + '\n'
        csv.write(line)
    csv.close()

def test_copy_to_stream(pipeline, clean_db):
    """
    Verify that copying data from a file into a stream works
    """
    q = 'SELECT sum(x::integer) AS s0, sum(y::float8) AS s1, avg(z::numeric) FROM stream'
    pipeline.create_cv('test_copy_to_stream', q)
    pipeline.create_table('test_copy_to_stream_t', x='integer', y='float8', z='numeric')

    path = os.path.abspath(os.path.join(pipeline.tmp_dir, 'test_copy.csv'))

    rows = []
    for n in range(10000):
        row = random.randint(1, 1024), random.randint(1, 1024), random.random()
        rows.append(row)

    _generate_csv(path, rows, desc=('x', 'y', 'z'))

    pipeline.execute('COPY test_copy_to_stream_t (x, y, z) FROM \'%s\' HEADER CSV' % path)

    pipeline.execute('COPY stream (x, y, z) FROM \'%s\' HEADER CSV' % path)

    expected = pipeline.execute('SELECT sum(x::integer) AS s0, sum(y::float8) AS s1, avg(z::numeric) FROM test_copy_to_stream_t').first()
    result = list(pipeline.execute('SELECT * FROM test_copy_to_stream'))

    assert len(result) == 1

    result = result[0]

    assert result[0] == expected[0]
    assert result[1] == expected[1]
    assert result[2] == expected[2]

def test_colums_superset(pipeline, clean_db):
    """
    Verify that copying data from a file into a stream works when the file's input
    columns are a superset of the stream's columns
    """
    q = 'SELECT sum(x::integer) AS s0, sum(y::float8) AS s1, avg(z::numeric) FROM stream'
    pipeline.create_cv('test_copy_superset', q)
    pipeline.create_table('test_copy_superset_t', x='integer', y='float8', z='numeric', t='text')

    path = os.path.abspath(os.path.join(pipeline.tmp_dir, 'test_copy.csv'))

    rows = []
    for n in range(10000):
        row = 'unread', random.randint(1, 1024), random.randint(1, 1024), random.random()
        rows.append(row)

    _generate_csv(path, rows, desc=('t', 'x', 'y', 'z'))

    pipeline.execute('COPY test_copy_superset_t (t, x, y, z) FROM \'%s\' HEADER CSV' % path)

    pipeline.execute('COPY stream (t, x, y, z) FROM \'%s\' HEADER CSV' % path)

    expected = pipeline.execute('SELECT sum(x::integer) AS s0, sum(y::float8) AS s1, avg(z::numeric) FROM test_copy_superset_t').first()
    result = list(pipeline.execute('SELECT * FROM test_copy_superset'))

    assert len(result) == 1

    result = result[0]

    assert result[0] == expected[0]
    assert result[1] == expected[1]
    assert result[2] == expected[2]

def test_colums_subset(pipeline, clean_db):
    """
    Verify that copying data from a file into a stream works when the file's input
    columns are a subset of the stream's columns
    """
    q = 'SELECT sum(x::integer) AS s0, sum(y::float8) AS s1, avg(z::numeric), max(m::integer) FROM stream'
    pipeline.create_cv('test_copy_subset', q)
    pipeline.create_table('test_copy_subset_t', x='integer', y='float8', z='numeric')

    path = os.path.abspath(os.path.join(pipeline.tmp_dir, 'test_copy.csv'))

    rows = []
    for n in range(10000):
        row = random.randint(1, 1024), random.randint(1, 1024), random.random()
        rows.append(row)

    _generate_csv(path, rows, desc=('x', 'y', 'z'))

    pipeline.execute('COPY test_copy_subset_t (x, y, z) FROM \'%s\' HEADER CSV' % path)

    pipeline.execute('COPY stream (x, y, z) FROM \'%s\' HEADER CSV' % path)

    expected = pipeline.execute('SELECT sum(x::integer) AS s0, sum(y::float8) AS s1, avg(z::numeric) FROM test_copy_subset_t').first()
    result = list(pipeline.execute('SELECT s0, s1, avg FROM test_copy_subset'))

    assert len(result) == 1

    result = result[0]

    assert result[0] == expected[0]
    assert result[1] == expected[1]
    assert result[2] == expected[2]
