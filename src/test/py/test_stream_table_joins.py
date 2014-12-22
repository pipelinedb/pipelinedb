from base import pipeline, clean_db
import random


def _match(r0, r1, cols):
    return tuple(r0[i] for i in cols) == tuple(r1[i] for i in cols)

def _join(left, right, cols):
    result = []
    for l in left:
        for r in right:
            if _match(l, r, cols):
                result.append(l + r)
    return result

def _insert(pipeline, table, rows):
    cols = ', '.join(['col%d' % c for c in range(len(rows[0]))])
    values = ', '.join([str(row) for row in rows])
    f = open('break.sql', 'a')
    f.write('INSERT INTO %s (%s) VALUES %s;\n' % (table, cols, values))
    pipeline.execute('INSERT INTO %s (%s) VALUES %s' % (table, cols, values))
    f.close()
    
def _generate_row(n):
    return tuple(random.choice([1, 0]) for n in range(n))

def _generate_rows(num_cols, max_rows):
    return [_generate_row(num_cols) for n in range(1, max_rows)]

def test_simple_join(pipeline, clean_db):
    """
    Verify that nested-loop joins work as the outer 
    node of a stream-table join
    """

def test_join_with_aggs(pipeline, clean_db):
    """
    """

def test_join_with_where(pipeline, clean_db):
    """
    """
    
def test_join_across_batches(pipeline, clean_db):
    """
    Verify that stream-table joins are properly built when they
    span across multiple input batches
    """
    num_cols = 4
    join_cols = [0]
    t_cols = dict([('col%d' % n, 'integer') for n in range(num_cols)])
    pipeline.create_table('t', **t_cols)
    
    q = """
    SELECT s.col0::integer FROM t JOIN stream s ON t.col0 = s.col0
    """
    pipeline.create_cv('test_join', q)
    pipeline.activate(batchsize=1)

    t = _generate_rows(num_cols, 64)
    _insert(pipeline, 't', t)
    
    s = _generate_rows(num_cols, 64)
    _insert(pipeline, 'stream', s)
    
    pipeline.deactivate()
    
    expected = _join(t, s, join_cols)
    result = pipeline.execute('SELECT COUNT(*) FROM test_join').first()
    
    assert result['count'] == len(expected)
    
def test_incremental_join(pipeline, clean_db):
    """
    Verify that join results increase appropriately as we incrementally
    add stream events to the input 
    """
    num_cols = 4
    join_cols = [0, 1]
    t_cols = dict([('col%d' % n, 'integer') for n in range(num_cols)])
    pipeline.create_table('t', **t_cols)
    
    q = """
    SELECT s.col0::integer FROM t JOIN stream s ON t.col0 = s.col0
    AND t.col1 = s.col1::integer
    """
    pipeline.create_cv('test_join', q)
    t = _generate_rows(num_cols, 64)
    _insert(pipeline, 't', t)
    
    pipeline.activate()
    s = []
    for n in range(2):
        row = _generate_row(num_cols)
        _insert(pipeline, 'stream', [row])
        s.append(row)
        
    pipeline.deactivate()
    
    expected = _join(t, s, join_cols)
    result = pipeline.execute('SELECT COUNT(*) FROM test_join').first()
    
    assert result['count'] == len(expected)

def test_join_multiple_tables(pipeline, clean_db):
    """
    Verify that stream-table joins involving multiple tables work
    """
    num_cols = 8
    join_cols = [0]
    t0_cols = dict([('col%d' % n, 'integer') for n in range(num_cols)])
    t1_cols = dict([('col%d' % n, 'integer') for n in range(num_cols)])
    
    pipeline.create_table('t0', **t0_cols)
    pipeline.create_table('t1', **t1_cols)
    q = """
    SELECT s.col0::integer FROM t0 JOIN t1 ON t0.col0 = t1.col0
    JOIN stream s ON t1.col0 = s.col0
    """
    pipeline.create_cv('test_join0', q)
    
    t0 = _generate_rows(num_cols, 64)
    t1 = _generate_rows(num_cols, 64)
    s = _generate_rows(num_cols, 64)
    
    _insert(pipeline, 't1', t1)
    pipeline.activate()

    # Now insert some table rows after activation
    _insert(pipeline, 't0', t0)
    _insert(pipeline, 'stream', s)

    pipeline.deactivate()

    expected = _join(t0, _join(s, t1, join_cols), join_cols)
    result = pipeline.execute('SELECT COUNT(*) FROM test_join0').first()        
    
    assert result['count'] == len(expected)