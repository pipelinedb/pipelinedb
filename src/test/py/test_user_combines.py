from base import pipeline, clean_db
import random
import json


def test_simple_aggs(pipeline, clean_db):
    """
    Verify that combines work properly on simple aggs
    """
    q = """
    SELECT x::integer %% 10 AS k,
    avg(x), sum(y::float8), count(*) FROM stream GROUP BY k;
    """
    desc = ('x', 'y')
    pipeline.create_cv('test_simple_aggs', q)
    pipeline.create_table('test_simple_aggs_t', x='integer', y='float8')
    
    rows = []
    for n in range(10000):
        row = (random.randint(0, 1000), random.random())
        rows.append(row)
        
    pipeline.activate()
    
    pipeline.insert('stream', desc, rows)
    pipeline.insert('test_simple_aggs_t', desc, rows)
    
    pipeline.deactivate()
    
    table_result = list(pipeline.execute('SELECT avg(x), sum(y::float8), count(*) FROM test_simple_aggs_t'))
    cv_result = list(pipeline.execute('SELECT combine(avg), combine(sum), combine(count) FROM test_simple_aggs'))
    
    assert len(table_result) == len(cv_result)
    
    for tr, cr in zip(table_result, cv_result):
        assert abs(tr[0] - cr[0]) < 0.00001
        assert abs(tr[1] - cr[1]) < 0.00001
        assert abs(tr[2] - cr[2]) < 0.00001

def test_object_aggs(pipeline, clean_db):
    """
    Verify that combines work properly on object aggs
    """
    q = """
    SELECT x::integer %% 10 AS k,
    json_agg(x), json_object_agg(x, y::float8), string_agg(s::text, \' :: \')FROM stream GROUP BY k;
    """
    desc = ('x', 'y', 's')
    pipeline.create_cv('test_object_aggs', q)
    pipeline.create_table('test_object_aggs_t', x='integer', y='float8', s='text')
    
    rows = []
    for n in range(10000):
        row = (random.randint(0, 1000), random.random(), str(n) * random.randint(1, 8))
        rows.append(row)
        
    pipeline.activate()
    
    pipeline.insert('stream', desc, rows)
    pipeline.insert('test_object_aggs_t', desc, rows)
    
    pipeline.deactivate()
    
    tq = """
    SELECT json_agg(x), json_object_agg(x, y::float8), string_agg(s::text, \' :: \') FROM test_object_aggs_t
    """
    table_result = list(pipeline.execute(tq))
    
    cq = """
    SELECT combine(json_agg), combine(json_object_agg), combine(string_agg) FROM test_object_aggs
    """
    cv_result = list(pipeline.execute(cq))
    
    assert len(table_result) == len(cv_result)
    
    for tr, cr in zip(table_result, cv_result):
        assert sorted(tr[0]) == sorted(cr[0])
        assert sorted(tr[1]) == sorted(cr[1])
        assert sorted(tr[2]) == sorted(cr[2])
    
def test_stats_aggs(pipeline, clean_db):
    """
    Verify that combines work on stats aggs
    """
    q = """
    SELECT x::integer %% 10 AS k,
    regr_sxx(x, y::float8), stddev(x) FROM stream GROUP BY k;
    """
    desc = ('x', 'y')
    pipeline.create_cv('test_stats_aggs', q)
    pipeline.create_table('test_stats_aggs_t', x='integer', y='float8')
    
    rows = []
    for n in range(10000):
        row = (random.randint(0, 1000), random.random())
        rows.append(row)
        
    pipeline.activate()
    
    pipeline.insert('stream', desc, rows)
    pipeline.insert('test_stats_aggs_t', desc, rows)
    
    pipeline.deactivate()
    
    tq = """
    SELECT regr_sxx(x, y::float8), stddev(x) FROM test_stats_aggs_t
    """
    table_result = list(pipeline.execute(tq))
    
    cq = """
    SELECT combine(regr_sxx), combine(stddev) FROM test_stats_aggs
    """
    cv_result = list(pipeline.execute(cq))
    
    assert len(table_result) == len(cv_result)
    
    for tr, cr in zip(table_result, cv_result):
        assert abs(tr[0] - cr[0]) < 0.00001
        assert abs(tr[1] - cr[1]) < 0.00001

def test_hypothetical_set_aggs(pipeline, clean_db):
    """
    Verify that combines work properly on HS aggs
    """
    q = """
    SELECT x::integer %% 10 AS k,
    rank(256) WITHIN GROUP (ORDER BY x),
    dense_rank(256) WITHIN GROUP (ORDER BY x)
    FROM stream GROUP BY k
    """
    desc = ('x', 'y')
    pipeline.create_cv('test_hs_aggs', q)
    pipeline.create_table('test_hs_aggs_t', x='integer', y='float8')
    
    rows = []
    for n in range(10000):
        row = (random.randint(0, 1000), random.random())
        rows.append(row)
        
    pipeline.activate()
    
    pipeline.insert('stream', desc, rows)
    pipeline.insert('test_hs_aggs_t', desc, rows)
    
    pipeline.deactivate()
    
    # Note that the CQ will use the HLL variant of dense_rank, 
    # so use hll_dense_rank on the table too
    tq = """
    SELECT rank(256) WITHIN GROUP (ORDER BY x), hll_dense_rank(256) WITHIN GROUP (ORDER BY x) 
    FROM test_hs_aggs_t
    """
    table_result = list(pipeline.execute(tq))
    
    cq = """
    SELECT combine(rank), combine(dense_rank) FROM test_hs_aggs
    """
    cv_result = list(pipeline.execute(cq))
    
    assert len(table_result) == len(cv_result)
    
    for tr, cr in zip(table_result, cv_result):
        assert tr == cr
        
def test_nested_expressions(pipeline, clean_db):
    """
    Verify that combines work properly on arbitrarily nested expressions
    """
    q = """
    SELECT x::integer %% 10 AS k,
    (rank(256) WITHIN GROUP (ORDER BY x) + dense_rank(256) WITHIN GROUP (ORDER BY x)) * 
        (avg(x + y::float8) - (sum(x) * avg(y))) AS whoa
    FROM stream GROUP BY k
    """
    desc = ('x', 'y')
    pipeline.create_cv('test_nested', q)
    pipeline.create_table('test_nested_t', x='integer', y='float8')
    
    rows = []
    for n in range(10000):
        row = (random.randint(0, 1000), random.random())
        rows.append(row)
        
    pipeline.activate()
    
    pipeline.insert('stream', desc, rows)
    pipeline.insert('test_nested_t', desc, rows)
    
    pipeline.deactivate()
    
    # Note that the CQ will use the HLL variant of dense_rank, 
    # so use hll_dense_rank on the table too
    tq = """
    SELECT
    (rank(256) WITHIN GROUP (ORDER BY x) + hll_dense_rank(256) WITHIN GROUP (ORDER BY x)) * 
        (avg(x + y::float8) - (sum(x) * avg(y))) AS whoa
    FROM test_nested_t
    """
    table_result = list(pipeline.execute(tq))
    
    cq = """
    SELECT combine(whoa) FROM test_nested
    """
    cv_result = list(pipeline.execute(cq))
    
    assert len(table_result) == len(cv_result)
    
    for tr, cr in zip(table_result, cv_result):
        assert abs(tr[0] - cr[0]) < 0.0001
        
def test_hll_distinct(pipeline, clean_db):
    """
    Verify that combines work on HLL COUNT DISTINCT queries
    """
    q = """
    SELECT x::integer %% 10 AS k, COUNT(DISTINCT x) AS count FROM stream GROUP BY k
    """
    desc = ('x', 'y')
    pipeline.create_cv('test_hll_distinct', q)
    pipeline.create_table('test_hll_distinct_t', x='integer', y='float8')
    
    rows = []
    for n in range(10000):
        row = (random.randint(0, 1000), random.random())
        rows.append(row)

    pipeline.activate()
    
    pipeline.insert('stream', desc, rows)
    pipeline.insert('test_hll_distinct_t', desc, rows)
    
    pipeline.deactivate()
    
    # Note that the CQ will use the HLL variant of COUNT DISTINCT, 
    # so use hll_count_distinct on the table too
    tq = """
    SELECT hll_count_distinct(x) FROM test_hll_distinct_t
    """
    table_result = list(pipeline.execute(tq))
    
    cq = """
    SELECT combine(count) FROM test_hll_distinct
    """
    cv_result = list(pipeline.execute(cq))
    
    assert len(table_result) == len(cv_result)
    
    for tr, cr in zip(table_result, cv_result):
        assert tr == cr
