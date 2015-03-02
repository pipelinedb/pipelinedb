from base import pipeline, clean_db
import time


def assert_result_changes(func, args):
    """
    Verifies that the result of the given function changes with time
    """
    name = 'assert_%s_decreases' % func
    pipeline.create_cv(name, 
                       "SELECT %s(%s) FROM stream WHERE arrival_timestamp > clock_timestamp() - interval '2 seconds'" % (func, args))
    pipeline.activate(name)
    
    rows = [(n, str(n), n + 1) for n in range(1000)]
    pipeline.insert('stream', ('x', 'y', 'z'), rows)
    
    pipeline.deactivate(name)
    current = 1
    
    results = []
    while current:    
        row = pipeline.execute('SELECT * FROM %s' % name).first()
        current = row[func]
        if current is None:
            break
        results.append(current)
        
    # Verify that we actually read something
    assert results
    
    pipeline.drop_cv(name)
    
def test_count(pipeline, clean_db):
    """
    count
    """
    assert_result_changes('count', '*')
    
def test_avg(pipeline, clean_db):
    """
    avg
    """
    assert_result_changes('avg', 'x::integer')
    
def test_sum(pipeline, clean_db):
    """
    sum
    """
    assert_result_changes('sum', 'x::integer')
    
def test_array_agg(pipeline, clean_db):
    """
    array_agg
    """
    assert_result_changes('array_agg', 'x::integer')

def test_json_agg(pipeline, clean_db):
    """
    json_agg
    """
    assert_result_changes('json_agg', 'x::integer')
    
def test_regr_sxx(pipeline, clean_db):
    """
    regr_sxx
    """
    assert_result_changes('regr_sxx', 'x::float8, x::float8')
    
def test_regr_syy(pipeline, clean_db):
    """
    regr_syy
    """
    assert_result_changes('regr_syy', 'x::float8, x::float8')

def test_regr_sxy(pipeline, clean_db):
    """
    regr_sxy
    """
    assert_result_changes('regr_sxy', 'x::float8, x::float8')

def test_regr_avgx(pipeline, clean_db):
    """
    regr_avgx
    """
    assert_result_changes('regr_avgx', 'x::float8, x::float8')

def test_regr_avgy(pipeline, clean_db):
    """
    regr_avgy
    """
    assert_result_changes('regr_avgy', 'x::float8, x::float8')

def test_regr_r2(pipeline, clean_db):
    """
    regr_r2
    """
    assert_result_changes('regr_r2', 'x::float8, x::float8')

def test_regr_slope(pipeline, clean_db):
    """
    regr_slope
    """
    assert_result_changes('regr_slope', 'x::float8, x::float8')

def test_regr_intercept(pipeline, clean_db):
    """
    regr_intercept
    """
    assert_result_changes('regr_intercept', 'x::float8, z::float8')

def test_covar_pop(pipeline, clean_db):
    """
    covar_pop
    """
    assert_result_changes('covar_pop', 'x::float8, x::float8')

def test_covar_samp(pipeline, clean_db):
    """
    covar_samp
    """
    assert_result_changes('covar_samp', 'x::float8, x::float8')

def test_corr(pipeline, clean_db):
    """
    corr
    """
    assert_result_changes('corr', 'x::float8, x::float8')
    
def test_var_pop(pipeline, clean_db):
    """
    var_pop
    """
    assert_result_changes('var_pop', 'x::float8')
    
def test_var_samp(pipeline, clean_db):
    """
    var_samp
    """
    assert_result_changes('var_samp', 'x::float8')
    
def test_stddev_samp(pipeline, clean_db):
    """
    stddev_samp
    """
    assert_result_changes('stddev_samp', 'x::float8')
    
def test_stddev_pop(pipeline, clean_db):
    """
    stddev_pop
    """
    assert_result_changes('stddev_pop', 'x::float8')
