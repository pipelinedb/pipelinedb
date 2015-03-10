from base import pipeline, clean_db
import random
import time


def test_cq_stats(pipeline, clean_db):
    """
    Verify that CQ statistics collection works
    """
    # 10 rows
    q = 'SELECT x::integer %% 10 AS g, COUNT(*) FROM stream GROUP BY g'
    pipeline.create_cv('test_10_groups', q)
    
    # 1 row
    q = 'SELECT COUNT(*) FROM stream'
    pipeline.create_cv('test_1_group', q)
    
    pipeline.activate()
    
    values = [(random.randint(1, 1024),) for n in range(1000)]
    
    pipeline.insert('stream', ('x',), values)
    
    proc_rows = 0
    cq_rows = 0
    while proc_rows != 4 and cq_rows != 4:
        pipeline.begin()
        proc_result = pipeline.execute('SELECT * FROM pipeline_proc_stats')
        cq_result = pipeline.execute('SELECT * FROM pipeline_query_stats')
        pipeline.commit()
        proc_rows = len(list(proc_result))
        cq_rows = len(list(cq_result))
    
    assert proc_rows == 4
    assert cq_rows == 4
    
    pipeline.deactivate()

    # Proc-level stats should be gone now
    pipeline.begin()
    result = pipeline.execute('SELECT * FROM pipeline_proc_stats')
    pipeline.commit()
    
    assert not result.first()
    
    time.sleep(0.5)
    
    # But CQ-level stats should still be there
    result = pipeline.execute("SELECT * FROM pipeline_query_stats WHERE name = 'test_10_groups' AND type = 'worker'").first()
    assert result['input_rows'] == 1000
    
    result = pipeline.execute("SELECT * FROM pipeline_query_stats WHERE name = 'test_10_groups' AND type = 'combiner'").first()
    assert result['output_rows'] == 10
    
    result = pipeline.execute("SELECT * FROM pipeline_query_stats WHERE name = 'test_1_group' AND type = 'worker'").first()
    assert result['input_rows'] == 1000
    
    result = pipeline.execute("SELECT * FROM pipeline_query_stats WHERE name = 'test_1_group' AND type = 'combiner'").first()
    assert result['output_rows'] == 1
