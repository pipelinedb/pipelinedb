from base import pipeline, clean_db


def test_many_continuous_views(pipeline, clean_db):
    """
    Verify that we can ACTIVATE/DEACTIVATE many CVs at once
    """
    for n in range(100):
        group = 'group%d' % (n % 2)
        name = '%scv%d' % (group, n)
        pipeline.create_cv(name, 'SELECT col::integer FROM stream')
    
    pipeline.execute('ACTIVATE WHERE name LIKE \'%%group0%%\'')
    result = list(pipeline.execute('SELECT * FROM pipeline_queries WHERE state = \'a\''))
    
    assert len(result) == 50
    
    for row in result:
        assert row['name'].startswith('group0')
    
    pipeline.deactivate()
    
    pipeline.execute('ACTIVATE WHERE name LIKE \'%%group1%%\'')
    result = list(pipeline.execute('SELECT * FROM pipeline_queries WHERE state = \'a\''))
    
    assert len(result) == 50
    
    for row in result:
        assert row['name'].startswith('group1')
    
    pipeline.deactivate()
    
    pipeline.activate()
    result = list(pipeline.execute('SELECT * FROM pipeline_queries WHERE state = \'a\''))
    
    assert len(result) == 100
    
    pipeline.deactivate()
    result = list(pipeline.execute('SELECT * FROM pipeline_queries WHERE state = \'a\''))
    
    assert len(result) == 0
