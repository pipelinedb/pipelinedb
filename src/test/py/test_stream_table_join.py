from base import pipeline, clean_db
from datetime import datetime

def create_test_table(pipeline, clean_db, name):
    """
    Create  and populate Tables needed for the test
    """
    cols = '(id integer, name text, description text)'
    pipeline.create_table(name, cols)

def populate_table(name):
    for n in range(100):
        idx = n
        values = str(idx) + ', \'Name'+str(idx/10)+'\', \'description'+str(idx/10)+'\''
        pipeline.execute('INSERT INTO T1 (id,name,description) VALUES (%s)' % values)


def populate_stream(pipeline, clean_db):
    """
    Insert elements into the stream
    """
    for n in range(10000):
        idx = n
        values = str(idx) + ', \'Name'+str(idx/10)+'\', \'description'+str(idx/10)+'\''
        pipeline.execute('INSERT INTO stream (sid,name,description) VALUES (%s)' % values)



def test_multi_column_join_groupby_sum(pipeline, clean_db):
    """
    Multi column join with a SUM and group by multiple columns
    """
    create_test_table(pipeline, clean_db, 'T1')
    populate_table('T1')

    tstart = datetime.now()
    query_string =('SELECT SUM(stream.sid::integer), stream.name::text, stream.description::text FROM '
                  'stream join T1 on '
                  'stream.sid = T1.id and '
               'stream.name = T1.name and '
               'stream.description = T1.description '
               'group by stream.name,stream.description')

    pipeline.create_cv('cv0', query_string) 
    pipeline.activate('cv0')

    populate_stream(pipeline, clean_db)

    pipeline.deactivate('cv0')
    result = list(pipeline.execute('SELECT * FROM cv0'))
    tend = datetime.now()
    delta =  (tend - tstart)

    assert len(result) != 0
    for i, row in enumerate(result):
        sum = row['sum']
    assert sum != 0
    
    pipeline.drop_cv('cv0')
    
def test_multi_column_join_where(pipeline, clean_db):
    """
    Multi column join using the where clause
    """

    query_string =('SELECT stream.sid::integer, stream.name::text, stream.description::text FROM '
                  'stream, T1 '
                  'where stream.sid = T1.id and '
               'stream.name = T1.name and '
               'stream.description = T1.description ')

    pipeline.create_cv('cv0', query_string) 
    pipeline.activate('cv0')

    populate_stream(pipeline, clean_db)

    pipeline.deactivate('cv0')
    result = list(pipeline.execute('SELECT * FROM cv0'))
    assert len(result) != 0
    
    pipeline.drop_cv('cv0')

def test_multi_column_join_stream_inner(pipeline, clean_db):
    """
    Multi column join with a SUM and group by multiple columns
    """
    tstart = datetime.now()
    query_string =('SELECT SUM(stream.sid::integer), stream.name::text, stream.description::text FROM '
                  'T1 join stream on '
                  'T1.id = stream.sid and '
               'T1.name = stream.name and '
               'T1.description = stream.description '
               'group by stream.name,stream.description')

    pipeline.create_cv('cv0', query_string) 
    pipeline.activate('cv0')

    populate_stream(pipeline, clean_db)
    pipeline.deactivate('cv0')

    result = list(pipeline.execute('SELECT * FROM cv0'))
    assert len(result) != 0
    for i, row in enumerate(result):
        sum = row['sum']
    assert sum != 0
    
    tend = datetime.now()
    delta =  (tend - tstart)
	
    pipeline.drop_cv('cv0')
    pipeline.drop_table('T1')
  
