from base import pipeline, clean_db

def create_test_table(pipeline, clean_db, name):
    """
    Create  and populate Tables needed for the test
    """
    print('CREATING TABLE STREAM')
    cols = '(id integer, name text, description text)'
    pipeline.create_table(name, cols)

def populate_table(name):
    print 'TABLE'
    for n in range(1000):
        idx = n
        values = str(idx) + ', \'Name'+str(idx)+'\', \'description'+str(idx)+'\''
        pipeline.execute('INSERT INTO T1 (id,name,description) VALUES (%s)' % values)


def populate_stream(pipeline, clean_db):
    """
    Insert elements into the stream
    """
    print('POPULATING STREAM')
    for n in range(1000):
        idx = n
        values = str(idx) + ', \'Name'+str(idx)+'\', \'description'+str(idx)+'\''
        pipeline.execute('INSERT INTO stream (sid,name,description) VALUES (%s)' % values)



def test_multi_column_join_groupby(pipeline, clean_db):
    """
    Basic single column join 
    """
    print('STAT')
    create_test_table(pipeline, clean_db, 'T1')
    populate_table('T1  ')
    pipeline.create_cv('cv0', 'SELECT SUM(stream.sid::integer), stream.name::text, stream.description::text FROM stream join T1 on stream.sid = T1.id and stream.name = T1.name and stream.description = T1.description group by stream.name,stream.description')
    pipeline.activate('cv0')
    populate_stream(pipeline, clean_db)
    pipeline.deactivate('cv0')
    result = list(pipeline.execute('SELECT * FROM cv0'))
    assert len(result) != 0
    for i, row in enumerate(result):
        print row


    assert False
    
    #pipeline.drop_cv('cv0')
    #pipeline.drop_cv('cv1')
    #pipeline.drop_cv('cv2')
    

