from base import pipeline, clean_db
import getpass
import psycopg2
import threading
import time


def test_userset_sync(pipeline, clean_db):
  
  pipeline.create_stream('s', x='int')
  pipeline.create_cv('sync', 'SELECT count(*) FROM s WHERE x = 0')
  pipeline.create_cv('async', 'SELECT count(*) FROM s WHERE x = 1')
  pipeline.create_cv('delay', 'SELECT x::int, cq_sleep(0.1) FROM s')

  NUM_INSERTS = 100

  def insert(sync):
    conn = psycopg2.connect('dbname=postgres user=%s host=localhost port=%s'
                % (getpass.getuser(), pipeline.port))
    cur = conn.cursor()
    cur.execute('SET pipelinedb.stream_insert_level=sync_%s' %
          ('commit' if sync else 'receive'))
    for i in xrange(NUM_INSERTS):
      cur.execute('INSERT INTO s (x) VALUES (%d)' % (0 if sync else 1))
      conn.commit()
    conn.close()

  sync = threading.Thread(target=insert, args=(True,))
  async = threading.Thread(target=insert, args=(False,))

  start = time.time()

  sync.start()
  async.start()

  async.join()
  async_time = time.time() - start
  assert async_time < NUM_INSERTS * 0.1

  num_sync = pipeline.execute('SELECT count FROM sync')[0]['count']
  num_async = pipeline.execute('SELECT count FROM async')[0]['count']
  total = pipeline.execute('SELECT count(*) FROM delay')[0]['count']
  assert num_async < NUM_INSERTS
  assert num_sync < NUM_INSERTS
  assert total < NUM_INSERTS * 2

  sync.join()
  assert time.time() - start > (NUM_INSERTS * 0.08 + async_time)

  pipeline.execute('COMMIT')
  num_sync = pipeline.execute('SELECT count FROM sync')[0]['count']
  num_async = pipeline.execute('SELECT count FROM async')[0]['count']
  total = pipeline.execute('SELECT count(*) FROM delay')[0]['count']
  assert num_sync == NUM_INSERTS
  assert num_async == NUM_INSERTS
  assert total == NUM_INSERTS * 2
