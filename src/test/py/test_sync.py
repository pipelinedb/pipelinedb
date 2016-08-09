from base import pipeline, clean_db
import getpass
import psycopg2
import threading
import time


def test_userset_sync(pipeline, clean_db):
  pipeline.create_stream('stream', x='int')
  pipeline.create_cv('sync',
                     'SELECT count(*) FROM stream WHERE x = 0')
  pipeline.create_cv('async',
                     'SELECT count(*) FROM stream WHERE x = 1')
  pipeline.create_cv('delay',
                     'SELECT x::int, pg_sleep(0.1) FROM stream')

  def insert(sync):
    conn = psycopg2.connect('dbname=pipeline user=%s host=localhost port=%s'
                            % (getpass.getuser(), pipeline.port))
    cur = conn.cursor()
    cur.execute('SET synchronous_stream_insert=%s' % ('on' if sync else 'off'))
    for i in xrange(10):
      cur.execute('INSERT INTO stream (x) VALUES (%d)' % (0 if sync else 1))
      conn.commit()
    conn.close()

  sync = threading.Thread(target=insert, args=(True, ))
  async = threading.Thread(target=insert, args=(False, ))

  start = time.time()

  sync.start()
  async.start()

  async.join()
  async_time = time.time() - start
  assert async_time < 1

  num_sync = pipeline.execute('SELECT count FROM sync').first()['count']
  num_async = pipeline.execute('SELECT count FROM async').first()['count']
  total = pipeline.execute('SELECT count(*) FROM delay').first()['count']
  assert num_async <= 10
  assert num_sync < 10
  assert total < 20

  sync.join()
  assert time.time() - start > (0.8 + async_time)

  pipeline.execute('COMMIT')
  num_sync = pipeline.execute('SELECT count FROM sync').first()['count']
  num_async = pipeline.execute('SELECT count FROM async').first()['count']
  total = pipeline.execute('SELECT count(*) FROM delay').first()['count']
  assert num_sync == 10
  assert num_async == 10
  assert total == num_async + num_sync
