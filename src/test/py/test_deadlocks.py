from base import pipeline, clean_db
import getpass
import psycopg2
import random
import threading
import time


def test_concurrent_add_drop(pipeline, clean_db):
  """
  Adds and drops continuous views while inserting into a stream so that we
  see add/drops in the middle of transactions in workers and combiners.
  """
  pipeline.create_stream('stream0', x='int')
  q = 'SELECT x::int,  COUNT(*) FROM stream0 GROUP BY x'
  pipeline.create_cv('cv', q)

  stop = False
  values = map(lambda x: (x,), xrange(10000))
  num_inserted = [0]

  def insert():
    while True:
      if stop:
        break
      pipeline.insert('stream0', ['x'], values)
      num_inserted[0] += 1

  def add_drop(prefix):
    # Don't share the connection object with the insert thread because we want
    # these queries to happen in parallel.
    conn = psycopg2.connect('dbname=postgres user=%s host=localhost port=%s' %
                (getpass.getuser(), pipeline.port))
    add = 'CREATE VIEW %s AS ' + q
    drop = 'DROP VIEW %s'
    cur = conn.cursor()
    cvs = []
    while True:
      if stop:
        break
      if not cvs:
        cv = '%s%s' % (prefix, str(random.random())[2:])
        cur.execute(add % cv)
        cvs.append(cv)
      elif len(cvs) > 10:
        cur.execute(drop % cvs.pop())
      else:
        r = random.random()
        if r > 0.5:
          cv = '%s%s' % (prefix, str(r)[2:])
          cur.execute(add % cv)
          cvs.append(cv)
        else:
          cur.execute(drop % cvs.pop())
      conn.commit()
      time.sleep(0.0025)
    cur.close()
    conn.close()

  threads = [threading.Thread(target=insert),
         threading.Thread(target=add_drop, args=('cv1_',)),
         threading.Thread(target=add_drop, args=('cv2_',))]

  map(lambda t: t.start(), threads)

  time.sleep(10)
  stop = True

  map(lambda t: t.join(), threads)

  views = pipeline.execute('SELECT name FROM pipelinedb.get_views()')
  mrels = pipeline.execute("SELECT relname FROM pg_class WHERE relname LIKE 'cv%%_mrel'")
  assert len(views) == len(mrels)

  counts = pipeline.execute('SELECT * FROM cv')
  assert len(counts) == 10000
  for r in counts:
    assert r['count'] == num_inserted[0]
