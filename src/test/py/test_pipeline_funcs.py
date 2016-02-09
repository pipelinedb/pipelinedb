from base import pipeline, clean_db
import getpass
import psycopg2
import threading
import time


def test_combine_table(pipeline, clean_db):
  pipeline.create_cv('combine_table',
                     'SELECT x::int, COUNT(*) FROM stream GROUP BY x')

  values = [(i, ) for i in xrange(1000)]
  pipeline.insert('stream', ('x', ), values)

  pipeline.execute('CREATE TABLE tmprel () INHERITS (combine_table_mrel)')
  pipeline.execute('INSERT INTO tmprel SELECT * FROM combine_table_mrel')

  stop = False
  ninserts = [0]
  return
  def insert():
    while not stop:
      pipeline.insert('stream', ('x', ), values)
      ninserts[0] +=1
      time.sleep(0.01)

  t = threading.Thread(target=insert)
  t.start()

  time.sleep(2)

  conn = psycopg2.connect('dbname=pipeline user=%s host=localhost port=%s' %
                          (getpass.getuser(), pipeline.port))
  cur = conn.cursor()
  cur.execute("SELECT pipeline_combine_table('combine_table', 'tmprel')")
  conn.close()

  stop = True
  t.join()

  rows = list(pipeline.execute('SELECT count FROM combine_table'))
  assert len(rows) == 1000
  for row in rows:
    assert row[0] == ninserts + 2

  pipeline.execute('DROP TABLE tmprel')
