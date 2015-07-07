from base import pipeline, clean_db
import getpass
import random
import psycopg2
import time


def test_prepared_inserts(pipeline, clean_db):
  """
  Verify that we can PREPARE and EXECUTE an INSERT statement
  """
  conn = psycopg2.connect('dbname=pipeline user=%s host=localhost port=%s' % (getpass.getuser(), pipeline.port))
  db = conn.cursor()
  db.execute('CREATE CONTINUOUS VIEW test_prepared0 AS SELECT x::integer, COUNT(*), sum(y::integer) FROM stream GROUP BY x')
  db.execute('CREATE CONTINUOUS VIEW test_prepared1 AS SELECT x::integer, COUNT(*), sum(y::float8) FROM stream GROUP BY x')
  conn.commit()

  db.execute('PREPARE ins AS INSERT INTO stream (x, y) VALUES ($1, $2)')

  for n in range(10000):
    row = (n % 100, random.random())
    db.execute('EXECUTE ins (%s, %s)' % row)

  time.sleep(0.1)

  conn.commit()

  result = list(pipeline.execute('SELECT * FROM test_prepared0 ORDER BY x'))

  assert len(result) == 100

  for n in range(100):
    assert result[n]['count'] == 100

  result = list(pipeline.execute('SELECT * FROM test_prepared1 ORDER BY x'))

  assert len(result) == 100

  for n in range(100):
    assert result[n]['count'] == 100

  conn.close()
