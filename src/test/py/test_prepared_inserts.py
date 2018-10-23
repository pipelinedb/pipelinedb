from base import pipeline, clean_db
import getpass
import psycopg2
import random
import subprocess
import time


def test_prepared_inserts(pipeline, clean_db):
  """
  Verify that we can PREPARE and EXECUTE an INSERT statement
  """
  pipeline.create_stream('stream0', x='int', y='float8')

  conn = psycopg2.connect('dbname=postgres user=%s host=localhost port=%s' % (getpass.getuser(), pipeline.port))
  db = conn.cursor()
  db.execute('CREATE VIEW test_prepared0 AS SELECT x::integer, COUNT(*), sum(y::integer) FROM stream0 GROUP BY x')
  db.execute('CREATE VIEW test_prepared1 AS SELECT x::integer, COUNT(*), sum(y::float8) FROM stream0 GROUP BY x')
  conn.commit()

  db.execute('PREPARE ins AS INSERT INTO stream0 (x, y) VALUES ($1, $2)')

  for n in range(10000):
    row = (n % 100, random.random())
    db.execute('EXECUTE ins (%s, %s)' % row)

  time.sleep(0.1)

  conn.commit()

  result = pipeline.execute('SELECT * FROM test_prepared0 ORDER BY x')

  assert len(result) == 100

  for n in range(100):
    assert result[n]['count'] == 100

  result = pipeline.execute('SELECT * FROM test_prepared1 ORDER BY x')

  assert len(result) == 100

  for n in range(100):
    assert result[n]['count'] == 100

  conn.close()


def test_prepared_extended(pipeline, clean_db):
  """
  Verify that we can write to streams using the extended protocol. This test
  shells out to a binary because psycopg2 doesn't use the extended protocol.
  """
  pipeline.create_stream('extended_stream', x='int', y='int', z='int')
  q = """
  SELECT COUNT(x::integer) AS x, COUNT(y::integer) AS y, COUNT(z::integer) AS z FROM extended_stream
  """
  pipeline.create_cv('test_prepared_extended', q)

  # This will insert 1000 via a paramaterized insert, and 1000 via unparamaterized insert
  cmd = ['./extended', 'postgres', str(pipeline.port), 'extended_stream', '1000']

  stdout, stderr = subprocess.Popen(cmd).communicate()

  assert stdout is None
  assert stderr is None

  rows = pipeline.execute('SELECT x, y, z FROM test_prepared_extended')
  assert len(rows) == 1

  result = rows[0]

  assert result['x'] == 2000
  assert result['y'] == 2000
  assert result['z'] == 2000
