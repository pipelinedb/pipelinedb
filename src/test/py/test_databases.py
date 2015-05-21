from base import pipeline, clean_db
import getpass
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def test_multiple_databases(pipeline, clean_db):
  conn = psycopg2.connect('dbname=pipeline user=%s host=localhost port=%s' % (getpass.getuser(), pipeline.port))
  conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

  cur = conn.cursor()
  cur.execute('CREATE DATABASE tmp_pipeline')
  cur.close()

  q = 'SELECT x::int FROM stream'
  pipeline.create_cv('test_multiple_databases', q)

  pipeline.insert('stream', ['x'], map(lambda x: (x, ), range(0, 10, 2)))

  result = pipeline.execute('SELECT * FROM test_multiple_databases')
  assert sorted(row['x'] for row in result) == range(0, 10, 2)

  tmp_conn = psycopg2.connect('dbname=tmp_pipeline user=%s host=localhost port=%s' % (getpass.getuser(), pipeline.port))
  cur = tmp_conn.cursor()
  cur.execute('CREATE CONTINUOUS VIEW test_multiple_databases AS %s' % q)
  tmp_conn.commit()
  cur.execute('INSERT INTO stream (x) VALUES %s' % ', '.join(map(lambda x: '(%d)' % x, range(1, 11, 2))))
  cur.execute('SELECT * FROM test_multiple_databases')
  assert sorted(row[0] for row in cur) == range(1, 11, 2)
  cur.close()
  tmp_conn.close()

  result = pipeline.execute('SELECT * FROM test_multiple_databases')
  assert sorted(row['x'] for row in result) == range(0, 10, 2)

  cur = conn.cursor()
  cur.execute('DROP DATABASE tmp_pipeline')
  cur.close()
  conn.close()
