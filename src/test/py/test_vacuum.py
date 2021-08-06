from base import pipeline, clean_db
from collections import namedtuple
import getpass
import os
import psycopg2
import psycopg2.extensions
import random
import threading
import time


def test_concurrent_vacuum_full(pipeline, clean_db):
  pipeline.create_stream('test_vacuum_stream', x='int')
  pipeline.create_cv(
    'test_vacuum_full',
    'SELECT x::int, COUNT(*) FROM test_vacuum_stream GROUP BY x')
  stop = False

  def insert():
    while not stop:
      values = [(random.randint(0, 1000000),) for _ in range(1000)]
      pipeline.insert('test_vacuum_stream', ('x',), values)
      time.sleep(0.01)

  threads = [threading.Thread(target=insert) for _ in range(4)]
  map(lambda t: t.start(), threads)

  # Insert data for a little bit so we have enough work to do while
  # vacuuming.
  time.sleep(20)

  conn = psycopg2.connect('dbname=postgres user=%s host=localhost port=%s' %
                          (getpass.getuser(), pipeline.port))
  conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
  cur = conn.cursor()
  cur.execute('VACUUM FULL test_vacuum_full')
  conn.close()

  # Now kill the insert threads.
  stop = True
  map(lambda t: t.join(), threads)
