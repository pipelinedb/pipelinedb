from base import pipeline, clean_db
from collections import namedtuple
import getpass
import psycopg2
import psycopg2.extensions
import random
import threading
import time


DURATION = 4 * 60 # 4 minutes

Stat = namedtuple('Stat', ['rows', 'matrel_rows', 'disk_pages']) 


def test_disk_spill(pipeline, clean_db):
  pipeline.create_cv(
    'test_vacuum', '''
    SELECT x::int, COUNT(DISTINCT y::text)
    FROM test_vacuum_stream
    WHERE arrival_timestamp > clock_timestamp() - INTERVAL '5 second'
    GROUP BY x
    ''')

  stop = False
  
  def insert():
    while not stop:
      values = map(lambda _: (random.randint(0, 20),
                              random.randint(0, 1000000)),
                   xrange(1000))
      pipeline.insert('test_vacuum_stream', ('x', 'y'), values)
      time.sleep(0.01)
  
  t = threading.Thread(target=insert)
  t.start()

  # Wait for SW AV to kick in
  time.sleep(20)

  def get_stat():
    rows = pipeline.execute(
      'SELECT COUNT(*) FROM test_vacuum').first()['count']
    matrel_rows = pipeline.execute(
      'SELECT COUNT(*) FROM test_vacuum_mrel0').first()['count']
    disk_pages = pipeline.execute("""
    SELECT pg_relation_filepath(oid), relpages
    FROM pg_class WHERE relname = 'test_vacuum_mrel0';
    """).first()['relpages']
    for r in pipeline.execute("""
    SELECT relname, relpages
    FROM pg_class,
         (SELECT reltoastrelid
          FROM pg_class
          WHERE relname = 'test_vacuum_mrel0') AS ss
    WHERE oid = ss.reltoastrelid OR
          oid = (SELECT indexrelid
                 FROM pg_index
                 WHERE indrelid = ss.reltoastrelid)
    ORDER BY relname;
    """):
      disk_pages += r['relpages']
    for r in pipeline.execute("""
    SELECT c2.relname, c2.relpages
    FROM pg_class c, pg_class c2, pg_index i
    WHERE c.relname = 'test_vacuum_mrel0' AND
          c.oid = i.indrelid AND
          c2.oid = i.indexrelid
    ORDER BY c2.relname;
    """):
      disk_pages += r['relpages']

    return Stat(rows, matrel_rows, disk_pages)

  start = time.time()
  stats = []
  
  while time.time() - start < DURATION:
    stats.append(get_stat())
    time.sleep(10)

  stop = True
  t.join()

  # We should always see the same number of rows
  rows = map(lambda s: s.rows, stats)
  assert len(set(rows)) == 1

  # The number of matrel rows should go up and down
  matrel_rows = map(lambda s: s.matrel_rows, stats)
  assert sorted(matrel_rows) != matrel_rows

  # The number of disk pages should also go up and down
  disk_pages = map(lambda s: s.disk_pages, stats)
  assert sorted(disk_pages) != disk_pages

  # Now vacuum while we're not inserting at all and see if it does a major
  # clean up
  conn = psycopg2.connect('dbname=pipeline user=%s host=localhost port=%s' %
                          (getpass.getuser(), pipeline.port))
  conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
  db = conn.cursor()
  db.execute('VACUUM test_vacuum')
  conn.close()

  stat = get_stat()

  assert stat.matrel_rows < stats[-1].matrel_rows
  assert stat.disk_pages <= stats[-1].disk_pages
