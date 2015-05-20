from base import pipeline, clean_db
import os
import random
import signal
import threading
import time
from subprocess import check_output, CalledProcessError

def _get_pid(grep_str):
  try:
    out = check_output('ps aux | grep "pipeline" | grep "%s"' % grep_str,
                       shell=True).split('\n')
  except CalledProcessError:
    return -1
  out = filter(lambda s: len(s), out)
  if not out:
    return -1

  out = filter(lambda s: len(s), out[0].split(' '))
  return int(out[1])

def _kill(pid):
  if pid <= 0:
    return False
  os.kill(pid, signal.SIGTERM)
  return True

def kill_worker(db_oid):
  return _kill(_get_pid('worker \[%d\]' % db_oid))

def kill_combiner(db_oid):
  return _kill(_get_pid('combiner \[%d\]' % db_oid))

def get_db_oid(pipeline):
  result = pipeline.execute("SELECT oid FROM pg_database WHERE datname='pipeline'").first()
  return int(result['oid'])

def test_simple_crash(pipeline, clean_db):
  """
  Test simple worker and combiner crashes.
  """
  oid = get_db_oid(pipeline)

  q = 'SELECT COUNT(*) FROM stream'
  pipeline.create_cv('test_simple_crash', q)

  pipeline.insert('stream', ['x'], [(1, ), (1, )])

  result = pipeline.execute('SELECT * FROM test_simple_crash').first()
  assert result['count'] == 2

  # We can potentially lose one batch for a worker or combiner crash.
  # In our case each batch adds a count 2 and since we're adding 3 batches
  # we should either see an increment from the previous count of 4 or 6.
  pipeline.insert('stream', ['x'], [(1, ), (1, )])

  kill_worker(oid)

  pipeline.insert('stream', ['x'], [(1, ), (1, )])

  result = pipeline.execute('SELECT * FROM test_simple_crash').first()
  assert result['count'] == 6

  pipeline.insert('stream', ['x'], [(1, ), (1, )])

  kill_combiner(oid)

  pipeline.insert('stream', ['x'], [(1, ), (1, )])

  result = pipeline.execute('SELECT * FROM test_simple_crash').first()
  assert result['count'] == 10

def test_concurrent_crash(pipeline, clean_db):
  """
  Test simple worker and combiner crashes.
  """
  oid = get_db_oid(pipeline)

  q = 'SELECT COUNT(*) FROM stream'
  pipeline.create_cv('test_concurrent_crash', q)

  desc = [0, 0, False]
  vals = [(1, )] * 25000

  def insert():
    while True:
        pipeline.insert('stream', ['x'], vals)
        desc[1] += 25000
        if desc[2]:
          break

  def kill():
    for _ in xrange(30):
      r = random.random()
      if r > 0.85:
        desc[0] += kill_combiner(oid)
      if r < 0.15:
        desc[0] += kill_worker(oid)
      time.sleep(0.1)

    desc[2] = True

  threads = [threading.Thread(target=insert),
             threading.Thread(target=kill)]
  map(lambda t: t.start(), threads)
  map(lambda t: t.join(), threads)

  num_killed = desc[0]
  num_inserted = desc[1]

  result = pipeline.execute('SELECT count FROM test_concurrent_crash').first()

  assert num_killed > 0
  assert result['count'] >= num_inserted
