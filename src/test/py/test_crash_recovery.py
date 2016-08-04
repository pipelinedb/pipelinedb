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

def kill_worker():
  return _kill(_get_pid('worker[0-9] \[pipeline\]'))

def kill_combiner():
  return _kill(_get_pid('combiner[0-9] \[pipeline\]'))

def test_simple_crash(pipeline, clean_db):
  """
  Test simple worker and combiner crashes.
  """
  q = 'SELECT COUNT(*) FROM stream'
  pipeline.create_cv('test_simple_crash', q)

  pipeline.insert('stream', ['x'], [(1, ), (1, )])

  result = pipeline.execute('SELECT * FROM test_simple_crash').first()
  assert result['count'] == 2

  # This batch can potentially get lost.
  pipeline.insert('stream', ['x'], [(1, ), (1, )])

  assert kill_worker()

  pipeline.insert('stream', ['x'], [(1, ), (1, )])

  result = pipeline.execute('SELECT * FROM test_simple_crash').first()
  assert result['count'] in [4, 6]

  # This batch can potentially get lost.
  pipeline.insert('stream', ['x'], [(1, ), (1, )])

  assert kill_combiner()

  pipeline.insert('stream', ['x'], [(1, ), (1, )])

  result = pipeline.execute('SELECT * FROM test_simple_crash').first()
  assert result['count'] in [6, 8, 10]

  # To ensure that all remaining events in ZMQ queues have been consumed
  time.sleep(2)

def test_concurrent_crash(pipeline, clean_db):
  """
  Test simple worker and combiner crashes.
  """
  q = 'SELECT COUNT(*) FROM stream'
  pipeline.create_cv('test_concurrent_crash', q)
  batch_size = 25000

  desc = [0, 0, False]
  vals = [(1, )] * batch_size

  def insert():
    while True:
        pipeline.insert('stream', ['x'], vals)
        desc[1] += batch_size
        if desc[2]:
          break

  def kill():
    for _ in xrange(30):
      r = random.random()
      if r > 0.85:
        desc[0] += kill_combiner()
      if r < 0.15:
        desc[0] += kill_worker()
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
  assert result['count'] <= num_inserted
  assert result['count'] >= num_inserted - (num_killed * batch_size)

  # To ensure that all remaining events in ZMQ queues have been consumed
  time.sleep(2)

def test_restart_recovery(pipeline, clean_db):
  q = 'SELECT COUNT(*) FROM stream'
  pipeline.create_cv('test_restart_recovery', q)

  pipeline.insert('stream', ['x'], [(1, ), (1, )])

  result = pipeline.execute('SELECT * FROM test_restart_recovery').first()
  assert result['count'] == 2

  # Need to sleep here, otherwise on restart the materialization table is
  # empty. Not sure why.
  time.sleep(0.1)

  # Restart.
  pipeline.stop()
  pipeline.run()

  result = pipeline.execute('SELECT * FROM test_restart_recovery').first()
  assert result['count'] == 2

  # XXX: Wait for all continuous processes to start up. If we insert before all
  # of them have started, a synchronous insert will fail because the generation
  # id changes and we'll think a background process might have crashed.
  time.sleep(1)

  pipeline.insert('stream', ['x'], [(1, ), (1, )])

  result = pipeline.execute('SELECT * FROM test_restart_recovery').first()
  assert result['count'] == 4

def test_postmaster_worker_recovery(pipeline, clean_db):
  """
  Verify that the Postmaster only restarts crashed worker processes, and does not
  attempt to start them when the continuous query scheduler should.
  """
  result = pipeline.execute('SELECT COUNT(*) FROM pipeline_proc_stats WHERE type = \'worker\'').first()
  expected_workers = result['count']

  result = pipeline.execute('SELECT COUNT(*) FROM pipeline_proc_stats WHERE type = \'combiner\'').first()
  expected_combiners = result['count']

  q = 'SELECT COUNT(*) FROM stream'
  pipeline.create_cv('test_pm_recovery', q)
  pipeline.insert('stream', ['x'], [(1, ), (1, )])

  def backend():
    try:
      # Just keep a long-running backend connection open
      client = pipeline.engine.connect()
      client.execute('SELECT pg_sleep(10000)')
    except:
      pass

  t = threading.Thread(target=backend)
  t.start()

  attempts = 0
  result = None
  backend_pid = 0

  while not result and attempts < 10:
    result = pipeline.execute("""SELECT pid, query FROM pg_stat_activity WHERE lower(query) LIKE '%%pg_sleep%%'""").first()
    time.sleep(1)
    attempts += 1

  assert result

  backend_pid = result['pid']
  os.kill(backend_pid, signal.SIGKILL)

  attempts = 0
  pipeline.conn = None

  while attempts < 20:
    try:
      pipeline.conn = pipeline.engine.connect()
      break
    except:
      time.sleep(1)
      pass
    attempts += 1

  assert pipeline.conn

  # Now verify that we have the correct number of CQ worker procs
  result = pipeline.execute('SELECT COUNT(*) FROM pipeline_proc_stats WHERE type = \'worker\'').first()
  assert result['count'] == expected_workers

  result = pipeline.execute('SELECT COUNT(*) FROM pipeline_proc_stats WHERE type = \'combiner\'').first()
  assert result['count'] == expected_combiners
