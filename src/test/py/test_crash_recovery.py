from base import pipeline, clean_db
import os
import random
import signal
from subprocess import check_output, CalledProcessError
import threading
import time


def _get_pids(grep_str):
  try:
    out = check_output('ps aux | grep "postgres" | grep "%s"' % grep_str,
               shell=True).split('\n')
  except CalledProcessError:
    return []
  out = filter(lambda s: len(s), out)
  if not out:
    return []

  pids = []
  for line in out:
    line = line.split()
    pid = int(line[1].strip())
    pids.append(pid)

  return pids


def _get_pid(grep_str):
  pids = _get_pids(grep_str)
  if not pids:
    return -1
  return random.choice(pids)


def _kill(pid):
  if pid <= 0:
    return False
  os.kill(pid, signal.SIGTERM)
  return True


def get_worker_pids():
  return _get_pids('worker[0-9] \[postgres\]')

def get_combiner_pids():
  return _get_pids('combiner[0-9] \[postgres\]')

def kill_worker():
  return _kill(_get_pid('worker[0-9] \[postgres\]'))


def kill_combiner():
  return _kill(_get_pid('combiner[0-9] \[postgres\]'))


def test_simple_crash(pipeline, clean_db):
  """
  Test simple worker and combiner crashes.
  """
  pipeline.create_stream('stream0', x='int')
  q = 'SELECT COUNT(*) FROM stream0'
  pipeline.create_cv('test_simple_crash', q)

  pipeline.insert('stream0', ['x'], [(1,), (1,)])

  result = pipeline.execute('SELECT * FROM test_simple_crash')[0]
  assert result['count'] == 2

  # This batch can potentially get lost.
  pipeline.insert('stream0', ['x'], [(1,), (1,)])

  assert kill_worker()

  pipeline.insert('stream0', ['x'], [(1,), (1,)])

  result = pipeline.execute('SELECT * FROM test_simple_crash')[0]
  assert result['count'] in [4, 6]

  # This batch can potentially get lost.
  pipeline.insert('stream0', ['x'], [(1,), (1,)])

  assert kill_combiner()

  pipeline.insert('stream0', ['x'], [(1,), (1,)])

  result = pipeline.execute('SELECT * FROM test_simple_crash')[0]
  assert result['count'] in [6, 8, 10]

  # To ensure that all remaining events in ZMQ queues have been consumed
  time.sleep(2)


def test_concurrent_crash(pipeline, clean_db):
  """
  Test simple worker and combiner crashes.
  """
  pipeline.create_stream('stream0', x='int')
  q = 'SELECT COUNT(*) FROM stream0'
  pipeline.create_cv('test_concurrent_crash', q)
  batch_size = 25000

  desc = [0, 0, False]
  vals = [(1,)] * batch_size

  def insert():
    while True:
      pipeline.insert('stream0', ['x'], vals)
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

  result = pipeline.execute('SELECT count FROM test_concurrent_crash')[0]

  assert num_killed > 0
  assert result['count'] <= num_inserted
  assert result['count'] >= num_inserted - (num_killed * batch_size)

  # To ensure that all remaining events in ZMQ queues have been consumed
  time.sleep(2)


def test_restart_recovery(pipeline, clean_db):
  pipeline.create_stream('stream0', x='int')
  q = 'SELECT COUNT(*) FROM stream0'
  pipeline.create_cv('test_restart_recovery', q)

  pipeline.insert('stream0', ['x'], [(1,), (1,)])

  result = pipeline.execute('SELECT * FROM test_restart_recovery')[0]
  assert result['count'] == 2

  # Need to sleep here, otherwise on restart the materialization table is
  # empty. Not sure why.
  time.sleep(0.1)

  # Restart.
  pipeline.stop()
  pipeline.run()

  result = pipeline.execute('SELECT * FROM test_restart_recovery')[0]
  assert result['count'] == 2

  pipeline.insert('stream0', ['x'], [(1,), (1,)])

  result = pipeline.execute('SELECT * FROM test_restart_recovery')[0]
  assert result['count'] == 4


def test_postmaster_worker_recovery(pipeline, clean_db):
  """
  Verify that the postmaster only restarts crashed worker processes, and does not
  attempt to start them when the continuous query scheduler should.
  """
  expected_workers = len(get_worker_pids())
  assert expected_workers > 0

  expected_combiners = len(get_combiner_pids())
  assert expected_combiners > 0

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
    result = pipeline.execute("""SELECT pid, query FROM pg_stat_activity WHERE lower(query) LIKE '%%pg_sleep%%'""")[0]
    time.sleep(1)
    attempts += 1

  assert result

  backend_pid = result['pid']
  os.kill(backend_pid, signal.SIGKILL)

  attempts = 0
  pipeline.conn = None

  while attempts < 20:
    try:
      pipeline.execute('SELECT 1')
      break
    except:
      time.sleep(1)
      pass
    attempts += 1

  assert pipeline.conn

  # Now verify that we have the correct number of CQ worker procs
  assert expected_workers == len(get_worker_pids())
  assert expected_combiners == len(get_combiner_pids())
