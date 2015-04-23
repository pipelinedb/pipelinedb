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

def kill_worker(cvname):
  return _kill(_get_pid('%s \[worker\]' % cvname))

def kill_combiner(cvname):
  return _kill(_get_pid('%s \[combiner\]' % cvname))

def test_simple_crash(pipeline, clean_db):
  """
  Test simple worker and combiner crashes.
  """
  q = 'SELECT COUNT(*) FROM stream'
  pipeline.create_cv('test_simple_crash', q)
  pipeline.activate()

  pipeline.insert('stream', ['x'], [(1, ), (1, )])

  pipeline.deactivate()

  result = pipeline.execute('SELECT * FROM test_simple_crash').first()
  count = result['count']
  assert count == 2

  # We can potentially lose one batch for a worker or combiner crash.
  # In our case each batch adds a count 2 and since we're adding 3 batches
  # we should either see an increment from the previous count of 4 or 6.
  pipeline.activate()
  pipeline.insert('stream', ['x'], [(1, ), (1, )])

  kill_worker('test_simple_crash')

  pipeline.insert('stream', ['x'], [(1, ), (1, )])
  pipeline.insert('stream', ['x'], [(1, ), (1, )])

  pipeline.deactivate()

  result = pipeline.execute('SELECT * FROM test_simple_crash').first()
  assert result['count'] - count in (4, 6)
  count = result['count']

  pipeline.activate()
  pipeline.insert('stream', ['x'], [(1, ), (1, )])

  kill_combiner('test_simple_crash')

  pipeline.insert('stream', ['x'], [(1, ), (1, )])
  pipeline.insert('stream', ['x'], [(1, ), (1, )])

  pipeline.deactivate()

  result = pipeline.execute('SELECT * FROM test_simple_crash').first()
  assert result['count'] - count in (4, 6)

def test_concurrent_crash(pipeline, clean_db):
  """
  Test simple worker and combiner crashes.
  """
  q = 'SELECT x::int, pg_sleep(0.02) FROM stream'
  pipeline.create_cv('test_concurrent_crash', q)
  pipeline.activate()

  desc = [0, 0, False]
  vals = [(1, )] * 1000

  def insert():
    while True:
        pipeline.insert('stream', ['x'], vals)
        time.sleep(0.05)
        desc[1] += 1000
        if desc[2]:
          break

  def kill():
    for _ in xrange(100):
      r = random.random()
      if r > 0.85:
        desc[0] += kill_combiner('test_concurrent_crash')
      if r < 0.15:
        desc[0] += kill_worker('test_concurrent_crash')
      time.sleep(0.1)

    desc[2] = True

  threads = [threading.Thread(target=insert),
             threading.Thread(target=kill)]
  map(lambda t: t.start(), threads)
  map(lambda t: t.join(), threads)

  num_killed = desc[0]
  num_inserted = desc[1]

  pipeline.deactivate()

  result = pipeline.execute('SELECT COUNT(*) FROM test_concurrent_crash').first()
  assert num_killed > 0

  assert result['count'] >= num_inserted - (num_killed * 1000)
  assert result['count'] < num_inserted
