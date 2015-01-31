from base import pipeline, clean_db
import os
import random
import signal
import threading
import time
from subprocess import check_output, CalledProcessError

def _get_pid(grep_str):
  try:
    out = check_output('ps aux | grep "postgres" | grep "%s"' % grep_str,
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
  q = 'SELECT COUNT(*) FROM stream'
  pipeline.create_cv('test_concurrent_crash', q)
  pipeline.activate()

  def insert(n):
    for _ in xrange(n):
        pipeline.insert('stream', ['x'], [(1, ), (1, )])
        time.sleep(0.1)

  killed = [0]
  n = 100

  def kill(n):
    for _ in xrange(n):
      r = random.random()
      if r > 0.9:
        killed[0] += kill_combiner('test_concurrent_crash')
      if r < 0.1:
        killed[0] += kill_worker('test_concurrent_crash')
      time.sleep(0.1)

  threads = [threading.Thread(target=insert, args=(n, )),
             threading.Thread(target=kill, args=(n, ))]
  map(lambda t: t.start(), threads)
  map(lambda t: t.join(), threads)

  killed = killed[0]

  pipeline.deactivate()

  result = pipeline.execute('SELECT * FROM test_concurrent_crash').first()
  assert killed > 0
  assert result['count'] >= (100 - killed) * 2
  assert result['count'] <= 100 * 2
