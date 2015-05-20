from base import pipeline, clean_db
import time

def test_restart_recovery(pipeline, clean_db):
  q = 'SELECT COUNT(*) FROM stream'
  pipeline.create_cv('test_restart_recovery', q)

  pipeline.insert('stream', ['x'], [(1, ), (1, )])

  result = pipeline.execute('SELECT * FROM test_restart_recovery').first()
  assert result['count'] == 2

  # Sleep a little to let everything be committed.
  time.sleep(0.1)

  # Restart.
  pipeline.stop()
  pipeline.run()

  pipeline.insert('stream', ['x'], [(1, ), (1, )])

  result = pipeline.execute('SELECT * FROM test_restart_recovery').first()
  assert result['count'] == 4
