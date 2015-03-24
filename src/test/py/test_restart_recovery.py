from base import pipeline, clean_db

def test_restart_recovery(pipeline, clean_db):
  q = 'SELECT COUNT(*) FROM stream'
  pipeline.create_cv('test_restart_recovery', q)

  pipeline.activate()
  pipeline.insert('stream', ['x'], [(1, ), (1, )])
  # Stop without deactivating.
  pipeline.stop()
  pipeline.run()
  pipeline.insert('stream', ['x'], [(1, ), (1, )])
  pipeline.deactivate()

  result = pipeline.execute('SELECT * FROM test_restart_recovery').first()
  assert result['count'] == 4
