from base import pipeline, clean_db
import math
import time


def test_backoff(pipeline, clean_db):
  throttle_time = int(pipeline.execute('SHOW continuous_query_error_throttle')
                      .first()['continuous_query_error_throttle'].rstrip('s'))
  
  def insert_events():
    n = 0

    # These should succeed
    for _ in xrange(100):
      pipeline.insert('error_stream', ('x', ), [(10, )])
    n += 100

    # Insert erroneous input till we're throttled to the max limit
    start = time.time()
    p = int(math.floor(math.log(throttle_time * 1000, 2)))
    total = -1 * (1 - math.pow(2, p + 1)) / 1000

    while True:
      # These will fail
      for _ in xrange(100):
        pipeline.insert('error_stream', ('x', ), [(0, )])
      n += 100
      if time.time() - start >= total:
        break

    # These should get throttled
    for _ in xrange(100):
      pipeline.insert('error_stream', ('x', ), [(10, )])
    n += 100

    time.sleep(throttle_time)
    # These should work
    for _ in xrange(100):
      pipeline.insert('error_stream', ('x', ), [(10, )])
    n += 100
    return n

  pipeline.create_cv('error_cv', 'SELECT 100/x::int FROM error_stream')
  insert_events()

  assert pipeline.execute('SELECT count(*) FROM error_cv').first()['count'] == 200

  pipeline.create_cv('no_error_cv', 'SELECT x::int FROM error_stream')

  n = insert_events()
  assert pipeline.execute('SELECT count(*) FROM error_cv').first()['count'] == 400
  assert pipeline.execute('SELECT count(*) FROM no_error_cv').first()['count'] == n
