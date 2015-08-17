from base import pipeline, clean_db
import time


def test_backoff(pipeline, clean_db):
  throttle_time = int(pipeline.execute('SHOW continuous_query_error_throttle').first()['continuous_query_error_throttle'].rstrip('s'))

  pipeline.execute("""
CREATE OR REPLACE FUNCTION error_sfunc (
  state int,
  i     int
) RETURNS int AS 
$$
BEGIN
  IF i = 1 THEN
    i := i / 0;
  END IF;

  IF state IS NULL THEN
    state := 0;
  END IF;

  RETURN state + 1;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;
  """)

  pipeline.execute("""
CREATE OR REPLACE FUNCTION error_combinefunc (
  state int,
  i     int
) RETURNS int AS 
$$
BEGIN
  IF state IS NULL THEN
    state := 0;
  END IF;

  RETURN state + i;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;
  """)

  
  pipeline.execute("""
CREATE AGGREGATE error_agg (int) (
  stype = int,
  sfunc = error_sfunc,
  combinefunc = error_combinefunc
);
  """)

  def insert_events():
    n = 0
    # These should succeed
    for _ in xrange(100):
      pipeline.insert('error_stream', ('x', ), [(10, )])
    n += 100
    # Insert erroneous input till we're throttled to the max limit
    start = time.time()
    while True:
      # These will fail
      for _ in xrange(100):
        pipeline.insert('error_stream', ('x', ), [(1, )])
      n += 100
      if time.time() - start >= throttle_time:
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

  pipeline.create_cv('error_cv', 'SELECT error_agg(x::int) FROM error_stream')
  insert_events()

  assert pipeline.execute('SELECT error_agg FROM error_cv').first()['error_agg'] == 200

  pipeline.create_cv('no_error_cv', 'SELECT count(*) FROM error_stream')

  n = insert_events()
  assert pipeline.execute('SELECT error_agg FROM error_cv').first()['error_agg'] == 400
  assert pipeline.execute('SELECT count FROM no_error_cv').first()['count'] == n
  
  pipeline.execute('DROP FUNCTION error_sfunc (int, int) CASCADE')
