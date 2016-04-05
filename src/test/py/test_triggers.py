from base import pipeline, clean_db
import time
import psycopg2
import psycopg2.extensions
import getpass

def test_group_no_filter(pipeline, clean_db):
  """
  Test grouping query with trigger - when clause passes everything
  Verify counts are correct
  """
  pipeline.create_cv('cv0', 'SELECT x::integer,count(*) FROM stream group by x')
  pipeline.create_cv_trigger('t0', 'cv0', 'true', 'pipeline_test_alert_new_row')
  time.sleep(1)

  rows = [(n % 3,) for n in range(1000)]

  pipeline.insert_batches('stream', ('x',), rows, 250)
  time.sleep(1)

  lines = pipeline.read_trigger_output();
  d = {}

  for l in lines:
    assert(len(l) == 2)

    k = int(l[0])
    v = int(l[1])

    assert(k >= 0 and k <= 2)
    assert(k >= 0 and k <= 2)

    if (not d.has_key(k)):
      d[k] = 0

    old_val = d[k]
    assert(v - old_val > 0)
    d[k] = v

  assert(d[0] == 334)
  assert(d[1] == 333)
  assert(d[2] == 333)

def test_old_vals(pipeline, clean_db):
  """
  Test grouping query with an old in the when clause.
  """
  pipeline.create_cv('cv0', 'SELECT x::integer,count(*) FROM stream group by x')
  pipeline.create_cv_trigger('t0', 'cv0', 'old.count < 100', 'pipeline_test_alert_new_row', ttype='UPDATE')
  time.sleep(1)

  rows = [(n % 3,) for n in range(1000)]

  pipeline.insert_batches('stream', ('x',), rows, 250)
  time.sleep(1)

  lines = pipeline.read_trigger_output();
  d = {}

  for l in lines:
    assert(len(l) == 2)

    k = int(l[0])
    v = int(l[1])

    assert(k >= 0 and k <= 2)
    assert(k >= 0 and k <= 2)

    if (not d.has_key(k)):
      d[k] = 0

    old_val = d[k]
    assert(v - old_val > 0)
    d[k] = v

  assert(d[0] == 167)
  assert(d[1] == 167)
  assert(d[2] == 166)

def test_avg_no_filter(pipeline, clean_db):
  """
  Test grouping query with avg, when clause passes everything
  Verifies averages are correct
  """
  pipeline.create_cv('cv0', 'SELECT x::integer,avg(y::real) FROM stream group by x')
  pipeline.create_cv_trigger('t0', 'cv0', 'true', 'pipeline_test_alert_new_row')
  time.sleep(1)

  rows = [(n % 3,n) for n in range(1000)]
  pipeline.insert('stream', ('x','y'), rows)
  time.sleep(1)

  lines = pipeline.read_trigger_output()
  d = {}

  for l in lines:
    assert(len(l) == 2)

    k = int(l[0])
    v = float(l[1])

    assert(k >= 0 and k <= 2)

    if (not d.has_key(k)):
      d[k] = 0

    d[k] = v

  assert(d[0] == 499.5)
  assert(d[1] == 499)
  assert(d[2] == 500)

def test_group_filter(pipeline, clean_db):
  """
  Test grouping query with when clause new.x = 1
  Verifies count is correct and other data is filtered out
  """

  pipeline.create_cv('cv0', 'SELECT x::integer,count(*) FROM stream group by x')
  pipeline.create_cv_trigger('t0', 'cv0', 'new.x = 1', 'pipeline_test_alert_new_row')
  time.sleep(1)

  rows = [(n % 3,) for n in range(1000)]
  pipeline.insert('stream', ('x',), rows)
  time.sleep(1)

  lines = pipeline.read_trigger_output()
  old_val = 0
  val = None

  for l in lines:
    assert(len(l) == 2)

    k = int(l[0])
    v = int(l[1])

    assert(k == 1)
    assert(v - old_val > 0)
    val = v

  assert(val == 333)

def test_single_no_filter(pipeline, clean_db):
  """
  Test grouping query with single result
  Verifies count is correct
  """

  pipeline.create_cv('cv0', 'SELECT count(*) FROM stream')
  pipeline.create_cv_trigger('t0', 'cv0', 'true', 'pipeline_test_alert_new_row')
  time.sleep(1)

  rows = [(n % 3,) for n in range(1000)]
  pipeline.insert('stream', ('x',), rows)
  time.sleep(1)

  lines = pipeline.read_trigger_output()
  old_val = 0
  val = None

  for l in lines:
    assert(len(l) == 1)
    v = int(l[0])

    assert(v - old_val > 0)
    val = v

  assert(val == 1000)

def test_single_with_threshold(pipeline, clean_db):
  """
  Test grouping query with single result and a filter when new.count > 100
  Verifies trigger rows meet the filter criteria, and final count is correct
  """

  pipeline.create_cv('cv0', 'SELECT count(*) FROM stream')
  pipeline.create_cv_trigger('t0', 'cv0', 'new.count > 100', 'pipeline_test_alert_new_row')
  time.sleep(1)

  rows = [(n % 3,) for n in range(1000)]
  pipeline.insert('stream', ('x',), rows)
  time.sleep(1)

  lines = pipeline.read_trigger_output()
  old_val = 0
  val = None

  for l in lines:
    assert(len(l) == 1)
    v = int(l[0])

    assert(v - old_val > 0)
    assert(v > 100)
    val = v

  assert(val == 1000)

def test_append_no_filter(pipeline, clean_db):
  """
  Test append type query with no filter
  Verifies that every inserted row triggers, and keys are valid
  Verifies we receive the correct number of each group
  """

  pipeline.create_cv('cv0', 'SELECT x::int FROM stream')
  pipeline.create_cv_trigger('t0', 'cv0', 'true', 'pipeline_test_alert_new_row')
  time.sleep(1)

  rows = [(n % 3,) for n in range(1000)]
  pipeline.insert('stream', ('x',), rows)
  time.sleep(2)

  lines = pipeline.read_trigger_output()
  assert(len(lines) == 1000)

  d = {}

  for l in lines:
    assert(len(l) == 1)
    k = int(l[0])

    assert(k >= 0 and k <= 2)

    if (not d.has_key(k)):
      d[k] = 0

    d[k] += 1

  assert(d[0] == 334)
  assert(d[1] == 333)
  assert(d[2] == 333)

# This test is quite basic at the moment, because internal vacuuming is not
# being done. This mostly exercises the sw code path.

def test_sw_group_no_filter(pipeline, clean_db):
  """
  Test sw grouping query with trigger - when clause passes everything
  Some basic verification of group values, and counts are done.
  This test is expected to break when internal vacuuming is added
  """

  pipeline.create_cv('cv0', 'SELECT x::integer,count(*) FROM stream where (arrival_timestamp > clock_timestamp() - interval \'10 seconds\') group by x')
  pipeline.create_cv_trigger('t0', 'cv0', 'true', 'pipeline_test_alert_new_row')
  time.sleep(1)

  rows = [(n % 3,) for n in range(1000)]
  pipeline.insert('stream', ('x',), rows)
  time.sleep(2)

  lines = pipeline.read_trigger_output()
  d = {}

  for l in lines:
    assert(len(l) == 2)

    k = int(l[0])
    v = int(l[1])

    assert(k >= 0 and k <= 2)

    if (not d.has_key(k)):
      d[k] = 0

    old_val = d[k]
    assert(v - old_val > 0)
    d[k] = v

  assert(d[0] == 334)
  assert(d[1] == 333)
  assert(d[2] == 333)

def test_single_create_drop_trigger(pipeline, clean_db):
  """
  Exercises the trigger diffing logic
  """
  pipeline.create_cv('cv0', 'SELECT count(*) FROM stream')
  pipeline.create_cv_trigger('t0', 'cv0', 'new.count <= 2', 'pipeline_test_alert_new_row')
  time.sleep(0.1)

  pipeline.insert('stream', ('x',), [(0,)])
  time.sleep(0.1)

  pipeline.drop_cv_trigger('t0', 'cv0')
  pipeline.insert('stream', ('x',), [(0,)])

  time.sleep(1)

def test_create_drop_trigger(pipeline, clean_db):
  """
  Verify that trigger cache entries are invalidated as triggers are created and dropped
  """
  pipeline.create_cv('cv0', 'SELECT count(*) FROM stream')
  pipeline.create_cv_trigger('t0', 'cv0', 'new.count <= 2', 'pipeline_test_alert_new_row')
  time.sleep(0.1)
  pipeline.create_cv_trigger('t1', 'cv0', 'new.count <= 2', 'pipeline_test_alert_new_row')
  time.sleep(0.1)

  pipeline.insert('stream', ('x',), [(0,)])
  time.sleep(1.0)

  # Drop the second one so only the first fires now
  pipeline.drop_cv_trigger('t1', 'cv0')
  time.sleep(0.1)

  pipeline.insert('stream', ('x',), [(0,)])
  time.sleep(0.1)

  pipeline.insert('stream', ('x',), [(0,)])
  time.sleep(0.1)

  pipeline.insert('stream', ('x',), [(x,) for x in range(100)])
  time.sleep(1)

  # t0 fired twice, t1 fired once
  lines = pipeline.read_trigger_output()
  assert len(lines) == 3

  result = pipeline.execute('SELECT count FROM cv0').first()

  # Recreate t1 with a differnt WHEN clause and verify that it fires again
  pipeline.create_cv_trigger('t1', 'cv0', 'new.count > 2', 'pipeline_test_alert_new_row')
  pipeline.insert('stream', ('x',), [(0,)])
  time.sleep(1)

  lines = pipeline.read_trigger_output()
  assert len(lines) == 4

def test_sw_external_vacuum(pipeline, clean_db):
  """
  Sets up a sliding window query, inserts data into it, and then
  vacuums. After vacuuming, it inserts more data.

  Final tallies are verified to be insert amount minus vacuumed amt
  """
  pipeline.create_cv('cv0', 'SELECT x::integer,count(*) FROM stream where (arrival_timestamp > clock_timestamp() - interval \'3 seconds\') group by x;', step_factor=10)

  pipeline.create_cv_trigger('t0', 'cv0', 'true', 'pipeline_test_alert_new_row')

  time.sleep(1)

  rows = [(n % 10,) for n in range(1000)]
  pipeline.insert('stream', ('x',), rows)
  time.sleep(1)

  # During this sleep, the internal vacuumer in trigger.c will be expiring
  # tuples and firing triggers
  time.sleep(5)

  # Note: the following external vacuum isn't expected to cause
  # any internal state to change, but we perform it to exercise that code path

  conn = psycopg2.connect('dbname=pipeline user=%s host=localhost port=%s' %
                          (getpass.getuser(), pipeline.port))
  conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
  cur = conn.cursor()
  cur.execute('VACUUM cv0')
  conn.close()

  time.sleep(1)
  pipeline.insert('stream', ('x',), rows)
  time.sleep(1)

  lines = pipeline.read_trigger_output()
  d = {}

  for l in lines:
    assert(len(l) == 2)

    k = int(l[0])
    v = int(l[1])

    assert(k >= 0 and k <= 9)
    d[k] = v

  assert(len(d) == 10)

  for x in d:
    assert(d[x] == 100)

def test_sw_trigger_sync(pipeline, clean_db):
  """
  Sets up a sliding window query, and inserts data into it before
  any triggers are added. A trigger is added, and then some more data is
  inserted.

  Verify that counts are equal to pre-creation plus post_creation amts
  """
  pipeline.create_cv('cv0', 'SELECT x::integer,count(*) FROM stream where (arrival_timestamp > clock_timestamp() - interval \'10 seconds\') group by x;', step_factor=10)

  rows = [(n % 10,) for n in range(1000)]
  pipeline.insert('stream', ('x',), rows)

  # sleep to make sure that new inserts are in a new arrival_ts group

  time.sleep(4)

  pipeline.create_cv_trigger('t0', 'cv0', 'true', 'pipeline_test_alert_new_row')
  time.sleep(1)

  rows = [(n%10,) for n in range(10)]
  pipeline.insert('stream', ('x',), rows)

  time.sleep(1)
  lines = pipeline.read_trigger_output()
  d = {}

  for l in lines:
    assert(len(l) == 2)

    k = int(l[0])
    v = int(l[1])

    assert(k >= 0 and k <= 9)
    d[k] = v

  assert(len(d) == 10)

  for x in d:
    assert(d[x] == 101)

def test_sw_internal_vacuum(pipeline, clean_db):
  """
  Sets up a sliding window query, and inserts data into it.
  This test does not verify results, it is for code coverage.
  """

  pipeline.create_cv('cv0', 'SELECT x::integer,count(*) FROM stream where (arrival_timestamp > clock_timestamp() - interval \'3 seconds\') group by x;', step_factor=10)

  pipeline.create_cv_trigger('t0', 'cv0', 'true', 'pipeline_test_alert_new_row')

  rows = [(n % 10,) for n in range(1000)]
  pipeline.insert('stream', ('x',), rows)

  rows = [(n % 1,) for n in range(1)]

  for x in range(5):
    pipeline.insert('stream', ('x',), [(0,)])
    time.sleep(1)
