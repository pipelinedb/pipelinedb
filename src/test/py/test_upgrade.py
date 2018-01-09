from base import pipeline, clean_db, PipelineDB
import os
import shutil
import subprocess
import time


def test_binary_upgrade(pipeline, clean_db):
  """
  Verify that binary upgrades properly transfer all objects and data
  into the new installation
  """
  # Create some regular tables with data, and create an index on half of them
  for n in range(16):
    name = 't_%d' % n
    pipeline.create_table(name, x='integer', y='text', z='text')
    rows = [(x, name, name) for x in range(1000)]
    pipeline.insert(name, ('x', 'y', 'z'), rows)
    if n >= 8:
      pipeline.execute('CREATE INDEX idx_%s ON %s(y)' % (name, name))

  # Create some streams
  for n in range(8):
    name = 's_%d' % n
    pipeline.create_stream(name, x='integer', y='text')

  # Now create some CVs with data, some with indices
  for n in range(32):
    name = 'cv_%d' % n
    pipeline.create_stream('stream_%d' % n, x='int', y='text', z='text')
    pipeline.create_cv(name, 'SELECT z::text, COUNT(DISTINCT z) AS distinct_count, COUNT(*) FROM stream_%d GROUP BY z' % n)
    rows = [(x, name, name) for x in range(1000)]
    pipeline.insert('stream_%d' % n, ('x', 'y', 'z'), rows)
    if n >= 16:
      pipeline.execute('CREATE INDEX idx_%s ON %s(z)' % (name, name))

  # Now create some in another namespace
  pipeline.execute('CREATE SCHEMA namespace')
  for n in range(8):
    name = 'namespace.cv_%d' % n
    pipeline.create_stream('namespace.stream_%d' % n, x='int', y='text', z='text')
    pipeline.create_cv(name, 'SELECT z::text, COUNT(DISTINCT z) AS distinct_count, COUNT(*) FROM namespace.stream_%d GROUP BY z' % n)
    rows = [(x, name, name) for x in range(1000)]
    pipeline.insert('namespace.stream_%d' % n, ('x', 'y', 'z'), rows)
    if n >= 4:
      pipeline.execute('CREATE INDEX namespace_idx_%d ON %s(z)' % (n, name))

  create_fn = """
  CREATE OR REPLACE FUNCTION tg_fn()
  RETURNS trigger AS
  $$
  BEGIN
   RETURN NEW;
  END;
  $$
  LANGUAGE plpgsql;
  """
  pipeline.execute(create_fn)

  pipeline.create_stream('stream0', z='text')

  # Create some transforms with trigger functions
  for n in range(8):
    name = 'ct_%d' % n
    pipeline.create_ct(name, 'SELECT z::text FROM stream0', 'tg_fn()')

  # Create some transforms without trigger functions
  for n in range(8):
    name = 'ct_no_trig_%d' % n
    pipeline.create_ct(name, 'SELECT z::text FROM stream0')

  time.sleep(10)

  old_bin_dir = new_bin_dir = pipeline.bin_dir
  old_data_dir = pipeline.data_dir
  new_data_dir = os.path.abspath('test_binary_upgrade_data_dir')

  if os.path.exists(new_data_dir):
    shutil.rmtree(new_data_dir)

  pipeline.stop()

  p = subprocess.Popen([
    os.path.join(pipeline.bin_dir, 'pipeline-init'), '-D', new_data_dir])
  stdout, stderr = p.communicate()

  result = subprocess.check_call([
    os.path.join(pipeline.bin_dir, 'pipeline-upgrade'),
    '-b', old_bin_dir, '-B', new_bin_dir,
    '-d', old_data_dir, '-D', new_data_dir])

  assert result == 0

  # The cleanup path expects this to be running, but we're done with it
  pipeline.run()

  # pipeline-upgrade returned successfully and has already done sanity checks
  # but let's manually verify that all objects were migrated to the new data directory
  upgraded = PipelineDB(data_dir=new_data_dir)
  upgraded.run()

  # Tables
  for n in range(16):
    name = 't_%d' % n
    q = 'SELECT x, y, z FROM %s ORDER BY x' % name
    rows = upgraded.execute(q)
    for i, row in enumerate(rows):
      x, y, z = row
      assert x == i
      assert y == name
      assert z == name

  # Streams
  for n in range(8):
    name = 's_%d' % n
    rows = list(upgraded.execute("SELECT oid FROM pg_class WHERE relkind = '$' AND relname = '%s'" % name))
    assert len(rows) == 1

  # CVs
  for n in range(32):
    name = 'cv_%d' % n
    rows = list(upgraded.execute('SELECT z, distinct_count, count FROM %s' % name))
    assert len(rows) == 1

    assert rows[0][0] == name
    assert rows[0][1] == 1
    assert rows[0][2] == 1000

  # CVs in separate schema
  for n in range(8):
    name = 'namespace.cv_%d' % n
    rows = list(upgraded.execute('SELECT z, distinct_count, count FROM %s' % name))
    assert len(rows) == 1

    assert rows[0][0] == name
    assert rows[0][1] == 1
    assert rows[0][2] == 1000

  # Transforms with trigger functions
  for n in range(8):
    name = 'ct_%d' % n
    q = """
    SELECT c.relname FROM pg_class c JOIN pipeline_query pq
    ON c.oid = pq.relid WHERE pq.type = 't' AND c.relname = '%s'
    """ % name
    rows = list(upgraded.execute(q))
    assert len(rows) == 1

  # Transforms without trigger functions
  for n in range(8):
    name = 'ct_no_trig_%d' % n
    q = """
    SELECT c.relname FROM pg_class c JOIN pipeline_query pq
    ON c.oid = pq.relid WHERE pq.type = 't' AND c.relname = '%s'
    """ % name
    rows = list(upgraded.execute(q))
    assert len(rows) == 1

  upgraded.stop()
  shutil.rmtree(new_data_dir)
