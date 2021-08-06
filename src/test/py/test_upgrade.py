from base import pipeline, clean_db, PipelineDB
import pytest
import os
import shutil
import subprocess
import time


def test_binary_upgrade(pipeline, clean_db):
  """
  Verify that binary upgrades properly transfer all objects and data
  into the new installation
  """
  if pipeline.version_num == 110000:
    pytest.skip('skipping until PG11 supports dump/restore WITH OIDS')

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
    if n >= 16:
      pipeline.execute('CREATE INDEX idx_%s ON %s(z)' % (name, name))

  # Create some STJs
  for n in range(8):
    pipeline.create_cv('stj_%d' % n,
      'SELECT t.x, count(*) FROM stream_%d s JOIN t_%d t ON s.x = t.x GROUP BY t.x' % (n, n))

  # Create some SW CVs
  for n in range(8):
    pipeline.create_cv('sw_%d' % n, 'SELECT count(*) FROM stream_%d' % n, sw='%d days' % (n + 1), step_factor=n + 1)

  # Create some CVs/CTs/streams that we'll rename
  for n in range(4):
    pipeline.create_stream('to_rename_s_%d' % n, x='int')
    pipeline.create_cv('to_rename_cv_%d' % n, 'SELECT x, count(*) FROM to_rename_s_%d GROUP BY x' % n)
    pipeline.create_ct('to_rename_ct_%d' % n, 'SELECT x FROM to_rename_s_%d' % n)
    pipeline.create_cv('to_rename_ct_reader_%d' % n, "SELECT count(*) FROM output_of('to_rename_ct_%d')" % n)

    rows = [(x,) for x in range(1000)]
    pipeline.insert('to_rename_s_%d' % n, ('x',), rows)

  # Now rename them
  for n in range(4):
    pipeline.execute('ALTER FOREIGN TABLE to_rename_s_%d RENAME TO renamed_s_%d' % (n, n))
    pipeline.execute('ALTER VIEW to_rename_cv_%d RENAME TO renamed_cv_%d' % (n, n))
    pipeline.execute('ALTER VIEW to_rename_ct_%d RENAME TO renamed_ct_%d' % (n, n))
    pipeline.execute('ALTER VIEW to_rename_ct_reader_%d RENAME TO renamed_ct_reader_%d' % (n, n))

    # And write some data using the new stream names
    rows = [(x,) for x in range(1000)]
    pipeline.insert('renamed_s_%d' % n, ('x',), rows)

  # Create a CV chain that combines output streams
  q = """
  SELECT (new).z, combine((delta).count) AS count, combine((delta).distinct_count) AS distinct_count FROM output_of('cv_0') GROUP BY (new).z
  """
  pipeline.create_cv('combine_cv_0', q)
  q = """
  SELECT combine((delta).count) AS count, combine((delta).distinct_count) AS distinct_count FROM output_of('combine_cv_0')
  """
  pipeline.create_cv('combine_cv_1', q)

  for n in range(32):
    name = 'cv_%d' % n
    rows = [(x, name, name) for x in range(1000)]
    pipeline.insert('stream_%d' % n, ('x', 'y', 'z'), rows)

  # Create a CV with a TTL to verify TTL info is restored properly
  pipeline.create_cv('ttlcv', 'SELECT second(arrival_timestamp), count(*) FROM stream_0 GROUP BY second', ttl='1 hour', ttl_column='second')

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
    pipeline.create_ct(name, 'SELECT z::text FROM stream0', 'tg_fn')

  # Create some transforms without trigger functions
  for n in range(8):
    name = 'ct_no_trig_%d' % n
    pipeline.create_ct(name, 'SELECT z::text FROM stream0')

  time.sleep(10)
  
  old_bin_dir = new_bin_dir = pipeline.bin_dir
  old_data_dir = pipeline.data_dir
  new_data_dir0 = os.path.abspath('test_binary_upgrade_data_dir0')

  if os.path.exists(new_data_dir0):
    shutil.rmtree(new_data_dir0)

  pipeline.stop()

  p = subprocess.Popen([
    os.path.join(pipeline.bin_dir, b'initdb'), '-D', new_data_dir0])
  stdout, stderr = p.communicate()

  with open(os.path.join(new_data_dir0, 'postgresql.conf'), 'a') as f:
    f.write('shared_preload_libraries=pipelinedb\n')
    f.write('max_worker_processes=128\n')
    f.write('pipelinedb.stream_insert_level=sync_commit\n')

  result = subprocess.check_call([
    os.path.join(pipeline.bin_dir, b'pg_upgrade'),
    '-b', old_bin_dir, '-B', new_bin_dir,
    '-d', old_data_dir, '-D', new_data_dir0])
  
  assert result == 0

  # The cleanup path expects this to be running, but we're done with it
  pipeline.run()

  # pg_upgrade returned successfully and has already done sanity checks
  # but let's manually verify that all objects were migrated to the new data directory
  upgraded = PipelineDB(data_dir=new_data_dir0)
  upgraded.run()

  # Tables
  for n in range(16):
    name = 't_%d' % n
    q = 'SELECT x, y, z FROM %s ORDER BY x' % name
    rows = upgraded.execute(q)
    for i, row in enumerate(rows):
      assert row['x'] == i
      assert row['y'] == name
      assert row['z'] == name

  # Streams
  for n in range(8):
    name = 's_%d' % n
    rows = list(upgraded.execute("SELECT oid FROM pg_class WHERE relkind = 'f' AND relname = '%s'" % name))
    assert len(rows) == 1

  # CVs
  for n in range(32):
    name = 'cv_%d' % n
    rows = list(upgraded.execute('SELECT z, distinct_count, count FROM %s' % name))
    assert len(rows) == 1

    assert rows[0][0] == name
    assert rows[0][1] == 1
    assert rows[0][2] == 1000

  # CV with TTL
  row = list(upgraded.execute("SELECT ttl, ttl_attno FROM pg_class c JOIN pipelinedb.cont_query pq on c.oid = pq.relid WHERE c.relname = 'ttlcv'"))[0]
  assert row[0] == 3600
  assert row[1] == 1

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
    SELECT c.relname FROM pg_class c JOIN pipelinedb.cont_query pq
    ON c.oid = pq.relid WHERE pq.type = 't' AND c.relname = '%s'
    """ % name
    rows = list(upgraded.execute(q))
    assert len(rows) == 1

  # Transforms without trigger functions
  for n in range(8):
    name = 'ct_no_trig_%d' % n
    q = """
    SELECT c.relname FROM pg_class c JOIN pipelinedb.cont_query pq
    ON c.oid = pq.relid WHERE pq.type = 't' AND c.relname = '%s'
    """ % name
    rows = list(upgraded.execute(q))
    assert len(rows) == 1

  # Verify SW CVs
  for n in range(8):
    name = 'sw_%d' % n
    row = upgraded.execute("SELECT ttl, step_factor FROM pipelinedb.cont_query cq JOIN pg_class c ON cq.relid = c.oid WHERE relname = '%s'" % name)[0]
    assert row['ttl'] == (n + 1) * 3600 * 24
    assert row['step_factor'] == n + 1

    row = upgraded.execute('SELECT count FROM %s' % name)[0]
    assert row['count'] == 1000

  # Verify renamed CVs/CTs/streams
  for n in range(4):
    row = upgraded.execute('SELECT combine(count) FROM renamed_cv_%d' % n)[0]
    assert row['combine'] == 2000
    row = upgraded.execute('SELECT combine(count) FROM renamed_ct_reader_%d' % n)[0]
    assert row['combine'] == 2000

  # Verify chained CVs
  row = upgraded.execute('SELECT z, count, distinct_count FROM combine_cv_0')[0]
  assert row['z'] == 'cv_0'
  assert row['count'] == 1000
  assert row['distinct_count'] == 1

  row = upgraded.execute('SELECT count, distinct_count FROM combine_cv_1')[0]
  assert row['count'] == 1000
  assert row['distinct_count'] == 1

  # Now insert some new data and verify CVs are still updating properly
  for n in range(32):
    name = 'cv_%d' % n
    rows = [(x, name, name) for x in range(1000)]
    upgraded.insert('stream_%d' % n, ('x', 'y', 'z'), rows)

  for n in range(32):
    name = 'cv_%d' % n
    rows = list(upgraded.execute('SELECT z, distinct_count, count FROM %s' % name))
    assert len(rows) == 1

    assert rows[0][0] == name
    assert rows[0][1] == 1
    assert rows[0][2] == 2000

  row = upgraded.execute('SELECT z, count, distinct_count FROM combine_cv_0')[0]
  assert row['z'] == 'cv_0'
  assert row['count'] == 2000
  assert row['distinct_count'] == 1

  row = upgraded.execute('SELECT count, distinct_count FROM combine_cv_1')[0]
  assert row['count'] == 2000
  assert row['distinct_count'] == 1

  # Verify STJs
  for n in range(8):
    cv = 'stj_%d' % n
    row = upgraded.execute('SELECT sum(count) FROM %s' % cv)[0]
    assert row['sum'] == 2000

  # Rename objects again before the second upgrade
  for n in range(4):
    upgraded.execute('ALTER FOREIGN TABLE renamed_s_%d RENAME TO renamed_again_s_%d' % (n, n))
    upgraded.execute('ALTER VIEW renamed_cv_%d RENAME TO renamed_again_cv_%d' % (n, n))
    upgraded.execute('ALTER VIEW renamed_ct_%d RENAME TO renamed_again_ct_%d' % (n, n))
    upgraded.execute('ALTER VIEW renamed_ct_reader_%d RENAME TO renamed_again_ct_reader_%d' % (n, n))

    # And write some data using the new stream names
    rows = [(x,) for x in range(1000)]
    upgraded.insert('renamed_again_s_%d' % n, ('x',), rows)

  upgraded.stop()

  new_data_dir1 = os.path.abspath('test_binary_upgrade_data_dir1')
  if os.path.exists(new_data_dir1):
    shutil.rmtree(new_data_dir1)

  p = subprocess.Popen([
  os.path.join(pipeline.bin_dir, b'initdb'), '-D', new_data_dir1])
  stdout, stderr = p.communicate()

  with open(os.path.join(new_data_dir1, 'postgresql.conf'), 'a') as f:
    f.write('shared_preload_libraries=pipelinedb\n')
    f.write('max_worker_processes=128\n')
    f.write('pipelinedb.stream_insert_level=sync_commit\n')

  # Now upgrade the upgraded DB to verify that restored DBs can be updated properly
  result = subprocess.check_call([
    os.path.join(pipeline.bin_dir, b'pg_upgrade'),
    '-b', old_bin_dir, '-B', new_bin_dir,
    '-d', new_data_dir0, '-D', new_data_dir1])
  
  assert result == 0

  # but let's manually verify that all objects were migrated to the new data directory
  upgraded = PipelineDB(data_dir=new_data_dir1)
  upgraded.run()

  # Tables
  for n in range(16):
    name = 't_%d' % n
    q = 'SELECT x, y, z FROM %s ORDER BY x' % name
    rows = upgraded.execute(q)
    for i, row in enumerate(rows):
      assert row['x'] == i
      assert row['y'] == name
      assert row['z'] == name

  # Streams
  for n in range(8):
    name = 's_%d' % n
    rows = list(upgraded.execute("SELECT oid FROM pg_class WHERE relkind = 'f' AND relname = '%s'" % name))
    assert len(rows) == 1

  # CVs
  for n in range(32):
    name = 'cv_%d' % n
    rows = list(upgraded.execute('SELECT z, distinct_count, count FROM %s' % name))
    assert len(rows) == 1

    assert rows[0][0] == name
    assert rows[0][1] == 1
    assert rows[0][2] == 2000

  # CV with TTL
  row = list(upgraded.execute("SELECT ttl, ttl_attno FROM pg_class c JOIN pipelinedb.cont_query pq on c.oid = pq.relid WHERE c.relname = 'ttlcv'"))[0]
  assert row[0] == 3600
  assert row[1] == 1

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
    SELECT c.relname FROM pg_class c JOIN pipelinedb.cont_query pq
    ON c.oid = pq.relid WHERE pq.type = 't' AND c.relname = '%s'
    """ % name
    rows = list(upgraded.execute(q))
    assert len(rows) == 1

  # Transforms without trigger functions
  for n in range(8):
    name = 'ct_no_trig_%d' % n
    q = """
    SELECT c.relname FROM pg_class c JOIN pipelinedb.cont_query pq
    ON c.oid = pq.relid WHERE pq.type = 't' AND c.relname = '%s'
    """ % name
    rows = list(upgraded.execute(q))
    assert len(rows) == 1

  # Verify SW Cvs
  for n in range(8):
    name = 'sw_%d' % n
    step_factor = n + 1
    row = upgraded.execute("SELECT ttl, step_factor FROM pipelinedb.cont_query cq JOIN pg_class c ON cq.relid = c.oid WHERE relname = '%s'" % name)[0]
    assert row['ttl'] == (n + 1) * 3600 * 24
    assert row['step_factor'] == n + 1

    row = upgraded.execute('SELECT count FROM %s' % name)[0]
    assert row['count'] == 2000

  # Verify renamed CVs/CTs/streams
  for n in range(4):
    row = upgraded.execute('SELECT combine(count) FROM renamed_again_cv_%d' % n)[0]
    assert row['combine'] == 3000
    row = upgraded.execute('SELECT combine(count) FROM renamed_again_ct_reader_%d' % n)[0]
    assert row['combine'] == 3000

  # Verify chained CV
  row = upgraded.execute('SELECT z, count, distinct_count FROM combine_cv_0')[0]
  assert row['z'] == 'cv_0'
  assert row['count'] == 2000
  assert row['distinct_count'] == 1

  row = upgraded.execute('SELECT count, distinct_count FROM combine_cv_1')[0]
  assert row['count'] == 2000
  assert row['distinct_count'] == 1

  # Now insert some new data and verify CVs are still updating properly
  for n in range(32):
    name = 'cv_%d' % n
    rows = [(x, name, name) for x in range(1000)]
    upgraded.insert('stream_%d' % n, ('x', 'y', 'z'), rows)

  for n in range(32):
    name = 'cv_%d' % n
    rows = list(upgraded.execute('SELECT z, distinct_count, count FROM %s' % name))
    assert len(rows) == 1

    assert rows[0][0] == name
    assert rows[0][1] == 1
    assert rows[0][2] == 3000

  row = upgraded.execute('SELECT z, count, distinct_count FROM combine_cv_0')[0]
  assert row['z'] == 'cv_0'
  assert row['count'] == 3000
  assert row['distinct_count'] == 1

  row = upgraded.execute('SELECT count, distinct_count FROM combine_cv_1')[0]
  assert row['count'] == 3000
  assert row['distinct_count'] == 1

  # Verify STJs
  for n in range(8):
    cv = 'stj_%d' % n
    row = upgraded.execute('SELECT sum(count) FROM %s' % cv)[0]
    assert row['sum'] == 3000

  upgraded.stop()

  pipeline.execute('DROP VIEW combine_cv_0 CASCADE')
  shutil.rmtree(new_data_dir0)
  shutil.rmtree(new_data_dir1)
