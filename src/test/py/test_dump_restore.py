from base import pipeline, clean_db
import os
import subprocess
import time


def _dump(pipeline, out_filename, tables=[], data_only=False, schema_only=False):
  out = open(out_filename, 'w')
  cmd = [os.path.join(pipeline.bin_dir, 'pg_dump'), '-p', str(pipeline.port), '-d', 'postgres']
  for table in tables:
    cmd += ['-t', table]
  if data_only:
    cmd += ['-a']
  if schema_only:
    cmd += ['-s']
  subprocess.Popen(cmd, stdout=out).communicate()
  out.close()

def _restore(pipeline, in_filename):
  cmd = [os.path.join(pipeline.bin_dir, 'psql'), '-p', str(pipeline.port), '-d', 'postgres', '-f', in_filename]
  subprocess.Popen(cmd).communicate()
  os.remove(in_filename)


def test_dump(pipeline, clean_db):
  """
  Verify that we can dump and restore CVs using INSERT statements
  """
  pipeline.create_stream('stream0', x='int')
  q = """
  SELECT x::integer % 100 AS g, avg(x) + 1 AS avg, count(*), count(distinct x) AS distincts FROM stream0
  GROUP BY g
  """
  pipeline.create_cv('test_dump', q)

  rows = [(x,) for x in range(1000)]
  pipeline.insert('stream0', ('x',), rows)

  def _verify():
    result = pipeline.execute('SELECT count(*) FROM test_dump')[0]
    assert result['count'] == 100

    result = pipeline.execute('SELECT sum(avg) FROM test_dump')[0]
    assert result['sum'] == 50050

    result = pipeline.execute('SELECT sum(distincts) FROM test_dump')[0]
    assert result['sum'] == 1000

  _verify()
  _dump(pipeline, 'test_dump.sql')

  pipeline.drop_all()
  _restore(pipeline, 'test_dump.sql')
  _verify()

  # Now verify that we can successfully add more data to the restored CV
  rows = [(x,) for x in range(2000)]
  pipeline.insert('stream0', ('x',), rows)

  result = pipeline.execute('SELECT sum(count) FROM test_dump')[0]
  assert result['sum'] == 3000

  result = pipeline.execute('SELECT sum(distincts) FROM test_dump')[0]
  assert result['sum'] == 2000

def test_sliding_windows(pipeline, clean_db):
  """
  Verify that sliding window queries are properly dumped and restored
  """
  pipeline.create_stream('stream0', x='int')
  pipeline.execute('CREATE VIEW sw_v WITH (sw = \'20 seconds\') AS SELECT count(*) FROM stream0')
  pipeline.insert('stream0', ('x',), [(x,) for x in range(10)])

  result = pipeline.execute('SELECT count FROM sw_v')[0]
  assert result['count'] == 10

  _dump(pipeline, 'test_sw.sql')

  pipeline.drop_all()
  _restore(pipeline, 'test_sw.sql')

  result = pipeline.execute('SELECT count FROM sw_v')[0]
  assert result['count'] == 10

  # We should still drop back to 0 within 20 seconds
  result = pipeline.execute('SELECT count FROM sw_v')[0]
  while result['count'] > 0:
    time.sleep(1)
    result = pipeline.execute('SELECT count FROM sw_v')[0]

  result = pipeline.execute('SELECT count FROM sw_v')[0]
    # Disabled until #157 (currently combine doesn't return 0 on NULL input for this aggregate)
  # assert result == 0
  assert result['count'] is None
  # assert result['count'] == 0

def test_single_continuous_view(pipeline, clean_db):
  """
  Verify that specific continuous views can be dropped and restored
  """
  pipeline.create_stream('stream0', x='int')
  pipeline.create_cv('test_single0', 'SELECT COUNT(*) FROM stream0')
  pipeline.create_cv('test_single1', 'SELECT COUNT(*) FROM stream0')
  pipeline.insert('stream0', ('x',), [(x,) for x in range(10)])

  result = pipeline.execute('SELECT count FROM test_single0')[0]
  assert result['count'] == 10

  result = pipeline.execute('SELECT count FROM test_single1')[0]
  assert result['count'] == 10

  _dump(pipeline, 'test_single.sql', tables=['test_single0', 'stream0', 'test_single0_mrel'])

  pipeline.drop_all()
  _restore(pipeline, 'test_single.sql')

  result = pipeline.execute('SELECT count FROM test_single0')[0]
  assert result['count'] == 10

  # We didn't dump this one
  result = list(pipeline.execute('SELECT * FROM pg_class WHERE relname LIKE \'%%test_single1%%\''))
  assert not result

def test_dump_data_only(pipeline, clean_db):
  """
  Verify that it is possible to only dump continuous view data and not schemas
  """
  pipeline.create_stream('stream0', x='int')
  pipeline.create_cv('test_data', 'SELECT COUNT(*) FROM stream0')
  pipeline.insert('stream0', ('x',), [(x,) for x in range(10)])

  result = pipeline.execute('SELECT count FROM test_data')[0]
  assert result['count'] == 10

  _dump(pipeline, 'test_data.sql', data_only=True)

  pipeline.drop_all()

  pipeline.create_stream('stream0', x='int')
  pipeline.create_cv('test_data', 'SELECT COUNT(*) FROM stream0')
  _restore(pipeline, 'test_data.sql')

  result = pipeline.execute('SELECT count FROM test_data')[0]
  assert result['count'] == 10

def test_schema_only(pipeline, clean_db):
  """
  Verify that it is possible to only dump continuous view schemas and not data
  """
  pipeline.create_stream('stream0', x='int')
  pipeline.create_cv('test_schema', 'SELECT COUNT(*) FROM stream0')
  pipeline.insert('stream0', ('x',), [(x,) for x in range(10)])

  result = pipeline.execute('SELECT count FROM test_schema')[0]
  assert result['count'] == 10

  _dump(pipeline, 'test_schema.sql', schema_only=True)

  pipeline.drop_all()
  _restore(pipeline, 'test_schema.sql')

  # No data loaded
  result = list(pipeline.execute('SELECT count FROM test_schema'))
  assert not result

def test_static_streams(pipeline, clean_db):
  """
  Verify that static stream definitions are dumped and restored
  """
  pipeline.create_stream('static', x='int', y='float8')

  _dump(pipeline, 'test_static.sql')

  pipeline.drop_stream('static')
  _restore(pipeline, 'test_static.sql')

  # Force the requirement of a static stream
  pipeline.create_cv('static_cv', 'SELECT x, y FROM static')
  pipeline.insert('static', ('x', 'y'), [(0, 1)])

  result = pipeline.execute('SELECT x, y FROM static_cv')[0]
  assert result['x'] == 0
  assert result['y'] == 1

def test_cont_transforms(pipeline, clean_db):
  pipeline.execute('CREATE FOREIGN TABLE cv_stream (x int, y text) SERVER pipelinedb')
  pipeline.execute('CREATE FOREIGN TABLE ct_stream (x int, y text) SERVER pipelinedb')
  pipeline.create_cv('test_cv', 'SELECT count(*) FROM cv_stream')
  pipeline.create_ct('test_ct1', 'SELECT x::int, y::text FROM ct_stream WHERE mod(x, 2) = 0',
                     "pipelinedb.insert_into_stream('cv_stream', 'cv_stream')")
  pipeline.create_table('test_t', x='int', y='text')
  pipeline.execute('''
  CREATE OR REPLACE FUNCTION test_tg()
  RETURNS trigger AS
  $$
  BEGIN
   INSERT INTO test_t (x, y) VALUES (NEW.x, NEW.y);
   RETURN NEW;
  END;
  $$
  LANGUAGE plpgsql;
  ''')
  pipeline.create_ct('test_ct2', 'SELECT x::int, y::text FROM ct_stream',
                     'test_tg')

  pipeline.insert('ct_stream', ('x', 'y'), [(1, 'hello'), (2, 'world')])
  time.sleep(1)

  assert pipeline.execute('SELECT count FROM test_cv')[0]['count'] == 2

  _dump(pipeline, 'test_cont_transform.sql')

  pipeline.drop_all()
  pipeline.drop_table('test_t')
  pipeline.execute('DROP FUNCTION test_tg()')

  _restore(pipeline, 'test_cont_transform.sql')
  
  pipeline.insert('ct_stream', ('x', 'y'), [(1, 'hello'), (2, 'world')])
  time.sleep(1)

  assert pipeline.execute('SELECT count FROM test_cv')[0]['count'] == 4
  ntups = 0
  
  for row in pipeline.execute('SELECT x, count(*) FROM test_t GROUP BY x'):
    assert row['count'] == 2
    assert row['x'] in (1, 2)
    ntups += 1
  assert ntups == 2

def test_chained_cqs(pipeline, clean_db):
  """
  Verify that multiple CQs chained together are properly dumped/restored
  """
  pipeline.create_stream('s', x='int')
  q = """
  SELECT x, count(*) FROM s GROUP BY x
  """
  pipeline.create_cv('cv0', q)
  q = """
  SELECT (new).x % 2 AS m FROM output_of('cv0')
  """
  pipeline.create_stream('ct_s', m='int')
  pipeline.create_ct('ct0', q, "pipelinedb.insert_into_stream('ct_s')")
  q = """
  SELECT m, count(*) FROM ct_s GROUP BY m
  """
  pipeline.create_cv('cv1', q)
  q = """
  SELECT combine((delta).count) AS count FROM output_of('cv1')
  """
  pipeline.create_cv('cv2', q)
  q = """
  SELECT combine((delta).count) FROM output_of('cv2')
  """
  pipeline.create_cv('cv3', q)
  pipeline.insert('s', ('x',), [(x,) for x in range(1000)])
  time.sleep(1)

  row = pipeline.execute('SELECT combine(count) FROM cv0')[0]
  assert row['combine'] == 1000

  row = pipeline.execute('SELECT combine(count) FROM cv1')[0]
  assert row['combine'] == 1000

  row = pipeline.execute('SELECT count FROM cv2')[0]
  assert row['count'] == 1000

  row = pipeline.execute('SELECT combine FROM cv3')[0]
  assert row['combine'] == 1000

  _dump(pipeline, 'test_chained_cqs.sql')

  pipeline.execute('DROP FOREIGN TABLE s CASCADE')
  pipeline.execute('DROP FOREIGN TABLE ct_s CASCADE')

  _restore(pipeline, 'test_chained_cqs.sql')

  pipeline.insert('s', ('x',), [(x,) for x in range(1000)])
  time.sleep(1)

  row = pipeline.execute('SELECT combine(count) FROM cv0')[0]
  assert row['combine'] == 2000

  row = pipeline.execute('SELECT combine(count) FROM cv1')[0]
  assert row['combine'] == 2000

  row = pipeline.execute('SELECT count FROM cv2')[0]
  assert row['count'] == 2000

  row = pipeline.execute('SELECT combine FROM cv3')[0]
  assert row['combine'] == 2000

  pipeline.execute('DROP FOREIGN TABLE s CASCADE')
  pipeline.execute('DROP FOREIGN TABLE ct_s CASCADE')

def test_repeated_dump_restore(pipeline, clean_db):
  """
  Verify that we can dump and restore a restored DB
  """
  pipeline.create_stream('stream0', x='int')
  q = """
  SELECT x::integer % 100 AS g, avg(x) + 1 AS avg, count(*), count(distinct x) AS distincts FROM stream0
  GROUP BY g
  """
  pipeline.create_cv('test_dump', q)

  rows = [(x,) for x in range(1000)]
  pipeline.insert('stream0', ('x',), rows)

  def _verify():
    result = pipeline.execute('SELECT count(*) FROM test_dump')[0]
    assert result['count'] == 100

    result = pipeline.execute('SELECT sum(avg) FROM test_dump')[0]
    assert result['sum'] == 50050

    result = pipeline.execute('SELECT sum(distincts) FROM test_dump')[0]
    assert result['sum'] == 1000

  _verify()
  _dump(pipeline, 'test_dump.sql')

  pipeline.drop_all()
  _restore(pipeline, 'test_dump.sql')

  _verify()
  _dump(pipeline, 'test_dump.sql')

  pipeline.drop_all()
  _restore(pipeline, 'test_dump.sql')
  _verify()

  # Now verify that we can successfully add more data to the restored CV
  rows = [(x,) for x in range(2000)]
  pipeline.insert('stream0', ('x',), rows)

  result = pipeline.execute('SELECT sum(count) FROM test_dump')[0]
  assert result['sum'] == 3000

  result = pipeline.execute('SELECT sum(distincts) FROM test_dump')[0]
  assert result['sum'] == 2000

def test_renamed_objects(pipeline, clean_db):
  """
  Verify that we can dump and restore renamed CQs and streams
  """
  pipeline.create_stream('s', x='int')

  q = """
  SELECT x, count(*) FROM s GROUP BY x;
  """
  pipeline.create_cv('cv_0', q)

  q = """
  SELECT (new).x, combine((delta).count) AS count FROM output_of('cv_0') GROUP BY x
  """
  pipeline.create_cv('combine_cv_0', q)

  q = """
  SELECT (new).count + 41 AS v FROM output_of('combine_cv_0')
  """
  pipeline.create_ct('transform_combine_cv_0', q)

  q = """
  SELECT max(v), count(*) FROM output_of('transform_combine_cv_0')
  """
  pipeline.create_cv('max_transform_combine_cv_0', q)

  rows = [(x,) for x in range(1000)]
  pipeline.insert('s', ('x',), rows)

  result = pipeline.execute('SELECT combine(count) FROM cv_0')[0]
  assert result['combine'] == 1000

  pipeline.execute('ALTER VIEW cv_0 RENAME TO cv_0_renamed')
  pipeline.execute('ALTER VIEW combine_cv_0 RENAME TO combine_cv_0_renamed')
  pipeline.execute('ALTER VIEW transform_combine_cv_0 RENAME TO transform_combine_cv_0_renamed')
  pipeline.execute('ALTER VIEW max_transform_combine_cv_0 RENAME TO max_transform_combine_cv_0_renamed')
  pipeline.execute('ALTER FOREIGN TABLE s RENAME TO s_renamed')

  result = pipeline.execute('SELECT combine(count) FROM cv_0_renamed')[0]
  assert result['combine'] == 1000

  result = pipeline.execute('SELECT combine(count) FROM combine_cv_0_renamed')[0]
  assert result['combine'] == 1000

  result = pipeline.execute('SELECT max, count FROM max_transform_combine_cv_0_renamed')[0]
  assert result['max'] == 42
  assert result['count'] == 1000

  _dump(pipeline, 'test_renamed_cqs.sql')

  pipeline.execute('DROP VIEW combine_cv_0_renamed CASCADE')
  pipeline.drop_all()

  _restore(pipeline, 'test_renamed_cqs.sql')

  result = pipeline.execute('SELECT combine(count) FROM cv_0_renamed')[0]
  assert result['combine'] == 1000

  result = pipeline.execute('SELECT combine(count) FROM combine_cv_0_renamed')[0]
  assert result['combine'] == 1000

  result = pipeline.execute('SELECT max, count FROM max_transform_combine_cv_0_renamed')[0]
  assert result['max'] == 42
  assert result['count'] == 1000

  # Now write some more rows to verify everything updates properly
  rows = [(x,) for x in range(1000)]
  pipeline.insert('s_renamed', ('x',), rows)

  result = pipeline.execute('SELECT combine(count) FROM cv_0_renamed')[0]
  assert result['combine'] == 2000

  result = pipeline.execute('SELECT combine(count) FROM combine_cv_0_renamed')[0]
  assert result['combine'] == 2000

  result = pipeline.execute('SELECT max, count FROM max_transform_combine_cv_0_renamed')[0]
  assert result['max'] == 43
  assert result['count'] == 2000

  pipeline.execute('DROP VIEW combine_cv_0_renamed CASCADE')
  