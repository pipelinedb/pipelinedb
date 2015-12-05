from base import pipeline, clean_db
import os
import subprocess
import time


def _dump(pipeline, out_filename, cv_name=None, data_only=False, schema_only=False):
  out = open(out_filename, 'w')
  cmd = [os.path.join(pipeline.bin_dir, 'pipeline-dump'), '-p', str(pipeline.port), '-d', 'pipeline']
  if cv_name:
    cmd += ['-t', cv_name, '-t', '%s_mrel' % cv_name]
  if data_only:
    cmd += ['-a']
  if schema_only:
    cmd += ['-s']
  subprocess.Popen(cmd, stdout=out).communicate()
  out.close()

def _restore(pipeline, in_filename):
  cmd = [os.path.join(pipeline.bin_dir, 'pipeline'), '-p', str(pipeline.port), '-d', 'pipeline', '-f', in_filename]
  subprocess.Popen(cmd).communicate()
  os.remove(in_filename)


def test_dump(pipeline, clean_db):
  """
  Verify that we can dump and restore CVs using INSERT statements
  """
  q = """
  SELECT x::integer %% 100 AS g, avg(x) + 1 AS avg, count(*), count(distinct x) AS distincts FROM stream
  GROUP BY g
  """
  pipeline.create_cv('test_dump', q)

  rows = [(x,) for x in range(1000)]
  pipeline.insert('stream', ('x',), rows)

  def _verify():
    result = pipeline.execute('SELECT count(*) FROM test_dump').first()
    assert result['count'] == 100

    result = pipeline.execute('SELECT sum(avg) FROM test_dump').first()
    assert result['sum'] == 50050

    result = pipeline.execute('SELECT sum(distincts) FROM test_dump').first()
    assert result['sum'] == 1000

  _verify()
  _dump(pipeline, 'test_dump.sql')

  pipeline.drop_all_views()
  _restore(pipeline, 'test_dump.sql')
  _verify()

  # Now verify that we can successfully add more data to the restored CV
  rows = [(x,) for x in range(2000)]
  pipeline.insert('stream', ('x',), rows)

  result = pipeline.execute('SELECT sum(count) FROM test_dump').first()
  assert result['sum'] == 3000

  result = pipeline.execute('SELECT sum(distincts) FROM test_dump').first()
  assert result['sum'] == 2000

def test_sliding_windows(pipeline, clean_db):
  """
  Verify that sliding window queries are properly dumped and restored
  """
  pipeline.execute('CREATE CONTINUOUS VIEW sw_v WITH (max_age = \'20 seconds\') AS SELECT count(*) FROM stream')
  pipeline.insert('stream', ('x',), [(x,) for x in range(10)])

  result = pipeline.execute('SELECT count FROM sw_v').first()
  assert result['count'] == 10

  _dump(pipeline, 'test_sw.sql')

  pipeline.drop_all_views()
  _restore(pipeline, 'test_sw.sql')

  result = pipeline.execute('SELECT count FROM sw_v').first()
  assert result['count'] == 10

  # We should still drop back to 0 within 20 seconds
  result = pipeline.execute('SELECT count FROM sw_v').first()
  while result['count'] > 0:
    time.sleep(1)
    result = pipeline.execute('SELECT count FROM sw_v').first()

  result = pipeline.execute('SELECT count FROM sw_v').first()
  assert result['count'] == 0

def test_single_continuous_view(pipeline, clean_db):
  """
  Verify that specific continuous views can be dropped and restored
  """
  pipeline.create_cv('test_single0', 'SELECT COUNT(*) FROM stream')
  pipeline.create_cv('test_single1', 'SELECT COUNT(*) FROM stream')
  pipeline.insert('stream', ('x',), [(x,) for x in range(10)])

  result = pipeline.execute('SELECT count FROM test_single0').first()
  assert result['count'] == 10

  result = pipeline.execute('SELECT count FROM test_single1').first()
  assert result['count'] == 10

  _dump(pipeline, 'test_single.sql', cv_name='test_single0')

  pipeline.drop_all_views()
  _restore(pipeline, 'test_single.sql')

  result = pipeline.execute('SELECT count FROM test_single0').first()
  assert result['count'] == 10

  # We didn't dump this one
  result = list(pipeline.execute('SELECT * FROM pg_class WHERE relname LIKE \'%%test_single1%%\''))
  assert not result

def test_dump_data_only(pipeline, clean_db):
  """
  Verify that it is possible to only dump continuous view data and not schemas
  """
  pipeline.create_cv('test_data', 'SELECT COUNT(*) FROM stream')
  pipeline.insert('stream', ('x',), [(x,) for x in range(10)])

  result = pipeline.execute('SELECT count FROM test_data').first()
  assert result['count'] == 10

  _dump(pipeline, 'test_data.sql', data_only=True)

  pipeline.drop_all_views()

  pipeline.create_cv('test_data', 'SELECT COUNT(*) FROM stream')
  _restore(pipeline, 'test_data.sql')

  result = pipeline.execute('SELECT count FROM test_data').first()
  assert result['count'] == 10

def test_schema_only(pipeline, clean_db):
  """
  Verify that it is possible to only dump continuous view schemas and not data
  """
  pipeline.create_cv('test_schema', 'SELECT COUNT(*) FROM stream')
  pipeline.insert('stream', ('x',), [(x,) for x in range(10)])

  result = pipeline.execute('SELECT count FROM test_schema').first()
  assert result['count'] == 10

  _dump(pipeline, 'test_schema.sql', schema_only=True)

  pipeline.drop_all_views()
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

  result = pipeline.execute('SELECT x, y FROM static_cv').first()
  assert result['x'] == 0
  assert result['y'] == 1

