from base import pipeline, clean_db
import glob
import os
import shutil


def test_tablespace(pipeline, clean_db):
  """
  Verify that CVs can be created within tablespaces
  """
  path = os.path.abspath('test_tablespace')
  if os.path.exists(path):
    shutil.rmtree(path)

  os.mkdir(path)
  pipeline.execute("CREATE TABLESPACE test_tablespace LOCATION '%s'" % path)

  pipeline.create_stream('test_tablespace_s', x='int')

  q = 'SELECT x % 10 AS g, count(DISTINCT x) FROM test_tablespace_s GROUP BY g'
  pipeline.create_cv('test_tablespace0', q)
  pipeline.create_cv('test_tablespace1', q, tablespace='test_tablespace')
  pipeline.insert('test_tablespace_s', ('x',), [(x,) for x in range(10000)])

  result0 = pipeline.execute('SELECT count(*) FROM test_tablespace0')
  result1 = pipeline.execute('SELECT count(*) FROM test_tablespace1')

  assert len(result0) == 1
  assert len(result1) == 1
  assert result0[0]['count'] == result1[0]['count']

  result0 = pipeline.execute('SELECT combine(count) FROM test_tablespace0')
  result1 = pipeline.execute('SELECT combine(count) FROM test_tablespace1')

  assert len(result0) == 1
  assert len(result1) == 1
  assert result0[0]['combine'] == result1[0]['combine']

  # Now verify that test_tablespace1 is physically in the tablespace
  row = pipeline.execute("SELECT oid FROM pg_class WHERE relname = 'test_tablespace1_mrel'")
  oid = row[0]['oid']

  found = glob.glob('test_tablespace/*/*/%d' % oid)
  assert len(found) == 1

  pipeline.drop_all()
  pipeline.execute('DROP TABLESPACE test_tablespace')
  shutil.rmtree(path)