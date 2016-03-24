from base import pipeline, clean_db
import os
import tempfile


def test_multiple_insert(pipeline, clean_db):
  pipeline.create_cv('cv0', 'SELECT count(*) FROM stream1')
  pipeline.create_cv('cv1', 'SELECT count(*) FROM stream2')
  pipeline.create_ct('ct1', 'SELECT x::int FROM stream WHERE mod(x, 2) = 0', "pipeline_stream_insert('stream1', 'stream2')")

  pipeline.insert('stream', ('x', ), [(n, ) for n in range(1000)])

  count = pipeline.execute('SELECT count FROM cv0').first()['count']
  assert count == 500
  count = pipeline.execute('SELECT count FROM cv1').first()['count']
  assert count == 500


def test_nested_transforms(pipeline, clean_db):
  pipeline.create_cv('cv0', 'SELECT count(*) FROM stream4')
  pipeline.create_cv('cv1', 'SELECT count(*) FROM stream2')
  pipeline.create_ct('ct0', 'SELECT x::int FROM stream2 WHERE mod(x, 4) = 0', "pipeline_stream_insert('stream4')")
  pipeline.create_ct('ct1', 'SELECT x::int FROM stream WHERE mod(x, 2) = 0', "pipeline_stream_insert('stream2')")

  pipeline.insert('stream', ('x', ), [(n, ) for n in range(1000)])

  count = pipeline.execute('SELECT count FROM cv0').first()['count']
  assert count == 250
  count = pipeline.execute('SELECT count FROM cv1').first()['count']
  assert count == 500

def test_deadlock_regress(pipeline, clean_db):
  tmp_file = os.path.join(tempfile.gettempdir(), 'tmp.json')

  pipeline.create_stream('a_stream', js='jsonb')
  pipeline.create_stream('b_stream', js='jsonb')
  pipeline.create_ct('a_transform', 'SELECT js FROM a_stream WHERE js IS NOT NULL',
                     "pipeline_stream_insert('b_stream')")
  pipeline.create_cv('b_cv', 'SELECT count(*) FROM b_stream')
  pipeline.execute("COPY (SELECT '{\"foo\":\"bar\", \"baz\":\"bux\"}'::jsonb FROM generate_series(1, 1000000) gs) TO '%s'" % tmp_file)
  pipeline.execute("COPY a_stream (js) FROM '%s'" % tmp_file)

  count = pipeline.execute('SELECT count FROM b_cv').first()['count']
  assert count == 1000000

  os.remove(tmp_file)
