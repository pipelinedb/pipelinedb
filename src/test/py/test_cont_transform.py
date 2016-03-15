from base import pipeline, clean_db


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
