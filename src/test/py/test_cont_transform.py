from base import pipeline, clean_db
import os
import tempfile
import time


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
  nitems = 2000000
  tmp_file = os.path.join(tempfile.gettempdir(), 'tmp.json')
  query = 'SELECT generate_series(1, %d) AS n' % nitems
  pipeline.execute("COPY (%s) TO '%s'" % (query, tmp_file))

  pipeline.create_stream('s1', n='int')
  pipeline.create_stream('s2', n='int')
  pipeline.create_ct('ct', 'SELECT n FROM s1 WHERE n IS NOT NULL',
                     "pipeline_stream_insert('s2')")
  pipeline.create_cv('cv', 'SELECT count(*) FROM s2')

  for copy in [True, False]:
    for nworkers in [1, 4]:
      for sync in ['off', 'on']:
        pipeline.stop()
        pipeline.run({
          'continuous_query_num_workers': nworkers,
          'synchronous_stream_insert': sync
          })

        pipeline.execute('TRUNCATE CONTINUOUS VIEW cv')
        pipeline.execute('COMMIT')

        if copy:
          pipeline.execute("COPY s1 (n) FROM '%s'" % tmp_file)
        else:
          pipeline.execute('INSERT INTO s1 (n) %s' % query)

        count = dict(pipeline.execute('SELECT count FROM cv').first() or {})
        ntries = 5
        while count.get('count') != nitems and ntries > 0:
          assert sync == 'off'
          time.sleep(1)
          count = dict(pipeline.execute('SELECT count FROM cv').first() or {})
          ntries -= 1
        assert count and count['count'] == nitems

  os.remove(tmp_file)

  pipeline.stop()
  pipeline.run()
