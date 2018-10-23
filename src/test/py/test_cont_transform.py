from base import pipeline, clean_db
import os
import tempfile
import time


def test_multiple_insert(pipeline, clean_db):
  pipeline.create_stream('stream0', x='int')
  pipeline.create_stream('stream1', x='int')
  pipeline.create_stream('stream2', x='int')

  pipeline.create_cv('cv0', 'SELECT count(*) FROM stream1')
  pipeline.create_cv('cv1', 'SELECT count(*) FROM stream2')
  pipeline.create_ct('ct1', 'SELECT x::int FROM stream0 WHERE mod(x, 2) = 0', "pipelinedb.insert_into_stream('stream1', 'stream2')")

  pipeline.insert('stream0', ('x',), [(n,) for n in range(1000)])

  count = pipeline.execute('SELECT count FROM cv0')[0]['count']
  assert count == 500
  count = pipeline.execute('SELECT count FROM cv1')[0]['count']
  assert count == 500


def test_nested_transforms(pipeline, clean_db):
  pipeline.create_stream('stream0', x='int')
  pipeline.create_stream('stream2', x='int')
  pipeline.create_stream('stream4', x='int')

  pipeline.create_cv('cv0', 'SELECT count(*) FROM stream4')
  pipeline.create_cv('cv1', 'SELECT count(*) FROM stream2')
  pipeline.create_ct('ct0', 'SELECT x::int FROM stream2 WHERE mod(x, 4) = 0',
             "pipelinedb.insert_into_stream('stream4')")
  pipeline.create_ct('ct1', 'SELECT x::int FROM stream0 WHERE mod(x, 2) = 0',
             "pipelinedb.insert_into_stream('stream2')")

  pipeline.insert('stream0', ('x',), [(n,) for n in range(1000)])

  count = pipeline.execute('SELECT count FROM cv0')[0]['count']
  assert count == 250
  count = pipeline.execute('SELECT count FROM cv1')[0]['count']
  assert count == 500


def test_deadlock_regress(pipeline, clean_db):
  nitems = 2000000
  tmp_file = os.path.join(tempfile.gettempdir(), 'tmp.json')
  query = 'SELECT generate_series(1, %d) AS n' % nitems
  pipeline.execute("COPY (%s) TO '%s'" % (query, tmp_file))

  pipeline.create_stream('s1', n='int')
  pipeline.create_stream('s2', n='int')
  pipeline.create_ct('ct', 'SELECT n FROM s1 WHERE n IS NOT NULL',
             "pipelinedb.insert_into_stream('s2')")
  pipeline.create_cv('cv', 'SELECT count(*) FROM s2')

  for copy in [True, False]:
    for nworkers in [1, 4]:
      for sync in ['receive', 'commit']:
        pipeline.stop()
        pipeline.run({
          'pipelinedb.num_workers': nworkers,
          'pipelinedb.stream_insert_level': 'sync_%s' % sync
          })

        pipeline.execute("SELECT pipelinedb.truncate_continuous_view('cv')")
        pipeline.execute('COMMIT')

        if copy:
          pipeline.execute("COPY s1 (n) FROM '%s'" % tmp_file)
        else:
          pipeline.execute('INSERT INTO s1 (n) %s' % query)

        count = dict(pipeline.execute('SELECT count FROM cv')[0] or {})
        ntries = 5
        while count.get('count') != nitems and ntries > 0:
          assert sync == 'receive'
          time.sleep(1)
          count = dict(pipeline.execute('SELECT count FROM cv')[0] or {})
          ntries -= 1
        assert count and count['count'] == nitems

  os.remove(tmp_file)

  pipeline.stop()
  pipeline.run()
