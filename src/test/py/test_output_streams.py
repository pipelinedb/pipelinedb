from base import pipeline, clean_db
import time


def test_output_tree(pipeline, clean_db):
  """
  Create a relatively complex tree of continuous views
  and transforms chained together by their output streams,
  and verify that all output correctly propagates to the leaves.
  """
  pipeline.create_cv('level0_0', 'SELECT x::integer, count(*) FROM root GROUP BY x')

  pipeline.create_cv('level1_0', 'SELECT (new).x, (new).count FROM level0_0_osrel')
  pipeline.create_cv('level1_1', 'SELECT (new).x, (new).count FROM level0_0_osrel')

  pipeline.create_cv('level2_0', 'SELECT (new).x, (new).count FROM level1_0_osrel')
  pipeline.create_cv('level2_1', 'SELECT (new).x, (new).count FROM level1_0_osrel')
  pipeline.create_cv('level2_2', 'SELECT (new).x, (new).count FROM level1_1_osrel')
  pipeline.create_cv('level2_3', 'SELECT (new).x, (new).count FROM level1_1_osrel')

  pipeline.create_cv('level3_0', 'SELECT (new).x, (new).count FROM level2_0_osrel')
  pipeline.create_cv('level3_1', 'SELECT (new).x, (new).count FROM level2_0_osrel')
  pipeline.create_cv('level3_2', 'SELECT (new).x, (new).count FROM level2_1_osrel')
  pipeline.create_cv('level3_3', 'SELECT (new).x, (new).count FROM level2_1_osrel')
  pipeline.create_cv('level3_4', 'SELECT (new).x, (new).count FROM level2_2_osrel')
  pipeline.create_cv('level3_5', 'SELECT (new).x, (new).count FROM level2_2_osrel')
  pipeline.create_cv('level3_6', 'SELECT (new).x, (new).count FROM level2_3_osrel')
  pipeline.create_cv('level3_7', 'SELECT (new).x, (new).count FROM level2_3_osrel')

  pipeline.insert('root', ('x',), [(x % 100,) for x in range(10000)])
  time.sleep(5)

  names = [r[0] for r in pipeline.execute('SELECT name FROM pipeline_views() ORDER BY name DESC')]
  assert len(names) == 15

  # Verify all values propagated to each node in the tree
  for name in names:
    rows = pipeline.execute('SELECT x, max(count) FROM %s GROUP BY x' % name)
    for row in rows:
      x, count = row
      assert count == 100

  pipeline.insert('root', ('x',), [(x % 100,) for x in range(10000)])
  time.sleep(5)

  # Verify all values propagated to each node in the tree again
  for name in names:
    rows = pipeline.execute('SELECT x, max(count) FROM %s GROUP BY x' % name)
    for row in rows:
      x, count = row
      assert count == 200

  # Drop these in reverse dependency order to prevent deadlocks
  for name in names:
    pipeline.drop_cv(name)


def test_no_ticking_non_sw(pipeline, clean_db):
  """
  Verify that no ticking is done for non-SW queries. We should
  only see new rows in the output stream when the combiner syncs
  rows to disk.
  """


def test_concurrent_sw_ticking(pipeline, clean_db):
  """
  Verify that several concurrent sliding-window queries each
  having different windows tick correctly at different intervals.
  """
