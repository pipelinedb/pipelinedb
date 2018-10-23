from base import pipeline, clean_db
import random
import time


def _rank(n, values):
  rank = 1
  peers = 0
  for v in sorted(values):
    if v < n:
      rank += 1
    if v == n:
      peers += 1

  return rank, peers

def _dense_rank(n, values):
  return _rank(n, set(values))

def _test_hs_agg(pipeline, agg):
  values = [random.randint(-100, 100) for n in range(1000)]
  h = random.choice(values) + random.randint(-10, 10)

  pipeline.create_stream('stream0', x='int')
  cq = 'SELECT %s(%d) WITHIN GROUP (ORDER BY x::integer) FROM stream0' % (agg, h)
  pipeline.create_cv('test_%s' % agg, cq)

  pipeline.insert('stream0', ('x',), [(v,) for v in values])

  result = pipeline.execute('SELECT %s FROM test_%s' % (agg, agg))[0]

  rank, peers = _rank(h, values)
  dense_rank, _ = _dense_rank(h, values)

  return rank, dense_rank, peers, len(values), result[agg]

def test_rank(pipeline, clean_db):
  """
  Verify that continuous rank produces the correct result given random input
  """
  rank, _, _, _, result = _test_hs_agg(pipeline, 'rank')

  assert rank == result

def test_dense_rank(pipeline, clean_db):
  """
  Verify that continuous dense_rank produces the correct result given random input
  """
  _, dense_rank, _, _, result = _test_hs_agg(pipeline, 'dense_rank')

  # We use HLL for dense_rank, so there may be a small margin of error,
  # but it should never be larger than 4%
  delta = abs(dense_rank - result)

  assert delta / float(dense_rank) <= 0.04

def test_percent_rank(pipeline, clean_db):
  """
  Verify that continuous percent_rank produces the correct result given random input
  """
  rank, _, _, row_count, result = _test_hs_agg(pipeline, 'percent_rank')

  percent_rank = (rank - 1) / (float(row_count))

  assert (percent_rank - result) < 0.0000001

def test_cume_dist(pipeline, clean_db):
  """
  Verify that continuous cume_dist produces the correct result given random input
  """
  rank, _, peers, row_count, result = _test_hs_agg(pipeline, 'cume_dist')

  rank += peers
  cume_dist = rank / (float(row_count + 1))

  assert abs(cume_dist - result) < 0.0000001
