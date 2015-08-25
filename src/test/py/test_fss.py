from base import pipeline, clean_db

import random

def get_geometric_dist(items):
  values = map(lambda i: [i] * pow(2, items.index(i)), items)
  values = sum(values, [])
  random.shuffle(values)
  return values


def test_fss_agg(pipeline, clean_db):
  q = """
  SELECT k::text, fss_agg(x::int, 5) FROM test_fss_stream
  GROUP BY k
  """
  desc = ('k', 'x')
  pipeline.create_cv('test_fss_agg', q)

  items = range(14)
  random.shuffle(items)
  a_items = items
  b_items = list(reversed(items))
  
  values = map(lambda i: ('a', i), get_geometric_dist(a_items))
  values.extend(map(lambda i: ('b', i), get_geometric_dist(b_items)))
  random.shuffle(values)
  
  pipeline.insert('test_fss_stream', desc, values)
  result = list(pipeline.execute('SELECT k, fss_topk(fss_agg) FROM test_fss_agg ORDER BY k'))
  topk = map(int, result[0][1].rstrip('}').lstrip('{').split(','))
  assert sorted(topk) == sorted(a_items[-5:])
  topk = map(int, result[1][1].rstrip('}').lstrip('{').split(','))
  assert sorted(topk) == sorted(b_items[-5:])
