from base import pipeline, clean_db
from collections import defaultdict
import random


def test_distinct(pipeline, clean_db):
  """
  Verify that streaming SELECT DISTINCT ON (...) works
  """
  q = 'SELECT DISTINCT ON (x::int, y::int - z::int) x::int, y::int FROM stream'
  pipeline.create_cv('test_distinct', q)

  uniques = defaultdict(set)
  values = []
  for _ in xrange(2000):
    x, y, z = random.randint(0, 20), random.randint(0, 20), random.randint(0, 20)
    values.append((x, y, z))
    uniques[(x, y - z)].add(y)

  pipeline.insert('stream', ['x', 'y', 'z'], values)

  expected = len(uniques)
  result = pipeline.execute('SELECT COUNT(*) FROM test_distinct').first()

  assert expected < 2000

  # Error rate should be well below %2
  delta = abs(expected - result['count'])

  assert delta / float(expected) <= 0.02

  # Check if the first row was selected for uniques
  result = pipeline.execute('SELECT * FROM test_distinct')
  reverse_uniques = defaultdict(set)

  for (x, _), ys in uniques.iteritems():
    for y in ys:
      reverse_uniques[y].add(x)

  for row in result:
    assert row['x'] in reverse_uniques[row['y']]
