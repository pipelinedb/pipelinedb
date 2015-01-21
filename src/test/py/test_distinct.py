from base import pipeline, clean_db
from collections import defaultdict
import random


def test_distinct(pipeline, clean_db):
  """
  Verify that streaming SELECT DISTINCT ON (...) works
  """
  q = 'SELECT DISTINCT ON (x::int, y::int - z::int) x::int, y::int FROM stream'
  pipeline.create_cv('test_distinct', q)
  pipeline.activate()

  uniques = {}
  for _ in xrange(2000):
    x, y, z = random.randint(0, 20), random.randint(0, 20), random.randint(0, 20)
    pipeline.execute('INSERT INTO stream (x, y, z) VALUES (%d, %d, %d)' %
                     (x, y, z))
    if (x, y - z) not in uniques:
      uniques[(x, y - z)] = y

  pipeline.deactivate()

  expected = len(uniques)
  result = pipeline.execute('SELECT COUNT(*) FROM test_distinct').first()

  assert expected < 2000

  # Error rate should be well below %2
  delta = abs(expected - result['count'])

  assert delta / float(expected) <= 0.02

  # Check if the first row was selected for uniques
  result = pipeline.execute('SELECT * FROM test_distinct')
  reverse_uniques = defaultdict(set)

  for (x, _), y in uniques.iteritems():
    reverse_uniques[y].add(x)

  print uniques
  print reverse_uniques

  for row in result:
    print row['x'], row['y']
    assert row['x'] in reverse_uniques[row['y']]
