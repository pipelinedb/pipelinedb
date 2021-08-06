from base import pipeline, clean_db
from collections import defaultdict
import random


def test_distinct(pipeline, clean_db):
  """
  Verify that streaming SELECT DISTINCT ON (...) works
  """
  pipeline.create_stream('stream0', x='int', y='int', z='int')
  pipeline.create_table('table0', x='int', y='int', z='int')
  q = 'SELECT DISTINCT ON (x::int, y::int - z::int) x::int, y::int FROM stream0'
  pipeline.create_cv('test_distinct', q)

  uniques = defaultdict(set)
  values = []
  for _ in range(2000):
    x, y, z = random.randint(0, 20), random.randint(0, 20), random.randint(0, 20)
    values.append((x, y, z))
    uniques[(x, y - z)].add(y)

  pipeline.insert('stream0', ['x', 'y', 'z'], values)
  pipeline.insert('table0', ['x', 'y', 'z'], values)

  q = """
  SELECT DISTINCT ON (x::int, y::int - z::int) x::int, y::int FROM table0
  """
  expected = pipeline.execute(q)
  expected = len(expected)

  assert expected < 2000

  result = pipeline.execute('SELECT COUNT(*) FROM test_distinct')[0]

  assert expected == result['count']

  # Check if the first row was selected for uniques
  result = pipeline.execute('SELECT * FROM test_distinct')
  reverse_uniques = defaultdict(set)

  for (x, _), ys in uniques.items():
    for y in ys:
      reverse_uniques[y].add(x)

  for row in result:
    assert row['x'] in reverse_uniques[row['y']]
