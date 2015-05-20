from base import pipeline, clean_db
import random


def test_gcs_contains(pipeline, clean_db):
  """
  Verify that gcs_contains works
  """
  q = """
  SELECT gcs_agg(x::integer) FROM test_gcs_stream
  """

  desc = ('x')
  pipeline.create_cv('test_gcs_contains', q)

  rows = []
  for i in range(10000):
    rows.append((2 * i, ))

  pipeline.insert('test_gcs_stream', desc, rows)

  cvq = """
  SELECT gcs_contains(gcs_agg, 0), gcs_contains(gcs_agg, 5000),
  gcs_contains(gcs_agg, 1), gcs_contains(gcs_agg, 5001)
  FROM test_gcs_contains
  """

  result = list(pipeline.execute(cvq))

  assert len(result) == 1
  result = result[0]
  assert result[0] == True
  assert result[1] == True
  assert result[2] == False
  assert result[3] == False
