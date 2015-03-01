from base import pipeline, clean_db
import getpass
import random
import psycopg2
import time


def test_schema_inference(pipeline, clean_db):
  """
  Verify that types are properly inferred 
  """
  pipeline.create_cv('test_infer0', 
                     'SELECT x::int8, y::bigint, COUNT(*) FROM infer_stream GROUP BY x, y')
  pipeline.create_cv('test_infer1', 
                     'SELECT x::int4, y::real, COUNT(*) FROM infer_stream GROUP BY x, y')
  pipeline.create_cv('test_infer2', 
                     'SELECT x::int2, y::integer, COUNT(*) FROM infer_stream GROUP BY x, y')
  pipeline.create_cv('test_infer3', 
                     'SELECT x::numeric, y::float8, COUNT(*) FROM infer_stream GROUP BY x, y')
  pipeline.create_cv('test_infer4', 
                     'SELECT x::int8, y::bigint, COUNT(*) FROM infer_stream GROUP BY x, y')
  desc = ('x', 'y')
  rows = []

  for n in range(10000):
    rows.append((random.random() + 1, random.random() * random.randint(0, 128)))
    
  pipeline.activate()
  
  pipeline.insert('infer_stream', desc, rows)
  
  pipeline.deactivate()
  
  result = pipeline.execute('SELECT * FROM test_infer0 ORDER BY x')
  for row in result:
    assert row['count']

  result = pipeline.execute('SELECT * FROM test_infer1 ORDER BY x')
  for row in result:
    assert row['count']

  result = pipeline.execute('SELECT * FROM test_infer2 ORDER BY x')
  for row in result:
    assert row['count']

  result = pipeline.execute('SELECT * FROM test_infer3 ORDER BY x')
  for row in result:
    assert row['count']
