from base import pipeline, clean_db
import getpass
import psycopg2
import random
import pprint
import subprocess
import time
import tempfile
import re
import os
from subprocess import Popen, PIPE, STDOUT

def gen_insert(target, desc, rows):
  """
  Insert a batch of rows
  """
  header = ', '.join(desc)
  values = []
  for r in rows:
    if len(r) == 1:
      values.append('(%s)' % r[0])
    else:
      values.append(str(r))
  values = ', '.join(values)
  values = values.replace('None', 'null')
  return 'INSERT INTO %s (%s) VALUES %s;\n' % (target, header, values)


def test_adhoc_group_query(pipeline, clean_db):
  """
  Runs an adhoc grouping query, and checks the results.
  """
  q = 'SELECT x::int, COUNT(*) FROM stream GROUP BY x'

  rows = [(n+1,) for n in range(100)]

  path = os.path.abspath(os.path.join(pipeline.tmp_dir, 'test_adhoc.sql'))
  tmp_file = open(path, 'w')
  print(tmp_file)

  v = gen_insert('stream', ['x'], rows)

  for x in range(0, 100):
    tmp_file.write(v)

  tmp_file.close
  tmp_file = None

  psql = os.path.abspath(os.path.join(pipeline.tmp_dir, 'bin/psql'))

  # need to use expect because psycopg won't work with adhoc
  cmd = ["./run_adhoc.expect", psql, str(pipeline.port), q, path]
  output = subprocess.Popen(cmd, stdout=PIPE).communicate()[0]

  lines = output.split('\n')
  lines = filter(lambda x: not re.match(r'^\s*$', x), lines)

  # 2 hdr lines, 100 * 100 expected
  assert len(lines) == (10000 + 2);

  assert(lines[0] == "h\tx\tcount")
  assert(lines[1] == "k\t1")

  lines.pop(0)
  lines.pop(0)

  lines = map(lambda x: x.split("\t"), lines)
  d = {}

  # check that all the updates are monotonically increasing

  for l in lines:
      k = int(l[1])
      v = int(l[2])

      assert(k >= 1 and k <= 100)

      if (not d.has_key(k)):
          d[k] = 0

      old_val = d[k]
      assert(v - old_val == 1)
      d[k] = v

  # check the final tallies

  for x in range(1, 101):
      assert(d[x] == 100)

  # check that query has been cleaned up
  result = list(pipeline.execute("select table_name, table_schema from INFORMATION_SCHEMA.COLUMNS where table_schema = 'public'"))
  assert(len(result) == 0)

def test_adhoc_single_query(pipeline, clean_db):
  """
  Runs an adhoc query with a single result, and checks it
  """
  q = 'SELECT COUNT(*) FROM stream'

  rows = [(n+1,) for n in range(100)]

  path = os.path.abspath(os.path.join(pipeline.tmp_dir, 'test_adhoc.sql'))
  tmp_file = open(path, 'w')
  print(tmp_file)

  v = gen_insert('stream', ['x'], rows)

  for x in range(0, 100):
    tmp_file.write(v)

  tmp_file.close
  tmp_file = None

  psql = os.path.abspath(os.path.join(pipeline.tmp_dir, 'bin/psql'))

  # need to use expect because psycopg won't work with adhoc
  cmd = ["./run_adhoc.expect", psql, str(pipeline.port), q, path]
  output = subprocess.Popen(cmd, stdout=PIPE).communicate()[0]

  lines = output.split('\n')
  lines = filter(lambda x: not re.match(r'^\s*$', x), lines)

  assert(lines[0] == "h\tresult\tcount")
  assert(lines[1] == "k\t1")

  lines.pop(0)
  lines.pop(0)

  lines = map(lambda x: x.split("\t"), lines)
  last = 0

  # check that count is increasing
  for l in lines:

    v = int(l[2])
    delta = v - last
    assert(delta > 0)
    last = v

  # check that count is correct at the end
  assert(v == 10000)

  # check that query has been cleaned up
  result = list(pipeline.execute("select table_name, table_schema from INFORMATION_SCHEMA.COLUMNS where table_schema = 'public'"))
  assert(len(result) == 0)
