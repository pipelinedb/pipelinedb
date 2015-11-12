from base import pipeline, clean_db
import random
import subprocess
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

  v = gen_insert('stream', ['x'], rows)

  for x in range(0, 100):
    tmp_file.write(v)

  tmp_file.close
  tmp_file = None

  psql = os.path.abspath(os.path.join(pipeline.tmp_dir, 'bin/psql'))

  # need to use expect because psycopg won't work with adhoc
  cmd = ["./run_adhoc.expect", psql, str(pipeline.port), "pipeline", q, path]

  output = subprocess.Popen(cmd, stdout=PIPE).communicate()[0]

  lines = output.split('\n')
  lines = filter(lambda x: not re.match(r'^\s*$', x), lines)


  assert(lines[0] == "h\tx\tcount")
  assert(lines[1] == "k\t1")

  lines.pop(0)
  lines.pop(0)

  lines = map(lambda x: x.split("\t"), lines)
  d = {}

  # check that all the updates are increasing

  for l in lines:
      k = int(l[1])
      v = int(l[2])

      assert(k >= 1 and k <= 100)

      if (not d.has_key(k)):
          d[k] = 0

      old_val = d[k]
      assert(v - old_val > 0)
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

  v = gen_insert('stream', ['x'], rows)

  for x in range(0, 100):
    tmp_file.write(v)

  tmp_file.close()

  psql = os.path.abspath(os.path.join(pipeline.tmp_dir, 'bin/psql'))

  # need to use expect because psycopg won't work with adhoc
  cmd = ["./run_adhoc.expect", psql, str(pipeline.port), "pipeline", q, path]
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

def test_adhoc_against_identical_cv(pipeline, clean_db):
  """
  Verify that an adhoc query produces the same output as an identical
  continuous view
  """
  q = """
  SELECT x::integer + 1 AS g, sum(y::integer), avg(z::integer), count(*)
  FROM test_adhoc_stream GROUP BY g;
  """
  pipeline.create_cv('test_adhoc_cv', q)

  rows = [(x % 10, random.randint(1, 1000), random.randint(1, 1000)) for x in range(1000)]

  path = os.path.abspath(os.path.join(pipeline.tmp_dir, 'test_adhoc_against_identical_cv.sql'))
  tmp_file = open(path, 'w')

  for row in rows:
    v = gen_insert('test_adhoc_stream', ('x', 'y', 'z'), [row])
    tmp_file.write(v)

  tmp_file.close()

  psql = os.path.abspath(os.path.join(pipeline.tmp_dir, 'bin/psql'))
  cmd = ['./run_adhoc.expect', psql, str(pipeline.port), 'pipeline', q, path]
  output = subprocess.Popen(cmd, stdout=PIPE).communicate()[0]

  lines = output.split('\n')
  lines = filter(lambda x: not re.match(r'^\s*$', x), lines)
  lines = [l.split('\t')[1:] for l in lines]
  lines = lines[-10:]

  adhoc_results = {}
  for line in lines:
    g, s, a, c = line
    adhoc_results[int(g)] = int(g), int(s), float(a), int(c)

  cv_result = pipeline.execute('SELECT * FROM test_adhoc_cv')
  for row in cv_result:
    adhoc_row = adhoc_results[row[0]]
    assert adhoc_row[1] == row[1]
    assert abs(adhoc_row[2] - float(row[2])) < 0.001
    assert adhoc_row[3] == row[3]


def test_multi_adhoc(pipeline, clean_db):
  """
  Verify that multiple cq and adhocs can run off the same stream.
  """

  q1 = 'select a::text, count(*) from stream group by a'
  q2 = 'select b::text, count(*) from stream group by b'
  q3 = 'select c::text, count(*) from stream group by c'
  q4 = 'select d::text, count(*) from stream group by d'

  pipeline.create_cv('cv_1', q1)
  pipeline.create_cv('cv_2', q2)

  rows = [('a%d'%x, 'b%d'%x, 'c%d'%x, 'd%d'%x) for x in range(100)]
  v = gen_insert('stream', ['a','b','c','d'], rows)

  path = os.path.abspath(os.path.join(pipeline.tmp_dir, 'test_multi_adhoc.sql'))

  tmp_file = open(path, 'w')

  for x in range(100):
    tmp_file.write(v)

  tmp_file.close()


  psql = os.path.abspath(os.path.join(pipeline.tmp_dir, 'bin/psql'))

  cmd = ['./run_multi_adhoc.expect', psql, str(pipeline.port), 'pipeline', q3, q4, path]

  output = subprocess.Popen(cmd, stdout=PIPE).communicate()[0]
  lines = output.split('\n')

  gate = None
  adhoc1_buf = []
  adhoc2_buf = []

  # multi adhoc expect will output a tag line before a set of lines
  # for each adhoc. this loops splits up the data

  for line in lines:

    line = line.strip()

    if (len(line) == 0):
      continue

    if (line == "adhoc1"):
      gate = adhoc1_buf
    elif (line == "adhoc2"):
      gate = adhoc2_buf
    elif (gate != None):
      gate.append(line.split('\t'))

  # test adhoc1 has all the groups we expect (cXX)
  d = {}

  for line in adhoc1_buf:
    t,group,count = line

    assert(group[0] == 'c')

    count = int(count)

    if (not d.has_key(group)):
      d[group] = 0

    old_val = d[group]
    delta = count - old_val

    assert(delta == 1)
    d[group] = count

  for k,v in d.iteritems():
    assert(v == 100)

  # test adhoc2 has all the groups we expect (dXX)
  d = {}

  for line in adhoc2_buf:
    t,group,count = line

    assert(group[0] == 'd')

    count = int(count)

    if (not d.has_key(group)):
      d[group] = 0

    old_val = d[group]
    delta = count - old_val

    assert(delta == 1)
    d[group] = count

  for k,v in d.iteritems():
    assert(v == 100)

  # check cv_1 gets exactly 100 groups
  cv_result_1 = pipeline.execute('SELECT count(*) FROM cv_1').first()
  assert(cv_result_1[0] == 100)

  # check cv_1 gets 100 aXX groups
  cv_result_1 = pipeline.execute('SELECT count(*) as c FROM cv_1 where a like \'a%%\' and count = 100').first()
  assert(cv_result_1[0] == 100)

  # check cv_2 gets exactly 100 groups
  cv_result_2 = pipeline.execute('SELECT count(*) FROM cv_2').first()
  assert(cv_result_2[0] == 100)

  # check cv_2 gets 100 bXX groups
  cv_result_2 = pipeline.execute('SELECT count(*) as c FROM cv_2 where b like \'b%%\' and count = 100').first()
  assert(cv_result_2[0] == 100)
