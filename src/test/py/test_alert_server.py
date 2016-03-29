from base import pipeline, clean_db
import time
import subprocess
import os
import json
import sys

def parse_log_file(fn):
  with open(fn) as f:
    lines = f.readlines()

  return map(lambda x: x.rstrip('\n').split('\t'), lines)

def shared_func(pipeline, clean_db):
  """
  Set up a grouping query, with a simple trigger on it (when = true)
  Connect to the trigger servers using the recv_alerts client.
  Redirect the output of the client to a file
  Parse the file and verify the muxed data
  """

  TRIGGER_OUTPUT_LOGFILE = '/tmp/.pipelinedb_pipeline_test.log'
  pipeline.create_cv('cv0', 'SELECT x::integer,count(*) FROM stream group by x')

  conn_str = pipeline.get_conn_string()

  pipeline.create_cv_trigger('t0', 'cv0', 'true',
                            'pipeline_triggers.send_alert_new_row')

  # recv_alerts client needs pipeline on its path

  client_env = os.environ.copy()
  client_env["PATH"] = client_env["PATH"] + ":" + pipeline.get_bin_dir()

  cmd = [pipeline.get_recv_alerts(), '-h', 'localhost', '-p', '5432', '-a', 'cv0.t0'];
  print(json.dumps(cmd))
  sys.stdout.flush()

  outfile = open(TRIGGER_OUTPUT_LOGFILE, 'w')
  client = subprocess.Popen(cmd, stdout=outfile, env=client_env)

  rows = [(n % 1000,) for n in range(10000)]
  pipeline.insert('stream', ('x',), rows)
  pipeline.insert('stream', ('x',), rows)
  pipeline.insert('stream', ('x',), rows)
  pipeline.insert('stream', ('x',), rows)
  pipeline.insert('stream', ('x',), rows)

  time.sleep(4)

  pipeline.drop_cv_trigger('t0', 'cv0')
  pipeline.insert('stream', ('x',), rows)

  time.sleep(1)

  client.wait()
  outfile.close()

  lines = parse_log_file(TRIGGER_OUTPUT_LOGFILE)
  d = {}

  for l in lines:
    assert(len(l) == 2)

    k = int(l[0])
    v = int(l[1])

    assert(k >= 0 and k <= 1000)

    if (not d.has_key(k)):
      d[k] = 0

    old_val = d[k]
    assert(v - old_val > 0)
    d[k] = v

  for k in range(1000):
    assert(d[k] == 50)

def test_t1(pipeline, clean_db):
  shared_func(pipeline, clean_db)

def test_t2(pipeline, clean_db):
  """
  Repeat the first test to exercise the cleanup logic
  """
  shared_func(pipeline, clean_db)
