import getpass
import os
import psycopg2
import pytest
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from functools import wraps


BOOTSTRAPPED_BASE = './.pdbbase'
INSTALL_FORMAT = './.pdb-%d'


class NamedRow(dict):
  pass


class PipelineDB(object):
  def __init__(self, data_dir=None):
    """
    Bootstraps the PipelineDB instance. Note that instead of incurring the
    cost of actually bootstrapping each instance we copy a clean,
    bootstrapped directory to our own directory, creating it once for
    other tests to use if it doesn't already exist.
    """
    if data_dir:
      self._data_dir = data_dir
      return
      
    # To keep tests fast, we only initdb once and then we just copy that fresh data directory whenever we want a new DB
    do_initdb = False

    # If the base data directory exists but is for a different version of PG, remove it
    if os.path.exists(BOOTSTRAPPED_BASE):
      p = subprocess.Popen(['pg_config', '--version'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      stdout, stderr = p.communicate()

      _, bootstrap_version = stdout.strip().split()
      bootstrap_version = int(float(bootstrap_version))
      install_version = 0

      with open(os.path.join(BOOTSTRAPPED_BASE, 'PG_VERSION')) as v:
        install_version = int(v.read().strip())

      if install_version != bootstrap_version:
        shutil.rmtree(BOOTSTRAPPED_BASE)

    if not os.path.exists(BOOTSTRAPPED_BASE):
      out, err = subprocess.Popen(['initdb', '-D', BOOTSTRAPPED_BASE]).communicate()

    # Copy the bootstrapped install to our working directory
    shutil.copytree(BOOTSTRAPPED_BASE, self.data_dir)

    self.engine = None

  @property
  def bin_dir(self):
    p = subprocess.Popen(['pg_config', '--bindir'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    return stdout.strip()
  
  @property
  def version_num(self):
    r = self.execute('SHOW server_version_num')[0]
    return int(r['server_version_num'])

  def run(self, params=None):
    """
    Runs a test instance of PipelineDB within our temporary directory on
    a free port
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', 0))
    _, port = sock.getsockname()
    self.port = port

    # Let's hope someone doesn't take our port before we try to bind
    # PipelineDB to it
    sock.close()

    default_params = {
      'shared_preload_libraries': 'pipelinedb',
      'pipelinedb.stream_insert_level': 'sync_commit',
      'max_worker_processes': 128,
      'pipelinedb.num_combiners': 2,
      'pipelinedb.num_workers': 2,
      'pipelinedb.anonymous_update_checks': 'off',
      'pipelinedb.max_wait': 5,
    }

    server = os.path.join(self.bin_dir, 'postgres')
    cmd = [server, '-D', self.data_dir, '-p', str(self.port)]

    default_params.update(params or {})
    for key, value in default_params.iteritems():
      cmd.extend(['-c', '%s=%s' % (key, value)])

    self.proc = subprocess.Popen(cmd, stderr=subprocess.PIPE)

    # Wait for PipelineDB to start up
    while True:
      line = self.proc.stderr.readline()
      sys.stderr.write(line)
      if ('database system is ready to accept connections' in line or
        'pipelinedb process "worker0 [postgres]" running with pid' in line):
        break
      elif ('database system is shut down' in line or
        (line == '' and self.proc.poll() != None)):
        raise Exception('Failed to start up PipelineDB')

    # Add log tailer
    def run():
      while True:
        if not self.proc:
          break
        line = self.proc.stderr.readline()
        if line == '' and self.proc and self.proc.poll() != None:
          return
        sys.stderr.write(line)
    threading.Thread(target=run).start()

    # Wait to connect to PipelineDB
    for i in xrange(10):
      try:
        self.conn = psycopg2.connect('host=localhost dbname=postgres user=%s port=%d' % (getpass.getuser(), self.port))
        self.conn.autocommit = True
        self.execute('CREATE EXTENSION IF NOT EXISTS pipelinedb')
        # We can't use pg_sleep in CQs because it returns void so we use a wrapper
        # that actually returns something
        fn = """
        CREATE OR REPLACE FUNCTION cq_sleep(float) RETURNS float
        AS 'select pg_sleep($1); select $1'
        LANGUAGE SQL;
        """
        self.execute(fn)
        break
      except psycopg2.OperationalError:
        time.sleep(0.1)
    else:
      raise Exception('Failed to connect to PipelineDB')

    # Wait for bgworkers to start
    for i in xrange(30):
      try:
        out = subprocess.check_output('ps aux | grep "\[postgres\]" | grep -e "worker[0-9]" -e "combiner[0-9]"',
                shell=True).split('\n')
      except subprocess.CalledProcessError:
        out = []
      # Pick out PIDs that are greater than the PID of the postmaster we fired above.
      # This way any running PipelineDB instances are ignored.
      out = filter(lambda s: s.strip(), out)
      out = map(lambda s: int(s.split()[1]), out)
      out = filter(lambda p: p > self.proc.pid, out)

      if len(out) == (default_params['pipelinedb.num_workers'] +
          default_params['pipelinedb.num_combiners']):
        break
      time.sleep(1)
    else:
      raise Exception('Background workers failed to start up')

    time.sleep(0.2)

  def stop(self):
    """
    Stops the PipelineDB instance
    """
    if self.conn:
      self.conn.close()
    if self.proc:
      self.proc.send_signal(signal.SIGINT)
      self.proc.wait()
      self.proc = None

  def destroy(self):
    """
    Cleans up resources used by this PipelineDB instance
    """
    self.stop()
    shutil.rmtree(self.data_dir)

  @property
  def data_dir(self):
    """
    Returns the temporary directory that this instance is based within,
    finding a new one of it hasn't already
    """
    if hasattr(self, '_data_dir'):
      return self._data_dir

    # Get all the indexed install dirs so we can created a new one with
    # highest index + 1. Install dirs are of the form: ./.pdb-<n>.
    index = max([int(l.split('-')[1]) for l in os.listdir('.')
         if l.startswith('.pdb-')] or [-1]) + 1
    self._data_dir = INSTALL_FORMAT % index
    return self._data_dir

  def drop_all(self):
    """
    Drop all continuous queries and streams
    """
    for transform in self.execute('SELECT schema, name FROM pipelinedb.get_transforms()'):
      self.execute('DROP VIEW %s.%s CASCADE' % (transform[0], transform[1]))
    for view in self.execute('SELECT schema, name FROM pipelinedb.get_views()'):
      self.execute('DROP VIEW %s.%s CASCADE' % (view[0], view[1]))
    for stream in self.execute('SELECT schema, name FROM pipelinedb.get_streams()'):
      self.execute('DROP FOREIGN TABLE %s.%s CASCADE' % (stream[0], stream[1]))

  def create_cv(self, name, stmt, **kw):
    """
    Create a continuous view
    """
    kw['action'] = 'materialize'
    opts = ', '.join(['%s=%r' % (k, v) for k, v in kw.items()])

    result = self.execute(
      'CREATE VIEW %s WITH (%s) AS %s' % (name, opts, stmt))

    return result

  def create_ct(self, name, stmt, trigfn=''):
    """
    Create a continuous transform
    """
    options = 'action=transform'
    if trigfn:
      options += ', outputfunc=%s' % trigfn
    result = self.execute(
      'CREATE VIEW %s WITH (%s) AS %s' % (name, options, stmt))
    return result

  def create_table(self, name, **cols):
    """
    Create a table
    """
    cols = ', '.join(['%s %s' % (k, v) for k, v in cols.iteritems()])
    self.execute('CREATE TABLE %s (%s)' % (name, cols))

  def create_stream(self, name, order=None, **cols):
    """
    Create a stream
    """
    if order:
      cols = ', '.join(['%s %s' % (k, cols[k]) for k in order])
    else:
      cols = ', '.join(['%s %s' % (k, v) for k, v in cols.iteritems()])
    self.execute(
      'CREATE FOREIGN TABLE %s (%s) SERVER pipelinedb' % (name, cols))

  def drop_table(self, name):
    """
    Drop a table
    """
    self.execute('DROP TABLE %s' % name)

  def drop_stream(self, name):
    """
    Drop a stream
    """
    self.execute('DROP FOREIGN TABLE %s' % name)

  def drop_cv(self, name):
    """
    Drop a continuous view
    """
    return self.execute('DROP VIEW %s' % name)

  def _create_named_rows(self, desc, rows):
    """
    Wrap rows in NamedRows for lookups by column name

    (Column(name='x', type_code=25, display_size=None, internal_size=-1, precision=None, scale=None, null_ok=None),
    Column(name='y', type_code=23, display_size=None, internal_size=4, precision=None, scale=None, null_ok=None))
    """
    result = []
    for row in rows:
      nr = NamedRow()
      for i, col in enumerate(desc):
        nr[col.name] = row[i]
        nr[i] = row[i]
      result.append(nr)
    return result
  
  def execute(self, stmt):
    """
    Execute a raw SQL statement
    """
    if not self.conn:
      self.conn = psycopg2.connect('host=localhost dbname=postgres user=%s port=%d' % (getpass.getuser(), self.port))
      self.conn.autocommit = True

    cur = self.conn.cursor()
    cur.execute(stmt)

    if cur.description:
      return self._create_named_rows(cur.description, cur.fetchall())
    else:
      return None

  def insert(self, target, desc, rows):
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
    return self.execute('INSERT INTO %s (%s) VALUES %s' % (target, header, values))

  def insert_batches(self, target, desc, rows, batch_size):
    """
    Insert a batch of rows, spreading them across randomly selected nodes
    """
    batches = [rows[i:i + batch_size]
         for i in range(0, len(rows), batch_size)]

    for i, batch in enumerate(batches):
      self.insert(target, desc, batch)
      time.sleep(0.5)

  def begin(self):
    """
    Begin a transaction
    """
    return self.execute('BEGIN')

  def commit(self):
    """
    Commit a transaction
    """
    return self.execute('COMMIT')


@pytest.fixture
def clean_db(request):
  """
  Called for every test so each test gets a clean db
  """
  pdb = request.module.pipeline
  request.addfinalizer(pdb.drop_all)


@pytest.fixture(scope='module')
def pipeline(request):
  """
  Builds and returns a running PipelineDB instance based out of a test
  directory within the current directory. This is called once per test
  module, so it's shared between tests even though underlying databases
  are recreated for each test.
  """
  pdb = PipelineDB()
  request.addfinalizer(pdb.destroy)

  # Attach it to the module so we can access it with test-scoped fixtures
  request.module.pipeline = pdb
  pdb.run()

  return pdb


def async_insert(f):
  @wraps(f)
  def wrapper(pipeline, clean_db):
    pipeline.stop()
    pipeline.run({'pipelinedb.stream_insert_level': 'sync_receive'})
    try:
      f(pipeline, clean_db)
    finally:
      pipeline.stop()
      pipeline.run()
  return wrapper
