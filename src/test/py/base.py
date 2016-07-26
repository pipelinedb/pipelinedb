import getpass
import os
import pytest
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time

from functools import wraps
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

BOOTSTRAPPED_BASE = './.pdbbase'
ROOT = '../../../'
INSTALL_FORMAT = './.pdb-%d'
SERVER = os.path.join(ROOT, 'src', 'backend', 'pipelinedb')
CONNSTR_TEMPLATE = 'postgres://%s@localhost:%d/pipeline'

class PipelineDB(object):
    def __init__(self):
        """
        Bootstraps the PipelineDB instance. Note that instead of incurring the
        cost of actually bootstrapping each instance we copy a clean,
        bootstrapped directory to our own directory, creating it once for
        other tests to use if it doesn't already exist.
        """
        do_initdb = False
        if not os.path.exists(BOOTSTRAPPED_BASE):
            os.mkdir(BOOTSTRAPPED_BASE)
            make = [
                'make', '-C', os.path.abspath(ROOT),
                'DESTDIR=%s' % os.path.abspath(BOOTSTRAPPED_BASE),
                'install'
            ]
            out, err = subprocess.Popen(make).communicate()
            do_initdb = True

        # Get the bin dir of our installation
        install_bin_dir = None
        for root, dirs, files in os.walk(BOOTSTRAPPED_BASE):
            if 'bin' in dirs:
                install_bin_dir = os.path.join(root, 'bin')

        # Install dir is one level above bin
        install_root, _ = os.path.split(install_bin_dir)
        install_data_dir = os.path.join(install_root, 'data')

        if do_initdb:
            initdb = os.path.join(install_bin_dir, 'pipeline-init')
            out, err = subprocess.Popen([initdb, '-D',
                                         install_data_dir]).communicate()

        # Copy the bootstrapped install to our working directory
        shutil.copytree(install_root, self.tmp_dir)

        for root, dirs, files in os.walk(self.tmp_dir):
            if 'data' in dirs:
                self.data_dir = os.path.join(root, 'data')

        self.bin_dir = install_bin_dir
        self.recv_alerts = os.path.join(self.bin_dir, 'pipeline-recv-alerts')
        self.engine = None

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
          'synchronous_stream_insert': 'on',
          'continuous_queries_adhoc_enabled': 'on',
          'continuous_query_num_combiners': 2,
          'continuous_query_num_workers': 2,
          'continuous_query_ipc_shared_mem': 65536,
          'anonymous_update_checks': 'off',
          'continuous_query_max_wait': 5,
          'wal_level': 'logical',
          'max_wal_senders': 1,
          'max_replication_slots': 1
        }

        cmd = [SERVER, '-D', self.data_dir, '-p', str(self.port)]

        default_params.update(params or {})
        for key, value in default_params.iteritems():
          cmd.extend(['-c', '%s=%s' % (key, value)])

        self.proc = subprocess.Popen(cmd, stderr=subprocess.PIPE)

        # Wait for PipelineDB to start up
        while True:
          line = self.proc.stderr.readline()
          sys.stderr.write(line)
          if ('database system is ready to accept connections' in line or
              'continuous query process "worker0 [pipeline]" running with pid' in line):
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

        connstr = CONNSTR_TEMPLATE % (getpass.getuser(), self.port)
        self.engine = create_engine(connstr)

        # Wait to connect to PipelineDB
        for i in xrange(10):
          try:
            self.conn = self.engine.connect()
            break
          except OperationalError:
            time.sleep(0.1)
        else:
          raise Exception('Failed to connect to PipelineDB')

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
        shutil.rmtree(self.tmp_dir)

    @property
    def tmp_dir(self):
        """
        Returns the temporary directory that this instance is based within,
        finding a new one of it hasn't already
        """
        if hasattr(self, '_tmp_dir'):
            return self._tmp_dir

        # Get all the indexed install dirs so we can created a new one with
        # highest index + 1. Install dirs are of the form: ./.pdb-<n>.
        index = max([int(l.split('-')[1]) for l in os.listdir('.')
                     if l.startswith('.pdb-')] or [-1]) + 1
        self._tmp_dir = INSTALL_FORMAT % index
        return self._tmp_dir

    def drop_all_queries(self):
        """
        Drop all continuous queries
        """
        for transform in self.execute('SELECT name FROM pipeline_transforms()'):
          self.execute('DROP CONTINUOUS TRANSFORM %s CASCADE' % transform['name'])
        for view in self.execute('SELECT name FROM pipeline_views()'):
          self.execute('DROP CONTINUOUS VIEW %s CASCADE' % view['name'])

    def create_cv(self, name, stmt, **kw):
        """
        Create a continuous view
        """
        opts = ', '.join(['%s=%r' % (k, v) for k, v in kw.items()])

        if kw:
            result = self.execute('CREATE CONTINUOUS VIEW %s WITH (%s) AS %s' % (name, opts, stmt))
        else:
            result = self.execute('CREATE CONTINUOUS VIEW %s AS %s' % (name, stmt))
        return result

    def create_ct(self, name, stmt, trigfn):
        """
        Create a continuous transform
        """
        result = self.execute(
          'CREATE CONTINUOUS TRANSFORM %s AS %s THEN EXECUTE PROCEDURE %s' %
          (name, stmt, trigfn))
        return result

    def create_table(self, name, **cols):
        """
        Create a table
        """
        cols = ', '.join(['%s %s' % (k, v) for k, v in cols.iteritems()])
        self.execute('CREATE TABLE %s (%s)' % (name, cols))

    def create_stream(self, name, **cols):
        """
        Create a stream
        """
        cols = ', '.join(['%s %s' % (k, v) for k, v in cols.iteritems()])
        self.execute('CREATE STREAM %s (%s)' % (name, cols))

    def drop_table(self, name):
        """
        Drop a table
        """
        self.execute('DROP TABLE %s' % name)

    def drop_stream(self, name):
        """
        Drop a stream
        """
        self.execute('DROP STREAM %s' % name)

    def drop_cv(self, name):
        """
        Drop a continuous view
        """
        return self.execute('DROP CONTINUOUS VIEW %s' % name)

    def execute(self, stmt):
        """
        Execute a raw SQL statement
        """
        if not self.conn:
          return None
        return self.conn.execute(stmt)

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
        batches = [rows[i:i + batch_size] for i in range(0, len(rows), batch_size)]

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

    def get_conn_string(self):
        """
        Get the connection string for this database
        """
        connstr = (CONNSTR_TEMPLATE % (getpass.getuser(), self.port))
        return connstr

    def get_bin_dir(self):
        return self.bin_dir

    def get_recv_alerts(self):
        return self.recv_alerts


@pytest.fixture
def clean_db(request):
    """
    Called for every test so each test gets a clean db
    """
    pdb = request.module.pipeline
    request.addfinalizer(pdb.drop_all_queries)


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
    pipeline.run({'synchronous_stream_insert': 'off'})
    try:
      f(pipeline, clean_db)
    finally:
      pipeline.stop()
      pipeline.run()
  return wrapper
