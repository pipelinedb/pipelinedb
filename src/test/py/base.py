import getpass
import os
import pytest
import shutil
import signal
import socket
import subprocess
import time

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

BOOTSTRAPPED_BASE = './.pdbbase'
ROOT = '../../../'
INSTALL_FORMAT = './.pdb-%d'
SERVER = os.path.join(ROOT, 'src', 'backend', 'pipeline-server')
TEST_DBNAME = 'pipelinedb_test'
CONNSTR_TEMPLATE = 'postgres://%s@localhost:%d/pipeline'
TEST_ENV_VAR = 'PIPELINE_PY_TESTS'

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
            gis = os.path.join(os.path.abspath(ROOT), 'src', 'gis')
            make = [
                'make', '-C', gis,
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

        self.engine = None

    def run(self):
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

        env = os.environ.copy()
        env[TEST_ENV_VAR] = '1'
        self.proc = subprocess.Popen([SERVER, '-D', self.data_dir,
                                      '-p', str(self.port),
                                      '-c', 'sync_stream_insert=true'],
                                     env=env)

        connstr = CONNSTR_TEMPLATE % (getpass.getuser(), self.port)
        self.engine = create_engine(connstr)

        # Wait for PipelineDB to start up
        for i in xrange(10):
          try:
            self.conn = self.engine.connect()
            break
          except OperationalError:
            time.sleep(0.1)
        else:
          raise Exception('Failed to start up PipelineDB')

    def stop(self):
      """
      Stops the PipelineDB instance
      """
      self.conn.close()
      if self.proc:
        self.proc.send_signal(signal.SIGINT)
        self.proc.wait()

    def destroy(self):
        """
        Cleans up resources used by this PipelineDB instance
        """
        self.deactivate()
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

    def drop_db(self, name=TEST_DBNAME):
        """
        Drop a database within this PipelineDB instance
        """
        # We can't drop a DB in a transaction block
        self.conn.execute('commit')
        self.deactivate()
        return self.execute('DELETE FROM pipeline_query')

    def create_cv(self, name, stmt, activate=False):
        """
        Create a continuous view
        """
        result = self.execute('CREATE CONTINUOUS VIEW %s AS %s' % (name, stmt))
        if activate:
            result = self.activate(name)
        return result

    def create_table(self, name, **cols):
        """
        Create a table
        """
        cols = ', '.join(['%s %s' % (k, v) for k, v in cols.iteritems()])
        self.execute('CREATE TABLE %s (%s)' % (name, cols))

    def drop_table(self, name):
        """
        Drop a table
        """
        self.execute('DROP TABLE %s' % name)

    def drop_cv(self, name):
        """
        Drop a continuous view
        """
        return self.execute('DROP CONTINUOUS VIEW %s' % name)

    def activate(self, name=None, **kw):
        """
        Activate a continuous view, or all of them if no name is given
        """
        args = ''
        kw.setdefault('batchsize', 1000)
        if kw:
            args = ','.join('%s = %s' % (k, (str(v))) for k, v in kw.iteritems())
            args = ' WITH (%s)' % args

        if name:
            return self.execute('ACTIVATE %s %s' % (name, args))
        else:
            return self.execute('ACTIVATE %s' % args)

    def deactivate(self, name=None):
        """
        Deactivate a continuous view, or all of them if no name is given
        """
        if name:
            return self.execute('DEACTIVATE %s' % name)
        else:
            return self.execute('DEACTIVATE')

    def execute(self, stmt):
        """
        Execute a raw SQL statement
        """
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
        values = values.replace("'null'", 'null')

        return self.execute('INSERT INTO %s (%s) VALUES %s' % (target, header, values))

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
    request.addfinalizer(pdb.drop_db)

    return TEST_DBNAME


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
