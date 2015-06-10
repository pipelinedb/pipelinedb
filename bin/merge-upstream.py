#! /usr/bin/python

import argparse
import difflib
import os
import progressbar
import re
import shutil
import tarfile
import tempfile
import urllib2


def download_upstream(version, tmp_dir):
  filename = 'postgresql-%s.tar.gz' % version
  tar_path = os.path.join(tmp_dir, filename)
  url = ('https://ftp.postgresql.org/pub/source/v%(version)s/'
         'postgresql-%(version)s.tar.gz' % {'version': args.version})

  print 'Downloading %s to %s...' % (url, tar_path)

  req = urllib2.urlopen(url)

  if (req.getcode() != 200):
    raise Exception('Failed to download Postgres tarball! Got status code %d.'
                    % req.getcode())

  size = int(req.info()['Content-Length'])
  progress = progressbar.ProgressBar().start()

  with open(tar_path, 'wb') as f:
    blk_sz = 8192
    count = 0
    while True:
      chunk = req.read(blk_sz)
      if not chunk:
        break
      f.write(chunk)
      count += 1
      if size > 0:
        percent = min(int(count * blk_sz * 100.0 / size), 100)
        progress.update(percent)

  progress.finish()
  print 'Download complete.'
  return tar_path


def safe_mkdirs(path):
  try:
    os.makedirs(path)
  except OSError:
    # Directory already exists
    pass


def list_files(path):
  dirs = os.walk(path)
  regex = re.compile('^%s/' % path)
  files = []

  for _dir, _, filenames in dirs:
    # Ignore any git internal files
    if regex.sub('', _dir).startswith('.git'):
      continue

    # Ignore dot files and emacs temp files.
    filenames = filter(lambda f: not f.startswith('.') and not f.endswith('~'),
                       filenames)

    files.extend(map(lambda f: regex.sub('', os.path.join(_dir, f)),\
                     filenames))

  return set(files)


def main(args):
  """
  This script works as follows:

  1) Download the upstream source tarball for the given version of Postgres
  2) Overwrite all files that we haven't modified with the newer version
     from the upstream source tarball
  3) Mercilessly override all files in the doc/ directory and all files with
     .po extension.
  4) For all new files in the upstream branch copy them as is to the repo
     directory and store their paths in ./added_files.txt.
  5) For all files removed in the upstream branch, store their paths in
     ./removed_files.txt.
  3) For all mismatching files, generate a diff against the
     corresponding upstream file in the downloaded source tarball.
     Save this file to ./diffs. We also put a copy of the upstream
     source file in ./diffs because it's useful to have when manually
     merging. A list of all these files is stored in ./mismatch_files.txt.
  """
  tmp_dir = args.tmp_dir or tempfile.mkdtemp()
  tar_path = '/tmp/tmph_B6JX/postgresql-9.4.3.tar.gz' # download_upstream(args.version, tmp_dir)

  tar = tarfile.open(tar_path)
  tar.extractall(path=tmp_dir)
  tar.close()

  upstream_root = os.path.join(tmp_dir, 'postgresql-%s' % (args.version))
  assert os.path.exists(upstream_root) and os.path.isdir(upstream_root)
  local_root = os.curdir

  diff_dir = os.path.join(os.curdir, 'diffs')
  safe_mkdirs(diff_dir)

  upstream_files = list_files(upstream_root)
  local_files = list_files(local_root)

  # Are there any new files in the upstream branch?
  if upstream_files - local_files:
    added_files = sorted(upstream_files - local_files)
    log_file = os.path.join(local_root, 'added_files.txt')
    print 'There are %d new files! Storing paths in %s.' % (len(added_files),
                                                            log_file)
    with open(log_file, 'w+') as f:
      f.write('\n'.join(added_files))

  # Are there any deleted files in the local branch?
  if local_files - upstream_files:
    rm_files = sorted(local_files - upstream_files)
    log_file = os.path.join(local_root, 'removed_files.txt')
    print 'There are %d removed files! Storing paths in %s.' % (len(rm_files),
                                                                log_file)
    with open(log_file, 'w+') as f:
      f.write('\n'.join(rm_files))

  # Go through all files in the upstream branch and add, overwrite or generate
  # diffs as necessary.
  print 'Generating diffs against upstream files for mismatching files...'
  need_merge = []
  for rel_path in upstream_files:
    upstream_path = os.path.join(upstream_root, rel_path)
    local_path = os.path.join(local_root, f)

    # File missing in local? Copy it.
    if not os.path.exists(local_path):
      subdir, _ = os.path.split(local_path)
      safe_mkdirs(subdir)
      shutil.copyfile(upstream_path, local_path)
      continue

    # If it is a documentation file, overwrite.
    if rel_path.startswith('doc/') or rel_path.endswith('.po'):
      shutil.copyfile(upstream_path, local_path)
      continue

    lf = open(local_path)
    rf = open(upstream_path)

    # We both potentially modified this file.
    left = [l.strip('\n') for l in lf.readlines()]
    right = [l.strip('\n') for l in rf.readlines()]

    lf.close()
    rf.close()

    diff_lines = list(difflib.unified_diff(left, right,
                                           local_path, upstream_path))

    # No difference? Overwrite.
    if not diff_lines:
      shutil.copyfile(upstream_path, local_path)
      continue

    diff_path = os.path.join(diff_dir, rel_path) + '.diff'
    subdir, _ = os.path.split(diff_path)
    safe_mkdirs(subdir)

    with open(diff_path, 'w+') as f:
      f.write('\n'.join(diff_lines))

    need_merge.append(local_path)
    print 'Created %s' % diff_path

    # It's also useful when merging to have the original copy
    # of upstream file, so save it as well
    upstream_copy_path = os.path.join(diff_dir, rel_path)
    shutil.copy_file(upstream_path, upstream_copy_path)

  print 'Diffs for manual merging saved to directory %s.' % diff_dir

  # It's useful to have a list of all files written to diffs/
  # so we can check off files as we merge them
  mismatch_files = os.path.join(local_root, 'mismatch_files.txt')
  with open(mismatch_files, 'w+') as f:
    f.write('\n'.join(sorted(need_merge)))
    print 'Stored all mismatching file paths in %s.' % mismatch_files

  shutil.rmtree(tmp_dir)
  print 'Cleaned up %s.' % tmp_dir


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--upstream-version', action='store', dest='version',
                      required=True, help='Upstream version to diff against')
  parser.add_argument('--tmp-dir', action='store', dest='tmp_dir',
                      required=False, help='Temporary directory to store '
                      'upstream source tree in')
  parser.add_argument('--dry-run', action='store_true', dest='dry_run',
                      required=False)
  args = parser.parse_args()
  main(args)
