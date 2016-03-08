import random

from base import pipeline, clean_db

# XXX(usmanm): update this, if it ever changes!
MAX_CQS = 1024

def test_create_views(pipeline, clean_db):
  cvs = []
  q = 'SELECT count(*) FROM stream'

  for i in xrange(1, MAX_CQS):
    cvs.append('cv_%d' % i)
    pipeline.create_cv(cvs[-1], q)

  try:
    pipeline.create_cv('cv_fail', q)
    assert False
  except Exception, e:
    assert 'maximum number of continuous views exceeded' in e.message

  ids = [r['id'] for r in
         pipeline.execute('SELECT id FROM pipeline_views()')]

  assert len(set(ids)) == len(ids)
  assert set(ids) == set(xrange(1, MAX_CQS))

  num_remove = random.randint(128, 512)

  for _ in xrange(num_remove):
    pipeline.drop_cv(cvs.pop())

  for _ in xrange(num_remove):
    cvs.append('cv_%d' % (len(cvs) + 1))
    pipeline.create_cv(cvs[-1], q)
