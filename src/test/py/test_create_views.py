import random

from base import pipeline, clean_db

# XXX(usmanm): update this, if it ever changes!
MAX_CQS = 1024

def test_create_views(pipeline, clean_db):
  cvs = []
  pipeline.create_stream('stream0', x='int')
  q = 'SELECT count(*) FROM stream0'

  for i in range(1, MAX_CQS):
    cvs.append('cv_%d' % i)
    pipeline.create_cv(cvs[-1], q)

  try:
    pipeline.create_cv('cv_fail', q)
    assert False
  except Exception as e:
    assert 'maximum number of continuous queries exceeded' in e.pgerror

  ids = [r['id'] for r in
       pipeline.execute('SELECT id FROM pipelinedb.get_views()')]

  assert len(set(ids)) == len(ids)
  assert set(ids) == set(range(1, MAX_CQS))

  num_remove = random.randint(128, 512)

  for _ in range(num_remove):
    pipeline.drop_cv(cvs.pop())

  for _ in range(num_remove):
    cvs.append('cv_%d' % (len(cvs) + 1))
    pipeline.create_cv(cvs[-1], q)
