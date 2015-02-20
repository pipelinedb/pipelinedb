from base import pipeline, clean_db
import threading
import time


def test_drain(pipeline, clean_db):
  """
  Ensure that after deactivation, no tuples are left in the buffer with
  the deactivated CQ as a reader
  """
  pipeline.create_cv('test_drain', 'SELECT COUNT(*) FROM stream')
  pipeline.activate()
  pipeline.set_sync_insert(False)

  values = [(i, ) for i in xrange(1000)]
  desc = ('x', )

  is_active = True

  def insert():
    while is_active:
      try:
        pipeline.insert('stream', desc, values)
      except:
        # This happens when the INSERT stmt seems the CQ as inactive.
        break

  threads = map(lambda _: threading.Thread(target=insert), xrange(20))
  map(lambda t: t.start(), threads)

  # Let the insert thread do some magic.
  time.sleep(0.2)

  # Deactivate and wait for insert thread to see the deactivation.
  pipeline.deactivate()
  is_active = False
  map(lambda t: t.join(), threads)

  count = pipeline.execute('SELECT * FROM test_drain').first()['count']

  # Activate again. If there were unread tuples left, then we should see
  # an increase in count.
  pipeline.activate()
  time.sleep(0.5)
  pipeline.deactivate()

  assert count == pipeline.execute('SELECT * FROM test_drain').first()['count']
