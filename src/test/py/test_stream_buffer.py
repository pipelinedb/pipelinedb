from base import pipeline, clean_db
import threading


def test_stream_buffer(pipeline, clean_db):
  """
  Run CQs which take different times to process an event in the stream buffer
  and each stream is read by a disjoint set of CQs.
  """
  pipeline.create_stream('stream1', x='int', string='text')
  pipeline.create_stream('stream2', x='int', string='text')
  pipeline.create_cv('test_sbuf_1',
             'SELECT x::int FROM stream1')
  pipeline.create_cv('test_sbuf_2',
             'SELECT x::int, cq_sleep(0.002) FROM stream1')
  pipeline.create_cv('test_sbuf_3',
             'SELECT x::int FROM stream2')
  pipeline.create_cv('test_sbuf_4',
             'SELECT x::int, cq_sleep(0.002) FROM stream2')

  num_per_batch = 5000
  num_batches = 6
  # We're sending this 2k string with each event just to consume
  # more space in the stream buffer. That way we get to test the
  # wrapping of the buffer as well.
  values = [(i, 'a' * 2048) for i in xrange(num_per_batch)]

  def insert(stream):
    for _ in xrange(num_batches - 1):
      pipeline.insert(stream, ('x', 'string'), values)

  threads = [threading.Thread(target=insert, args=('stream1', )),
         threading.Thread(target=insert, args=('stream2', ))]

  map(lambda t: t.start(), threads)
  map(lambda t: t.join(), threads)

  pipeline.insert('stream1', ('x', 'string'), values)
  pipeline.insert('stream2', ('x', 'string'), values)

  q = 'SELECT COUNT(*) FROM test_sbuf_%d'
  r1 = pipeline.execute(q % 1)[0][0]
  r2 = pipeline.execute(q % 2)[0][0]
  r3 = pipeline.execute(q % 3)[0][0]
  r4 = pipeline.execute(q % 4)[0][0]

  assert r1 == r2 == r3 == r4 == num_batches * num_per_batch
