from base import pipeline, clean_db
import random

MAX_BACKOFF = 5 # should be kept in sync with whatever is set in our code base.

def test_backoff(pipeline, clean_db):

