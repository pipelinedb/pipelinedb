PipelineDB [has joined Confluent](https://www.confluent.io/blog/pipelinedb-team-joins-confluent), read the blog post [here](https://www.pipelinedb.com/blog/pipelinedb-is-joining-confluent).

PipelineDB will not have new releases beyond `1.0.0`, although critical bugs will still be fixed.

# PipelineDB

[![Gitter chat](https://img.shields.io/badge/gitter-join%20chat-brightgreen.svg?style=flat-square)](https://gitter.im/pipelinedb/pipelinedb)
[![Twitter](https://img.shields.io/badge/twitter-@pipelinedb-55acee.svg?style=flat-square)](https://twitter.com/pipelinedb)

## Overview

PipelineDB is a PostgreSQL extension for high-performance time-series aggregation, designed to power realtime reporting and analytics applications.

PipelineDB allows you to define [continuous SQL queries](http://docs.pipelinedb.com/continuous-views.html) that perpetually aggregate time-series data and store **only the aggregate output** in regular, queryable tables. You can think of this concept as extremely high-throughput, incrementally updated materialized views that never need to be manually refreshed.

Raw time-series data is never written to disk, making PipelineDB extremely efficient for aggregation workloads.

Continuous queries produce their own [output streams](http://docs.pipelinedb.com/streams.html#output-streams), and thus can be [chained together](http://docs.pipelinedb.com/continuous-transforms.html) into arbitrary networks of continuous SQL.

## PostgreSQL compatibility

PipelineDB runs on 64-bit architectures and currently supports the following PostgreSQL versions:

* **PostgreSQL 10**: 10.1, 10.2, 10.3, 10.4, 10.5
* **PostgreSQL 11**: 11.0

## Getting started

If you just want to start using PipelineDB right away, head over to the [installation docs](http://docs.pipelinedb.com/installation.html) to get going.

If you'd like to build PipelineDB from source, keep reading!

## Building from source

Since PipelineDB is a PostgreSQL extension, you'll need to have the [PostgreSQL development packages](https://www.postgresql.org/download/) installed to build PipelineDB.

Next you'll have to install [ZeroMQ](http://zeromq.org/) which PipelineDB uses for inter-process communication. [Here's](https://gist.github.com/derekjn/14f95b7ceb8029cd95f5488fb04c500a) a gist with instructions to build and install ZeroMQ from source.
You'll also need to install some Python dependencies if you'd like to run PipelineDB's Python test suite:

```
pip install -r src/test/py/requirements.txt
```

#### Build PipelineDB:

Once PostgreSQL is installed, you can build PipelineDB against it:

```
make USE_PGXS=1
make install
```

#### Test PipelineDB *(optional)*
Run the following command:

```
make test
```

#### Bootstrap the PipelineDB environment
Create PipelineDB's physical data directories, configuration files, etc:

```
make bootstrap
```

**`make bootstrap` only needs to be run the first time you install PipelineDB**. The resources that `make bootstrap` creates may continue to be used as you change and rebuild PipeineDB.


#### Run PipelineDB
Run all of the daemons necessary for PipelineDB to operate:

```
make run
```

Enter `Ctrl+C` to shut down PipelineDB.

`make run` uses the binaries in the PipelineDB source root compiled by `make`, so you don't need to `make install` before running `make run` after code changes--only `make` needs to be run.

The basic development flow is:

```
make
make run
^C

# Make some code changes...
make
make run
```

#### Send PipelineDB some data

Now let's generate some test data and stream it into a simple continuous view. First, create the stream and the continuous view that reads from it:

    $ psql
    =# CREATE FOREIGN TABLE test_stream (key integer, value integer) SERVER pipelinedb;
    CREATE FOREIGN TABLE
    =# CREATE VIEW test_view WITH (action=materialize) AS SELECT key, COUNT(*) FROM test_stream GROUP BY key;
    CREATE VIEW

Events can be emitted to PipelineDB streams using regular SQL `INSERTS`. Any `INSERT` target that isn't a table is considered a stream by PipelineDB, meaning streams don't need to have a schema created in advance. Let's emit a single event into the `test_stream` stream since our continuous view is reading from it:

    $ psql
    =# INSERT INTO test_stream (key, value) VALUES (0, 42);
    INSERT 0 1

The 1 in the `INSERT 0 1` response means that 1 event was emitted into a stream that is actually being read by a continuous query. Now let's insert some random data:

    =# INSERT INTO test_stream (key, value) SELECT random() * 10, random() * 10 FROM generate_series(1, 100000);
    INSERT 0 100000

Query the continuous view to verify that the continuous view was properly updated. Were there actually 100,001 events counted?

    $ psql -c "SELECT sum(count) FROM test_view"
      sum
    -------
    100001
    (1 row)

What were the 10 most common randomly generated keys?

    $ psql -c "SELECT * FROM test_view ORDER BY count DESC limit 10"
	key  | count 
	-----+-------
	 2   | 10124
	 8   | 10100
	 1   | 10042
	 7   |  9996
	 4   |  9991
	 5   |  9977
	 3   |  9963
	 6   |  9927
	 9   |  9915
	10   |  4997
	 0   |  4969

	(11 rows)
