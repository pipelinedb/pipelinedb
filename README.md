# PipelineDB ![Build Status](https://circleci.com/gh/pipelinedb/pipelinedb.png?circle-token=db1a70c164cd6d96544d8eb38b279c48dea24709)

## Getting started

Install some dependencies first:
```
sudo apt-get install libreadline6 libreadline6-dev check g++ flex bison python-pip zlib1g-dev python-dev
sudo pip install -r src/test/py/requirements.txt
```

#### Sync all submodules
This only needs to be done once for fresh checkouts.
```
git submodule sync
git submodule update --init
```

#### Build the PipelineDB core (with debug symbols)
```
./configure CFLAGS="-g -O0" --prefix=</path/to/dev/installation>
make
make install
```

#### Add your dev installation path to the PATH environment variable
```
export PATH=/path/to/dev/installation/bin:$PATH
```

#### Build the PipelineDB GIS module

First, install its dependencies:

    sudo apt-get install libxml2-dev libgeos-dev libproj-dev libgdal-dev xsltproc autoconf libtool

Now it can be built:

```
cd src/gis
./autogen.sh
./configure CFLAGS="-g -O0" # Note that the --prefix argument isn't necessary here
make
make install
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

Now let's generate some test data and stream it into a simple continuous view. First, create the continuous view:

    pipeline
    =# CREATE CONTINUOUS VIEW test_view AS SELECT key::text, COUNT(*) FROM test_stream GROUP BY key;
    CREATE CONTINUOUS VIEW
    =# ACTIVATE test_view;
    ACTIVATE 1

Events can be emitted to PipelineDB streams using regular SQL `INSERTS`. Any `INSERT` target that isn't a table is considered a stream by PipelineDB, meaning streams don't need to have a schema created in advance. Let's emit a single event into the `test_stream` stream since our continuous view is reading from it:

    pipeline
    =# INSERT INTO test_stream (key, value) VALUES ('key', 42);
    INSERT 0 1

The 1 in the "INSERT 0 1" response means that 1 event was emitted into a stream that is actually being read by a continuous query.

The `generate-inserts` script is useful for generating and streaming larger amounts of test data. The following invocation of `generate-inserts` will build a SQL multi `INSERT` with 100,000 tuples having random strings assigned to the `key` field, and random `ints` assigned to the `value` field. All of these events will be emitted to `test_stream`, and subsequently read by the `test_view` continuous view. And since our script is just generating SQL, we can pipe its output directly into the `pipeline` client:

    cd pipeline/emit
    ./generate-inserts --stream test_stream --key=str --value=int --batchsize=100000 --n=1 | pipeline

Try running `generate-inserts` without piping it into `pipeline` to get an idea of what's actually happening (reduce the `batchsize` first!).

Let's verify that the continuous view was properly updated. Were there actually 100,001 events counted?

    pipeline -c "SELECT sum(count) FROM test_view"
      sum
    -------
    100001
    (1 row)

What were the 10 most common randomly generated keys?

    pipeline -c "SELECT * FROM test_view ORDER BY count DESC limit 10"
     key | count
    -----+-------
    a   |  4571
    e   |  4502
    c   |  4479
    f   |  4473
    d   |  4462
    b   |  4451
    9   |  2358
    5   |  2350
    4   |  2350
    7   |  2327

    (10 rows)

Finally, note that most of the time taken to execute

    ./generate-inserts --stream test_stream --key=str --value=int --batchsize=100000 --n=1 | pipeline

is actually due to the `generate-inserts` script (Python) doing a large number of string operations. To get a better feel for the speed at which PipelineDB can process data, run the `INSERTS` independently of Python:

    ./generate-inserts --stream test_stream --key=str --value=int --batchsize=100000 --n=1 > inserts.sql
    pipeline -f inserts.sql
