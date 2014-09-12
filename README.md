## Getting started


#### Build PipelineDB from source (with debug symbols)
```
./configure CFLAGS="-g -O0" --prefix=/path/to/dev/installation
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

Now let's generate some test data and stream it into a simple continuous view. First, create a simple continuous view:

    =# CREATE CONTINUOUS VIEW test_view AS SELECT key::text, COUNT(*) FROM test_stream GROUP BY key;
    CREATE CONTINUOUS VIEW
    =# ACTIVATE test_view; --this will block

Now use the `generate-inserts` script to stream data into the continuous view. The following invocation of `generate-inserts` will generate a SQL multi `INSERT` with 100,000 tuples having random strings assigned to the `key` field, and random `ints` assigned to the `value` field. And since our script is just generating SQL, we can pipe its output directly into the `pipeline` client:

    cd pipeline/emit
    ./generate-inserts --stream test_stream --key=str --value=int --batchsize=100000 --n=1 | pipeline
    
Try running `generate-inserts` without piping it into `pipeline` to get an idea of what's actually happening (reduce the `batchsize` first!). Basically, any `INSERT` target that isn't a table is considered a stream by PipelineDB. 
    
Let's verify that the continuous view was properly updated. Were there actually 100,000 events counted?

    psql -c "SELECT sum(count) FROM test_view"
      sum  
    -------
    100000
    (1 row)

What were the 10 most common randomly generated keys?

    psql -c "SELECT * FROM test_view ORDER BY count DESC limit 10"
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



    


