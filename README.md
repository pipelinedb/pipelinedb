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

Now let's generate some test data and stream it into a simple continuous view. First we'll need to create an encoding that we can use to describe our test data:

    psql
    =# CREATE ENCODING test_data (key text, value float) DECODED BY json_in;
    CREATE ENCODING

Next, create a simple continuous view:

    =# CREATE CONTINUOUS VIEW test_view AS SELECT key::text, COUNT(*) FROM test_stream GROUP BY key;
    CREATE CONTINUOUS VIEW
    =# ACTIVATE test_view --this will block

Now use the `generate-json` and `emit-local` scripts to stream data into the continuous view. The following invocation of `generate-json` will generate 10,000 JSON payloads with random strings assigned to the `key` field, and random `floats` assigned to the `value` field:

    cd pipeline/emit
    ./generate-json --key=str --value=float --n=10000 | ./emit-local --encoding test_data --stream test_stream 
    
Let's verify that the continuous view was properly updated. Were there actually 10,000 events counted?

    psql -c "SELECT sum(count) FROM test_view"
      sum  
    -------
    10000
    (1 row)

What were the 10 most common randomly generated keys?

    psql -c "SELECT * FROM test_view ORDER BY count DESC limit 10"
     key | count 
    -----+-------
     2   |    24
     c   |    22
     a   |    21
     4   |    20
     9   |    20
     b   |    19
     8   |    18
     0   |    17
     B   |    16
     E   |    16
    (10 rows)



    


