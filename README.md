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

`make bootstrap` only needs to be run the first time you install PipelineDB. The resources that `make bootstrap` creates may continue to be used as you change and rebuild PipeineDB. 


#### Run PipelineDB
Run all of the daemons necessary for PipelineDB to operate: 

```
make run
```

Enter `Ctrl+C` to shut down PipelineDB.

`make run` uses the binaries in the PipelineDB root compiled by `make`, so you don't need to `make install` before running `make run` after code changes--only `make` needs to be run. 

The basic development flow is:

```
make
make run
^C

# Make some code changes...
make
make run
```


