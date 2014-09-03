## Getting started


#### Build from source (with debug symbols)
```
./configure CFLAGS="-g -O0" --prefix=/path/to/dev/installation
make
make install
```

#### Bootstrap the environment
This creates PipelineDB's physical data directories, configuration files, etc.

```
make bootstrap
```

#### Run PipelineDB
This will run all of the daemons necessary for the PipelineDB to operate. Enter `Ctrl+C` to shut down PipelineDB.

```
make run
```
