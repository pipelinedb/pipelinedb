## Getting started


#### Build from source (with debug symbols)
```
./configure CFLAGS="-g -O0" --prefix=/path/to/dev/installation
make
make install
```

#### Bootstrap the environment
Create PipelineDB's physical data directories, configuration files, etc:

```
make bootstrap
```

#### Run PipelineDB
Run all of the daemons necessary for PipelineDB to operate: 

```
make run
```

Enter `Ctrl+C` to shut down PipelineDB.
