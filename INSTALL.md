## Dependencies

RocksDB is developed on Linux (CentOS release 5.2), with gcc 4.8.1.
It depends on gcc with C++11 support.

* RocksDB depends on the following libraries:
  - [zlib](http://www.zlib.net/) - a library for data compression.
  - [bzip2](http://www.bzip.org/) - a library for data compression.
  - [snappy](https://code.google.com/p/snappy/) - a library for fast
      data compression.
  - [gflags](https://code.google.com/p/gflags/) - a library that handles
      command line flags processing.

RocksDB will successfully compile without the compression libraries included,
but some things may fail. We do not support releases without the compression
libraries. You are on your own.

## Supported platforms

* **Linux**
    * Upgrade your gcc to version at least 4.7 to get C++11 support.
    * Install gflags. If you're on Ubuntu, here's a nice tutorial:
      (http://askubuntu.com/questions/312173/installing-gflags-12-04)
    * Install snappy. This is usually as easy as:
      `sudo apt-get install libsnappy-dev`.
    * Install zlib. Try: `sudo apt-get install zlib1g-dev`.
    * Install bzip2: `sudo apt-get install libbz2-dev`.
* **OS X**:
    * Update your xcode to the latest version to get the compiler with
      C++ 11 support.
    * Install zlib, bzip2 and snappy libraries for compression.
    * Install gflags. We have included a script
    `build_tools/mac-install-gflags.sh`, which should automatically install it.
    If you installed gflags by other means (for example, `brew install gflags`),
    please set `LIBRARY_PATH` and `CPATH` accordingly.
    * Please note that some of the optimizations/features are disabled in OSX.
    We did not run any production workloads on it.

## Compilation
`make clean; make` will compile librocksdb.a (RocskDB static library) and all
the unit tests. You can run all unit tests with `make check`.

If you followed the above steps and your compile or unit tests fail,
please submit an issue: (https://github.com/facebook/rocksdb/issues)
