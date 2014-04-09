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

* **Linux - Ubuntu**
    * Upgrade your gcc to version at least 4.7 to get C++11 support.
    * Install gflags. First, try: `sudo apt-get install libgflags-dev`
      If this doesn't work and you're using Ubuntu, here's a nice tutorial:
      (http://askubuntu.com/questions/312173/installing-gflags-12-04)
    * Install snappy. This is usually as easy as:
      `sudo apt-get install libsnappy-dev`.
    * Install zlib. Try: `sudo apt-get install zlib1g-dev`.
    * Install bzip2: `sudo apt-get install libbz2-dev`.
* **Linux - CentOS**
    * Upgrade your gcc to version at least 4.7 to get C++11 support:
      `yum install gcc47-c++`
    * Install gflags:

              wget https://gflags.googlecode.com/files/gflags-2.0-no-svn-files.tar.gz
              tar -xzvf gflags-2.0-no-svn-files.tar.gz
              cd gflags-2.0
              ./configure && make && sudo make install

    * Install snappy:

              wget https://snappy.googlecode.com/files/snappy-1.1.1.tar.gz
              tar -xzvf snappy-1.1.1.tar.gz
              cd snappy-1.1.1
              ./configure && make && sudo make install

    * Install zlib:

              sudo yum install zlib
              sudo yum install zlib-devel

    * Install bzip2:

              sudo yum install bzip2
              sudo yum install bzip2-devel

* **OS X**:
    * Install latest C++ compiler that supports C++ 11:
        * Update XCode:  run `xcode-select --install` (or install it from XCode App's settting).
        * Install via [homebrew](http://brew.sh/).
            * If you're first time developer in MacOS, you still need to run: `xcode-select --install` in your command line.
            * run `brew tap homebrew/dupes; brew install gcc47 --use-llvm` to install gcc 4.7 (or higher).
    * Install zlib, bzip2 and snappy libraries for compression.
    * Install gflags. We have included a script
    `build_tools/mac-install-gflags.sh`, which should automatically install it.
    If you installed gflags by other means (for example, `brew install gflags`),
    please set `LIBRARY_PATH` and `CPATH` accordingly.
    * Please note that some of the optimizations/features are disabled in OSX.
    We did not run any production workloads on it.

* **iOS**:
  * Run: `TARGET_OS=IOS make static_lib`

## Compilation
`make clean; make` will compile librocksdb.a (RocksDB static library) and all
the unit tests. You can run all unit tests with `make check`.

For shared library builds, exec `make shared_lib` instead.

If you followed the above steps and your compile or unit tests fail,
please submit an issue: (https://github.com/facebook/rocksdb/issues)
