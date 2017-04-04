## Compilation

**Important**: If you plan to run RocksDB in production, don't compile using default
`make` or `make all`. That will compile RocksDB in debug mode, which is much slower
than release mode.

RocksDB's library should be able to compile without any dependency installed,
although we recommend installing some compression libraries (see below).
We do depend on newer gcc/clang with C++11 support.

There are few options when compiling RocksDB:

* [recommended] `make static_lib` will compile librocksdb.a, RocksDB static library. Compiles static library in release mode.

* `make shared_lib` will compile librocksdb.so, RocksDB shared library. Compiles shared library in release mode.

* `make check` will compile and run all the unit tests. `make check` will compile RocksDB in debug mode.

* `make all` will compile our static library, and all our tools and unit tests. Our tools
depend on gflags. You will need to have gflags installed to run `make all`. This will compile RocksDB in debug mode. Don't
use binaries compiled by `make all` in production.

* By default the binary we produce is optimized for the platform you're compiling on
(-march=native or the equivalent). If you want to build a portable binary, add 'PORTABLE=1' before
your make commands, like this: `PORTABLE=1 make static_lib`. If you want to build a binary that
makes use of SSE4, add 'USE_SSE=1' before your make commands, like this: `USE_SSE=1 make static_lib`.

## Dependencies

* You can link RocksDB with following compression libraries:
  - [zlib](http://www.zlib.net/) - a library for data compression.
  - [bzip2](http://www.bzip.org/) - a library for data compression.
  - [snappy](http://google.github.io/snappy/) - a library for fast
      data compression.
  - [zstandard](http://www.zstd.net) - Fast real-time compression
      algorithm.

* All our tools depend on:
  - [gflags](https://gflags.github.io/gflags/) - a library that handles
      command line flags processing. You can compile rocksdb library even
      if you don't have gflags installed.

## Supported platforms

* **Linux - Ubuntu**
    * Upgrade your gcc to version at least 4.8 to get C++11 support.
    * Install gflags. First, try: `sudo apt-get install libgflags-dev`
      If this doesn't work and you're using Ubuntu, here's a nice tutorial:
      (http://askubuntu.com/questions/312173/installing-gflags-12-04)
    * Install snappy. This is usually as easy as:
      `sudo apt-get install libsnappy-dev`.
    * Install zlib. Try: `sudo apt-get install zlib1g-dev`.
    * Install bzip2: `sudo apt-get install libbz2-dev`.
    * Install zstandard: `sudo apt-get install libzstd-dev`.

* **Linux - CentOS / RHEL**
    * Upgrade your gcc to version at least 4.8 to get C++11 support:
      `yum install gcc48-c++`
    * Install gflags:

              git clone https://github.com/gflags/gflags.git
              cd gflags
              git checkout v2.0
              ./configure && make && sudo make install

      **Notice**: Once installed, please add the include path for gflags to your CPATH env var and the
      lib path to LIBRARY_PATH. If installed with default settings, the lib will be /usr/local/lib
      and the include path will be /usr/local/include.

    * Install snappy:

              wget https://github.com/google/snappy/releases/download/1.1.4/snappy-1.1.4.tar.gz
              tar -xzvf snappy-1.1.4.tar.gz
              cd snappy-1.1.4
              ./configure && make && sudo make install

    * Install zlib:

              sudo yum install zlib
              sudo yum install zlib-devel

    * Install bzip2:

              sudo yum install bzip2
              sudo yum install bzip2-devel

    * Install zstandard:

             wget https://github.com/facebook/zstd/archive/v1.1.3.tar.gz
             mv v1.1.3.tar.gz zstd-1.1.3.tar.gz
             tar zxvf zstd-1.1.3.tar.gz
             cd zstd-1.1.3
             make && sudo make install

* **OS X**:
    * Install latest C++ compiler that supports C++ 11:
        * Update XCode:  run `xcode-select --install` (or install it from XCode App's settting).
        * Install via [homebrew](http://brew.sh/).
            * If you're first time developer in MacOS, you still need to run: `xcode-select --install` in your command line.
            * run `brew tap homebrew/versions; brew install gcc48 --use-llvm` to install gcc 4.8 (or higher).
    * run `brew install rocksdb`

* **iOS**:
  * Run: `TARGET_OS=IOS make static_lib`. When building the project which uses rocksdb iOS library, make sure to define two important pre-processing macros: `ROCKSDB_LITE` and `IOS_CROSS_COMPILE`.

* **Windows**:
  * For building with MS Visual Studio 13 you will need Update 4 installed.
  * Read and follow the instructions at CMakeLists.txt
