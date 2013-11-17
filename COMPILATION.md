Currently Rocksdb is developed on Linux (CentOS release 5.2), with
gcc 4.71. To build it on your own platform, you'll need to:

* Make sure your compiler (either `clang` or `gcc`) supports
  C++ 11. Please make the version for `gcc` is 4.8 or above; for
  `clang` the version is 5.0 or above.

* Install the the libraries that rocksdb depends on:
  - [zlib](http://www.zlib.net/), a library for data compression.
  - [gflags](https://code.google.com/p/gflags/) that handles command line
    flags processing.

## Platforms on which rocksdb can compile:

* Linux
* `OS X`: right now rocksdb can be compiled in Mac if you
    * update your xcode to latest version, which allows you to use compiler
     that supports C++ 11.
    * install gflags, you may run build_tool/mac-install-gflags.sh to install
      it. If you install gflags with other means (for example,
      `brew install gflags`), please set `LIBRARY_PATH` and `CPATH` accordingly.
    * Please note some of the optimizations/features are disabled in OSX. And we
      did not run any production workload on it.
