<p align="center">
  <a href="https://rocksdb.org/">
    <img src="https://rocksdb.org/static/logo.svg" alt="logo" width="316" />
  </a>
</p>
<h2 align="center">
  RocksDB: A Persistent Key-Value Store for Flash and RAM Storage
</h2>
<p align="center">
  <a href="https://circleci.com/gh/facebook/rocksdb">
    <img src="https://circleci.com/gh/facebook/rocksdb.svg?style=svg" alt="CircleCI Status" />
  </a>
  <a href="https://travis-ci.org/facebook/rocksdb">
    <img src="https://travis-ci.org/facebook/rocksdb.svg?branch=master" alt="CircleCI Status" />
  </a>
  <a href="https://ci.appveyor.com/project/Facebook/rocksdb/branch/master">
    <img src="https://ci.appveyor.com/api/projects/status/fbgfu0so3afcno78/branch/master?svg=true" alt="Appveyor Build status" />
  </a>
  <a href="http://140.211.168.68:8080/job/Rocksdb">
    <img src="https://140.211.168.68:8080/buildStatus/icon?job=Rocksdb" alt="PPC64le Build Status" />
  </a>
</p>

RocksDB is developed and maintained by Facebook Database Engineering Team.
It is built on earlier work on [LevelDB](https://github.com/google/leveldb) by Sanjay Ghemawat (sanjay@google.com)
and Jeff Dean (jeff@google.com)

This code is a library that forms the core building block for a fast
key-value server, especially suited for storing data on flash drives.
It has a Log-Structured-Merge-Database (LSM) design with flexible tradeoffs
between Write-Amplification-Factor (WAF), Read-Amplification-Factor (RAF)
and Space-Amplification-Factor (SAF). It has multi-threaded compactions,
making it especially suitable for storing multiple terabytes of data in a
single database.

Start with example usage here: https://github.com/facebook/rocksdb/tree/master/examples

See the [github wiki](https://github.com/facebook/rocksdb/wiki) for more explanation.

The public interface is in `include/`.  Callers should not include or
rely on the details of any other header files in this package.  Those
internal APIs may be changed without warning.

Design discussions are conducted in https://www.facebook.com/groups/rocksdb.dev/ and https://rocksdb.slack.com/

## License

RocksDB is dual-licensed under both the GPLv2 (found in the COPYING file in the root directory) and Apache 2.0 License (found in the LICENSE.Apache file in the root directory).  You may select, at your option, one of the above-listed licenses.
