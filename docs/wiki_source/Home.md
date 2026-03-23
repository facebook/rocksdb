# Welcome to RocksDB
RocksDB is a storage engine with key/value interface, where keys and values are arbitrary byte streams. It is a C++ library. It was developed at Facebook based on LevelDB and provides backwards-compatible support for LevelDB APIs.

RocksDB supports various storage hardware, with fast flash as the initial focus. It uses a Log Structured Database Engine for storage, is written entirely in C++, and has a Java wrapper called RocksJava. See [[RocksJava Basics]].

RocksDB can adapt to a variety of production environments, including pure memory, Flash, hard disks or remote storage. Where RocksDB cannot automatically adapt, highly flexible configuration settings are provided to allow users to tune it for them. It supports various compression algorithms and good tools for production support and debugging.

## Features
* Designed for application servers wanting to store up to a few terabytes of data on local or remote storage systems.
* Optimized for storing small to medium size key-values on fast storage -- flash devices or in-memory
* It works well on processors with many cores

## Features Not in LevelDB
RocksDB introduces dozens of new major features. See [the list of features not in LevelDB](https://github.com/facebook/rocksdb/wiki/Features-Not-in-LevelDB).

## Getting Started
For a complete Table of Contents, see the sidebar to the right. Most readers will want to start with the [Overview](https://github.com/facebook/rocksdb/wiki/RocksDB-Overview) and the [Basic Operations](https://github.com/facebook/rocksdb/wiki/Basic-Operations) section of the Developer's Guide. Get your initial options set-up following [[Setup Options and Basic Tuning]]. Also check [[RocksDB FAQ]]. There is also a [[RocksDB Tuning Guide]] for advanced RocksDB users.

Check [INSTALL.md](https://github.com/facebook/rocksdb/blob/main/INSTALL.md) for instructions on how to build Rocksdb.

## Releases
RocksDB releases are done in github. For Java users, Maven Artifacts are also updated. See [[RocksDB Release Methodology]].

## Contributing to RocksDB
You are welcome to send pull requests to contribute to RocksDB code base! Check [[RocksDB-Contribution-Guide]] for guideline.

## Troubleshooting and asking for help
Follow [[RocksDB Troubleshooting Guide]] for the guidelines.

## Blog 
* Check out our blog at [rocksdb.org/blog](http://rocksdb.org/blog)

## Project History
* [The History of RocksDB](http://rocksdb.blogspot.com/2013/11/the-history-of-rocksdb.html)
* [Under the Hood: Building and open-sourcing RocksDB](https://www.facebook.com/notes/facebook-engineering/under-the-hood-building-and-open-sourcing-rocksdb/10151822347683920).

## Links 
* [Examples](https://github.com/facebook/rocksdb/tree/main/examples)
* [Official Blog](http://rocksdb.org/blog/)
* [Stack Overflow: RocksDB](https://stackoverflow.com/questions/tagged/rocksdb)
* [Talks](https://github.com/facebook/rocksdb/wiki/Talks)

## Contact
* [RocksDB Google Group](https://groups.google.com/forum/#!forum/rocksdb)
* [RocksDB Facebook Group](https://www.facebook.com/groups/rocksdb.dev/)
* [RocksDB Github Issues](https://github.com/facebook/rocksdb/issues)
* [Asking For Help](https://github.com/facebook/rocksdb/wiki/RocksDB-Troubleshooting-Guide#asking-for-help) <br>
We use github issues only for bug reports, and use RocksDB's Google Group or Facebook Group for other issues. It’s not always clear to users whether it is RocksDB bug or not. Pick one using your best judgement.



