## RocksDB-Cloud: A Key-Value Store for Cloud Applications

[![Build Status](https://travis-ci.org/facebook/rocksdb.svg?branch=master)](https://travis-ci.org/facebook/rocksdb)
[![Build status](https://ci.appveyor.com/api/projects/status/fbgfu0so3afcno78/branch/master?svg=true)](https://ci.appveyor.com/project/Facebook/rocksdb/branch/master)

RocksDB-Cloud is a C++ library that brings the power of RocksDB to AWS, Google Cloud and Microsoft Azure.
It leverages the power of RocksDB to provide fast key-value access to data stored
in Flash and RAM systems. It provides for data durability even in the face of
machine failures by integrations with cloud services like AWS-S3 and Google Cloud
Services. It allows a cost-effective way to utilize the rich hierarchy of
storage services (based on RAM, NvMe, SSD, Disk Cold Storage, etc) that are offered by
most cloud providers. RocksDB-Cloud is developed and maintained by the engineering
team at Rockset Inc. Start with https://github.com/rockset/rocksdb-cloud/tree/master/cloud.

Rocksdb-Cloud provides three main advantages for AWS environments:

1. A rocksdb instance is durable. Continuous and automatic replication of db data and metadata to S3. In the event that the rocksdb machine dies, another process on any other EC2 machine can reopen the same rocksdb database (by configuring it with the S3 bucketname where the entire db state was stored).
2. A rocksdb instance is cloneable. Rocksdb-Cloud support a primitive called zero-copy-clone() that allows a slave instance of rocksdb on another machine to clone an existing db. Both master and slave rocksdb instance can run in parallel and they share some set of common database files.
3. A rocksdb instance can leverage hierarchical storage. The entire rocksdb storage footprint need not be resident on local storage. S3 contains the entire database and the local storage contains only the files that are in the working set.

### Inherits from RocksDB: 

RocksDB-Cloud is API compatible and data format compatible with [RocksDB](https://github.com/facebook/rocksdb). 

RocksDB is developed and maintained by Facebook Database Engineering Team.
It is built on earlier work on LevelDB by Sanjay Ghemawat (sanjay@google.com)
and Jeff Dean (jeff@google.com)

This code is a library that forms the core building block for a fast
key value server, especially suited for storing data on flash drives.
It has a Log-Structured-Merge-Database (LSM) design with flexible tradeoffs
between Write-Amplification-Factor (WAF), Read-Amplification-Factor (RAF)
and Space-Amplification-Factor (SAF). It has multi-threaded compactions,
making it specially suitable for storing multiple terabytes of data in a
single database.

Start with example usage here: https://github.com/facebook/rocksdb/tree/master/examples

See the [github wiki](https://github.com/facebook/rocksdb/wiki) for more explanation.

The public interface is in `include/`.  Callers should not include or
rely on the details of any other header files in this package.  Those
internal APIs may be changed without warning.

Design discussions are conducted in https://www.facebook.com/groups/rocksdb.dev/
