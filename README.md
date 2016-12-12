## RocksDB-Cloud: A Key-Value Store for Cloud Applications

[![Build Status](https://travis-ci.org/facebook/rocksdb.svg?branch=master)](https://travis-ci.org/facebook/rocksdb)
[![Build status](https://ci.appveyor.com/api/projects/status/fbgfu0so3afcno78/branch/master?svg=true)](https://ci.appveyor.com/project/Facebook/rocksdb/branch/master)

RocksDB-Cloud brings the power of RocksDB to AWS, Google Cloud and Microsoft Azure.
It leverages the power of RocksDB to provide fast key-value access to data stored
in Flash and RAM systems. It provides for data durability even in the face of
machine failures by integrations with cloud services like AWS-S3 and Google Cloud
Services. It allows a cost-effective way to utilize the rich variety of
storage services (based on RAM, NvMe, SSD, Disk Cold Storage, etc) that are offered by
most cloud providers. RocksDB-Cloud is developed and maintained by the engineering
team at Rockset Inc. Start with https://github.com/rockset/rocksdb-cloud/tree/master/aws.

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
