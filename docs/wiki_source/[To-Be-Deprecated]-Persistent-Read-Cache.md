# Introduction

For a very long time, disks were the means of persistent for datastores. With the introduction of SSD, we now have a persistent medium that is significantly faster than the traditional disks but with limited write endurance and capacity, enabling us to explore the opportunities of tiered storage architecture. Open source implementations like flash cache used SSD and disk as tiered storage outperforming disk for server applications. RocksDB Persistent read cache is an effort to take advantage of the tiered storage architecture in a device agnostic and operating system independent manner for the RocksDB ecosystem. 

![](https://s30.postimg.org/f32jlwu9t/Motivation.jpg)

# Tiered Storage Vs Tiered Cache

RocksDB users can take advantage of the tiered storage architecture either by adopting tiered storage deployment approach or by adopting tiered cache deployment approach. With the tiered storage approach, you can distribute the contents of the LSM on multiple persistent storage tiers. With the tiered cache approach, users can use the faster persistent medium as a read cache serving frequently accessed parts of the LSM and enhance the overall performance of RocksDB.

Tiered cache has a few advantage in terms of data mobility since the cache is an add-on for performance. The store can continue to function without the cache. 

![](https://s27.postimg.org/ae63oe1fn/Tiered_Storage.jpg)

# Key Features

#### Hardware agnostic

The persistent read cache is a generic implementation and is not specifically designed for any kind of device in particular. Instead of designing for specific types of hardware, we have taken the approach of designing the cache to provide the user with a mechanism to describe the best way to access the device, and the IO paths will be configured to work as per the description.

Write code path can be described using the formula

_{ Block Size, Queue depth, Access/Caching Technique }_

![](https://s23.postimg.org/5ri8jpojf/Write_IOPath.jpg)

Read code path can be described using the formula

_{ Access/Caching Technique }_

![](https://s27.postimg.org/430w7z077/Read_IOPath.jpg)

_Block Size_ describes the size to read/write. In the case of SSDs, this would typically be erasure block size.

_Queue depth_ is the parallelism at which the device exhibits the best performance.

_Access/Caching Technique_ is used to describes the best way to access the device. Using direct IO access for example is suitable for certain devices/applications and buffered access is preferred for others.

#### OS agnostic

Persistent read cache is build using RocksDB abstraction and is supported on all platforms where RocksDB is supported.

#### Pluggable

Since this is a cache implementation, the cache may or may not be supplied on a restart. 

# Design and Implementation Details

The implementation of Persistent Read Cache has three fundamental components.

### Block Lookup Index

This is a scalable in-memory hash index that maps a given LSM block address to a cache record locator. The cache record locator helps locate the block data in the cache. The cache record can be described as { file-id, offset, size }.

![](https://s24.postimg.org/6nd0rx19x/Block_Index.jpg)

### File Lookup Index / LRU

The is a scalable in-memory hash index which allows for eviction based on LRU. This index maps a given file identifier to its reference object abstraction. The object abstraction can be used for reading data from the cache. When we run out of space on the persistent cache, we evict the least recently used file from this index.

![](https://s28.postimg.org/wlo6c4nkd/File_Index.jpg)

### File Layout

The cache is stored in the file system as a sequence of files. Each file contains a sequence of records which contain data corresponding to a block on RocksDB LSM.

![](https://s23.postimg.org/jshgdqsrv/File_Layout.jpg)

# API

Please follow the link below for the public API.

https://github.com/facebook/rocksdb/blob/main/include/rocksdb/persistent_cache.h