This document lists users of RocksDB and their use cases. If you are using RocksDB, please open a pull request and add yourself to the list.

## Facebook
At Facebook, we use RocksDB as storage engines in multiple data management services and a backend for many different stateful services, including:

1. MyRocks -- https://github.com/MySQLOnRocksDB/mysql-5.6
2. MongoRocks -- https://github.com/mongodb-partners/mongo-rocks
3. ZippyDB --  Facebook's distributed key-value store with Paxos-style replication, built on top of RocksDB.[1] https://www.youtube.com/watch?v=DfiN7pG0D0khtt
4. Laser -- Laser is a high query throughput, low (millisecond) latency, key-value storage service built on top of RocksDB.[1]
4. Dragon -- a distributed graph query engine. https://code.facebook.com/posts/1737605303120405/dragon-a-distributed-graph-query-engine/
5. Stylus -- a low-level stream processing framework writtenin C++.[1]
6. LogDevice -- a distributed data store for logs [2]

[1] https://research.facebook.com/publications/realtime-data-processing-at-facebook/

[2] https://code.facebook.com/posts/357056558062811/logdevice-a-distributed-data-store-for-logs/

## LinkedIn
Two different use cases at Linkedin are using RocksDB as a storage engine:

1. LinkedIn's follow feed for storing user's activities. Check out the blog post: https://engineering.linkedin.com/blog/2016/03/followfeed--linkedin-s-feed-made-faster-and-smarter
2. Apache Samza, open source framework for stream processing

Learn more about those use cases in a Tech Talk by Ankit Gupta and Naveen Somasundaram: http://www.youtube.com/watch?v=plqVp_OnSzg

## Yahoo
Yahoo is using RocksDB as a storage engine for their biggest distributed data store Sherpa. Learn more about it here: http://yahooeng.tumblr.com/post/120730204806/sherpa-scales-new-heights

## Baidu
[Apache Doris](http://doris.apache.org/master/en/) is a MPP analytical database engine released by Baidu. It [uses RocksDB](http://doris.apache.org/master/en/administrator-guide/operation/tablet-meta-tool.html) to manage its tablet's metadata.

## CockroachDB
CockroachDB is an open-source geo-replicated transactional database. They are using RocksDB as their storage engine. Check out their github: https://github.com/cockroachdb/cockroach

## DNANexus
DNANexus is using RocksDB to speed up processing of genomics data.
You can learn more from this great blog post by Mike Lin: http://devblog.dnanexus.com/faster-bam-sorting-with-samtools-and-rocksdb/

## Iron.io
Iron.io is using RocksDB as a storage engine for their distributed queueing system.
Learn more from Tech Talk by Reed Allman: http://www.youtube.com/watch?v=HTjt6oj-RL4

## Tango Me
Tango is using RocksDB as a graph storage to store all users' connection data and other social activity data.

## Turn
Turn is using RocksDB as a storage layer for their key/value store, serving at peak 2.4MM QPS out of different datacenters.
Check out our RocksDB Protobuf merge operator at: https://github.com/vladb38/rocksdb_protobuf

## Santander UK/Cloudera Profession Services
Check out their blog post: http://blog.cloudera.com/blog/2015/08/inside-santanders-near-real-time-data-ingest-architecture/

## Airbnb
Airbnb is using RocksDB as a storage engine for their personalized search service. You can learn more about it here: https://www.youtube.com/watch?v=ASQ6XMtogMs

## Alluxio
[Alluxio](https://www.alluxio.io) uses RocksDB to serve and scale file system metadata to beyond 1 Billion files. The detailed design and implementation is described in this engineering blog:
https://www.alluxio.io/blog/scalable-metadata-service-in-alluxio-storing-billions-of-files/

## Pinterest
Pinterest's Object Retrieval System uses RocksDB for storage: https://www.youtube.com/watch?v=MtFEVEs_2Vo

## Smyte
[Smyte](https://www.smyte.com/) uses RocksDB as the storage layer for their core key-value storage, high-performance counters and time-windowed HyperLogLog services.

## Rakuten Marketing
[Rakuten Marketing](https://marketing.rakuten.com/) uses RocksDB as the disk cache layer for the real-time bidding service in their Performance DSP.

## VWO, Wingify
[VWO's](https://vwo.com/) Smart Code checker and URL helper uses RocksDB to store all the URLs where VWO's Smart Code is installed.

## quasardb
[quasardb](https://www.quasardb.net) is a high-performance, distributed, transactional key-value database that integrates well with in-memory analytics engines such as Apache Spark.
quasardb uses a heavily tuned RocksDB as its persistence layer.

## Netflix
[Netflix](http://techblog.netflix.com/2016/05/application-data-caching-using-ssds.html) Netflix uses RocksDB on AWS EC2 instances with local SSD drives to cache application data.

## TiKV
[TiKV](https://github.com/pingcap/tikv) is a GEO-replicated, high-performance, distributed, transactional key-value database. TiKV is powered by Rust and Raft. TiKV uses RocksDB as its persistence layer.

## Apache Flink
[Apache Flink](https://flink.apache.org/news/2016/03/08/release-1.0.0.html) uses RocksDB to store state locally on a machine.

## Dgraph
[Dgraph](https://github.com/dgraph-io/dgraph) is an open-source, scalable, distributed, low latency, high throughput Graph database .They use RocksDB to store state locally on a machine.

## Uber
[Uber](http://eng.uber.com/cherami/) uses RocksDB as a durable and scalable task queue.

## 360 Pika
[360](http://www.360.cn/) [Pika](https://github.com/Qihoo360/pika) is a nosql compatible with redis. With the huge amount of data stored, redis may suffer for a capacity bottleneck, and pika was born for solving it. It has widely been used in many companies.

## LzLabs
LzLabs is using RocksDB as a storage engine in their multi-database distributed framework to store application configuration and user data.

## ProfaneDB
[ProfaneDB](https://profanedb.gitlab.io/) is a database for Protocol Buffers, and uses RocksDB for storage. It is accessible via gRPC, and the schema is defined using directly `.proto` files.

## IOTA Foundation
 [IOTA Foundation](https://www.iota.org/) is using RocksDB in the [IOTA Reference Implementation (IRI)](https://github.com/iotaledger/iri) to store the local state of the Tangle. The Tangle is the first open-source distributed ledger powering the future of the Internet of Things.

## Avrio Project
 [Avrio Project](http://avrio-project.github.io/avrio.network/) is using RocksDB in [Avrio ](https://github.com/avrio-project/avrio) to store blocks, account balances and data and other blockchain-releated data. Avrio is a multiblockchain decentralized cryptocurrency empowering monetary transactions.

## Crux
[Crux](https://github.com/juxt/crux) is a document database that uses RocksDB for local [EAV](https://en.wikipedia.org/wiki/Entity%E2%80%93attribute%E2%80%93value_model) index storage to enable point-in-time bitemporal Datalog queries. The "unbundled" architecture uses Kafka to provide horizontal scalability.

## Nebula Graph
[Nebula Graph](https://github.com/vesoft-inc/nebula) is a distributed, scalable, lightning-fast, open source graph database capable of hosting super large scale graphs with dozens of billions of vertices (nodes) and trillions of edges, with milliseconds of latency.

## YugabyteDB
[YugabyteDB](https://www.yugabyte.com/) is an open source, high performance, distributed SQL database that uses RocksDB as its storage layer. For more information, please see https://github.com/yugabyte/yugabyte-db/.

## ArangoDB
[ArangoDB](https://www.arangodb.com/) is a native multi-model database with flexible data models for documents, graphs, and key-values, for building high performance applications using a convenient SQL-like query language or JavaScript extensions. It uses RocksDB as its sotrage engine.

