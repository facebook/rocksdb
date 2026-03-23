# RocksDB Users and Their Use Cases

## Facebook
At Facebook, we use RocksDB as storage engines in multiple data management services and a backend for many different stateful services, including:

1. MyRocks -- https://github.com/MySQLOnRocksDB/mysql-5.6
2. MongoRocks -- https://github.com/mongodb-partners/mongo-rocks
3. ZippyDB -- Facebook's distributed key-value store with Paxos-style replication, built on top of RocksDB.[1] https://www.youtube.com/watch?v=DfiN7pG0D0khtt
4. Laser -- Laser is a high query throughput, low (millisecond) latency, key-value storage service built on top of RocksDB.[1]
5. Dragon -- a distributed graph query engine. https://code.facebook.com/posts/1737605303120405/dragon-a-distributed-graph-query-engine/
6. Stylus -- a low-level stream processing framework writtenin C++.[1]
7. LogDevice -- a distributed data store for logs [2]

[1] https://research.facebook.com/publications/realtime-data-processing-at-facebook/

[2] https://code.facebook.com/posts/357056558062811/logdevice-a-distributed-data-store-for-logs/

8. Libra -- Blockchain https://libra.org

## LinkedIn
Two different use cases at Linkedin are using RocksDB as a storage engine:
1. LinkedIn's follow feed for storing user's activities. Check out the blog post: https://engineering.linkedin.com/blog/2016/03/followfeed--linkedin-s-feed-made-faster-and-smarter
2. Apache Samza, open source framework for stream processing
Learn more about those use cases in a Tech Talk by Ankit Gupta and Naveen Somasundaram: http://www.youtube.com/watch?v=plqVp_OnSzg

## Yahoo
Yahoo is using RocksDB as a storage engine for their biggest distributed data store Sherpa. Learn more about it here: http://yahooeng.tumblr.com/post/120730204806/sherpa-scales-new-heights

## CockroachDB
CockroachDB is an open-source geo-replicated transactional database. They are using RocksDB as their storage engine. Check out their github: https://github.com/cockroachdb/cockroach

## DNANexus
DNANexus is using RocksDB to speed up processing of genomics data. You can learn more from this great blog post by Mike Lin: http://devblog.dnanexus.com/faster-bam-sorting-with-samtools-and-rocksdb/

## Iron.io
Iron.io is using RocksDB as a storage engine for their distributed queueing system. Learn more from Tech Talk by Reed Allman: http://www.youtube.com/watch?v=HTjt6oj-RL4

## Tango Me
Tango is using RocksDB as a graph storage to store all users' connection data and other social activity data.

## Turn
Turn is using RocksDB as a storage layer for their key/value store, serving at peak 2.4MM QPS out of different datacenters. Check out our RocksDB Protobuf merge operator at: https://github.com/vladb38/rocksdb_protobuf

## Santanader UK/Cloudera Profession Services
Check out their blog post: http://blog.cloudera.com/blog/2015/08/inside-santanders-near-real-time-data-ingest-architecture/

## Airbnb
Airbnb is using RocksDB as a storage engine for their personalized search service. You can learn more about it here: https://www.youtube.com/watch?v=ASQ6XMtogMs

## Alluxio
Alluxio uses RocksDB to serve and scale file system metadata to beyond 1 Billion files. The detailed design and implementation is described in this engineering blog: https://www.alluxio.io/blog/scalable-metadata-service-in-alluxio-storing-billions-of-files/

## Pinterest
Pinterest's Object Retrieval System uses RocksDB for storage: https://www.youtube.com/watch?v=MtFEVEs_2Vo

## Smyte
Smyte uses RocksDB as the storage layer for their core key-value storage, high-performance counters and time-windowed HyperLogLog services.

## Rakuten Marketing
Rakuten Marketing uses RocksDB as the disk cache layer for the real-time bidding service in their Performance DSP.

## VWO, Wingify
VWO's Smart Code checker and URL helper uses RocksDB to store all the URLs where VWO's Smart Code is installed.

## quasardb
quasardb is a high-performance, distributed, transactional key-value database that integrates well with in-memory analytics engines such as Apache Spark. quasardb uses a heavily tuned RocksDB as its persistence layer.

## Netflix
Netflix Netflix uses RocksDB on AWS EC2 instances with local SSD drives to cache application data.

## TiKV
TiKV is a GEO-replicated, high-performance, distributed, transactional key-value database. TiKV is powered by Rust and Raft. TiKV uses RocksDB as its persistence layer.

## Apache Flink
Apache Flink uses RocksDB to store state locally on a machine.

## Dgraph
Dgraph is an open-source, scalable, distributed, low latency, high throughput Graph database .They use RocksDB to store state locally on a machine.

## Uber
Uber uses RocksDB as a durable and scalable task queue.

## 360 Pika
360 Pika is a nosql compatible with redis. With the huge amount of data stored, redis may suffer for a capacity bottleneck, and pika was born for solving it. It has widely been widely used in many company

## LzLabs
LzLabs is using RocksDB as a storage engine in their multi-database distributed framework to store application configuration and user data.

## ProfaneDB
ProfaneDB is a database for Protocol Buffers, and uses RocksDB for storage. It is accessible via gRPC, and the schema is defined using directly .proto files.

## IOTA Foundation
IOTA Foundation is using RocksDB in the IOTA Reference Implementation (IRI) to store the local state of the Tangle. The Tangle is the first open-source distributed ledger powering the future of the Internet of Things.

## Avrio Project
Avrio Project is using RocksDB in Avrio to store blocks, account balances and data and other blockchain-releated data. Avrio is a multiblockchain decentralized cryptocurrency empowering monetary transactions.

## XTDB
XTDB (Formerly known as Crux) is a document database that uses RocksDB for local EAV index storage to enable point-in-time bitemporal Datalog queries. The "unbundled" architecture uses Kafka to provide horizontal scalability.

## Nebula Graph
Nebula Graph is a distributed, scalable, lightning-fast, open source graph database capable of hosting super large scale graphs with dozens of billions of vertices (nodes) and trillions of edges, with milliseconds of latency.

## Apache Hadoop Ozone
Ozone is a scalable, redundant, and distributed object store for Hadoop. Apart from scaling to billions of objects of varying sizes, Ozone can function effectively in containerized environments such as Kubernetes and YARN.
https://blog.cloudera.com/apache-hadoop-ozone-object-store-architecture/

## Apache Doris
http://doris.apache.org/master/en/administrator-guide/operation/tablet-meta-tool.html

## Apache Pegasus
Apache Pegasus is a horizontally scalable, strongly consistent and high-performance key-value store.
https://github.com/apache/incubator-pegasus