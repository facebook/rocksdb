Users sometimes need to do large amount of background write. One example is that they want to load a large amount of data. Another one is that they want to do some data migration.

The best way to handle these cases is to [[Creating and Ingesting SST files|Creating and Ingesting SST files]]. However, there may be use cases where users are not able to load in batch and have to directly write the data to the DB continuously. When doing that, they may hit this problem: if they issue background writes as fast as possible, it will trigger DB's throttling mechanism (see [[Write Stalls]]), it will slowdown not only the background writes, but the online queries by users too.

Low Priority Write can help users manage use cases like this. Since version 5.6, users can set `WriteOptions.low_pri=true` for background writes. RocksDB will do more aggressive throttling to low priority writes to make sure high priority writes won't hit stalls.

While DB is running, RocksDB will keep evaluating whether we have any compaction pressure by looking at outstanding L0 files and bytes pending compaction. If RocksDB thinks there is a compaction pressure, it will ingest artificial sleeping to low priority write so that the total rate of low priority write is small. By doing that, the total write rate will drop much earlier than the overall write throttling condition, so the qualify of service of high priority writes is more likely to be guaranteed.

In [[Two Phase Commit|Two Phase Commit Implementation]], the slowdown of low priority writes is done in the prepare phase, rather than the commit phase.