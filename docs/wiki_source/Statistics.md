DB Statistics provides cumulative stats over time. It serves different function from DB properties and [[perf and IO Stats context|Perf Context And IO Stats Context]]: statistics accumulate stats for history, while DB properties report current state of the database; DB statistics give an aggregated view across all operations, whereas perf and IO stats context allow us to look inside of individual operations.  

## Usage
Function `CreateDBStatistics()` creates a statistics object. 

Here is an example to pass it to one DB:

```
Options options;
options.statistics = rocksdb::CreateDBStatistics();
```
Technically, you can create a statistics object and pass to multiple DBs. Then the statistics object will contain aggregated values for all those DBs. Note that some stats are undefined and have no meaningful information across multiple DBs. One such statistic is "rocksdb.sequence.number".

Advanced users can implement their own statistics class. See the last section for details.

## Stats Level And Performance Costs
The overhead of statistics is usually small but non-negligible. We usually observe an overhead of 5%-10%.

Stats are implemented using atomic integers (atomic increments). Furthermore, stats measuring time duration require to calls the get the current time. Both of the atomic increment and timing functions introduce overhead, which varies across different platforms. 

We have five levels of statistics, `kExceptHistogramOrTimers`, `kExceptTimers`, `kExceptDetailedTimers`, `kExceptTimeForMutex` and `kAll`. (`kExceptHistogramOrTimers` and `kExceptTimers` will only be available since 6.1 Release)
* `kAll`: Collects all stats, including measuring duration of mutex operations. If getting time is expensive on the platform to run, it can reduce scalability to more threads, especially for writes. 
* `kExceptTimeForMutex`: Collects all stats except the counters requiring to get time inside the mutex lock. `rocksdb.db.mutex.wait.micros` counter is not measured. By measuring the counter, we call the timing function inside DB mutex. If the timing function is slow, it can reduce write throughput significantly.
* `kExceptDetailedTimers`: Collects all stats except time inside mutex lock AND time spent on compression.
* `kExceptTimers`: Excluding all timing stats.
* `kExceptHistogramOrTimers`: Excluding all timing stats, as well as histograms. Histograms are more expensive than pure counter stats, because it needs to seek to specific bucket and maintain min/max/count/std, etc. This is the most lightweight level.

## Access The Stats
#### Stats Types
There are two types of stats, ticker and histogram.

The ticker type is represented by 64-bit unsigned integer. The value never decreases or resets. Ticker stats are used to measure counters (e.g. "rocksdb.block.cache.hit"), cumulative bytes (e.g. "rocksdb.bytes.written") or time (e.g. "rocksdb.l0.slowdown.micros").

The histogram type measures distribution of a stat across all operations. Most of the histograms are for distribution of duration of a DB operation. Taking "rocksdb.db.get.micros" as an example, we measure time spent on each Get() operation and calculate the distribution for all of them.

#### Print Human Readable String
We can get a human readable string of all the counters by calling `ToString()`.

### Dump Statistics Periodically in information logs
Statistics are automatically dumped to information logs, for periodic interval of `options.stats_dump_period_sec`. Before 5.18 release, it is only dumped after a compaction, so if the database doesn't serve any write for a long time, statistics may not be dumped, despite of `options.stats_dump_period_sec`.

#### Access Stats Programmatically
We can also access specific stat directly from the statistics object. The list of ticker types can be found in enum Tickers. By calling statistics.getTickerCount() for a ticker type, we can retrieve the value. Similarly, single histogram stat can be queried by calling statistics.histogramData() with enum Histograms, or statistics.getHistogramString().

#### Stats For Time-Interval
All the statistics are cumulative since the opening of the DB. If you need to monitor or report it on time-interval basis, you can check the value periodically and compute the time interval value by taking the difference between the current value and the previous value.

## User-Defined Statistics
Statistics is an abstract class and users can implement their own class and pass it to options.statistics. This is useful when you want to integrate RocksDB's stats to your own stats system. When you implement a user-defined statistic, be aware of the volume of calls to recordTick() and measureTime() by RocksDB. The user-defined stats can easily be the performance bottleneck if not implemented carefully.