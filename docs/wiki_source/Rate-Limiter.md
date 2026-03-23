When using RocksDB, users may want to throttle the maximum write speed within a certain limit for lots of reasons. For example, flash writes cause terrible spikes in read latency if they exceed a certain threshold. Since you've been reading this site, I believe you already know why you need a rate limiter. Actually, 
RocksDB contains a native [RateLimiter](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/rate_limiter.h) which should be adequate for most use cases. 

## How to use
Create a RateLimiter object by calling `NewGenericRateLimiter`, which can be created separately for each RocksDB instance or by shared among RocksDB instances to control the aggregated write rate of flush and compaction.
```cpp
RateLimiter* rate_limiter = NewGenericRateLimiter(
    rate_bytes_per_sec /* int64_t */, 
    refill_period_us /* int64_t */,
    fairness /* int32_t */);
```
### Params
* `rate_bytes_per_sec`: this is the only parameter you want to set most of the time. It controls the total write rate of compaction and flush in bytes per second. Currently, RocksDB does not enforce rate limit for anything other than flush and compaction, e.g. write to WAL
* `refill_period_us`: this controls how often tokens are refilled. For example, when rate_bytes_per_sec is set to 10MB/s and refill_period_us is set to 100ms, then 1MB is refilled every 100ms internally. Larger value can lead to burst writes while smaller value introduces more CPU overhead. The default value 100,000 should work for most cases.
* `fairness`: RateLimiter accepts and queues requests of `rocksdb::Env::IO_USER`, `rocksdb::Env::IO_HIGH`, `rocksdb::Env::IO_MID`, `rocksdb::Env::IO_LOW`. `rocksdb::Env::IO_USER` always precedes other priority levels and gets processed first whenever it presents. For other priority levels, the higher a priority is, the more likely it gets processed early while the likelihood is subjected to a parameter called `fairness`. You should be good by leaving the fairness parameter at default 10.

  RocksDB specified `rocksdb::Env::IO_LOW` for internal compaction related IO and `rocksdb::Env::IO_HIGH` for internal flush related IO.

  The following is how the processing order is decided among {`rocksdb::Env::IO_HIGH`, `rocksdb::Env::IO_MID`, `rocksdb::Env::IO_LOW`} using the rate_limiter created below as an example:
  ```cpp
  RateLimiter* rate_limiter = NewGenericRateLimiter(
    200 /* rate_bytes_per_sec */, 
    1000 * 1000 /* refill_period_us */,
    10 /* fairness */);
  ```
  <img align="center" src="https://github.com/hx235/rocksdb/blob/wiki-figure/fig1.1-revised-2.jpg">

  One special note is that the result of “flipping the coin” applies to the whole priority-level queue of requests instead of per request. For example, should we start with two `Low` requests and the same coin flipping results as above, we will end up with the following final order in processing requests.

  <img align="center" src="https://github.com/hx235/rocksdb/blob/wiki-figure/fig1.2-revised.jpg">

### Maximum bytes of a single request

* Notice that although tokens are refilled with a certain interval set by `refill_period_us`, the maximum bytes that can be granted in a single request have to be bounded since we are not happy to see the following scenario:

   Suppose we have `rate_bytes_per_sec = 200` and request A from thread A asking for 1 million bytes comes slightly before request B from thread B asking for 1 byte. Suppose these two requests are of same priority so they will be enqueued to the same queue. By FIFO principle, request A will be satisfied first, taking a long time. If we don't constrain the maximum bytes, then request B will be blocked until request A is granted with all the bytes needed, while if we do constrain the maximum bytes, request A will need to make multiple requests, allowing request B to "squeeze" between these requests in the queue and get satisfied early.

   Therefore our implementation bounds the maximum bytes that can be granted in a single request by `GetSingleBurstBytes()`.

### Request token before writes

* Each time token should be requested before writes happen. If this request can not be satisfied now, the call will be blocked until tokens get refilled to fulfill the request. For example,
```cpp
// block if tokens are not enough
rate_limiter->Request(1024 /* bytes */, rocksdb::Env::IO_HIGH); 
Status s = db->Flush();
```
* Users could also dynamically change rate limiter's bytes per second with `SetBytesPerSecond()` when they need. see [include/rocksdb/rate_limiter.h](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/rate_limiter.h) for more API details.

## Customization
For the users whose requirements are beyond the functions provided by RocksDB native Ratelimiter, they can implement their own Ratelimiter by extending [include/rocksdb/rate_limiter.h](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/rate_limiter.h)
