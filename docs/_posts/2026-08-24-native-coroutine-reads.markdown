---
title: "Native Async/Coroutine Reads in RocksDB"
layout: post
author: joshkang97
category: blog
---

A point lookup that misses RocksDB's block cache can spend most of its time waiting for storage. The traditional way to keep more reads in flight is to add threads. That works, but each outstanding read parks a thread, carries a stack, and adds context-switching overhead.

RocksDB now has experimental asynchronous `Get` and `MultiGet` APIs backed by native C++ coroutines. When a read reaches storage, RocksDB can suspend the request, let its read-executor worker run another ready task, and resume the request when the filesystem reports completion. A small executor can therefore maintain more storage queue depth without requiring one blocked application thread per read.

These APIs are available in RocksDB 11.10.0.

This is primarily a throughput feature for I/O-bound point lookups. It does not make an individual device read faster. Its benefit comes from keeping the device busy and using CPU threads for runnable work.

## The API surface

RocksDB exposes the new read path through two public interfaces:

* `DB::GetAsync` and `DB::MultiGetAsync` return immediately on the native path and report completion through `AsyncCallback::OnComplete`.
* `CoroDB::CoGet` and `CoroDB::CoMultiGet` return lazy `folly::coro::Task` objects. `CoGet` produces a `Status`; `CoMultiGet` fills the same per-key values and statuses as synchronous `MultiGet`.

The callback APIs suit applications that do not expose Folly tasks at their boundaries. The `CoroDB` APIs let coroutine-based callers await RocksDB directly, avoiding an application-side callback-to-`Baton` adapter and its extra completion handoff.

Native execution requires RocksDB to be built with Folly and `USE_COROUTINES=1`.

Neither interface requires `ReadOptions::async_io`. That flag continues to control the older internal async-I/O optimizations for synchronous `MultiGet` and iterators.

The task APIs are lazy: no read begins until a task is awaited or started. Both interfaces take pointer and reference parameters, so keep the DB, column-family handles, `ReadOptions`, keys and their backing storage, output objects, and callback when applicable alive until completion. The current APIs do not provide a handle for cancelling a submitted read; outstanding filesystem requests are driven to completion.

## Two meanings of asynchronous

RocksDB has used coroutines and async I/O internally before. The [2022 asynchronous I/O work](https://rocksdb.org/blog/2022/10/07/asynchronous-io-in-rocksdb.html) lets a synchronous `MultiGet` overlap reads from multiple SST files and lets iterators prefetch in the background. The caller still waits inside the synchronous API until the whole operation finishes.

The new APIs make the operation itself suspendable:

| Interface | Caller contract | Where concurrency comes from |
| --- | --- | --- |
| `Get` / `MultiGet` | Returns the completed result | More calling threads |
| `ReadOptions::async_io` with synchronous `MultiGet` | Caller still waits | Overlapping child I/O inside one call |
| `GetAsync` / `MultiGetAsync` | Completes through a callback, which can be inline on fallback | Requests scheduled on RocksDB's read executor |
| `CoroDB::CoGet` / `CoMultiGet` | Returns a lazy Folly task that can be awaited | Suspended tasks multiplexed on the read executor |

The distinction matters. An async wrapper can free the caller while still blocking a worker in the filesystem. Conversely, internal async I/O can overlap storage requests while the public API remains synchronous. The full benefit requires suspendability at both layers.

<div style="overflow-x: auto; margin-left: auto; margin-right: auto; width: 100%;">
  <a href="/static/images/native-coroutine-reads/sync-vs-coroutine.svg">
    <img src="/static/images/native-coroutine-reads/sync-vs-coroutine.svg" alt="Three stacked timelines comparing two-block MultiGet requests from one caller thread" style="display: block; min-width: 900px; width: 100%;" />
  </a>
</div>

*Each illustrated request fetches one cache-missed block from each of two eligible SST files. `ReadOptions::async_io` overlaps those reads inside one synchronous `MultiGet`, but the caller cannot start B until A returns. In the third row, the caller already runs on a RocksDB read-executor `EventBase`: it dispatches A and B, then the event loop starts their block reads and later invokes both callbacks.*
{: style="text-align: center"}

## What happens on an SST cache miss

A single native `Get` follows the normal RocksDB lookup path through the memtable, Version, table cache, block-based table reader, and random-access file reader. Most of that work is ordinary synchronous CPU work. The new suspension point is at the filesystem boundary:

1. `CoGet` discovers the concrete DB's coroutine capability and binds the inner read to a read-executor `EventBase`.
2. RocksDB checks the memtable and caches. On an SST block miss, it submits the storage read and suspends the database coroutine.
3. While storage is outstanding, that read no longer occupies the executor. The event loop is free to accept and run another ready request.
4. When the filesystem reports completion, RocksDB posts the suspended read back to the same read-executor `EventBase` thread that submitted the I/O.
5. RocksDB verifies and decodes the block, populates the cache and user output, and completes the inner task.
6. The awaiting caller continues on its own executor after the inner task completes.

The database coroutine itself does not migrate between read-executor threads: its pre-I/O and post-I/O slices run on the same selected `EventBase` thread. This affinity avoids an extra cross-thread handoff and its context-switch cost when the read completes. Returning the completed result to the awaiting caller is a separate executor boundary.

<div style="overflow-x: auto; margin-left: auto; margin-right: auto; width: 100%;">
  <a href="/static/images/native-coroutine-reads/thread-handoff.svg">
    <img src="/static/images/native-coroutine-reads/thread-handoff.svg" alt="Top-to-bottom sequence showing SubmitReadAsync, coroutine suspension, storage completion, and resumption" style="display: block; min-width: 960px; width: 100%;" />
  </a>
</div>

*After `SubmitReadAsync` returns, the database coroutine suspends and the read executor can accept another request. The filesystem completion makes the read runnable again, and RocksDB resumes it to verify and decode the block.*
{: style="text-align: center"}

The filesystem callback makes the suspended read runnable by posting back to its selected event loop. The same `EventBase` thread that submitted the read performs the post-I/O RocksDB work.

The callback API has a different final handoff. On the native async path, `AsyncCallback::OnComplete` runs inside the detached task on the RocksDB read executor. If there is no coroutine capability or no read executor, RocksDB uses the synchronous API and may invoke `OnComplete` inline before `GetAsync` returns. Callback users must be correct in both cases and must not start another async read from `OnComplete`.

For `MultiGet`, RocksDB additionally creates child tasks for eligible, non-overlapping SST files within a level and awaits them together. This is logical concurrency on an `EventBase`, not one OS thread per SST. Each child can submit one or more block reads, suspend, and let other children or requests run.

## Filesystem integration

Filesystem implementers support two related paths:

* Synchronous `MultiGet` with `ReadOptions::async_io` uses the existing `FSRandomAccessFile::ReadAsync`, `FileSystem::Poll`, and `FileSystem::AbortIO` APIs. The filesystem also advertises `FSSupportedOps::kAsyncIO`.
* Native `GetAsync`, `MultiGetAsync`, `CoGet`, and `CoMultiGet` additionally require `FileSystem::GetReadExecutor` and `FSRandomAccessFile::SubmitReadAsync`. `SetReadIOExecutorThreads` lets RocksDB apply `DBOptions::read_io_executor_threads` to that executor.

The event-loop-backed `IOExecutor` is necessary because an I/O completion only makes a suspended coroutine ready; it does not resume the coroutine by itself. RocksDB runs each read coroutine on one of the executor's `EventBase`s. After the filesystem invokes the `SubmitReadAsync` callback, RocksDB posts the continuation back to that same event loop. Because an `EventBase` is thread-affine, the coroutine resumes on the thread that submitted the read, avoiding an additional cross-thread context switch.

`SubmitReadAsync` must populate the `FSReadRequest` and invoke its callback exactly once. It returns `true` for a non-blocking submission and `false` when it used the synchronous fallback. If `GetReadExecutor` returns null, the native APIs use their outer synchronous fallback instead.

On supported Linux builds, the default POSIX filesystem supplies these hooks with a Folly `IOExecutor`. Each read-executor `EventBase` attempts to use `folly::IoUringBackend`, which submits and receives completions through io_uring.

## Statistics across suspension

`PerfContext` and `IOStatsContext` traditionally live in thread-local storage (TLS), which works while one operation owns a thread from entry to return. A coroutine read stays on one `EventBase` thread, but it does not own that thread while suspended: request B can reuse the same worker and its TLS before request A resumes.

RocksDB gives each coroutine read its own `PerfContext` and `IOStatsContext`. A Folly `RequestContext` loads those contexts into TLS only while that request is active, saves them before suspension, and restores them on resumption. It also carries the submitting thread's statistics configuration.

Collecting and transferring these request-local statistics adds request-context and TLS bookkeeping. Applications that do not consume them should disable both `PerfContext` and `IOStatsContext` on every read-executor thread for optimal performance.

<div style="overflow-x: auto; margin-left: auto; margin-right: auto; width: 100%;">
  <a href="/static/images/native-coroutine-reads/tls-across-suspension.svg">
    <img src="/static/images/native-coroutine-reads/tls-across-suspension.svg" alt="Per-request statistics and CPU accounting survive coroutine interleaving" style="display: block; min-width: 900px; width: 100%;" />
  </a>
</div>

*Request contexts prevent two reads interleaved on one event-loop worker from sharing counters. CPU time is accumulated only during the slices in which that request is running.*
{: style="text-align: center"}

### Wall-clock timers

Suspension does not pause RocksDB's ordinary latency timers. A timer's start point remains in the coroutine frame while the request context saves its counters. After resumption, RocksDB restores those counters and the timer records its full start-to-finish duration. Consequently `PerfContext::get_from_output_files_time`, `PerfContext::block_read_time`, and `IOStatsContext::read_nanos` include storage wait.

### CPU time

Wall time and CPU time need different treatment around a suspension. End-to-end wall time should include the device wait. A CPU timer left running across `co_await`, however, could charge request A for time spent running request B on the same event-loop worker.

At `PerfLevel::kEnableTimeAndCPUTimeExceptForMutex`, RocksDB starts the thread CPU clock when a request's context is installed and stops it before that context is removed. `PerfContext::get_cpu_nanos` is the sum of those active slices. It excludes time waiting for I/O, CPU consumed by another coroutine between A's slices, and work performed later in `OnComplete`.

Some narrower CPU timers that rely on a synchronous scope cannot safely span asynchronous I/O. In particular, `PerfContext::block_read_cpu_time` and `IOStatsContext::cpu_read_nanos` are not supported for async reads.

## Why not increase application threads?

More application threads can also keep more reads in flight, but each cache miss occupies a thread until storage responds. Raising the thread count therefore grows per-thread stack memory, scheduler work, and context switching even though the parked threads are not consuming CPU while they wait. At high concurrency, those costs compete with the CPU work needed to process completed reads.

Thread pools are also difficult to resize dynamically. The useful count changes with device latency, cache-hit ratio, request rate, and the amount of CPU work after each read. Increasing a pool under load can create a burst of runnable work and contention; reducing it safely requires waiting for workers and their in-flight requests to drain. A fixed count chosen for the worst case wastes resources during ordinary operation, while a smaller count can leave storage queue depth unused during a latency spike.

Coroutines separate **concurrency** from **threads**. The application can vary the number of in-flight tasks without continually resizing its OS-thread pool. Suspended reads retain their coroutine frames, while a small read executor stays focused on runnable work. Coroutines still require backpressure, but that limit can describe outstanding requests instead of parked threads.

## Performance

I generated an 11 GB database with 11 million 16-byte keys and 1024-byte values. Compression was disabled, the block size was 4 KiB, reads used direct I/O, and the RocksDB block cache was disabled.

I compared 4, 20, 40, and 60 concurrent tasks on four pinned cores. In the synchronous version, each task is an OS thread. In the coroutine version, the same number of coroutines runs on a four-thread read executor. I tested `MultiGet` batches of 1, 2, 4, and 8 keys; results are key lookups per second.

The coroutine columns include the throughput change relative to the matching synchronous result:

| Tasks | Batch 1 sync | Batch 1 coro | Batch 2 sync | Batch 2 coro | Batch 4 sync | Batch 4 coro | Batch 8 sync | Batch 8 coro |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | 29,198 | 25,199 (-14%) | 30,087 | 50,262 (+67%) | 29,140 | 81,288 (+179%) | 26,019 | 105,366 (+305%) |
| 20 | 75,094 | 97,137 (+29%) | 85,481 | 127,620 (+49%) | 80,865 | 148,247 (+83%) | 85,569 | 174,112 (+103%) |
| 40 | 90,827 | 134,714 (+48%) | 94,392 | 162,181 (+72%) | 92,818 | 166,160 (+79%) | 90,856 | 163,306 (+80%) |
| 60 | 88,461 | 147,062 (+66%) | 89,034 | 170,045 (+91%) | 75,060 | 178,424 (+138%) | 86,409 | 177,344 (+105%) |

For the batch-1 synchronous workload, an OS thread cannot use its core while blocked on I/O. Adding more OS threads overlaps those waits and increases CPU utilization, but also increases context switching.

| Tasks | Average CPU cores | Four-core CPU utilization | Context switches/second |
| ---: | ---: | ---: | ---: |
| 4 | 1.03 | 25.9% | 61,192 |
| 20 | 3.72 | 93.0% | 210,756 |
| 40 | 3.77 | 94.3% | 212,801 |
| 60 | 3.81 | 95.2% | 211,355 |

With four threads on four cores, the workload uses only 25.9% of the available CPU. Multiplexing 20 or more threads on the same cores raises utilization above 93%, while increasing context switches from about 61,000 to 211,000 per second.

When there is no extra I/O parallelism to exploit, as with four batch-1 tasks, the synchronous version is faster because it avoids coroutine overhead. Larger batches let `CoMultiGet` expose more concurrent reads, so the coroutine path overtakes the synchronous version. At higher concurrency the gains flatten as the workload approaches saturating the CPUs.

### Async I/O comparison

I also compared synchronous `MultiGet` with `ReadOptions::async_io` against `CoMultiGet`. Async I/O does not support direct I/O, so this comparison used buffered reads and cleared the Linux page cache before every run. The RocksDB block cache was also disabled.

| Tasks | Batch 1 async I/O | Batch 1 coro | Batch 2 async I/O | Batch 2 coro | Batch 4 async I/O | Batch 4 coro | Batch 8 async I/O | Batch 8 coro |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | 35,598 | 34,455 (-3.2%) | 59,121 | 58,214 (-1.5%) | 90,923 | 87,929 (-3.3%) | 133,098 | 116,565 (-12.4%) |
| 20 | 108,945 | 120,807 (+10.9%) | 123,399 | 140,163 (+13.6%) | 134,297 | 148,535 (+10.6%) | 154,842 | 166,865 (+7.8%) |
| 40 | 116,002 | 144,819 (+24.8%) | 125,451 | 151,117 (+20.5%) | 136,350 | 190,499 (+39.7%) | 142,375 | 188,398 (+32.3%) |
| 60 | 113,170 | 154,815 (+36.8%) | 120,759 | 186,808 (+54.7%) | 129,513 | 195,746 (+51.1%) | 145,191 | 200,781 (+38.3%) |
