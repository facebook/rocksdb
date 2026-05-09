---
title: IO Activity Tagging
layout: post
author: hx235
category: blog
---

## Context

RocksDB performs a variety of IO operations—user reads, background compactions, flushes, database opens, and verification tasks. Treating all these operations the same makes it difficult for file system implementers to optimize performance, prioritize latency-sensitive IOs, and diagnose bottlenecks. To solve that, RocksDB internally tags every IO operation with its activity type using the `IOActivity` enum. This automatic tagging provides precise context for each IO, enabling file systems to make smarter, context-aware decisions for scheduling, caching, and resource management.

## How Internal IO Tagging Works
RocksDB automatically assigns an `IOActivity` tag to each IO operation. This tag is propagated through the storage stack and included in the IO options passed to the file system.

```cpp
enum class IOActivity : uint8_t {
    kFlush = 0,                        // IO for flush operations (background write)
    kCompaction = 1,                   // IO for compaction (background read/write)
    kDBOpen = 2,                       // IO during database open (read/write)
    kGet = 3,                          // User Get() read
    kMultiGet = 4,                     // User MultiGet() read
    kDBIterator = 5,                   // User iterator read
    kVerifyDBChecksum = 6,             // Verification: DB checksum
    kVerifyFileChecksums = 7,          // Verification: file checksums
    kGetEntity = 8,                    // Entity Get (e.g., wide-column)
    kMultiGetEntity = 9,               // Entity MultiGet
    kGetFileChecksumsFromCurrentManifest = 10, // Manifest checksum reads
    // 0x80–0xFE: Reserved for custom/internal use
    kUnknown = 0xFF                    // Unknown/unspecified activity
};
```

## Access IO Tag in File System
Custom file systems can access the IOActivity tag via the IO options structure provided by RocksDB. This allows them to optimize behavior based on the specific IO activity.

```cpp
Status CustomFileSystem::Append(uint64_t offset, const Slice& data, const IOOptions& io_opts, ...) {
    switch (io_opts.io_activity) {
        case Env::IOActivity::kGet:
            // Prioritize or cache user reads
            break;
        case Env::IOActivity::kCompaction:
            // Throttle or deprioritize background compaction IO
            break;
        case Env::IOActivity::kDBOpen:
            // Track or optimize DB open IO
            break;
        // ... handle other activities ...
        default:
            // Default handling
            break;
    }
}
```
## IO Activity Statistics in RocksDB
RocksDB provides detailed histograms for IO activities, allowing you to analyze both the aggregate time spent (in microseconds) and the count of IOs for each activity type.
```cpp
// Read Histograms
FILE_READ_FLUSH_MICROS
FILE_READ_COMPACTION_MICROS
FILE_READ_DB_OPEN_MICROS
FILE_READ_GET_MICROS
FILE_READ_MULTIGET_MICROS
FILE_READ_DB_ITERATOR_MICROS
FILE_READ_VERIFY_DB_CHECKSUM_MICROS
FILE_READ_VERIFY_FILE_CHECKSUMS_MICROS

// Write Histograms
FILE_WRITE_FLUSH_MICROS
FILE_WRITE_COMPACTION_MICROS
FILE_WRITE_DB_OPEN_MICROS
```

Thanks to Maciej Szeszko and Andrew Chang from the RocksDB team for their contributions in expanding and maintaining the IOActivity enum.
