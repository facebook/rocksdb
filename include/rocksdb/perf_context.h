#ifndef STORAGE_ROCKSDB_INCLUDE_PERF_CONTEXT_H
#define STORAGE_ROCKSDB_INCLUDE_PERF_CONTEXT_H

#include <stdint.h>

namespace rocksdb {

enum PerfLevel {
  kDisable        = 0,  // disable perf stats
  kEnableCount    = 1,  // enable only count stats
  kEnableTime     = 2   // enable time stats too
};

// set the perf stats level
void SetPerfLevel(PerfLevel level);

// A thread local context for gathering performance counter efficiently
// and transparently.

struct PerfContext {

  void Reset(); // reset all performance counters to zero

  uint64_t user_key_comparison_count; // total number of user key comparisons
  uint64_t block_cache_hit_count;     // total number of block cache hits
  uint64_t block_read_count;          // total number of block reads (with IO)
  uint64_t block_read_byte;           // total number of bytes from block reads
  uint64_t block_read_time;           // total time spent on block reads
  uint64_t block_checksum_time;       // total time spent on block checksum
  uint64_t block_decompress_time;     // total time spent on block decompression
  // total number of internal keys skipped over during iteration (overwritten or
  // deleted, to be more specific, hidden by a put or delete of the same key)
  uint64_t internal_key_skipped_count;
  // total number of deletes skipped over during iteration
  uint64_t internal_delete_skipped_count;
  uint64_t wal_write_time;            // total time spent on writing to WAL
};

extern __thread PerfContext perf_context;

}

#endif
