#ifndef STORAGE_ROCKSDB_INCLUDE_PERF_CONTEXT_H
#define STORAGE_ROCKSDB_INCLUDE_PERF_CONTEXT_H

#include <stdint.h>

namespace leveldb {

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
  uint64_t block_cache_hit_count;
  uint64_t block_read_count;
  uint64_t block_read_byte;
  uint64_t block_read_time;
  uint64_t block_checksum_time;
  uint64_t block_decompress_time;
  uint64_t internal_key_skipped_count;
  uint64_t internal_delete_skipped_count;
};

extern __thread PerfContext perf_context;

}


#endif
