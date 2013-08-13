#ifndef STORAGE_LEVELDB_INCLUDE_PERF_CONTEXT_H
#define STORAGE_LEVELDB_INCLUDE_PERF_CONTEXT_H

#include <stdint.h>

namespace leveldb {

// A thread local context for gathering performance counter efficiently
// and transparently.

struct PerfContext {

  void Reset(); // reset all performance counters to zero


  uint64_t user_key_comparison_count; // total number of user key comparisons
};

extern __thread PerfContext perf_context;

}


#endif
