#include "rocksdb/perf_context.h"


namespace leveldb {

void PerfContext::Reset() {
  user_key_comparison_count = 0;
}

__thread PerfContext perf_context;

}
