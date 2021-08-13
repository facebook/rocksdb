#include "rocksdb/perf_flag.h"

namespace ROCKSDB_NAMESPACE {

#ifdef ROCKSDB_SUPPORT_THREAD_LOCAL
__thread uint8_t perf_flags[FLAGS_LEN] = {0};
#else
uint8_t perf_flags[FLAGS_LEN] = {0};
#endif

#define GET_FLAG(flag) perf_flags[(uint64_t)(flag) >> 3]

void EnablePerfFlag(uint64_t flag) {
  if (!CheckPerfFlag(flag)) {
    // & 0b111 means find the flag location is a alternative way to do mod
    // operation
    GET_FLAG(flag) ^= (uint64_t)0b1 << ((uint64_t)flag & (uint64_t)0b111);
  }
}

void DisablePerfFlag(uint64_t flag) {
  if (CheckPerfFlag(flag)) {
    GET_FLAG(flag) ^= (uint64_t)0b1 << ((uint64_t)flag & (uint64_t)0b111);
  }
}

bool CheckPerfFlag(uint64_t flag) {
  return ((uint64_t)GET_FLAG(flag) & (uint64_t)0b1
                                         << (flag & (uint64_t)0b111)) != 0;
}

}  // namespace ROCKSDB_NAMESPACE
