//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include <sstream>

#include "monitoring/perf_context_imp.h"

namespace ROCKSDB_NAMESPACE {

#if defined(NPERF_CONTEXT)
// Should not be used because the counters are not thread-safe.
// Put here just to make get_perf_context() simple without ifdef.
PerfContext perf_context;
#else
thread_local PerfContext perf_context;
#endif

PerfContext* get_perf_context() { return &perf_context; }

PerfContext::~PerfContext() {
#if !defined(NPERF_CONTEXT) && !defined(OS_SOLARIS)
  ClearPerLevelPerfContext();
#endif
}

PerfContext::PerfContext(const PerfContext& other) {
#ifdef NPERF_CONTEXT
  (void)other;
#else
  copyMetrics(&other);
#endif
}

PerfContext::PerfContext(PerfContext&& other) noexcept {
#ifdef NPERF_CONTEXT
  (void)other;
#else
  copyMetrics(&other);
#endif
}

PerfContext& PerfContext::operator=(const PerfContext& other) {
#ifdef NPERF_CONTEXT
  (void)other;
#else
  copyMetrics(&other);
#endif
  return *this;
}

void PerfContext::copyMetrics(const PerfContext* other) noexcept {
#ifdef NPERF_CONTEXT
  (void)other;
#else
#define EMIT_COPY_FIELDS(x) x = other->x;
  DEF_PERF_CONTEXT_METRICS(EMIT_COPY_FIELDS)
#undef EMIT_COPY_FIELDS
  if (per_level_perf_context_enabled && level_to_perf_context != nullptr) {
    ClearPerLevelPerfContext();
  }
  if (other->level_to_perf_context != nullptr) {
    level_to_perf_context = new std::map<uint32_t, PerfContextByLevel>();
    *level_to_perf_context = *other->level_to_perf_context;
  }
  per_level_perf_context_enabled = other->per_level_perf_context_enabled;
#endif
}

void PerfContext::Reset() {
#ifndef NPERF_CONTEXT
#define EMIT_FIELDS(x) x = 0;
  DEF_PERF_CONTEXT_METRICS(EMIT_FIELDS)
#undef EMIT_FIELDS
  if (per_level_perf_context_enabled && level_to_perf_context) {
    for (auto& kv : *level_to_perf_context) {
      kv.second.Reset();
    }
  }
#endif
}

void PerfContextByLevel::Reset() {
#ifndef NPERF_CONTEXT
#define EMIT_FIELDS(x) x = 0;
  DEF_PERF_CONTEXT_LEVEL_METRICS(EMIT_FIELDS)
#undef EMIT_FIELDS
#endif
}

std::string PerfContext::ToString(bool exclude_zero_counters) const {
#ifdef NPERF_CONTEXT
  (void)exclude_zero_counters;
  return "";
#else
  std::ostringstream ss;
#define PERF_CONTEXT_OUTPUT(counter)             \
  if (!exclude_zero_counters || (counter > 0)) { \
    ss << #counter << " = " << counter << ", ";  \
  }
  DEF_PERF_CONTEXT_METRICS(PERF_CONTEXT_OUTPUT)
#undef PERF_CONTEXT_OUTPUT
  if (per_level_perf_context_enabled && level_to_perf_context) {
#define PERF_CONTEXT_BY_LEVEL_OUTPUT_ONE_COUNTER(counter)      \
  ss << #counter << " = ";                                     \
  for (auto& kv : *level_to_perf_context) {                    \
    if (!exclude_zero_counters || (kv.second.counter > 0)) {   \
      ss << kv.second.counter << "@level" << kv.first << ", "; \
    }                                                          \
  }
    DEF_PERF_CONTEXT_LEVEL_METRICS(PERF_CONTEXT_BY_LEVEL_OUTPUT_ONE_COUNTER)
#undef PERF_CONTEXT_BY_LEVEL_OUTPUT_ONE_COUNTER
  }
  std::string str = ss.str();
  str.erase(str.find_last_not_of(", ") + 1);
  return str;
#endif
}

void PerfContext::EnablePerLevelPerfContext() {
  if (level_to_perf_context == nullptr) {
    level_to_perf_context = new std::map<uint32_t, PerfContextByLevel>();
  }
  per_level_perf_context_enabled = true;
}

void PerfContext::DisablePerLevelPerfContext() {
  per_level_perf_context_enabled = false;
}

void PerfContext::ClearPerLevelPerfContext() {
  if (level_to_perf_context != nullptr) {
    level_to_perf_context->clear();
    delete level_to_perf_context;
    level_to_perf_context = nullptr;
  }
  per_level_perf_context_enabled = false;
}

}  // namespace ROCKSDB_NAMESPACE
