//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "db/version_edit.h"
#include "test_util/sync_point.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {
static const uint32_t kFileReadSampleRate = 1024;
static const uint32_t kFileReadNextSampleRate =
    kFileReadSampleRate * 64;  // Must be kept a power of 2

inline bool should_sample_file_read() {
  bool result = (Random::GetTLSInstance()->Next() % kFileReadSampleRate == 307);
  TEST_SYNC_POINT_CALLBACK("should_sample_file_read:override", &result);
  return result;
}

inline bool should_sample_file_read_next() {
  // Decrease probability of sampling next() to discount it as it is cheaper
  // than seek()
  thread_local uint32_t counter = 0;
  bool result = (++counter & (kFileReadNextSampleRate - 1)) == 0;
  TEST_SYNC_POINT_CALLBACK("should_sample_file_read:override", &result);
  return result;
}

inline void sample_file_read_inc(const FileMetaData* meta) {
  meta->stats.num_reads_sampled.fetch_add(kFileReadSampleRate,
                                          std::memory_order_relaxed);
}

inline void sample_collapsible_entry_file_read_inc(const FileMetaData* meta) {
  meta->stats.num_collapsible_entry_reads_sampled.fetch_add(
      kFileReadSampleRate, std::memory_order_relaxed);
}
}  // namespace ROCKSDB_NAMESPACE
