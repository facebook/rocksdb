//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction_state.h"

namespace ROCKSDB_NAMESPACE {

Slice CompactionState::SmallestUserKey() {
  for (const auto& sub_compact_state : sub_compact_states) {
    Slice smallest = sub_compact_state.SmallestUserKey();
    if (!smallest.empty()) {
      return smallest;
    }
  }
  // If there is no finished output, return an empty slice.
  return Slice{nullptr, 0};
}

Slice CompactionState::LargestUserKey() {
  for (auto it = sub_compact_states.rbegin(); it < sub_compact_states.rend();
       ++it) {
    Slice largest = it->LargestUserKey();
    if (!largest.empty()) {
      return largest;
    }
  }
  // If there is no finished output, return an empty slice.
  return Slice{nullptr, 0};
}

void CompactionState::AggregateCompactionStats(
    InternalStats::CompactionStatsFull& compaction_stats,
    CompactionJobStats& compaction_job_stats) {
  for (const auto& sc : sub_compact_states) {
    sc.AggregateCompactionStats(compaction_stats);
    compaction_job_stats.Add(sc.compaction_job_stats);
  }
}
}  // namespace ROCKSDB_NAMESPACE
