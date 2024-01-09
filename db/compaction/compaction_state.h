//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "db/compaction/compaction.h"
#include "db/compaction/subcompaction_state.h"
#include "db/internal_stats.h"

// Data structures used for compaction_job and compaction_service_job which has
// the list of sub_compact_states and the aggregated information for the
// compaction.
namespace ROCKSDB_NAMESPACE {

// Maintains state for the entire compaction
class CompactionState {
 public:
  Compaction* const compaction;

  // REQUIRED: subcompaction states are stored in order of increasing key-range
  std::vector<SubcompactionState> sub_compact_states;
  Status status;

  void AggregateCompactionStats(
      InternalStats::CompactionStatsFull& compaction_stats,
      CompactionJobStats& compaction_job_stats);

  explicit CompactionState(Compaction* c) : compaction(c) {}

  Slice SmallestUserKey();

  Slice LargestUserKey();
};

}  // namespace ROCKSDB_NAMESPACE
