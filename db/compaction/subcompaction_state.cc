//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/subcompaction_state.h"

#include "rocksdb/sst_partitioner.h"

namespace ROCKSDB_NAMESPACE {
void SubcompactionState::AggregateCompactionStats(
    InternalStats::CompactionStatsFull& compaction_stats) const {
  compaction_stats.stats.Add(compaction_outputs_.stats_);
  if (HasPenultimateLevelOutputs()) {
    compaction_stats.has_penultimate_level_output = true;
    compaction_stats.penultimate_level_stats.Add(
        penultimate_level_outputs_.stats_);
  }
}

OutputIterator SubcompactionState::GetOutputs() const {
  return OutputIterator(penultimate_level_outputs_.outputs_,
                        compaction_outputs_.outputs_);
}

void SubcompactionState::Cleanup(Cache* cache) {
  penultimate_level_outputs_.Cleanup();
  compaction_outputs_.Cleanup();

  if (!status.ok()) {
    for (const auto& out : GetOutputs()) {
      // If this file was inserted into the table cache then remove
      // them here because this compaction was not committed.
      TableCache::Evict(cache, out.meta.fd.GetNumber());
    }
  }
  // TODO: sub_compact.io_status is not checked like status. Not sure if thats
  // intentional. So ignoring the io_status as of now.
  io_status.PermitUncheckedError();
}

Slice SubcompactionState::SmallestUserKey() const {
  if (has_penultimate_level_outputs_) {
    Slice a = compaction_outputs_.SmallestUserKey();
    Slice b = penultimate_level_outputs_.SmallestUserKey();
    if (a.empty()) {
      return b;
    }
    if (b.empty()) {
      return a;
    }
    const Comparator* user_cmp =
        compaction->column_family_data()->user_comparator();
    if (user_cmp->Compare(a, b) > 0) {
      return b;
    } else {
      return a;
    }
  } else {
    return compaction_outputs_.SmallestUserKey();
  }
}

Slice SubcompactionState::LargestUserKey() const {
  if (has_penultimate_level_outputs_) {
    Slice a = compaction_outputs_.LargestUserKey();
    Slice b = penultimate_level_outputs_.LargestUserKey();
    if (a.empty()) {
      return b;
    }
    if (b.empty()) {
      return a;
    }
    const Comparator* user_cmp =
        compaction->column_family_data()->user_comparator();
    if (user_cmp->Compare(a, b) < 0) {
      return b;
    } else {
      return a;
    }
  } else {
    return compaction_outputs_.LargestUserKey();
  }
}

Status SubcompactionState::AddToOutput(
    const CompactionIterator& iter,
    const CompactionFileOpenFunc& open_file_func,
    const CompactionFileCloseFunc& close_file_func) {
  // update target output first
  is_current_penultimate_level_ = iter.output_to_penultimate_level();
  current_outputs_ = is_current_penultimate_level_ ? &penultimate_level_outputs_
                                                   : &compaction_outputs_;
  if (is_current_penultimate_level_) {
    has_penultimate_level_outputs_ = true;
  }

  return Current().AddToOutput(iter, open_file_func, close_file_func);
}

}  // namespace ROCKSDB_NAMESPACE
