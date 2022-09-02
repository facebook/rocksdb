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

void SubcompactionState::FillFilesToCutForTtl() {
  if (compaction->immutable_options()->compaction_style !=
          CompactionStyle::kCompactionStyleLevel ||
      compaction->immutable_options()->compaction_pri !=
          CompactionPri::kMinOverlappingRatio ||
      compaction->mutable_cf_options()->ttl == 0 ||
      compaction->num_input_levels() < 2 || compaction->bottommost_level()) {
    return;
  }

  // We define new file with the oldest ancestor time to be younger than 1/4
  // TTL, and an old one to be older than 1/2 TTL time.
  int64_t temp_current_time;
  auto get_time_status = compaction->immutable_options()->clock->GetCurrentTime(
      &temp_current_time);
  if (!get_time_status.ok()) {
    return;
  }
  auto current_time = static_cast<uint64_t>(temp_current_time);
  if (current_time < compaction->mutable_cf_options()->ttl) {
    return;
  }
  uint64_t old_age_thres =
      current_time - compaction->mutable_cf_options()->ttl / 2;

  const std::vector<FileMetaData*>& olevel =
      *(compaction->inputs(compaction->num_input_levels() - 1));
  for (FileMetaData* file : olevel) {
    // Worth filtering out by start and end?
    uint64_t oldest_ancester_time = file->TryGetOldestAncesterTime();
    // We put old files if they are not too small to prevent a flood
    // of small files.
    if (oldest_ancester_time < old_age_thres &&
        file->fd.GetFileSize() >
            compaction->mutable_cf_options()->target_file_size_base / 2) {
      files_to_cut_for_ttl_.push_back(file);
    }
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

bool SubcompactionState::ShouldStopBefore(const Slice& internal_key) {
  uint64_t curr_file_size = Current().GetCurrentOutputFileSize();
  const InternalKeyComparator* icmp =
      &compaction->column_family_data()->internal_comparator();

  // Invalid local_output_split_key indicates that we do not need to split
  if (local_output_split_key_ != nullptr && !is_split_) {
    // Split occurs when the next key is larger than/equal to the cursor
    if (icmp->Compare(internal_key, local_output_split_key_->Encode()) >= 0) {
      is_split_ = true;
      return true;
    }
  }

  const std::vector<FileMetaData*>& grandparents = compaction->grandparents();
  bool grandparant_file_switched = false;
  // Scan to find the earliest grandparent file that contains key.
  while (grandparent_index_ < grandparents.size() &&
         icmp->Compare(internal_key,
                       grandparents[grandparent_index_]->largest.Encode()) >
             0) {
    if (seen_key_) {
      overlapped_bytes_ += grandparents[grandparent_index_]->fd.GetFileSize();
      grandparant_file_switched = true;
    }
    assert(grandparent_index_ + 1 >= grandparents.size() ||
           icmp->Compare(
               grandparents[grandparent_index_]->largest.Encode(),
               grandparents[grandparent_index_ + 1]->smallest.Encode()) <= 0);
    grandparent_index_++;
  }
  seen_key_ = true;

  if (grandparant_file_switched &&
      overlapped_bytes_ + curr_file_size > compaction->max_compaction_bytes()) {
    // Too much overlap for current output; start new output
    overlapped_bytes_ = 0;
    return true;
  }

  if (!files_to_cut_for_ttl_.empty()) {
    if (cur_files_to_cut_for_ttl_ != -1) {
      // Previous key is inside the range of a file
      if (icmp->Compare(internal_key,
                        files_to_cut_for_ttl_[cur_files_to_cut_for_ttl_]
                            ->largest.Encode()) > 0) {
        next_files_to_cut_for_ttl_ = cur_files_to_cut_for_ttl_ + 1;
        cur_files_to_cut_for_ttl_ = -1;
        return true;
      }
    } else {
      // Look for the key position
      while (next_files_to_cut_for_ttl_ <
             static_cast<int>(files_to_cut_for_ttl_.size())) {
        if (icmp->Compare(internal_key,
                          files_to_cut_for_ttl_[next_files_to_cut_for_ttl_]
                              ->smallest.Encode()) >= 0) {
          if (icmp->Compare(internal_key,
                            files_to_cut_for_ttl_[next_files_to_cut_for_ttl_]
                                ->largest.Encode()) <= 0) {
            // With in the current file
            cur_files_to_cut_for_ttl_ = next_files_to_cut_for_ttl_;
            return true;
          }
          // Beyond the current file
          next_files_to_cut_for_ttl_++;
        } else {
          // Still fall into the gap
          break;
        }
      }
    }
  }

  return false;
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
