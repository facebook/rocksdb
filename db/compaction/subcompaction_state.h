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

#include "db/blob/blob_file_addition.h"
#include "db/blob/blob_garbage_meter.h"
#include "db/compaction/compaction.h"
#include "db/compaction/compaction_iterator.h"
#include "db/compaction/compaction_outputs.h"
#include "db/internal_stats.h"
#include "db/output_validator.h"
#include "db/range_del_aggregator.h"

namespace ROCKSDB_NAMESPACE {

// Maintains state and outputs for each sub-compaction
// It contains 2 `CompactionOutputs`:
//  1. one for the normal output files
//  2. another for the penultimate level outputs
// a `current` pointer maintains the current output group, when calling
// `AddToOutput()`, it checks the output of the current compaction_iterator key
// and point `current` to the target output group. By default, it just points to
// normal compaction_outputs, if the compaction_iterator key should be placed on
// the penultimate level, `current` is changed to point to
// `penultimate_level_outputs`.
// The later operations uses `Current()` to get the target group.
//
// +----------+          +-----------------------------+      +---------+
// | *current |--------> | compaction_outputs          |----->| output  |
// +----------+          +-----------------------------+      +---------+
//       |                                                    | output  |
//       |                                                    +---------+
//       |                                                    |  ...    |
//       |
//       |               +-----------------------------+      +---------+
//       +-------------> | penultimate_level_outputs   |----->| output  |
//                       +-----------------------------+      +---------+
//                                                            |  ...    |

class SubcompactionState {
 public:
  const Compaction* compaction;

  // The boundaries of the key-range this compaction is interested in. No two
  // sub-compactions may have overlapping key-ranges.
  // 'start' is inclusive, 'end' is exclusive, and nullptr means unbounded
  const Slice *start, *end;

  // The return status of this sub-compaction
  Status status;

  // The return IO Status of this sub-compaction
  IOStatus io_status;

  // Notify on sub-compaction completion only if listener was notified on
  // sub-compaction begin.
  bool notify_on_subcompaction_completion = false;

  // compaction job stats for this sub-compaction
  CompactionJobStats compaction_job_stats;

  // sub-compaction job id, which is used to identify different sub-compaction
  // within the same compaction job.
  const uint32_t sub_job_id;

  Slice SmallestUserKey() const;

  Slice LargestUserKey() const;

  // Get all outputs from the subcompaction. For per_key_placement compaction,
  // it returns both the last level outputs and penultimate level outputs.
  OutputIterator GetOutputs() const;

  // Assign range dels aggregator, for each range_del, it can only be assigned
  // to one output level, for per_key_placement, it's going to be the
  // penultimate level.
  void AssignRangeDelAggregator(
      std::unique_ptr<CompactionRangeDelAggregator>&& range_del_agg) {
    if (compaction->SupportsPerKeyPlacement()) {
      penultimate_level_outputs_.AssignRangeDelAggregator(
          std::move(range_del_agg));
    } else {
      compaction_outputs_.AssignRangeDelAggregator(std::move(range_del_agg));
    }
  }

  void RemoveLastEmptyOutput() {
    compaction_outputs_.RemoveLastEmptyOutput();
    penultimate_level_outputs_.RemoveLastEmptyOutput();
  }

#ifndef ROCKSDB_LITE
  void BuildSubcompactionJobInfo(
      SubcompactionJobInfo& subcompaction_job_info) const {
    const Compaction* c = compaction;
    const ColumnFamilyData* cfd = c->column_family_data();

    subcompaction_job_info.cf_id = cfd->GetID();
    subcompaction_job_info.cf_name = cfd->GetName();
    subcompaction_job_info.status = status;
    subcompaction_job_info.subcompaction_job_id = static_cast<int>(sub_job_id);
    subcompaction_job_info.base_input_level = c->start_level();
    subcompaction_job_info.output_level = c->output_level();
    subcompaction_job_info.stats = compaction_job_stats;
  }
#endif  // !ROCKSDB_LITE

  SubcompactionState() = delete;
  SubcompactionState(const SubcompactionState&) = delete;
  SubcompactionState& operator=(const SubcompactionState&) = delete;

  SubcompactionState(Compaction* c, Slice* _start, Slice* _end,
                     uint32_t _sub_job_id)
      : compaction(c),
        start(_start),
        end(_end),
        sub_job_id(_sub_job_id),
        compaction_outputs_(c, /*is_penultimate_level=*/false),
        penultimate_level_outputs_(c, /*is_penultimate_level=*/true) {
    assert(compaction != nullptr);
    const InternalKeyComparator* icmp =
        &compaction->column_family_data()->internal_comparator();
    const InternalKey* output_split_key = compaction->GetOutputSplitKey();
    // Invalid output_split_key indicates that we do not need to split
    if (output_split_key != nullptr) {
      // We may only split the output when the cursor is in the range. Split
      if ((end == nullptr || icmp->user_comparator()->Compare(
                                 ExtractUserKey(output_split_key->Encode()),
                                 ExtractUserKey(*end)) < 0) &&
          (start == nullptr || icmp->user_comparator()->Compare(
                                   ExtractUserKey(output_split_key->Encode()),
                                   ExtractUserKey(*start)) > 0)) {
        local_output_split_key_ = output_split_key;
      }
    }
  }

  SubcompactionState(SubcompactionState&& state) noexcept
      : compaction(state.compaction),
        start(state.start),
        end(state.end),
        status(std::move(state.status)),
        io_status(std::move(state.io_status)),
        notify_on_subcompaction_completion(
            state.notify_on_subcompaction_completion),
        compaction_job_stats(std::move(state.compaction_job_stats)),
        sub_job_id(state.sub_job_id),
        files_to_cut_for_ttl_(std::move(state.files_to_cut_for_ttl_)),
        cur_files_to_cut_for_ttl_(state.cur_files_to_cut_for_ttl_),
        next_files_to_cut_for_ttl_(state.next_files_to_cut_for_ttl_),
        grandparent_index_(state.grandparent_index_),
        overlapped_bytes_(state.overlapped_bytes_),
        seen_key_(state.seen_key_),
        compaction_outputs_(std::move(state.compaction_outputs_)),
        penultimate_level_outputs_(std::move(state.penultimate_level_outputs_)),
        is_current_penultimate_level_(state.is_current_penultimate_level_),
        has_penultimate_level_outputs_(state.has_penultimate_level_outputs_) {
    current_outputs_ = is_current_penultimate_level_
                           ? &penultimate_level_outputs_
                           : &compaction_outputs_;
  }

  bool HasPenultimateLevelOutputs() const {
    return has_penultimate_level_outputs_ ||
           penultimate_level_outputs_.HasRangeDel();
  }

  void FillFilesToCutForTtl();

  // Returns true iff we should stop building the current output
  // before processing "internal_key".
  bool ShouldStopBefore(const Slice& internal_key);

  bool IsCurrentPenultimateLevel() const {
    return is_current_penultimate_level_;
  }

  // Add all the new files from this compaction to version_edit
  void AddOutputsEdit(VersionEdit* out_edit) const {
    for (const auto& file : penultimate_level_outputs_.outputs_) {
      out_edit->AddFile(compaction->GetPenultimateLevel(), file.meta);
    }
    for (const auto& file : compaction_outputs_.outputs_) {
      out_edit->AddFile(compaction->output_level(), file.meta);
    }
  }

  void Cleanup(Cache* cache);

  void AggregateCompactionStats(
      InternalStats::CompactionStatsFull& compaction_stats) const;

  CompactionOutputs& Current() const {
    assert(current_outputs_);
    return *current_outputs_;
  }

  // Add compaction_iterator key/value to the `Current` output group.
  Status AddToOutput(const CompactionIterator& iter,
                     const CompactionFileOpenFunc& open_file_func,
                     const CompactionFileCloseFunc& close_file_func);

  // Close all compaction output files, both output_to_penultimate_level outputs
  // and normal outputs.
  Status CloseCompactionFiles(const Status& curr_status,
                              const CompactionFileOpenFunc& open_file_func,
                              const CompactionFileCloseFunc& close_file_func) {
    // Call FinishCompactionOutputFile() even if status is not ok: it needs to
    // close the output file.
    Status s = penultimate_level_outputs_.CloseOutput(
        curr_status, open_file_func, close_file_func);
    s = compaction_outputs_.CloseOutput(s, open_file_func, close_file_func);
    return s;
  }

 private:
  // Some identified files with old oldest ancester time and the range should be
  // isolated out so that the output file(s) in that range can be merged down
  // for TTL and clear the timestamps for the range.
  std::vector<FileMetaData*> files_to_cut_for_ttl_;
  int cur_files_to_cut_for_ttl_ = -1;
  int next_files_to_cut_for_ttl_ = 0;

  // An index that used to speed up ShouldStopBefore().
  size_t grandparent_index_ = 0;
  // The number of bytes overlapping between the current output and
  // grandparent files used in ShouldStopBefore().
  uint64_t overlapped_bytes_ = 0;
  // A flag determines whether the key has been seen in ShouldStopBefore()
  bool seen_key_ = false;

  // A flag determines if this subcompaction has been split by the cursor
  bool is_split_ = false;

  // We also maintain the output split key for each subcompaction to avoid
  // repetitive comparison in ShouldStopBefore()
  const InternalKey* local_output_split_key_ = nullptr;

  // State kept for output being generated
  CompactionOutputs compaction_outputs_;
  CompactionOutputs penultimate_level_outputs_;
  CompactionOutputs* current_outputs_ = &compaction_outputs_;
  bool is_current_penultimate_level_ = false;
  bool has_penultimate_level_outputs_ = false;
};

}  // namespace ROCKSDB_NAMESPACE
