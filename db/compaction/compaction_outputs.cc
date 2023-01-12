//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction_outputs.h"

#include "db/builder.h"

namespace ROCKSDB_NAMESPACE {

void CompactionOutputs::NewBuilder(const TableBuilderOptions& tboptions) {
  builder_.reset(NewTableBuilder(tboptions, file_writer_.get()));
}

Status CompactionOutputs::Finish(const Status& intput_status,
                                 const SeqnoToTimeMapping& seqno_time_mapping) {
  FileMetaData* meta = GetMetaData();
  assert(meta != nullptr);
  Status s = intput_status;
  if (s.ok()) {
    std::string seqno_time_mapping_str;
    seqno_time_mapping.Encode(seqno_time_mapping_str, meta->fd.smallest_seqno,
                              meta->fd.largest_seqno, meta->file_creation_time);
    builder_->SetSeqnoTimeTableProperties(seqno_time_mapping_str,
                                          meta->oldest_ancester_time);
    s = builder_->Finish();

  } else {
    builder_->Abandon();
  }
  Status io_s = builder_->io_status();
  if (s.ok()) {
    s = io_s;
  } else {
    io_s.PermitUncheckedError();
  }
  const uint64_t current_bytes = builder_->FileSize();
  if (s.ok()) {
    meta->fd.file_size = current_bytes;
    meta->marked_for_compaction = builder_->NeedCompact();
  }
  current_output().finished = true;
  stats_.bytes_written += current_bytes;
  stats_.num_output_files = outputs_.size();

  return s;
}

IOStatus CompactionOutputs::WriterSyncClose(const Status& input_status,
                                            SystemClock* clock,
                                            Statistics* statistics,
                                            bool use_fsync) {
  IOStatus io_s;
  if (input_status.ok()) {
    StopWatch sw(clock, statistics, COMPACTION_OUTFILE_SYNC_MICROS);
    io_s = file_writer_->Sync(use_fsync);
  }
  if (input_status.ok() && io_s.ok()) {
    io_s = file_writer_->Close();
  }

  if (input_status.ok() && io_s.ok()) {
    FileMetaData* meta = GetMetaData();
    meta->file_checksum = file_writer_->GetFileChecksum();
    meta->file_checksum_func_name = file_writer_->GetFileChecksumFuncName();
  }

  file_writer_.reset();

  return io_s;
}

size_t CompactionOutputs::UpdateGrandparentBoundaryInfo(
    const Slice& internal_key) {
  size_t curr_key_boundary_switched_num = 0;
  const std::vector<FileMetaData*>& grandparents = compaction_->grandparents();

  if (grandparents.empty()) {
    return curr_key_boundary_switched_num;
  }
  assert(!internal_key.empty());
  InternalKey ikey;
  ikey.DecodeFrom(internal_key);
  assert(ikey.Valid());

  const Comparator* ucmp = compaction_->column_family_data()->user_comparator();

  // Move the grandparent_index_ to the file containing the current user_key.
  // If there are multiple files containing the same user_key, make sure the
  // index points to the last file containing the key.
  while (grandparent_index_ < grandparents.size()) {
    if (being_grandparent_gap_) {
      if (sstableKeyCompare(ucmp, ikey,
                            grandparents[grandparent_index_]->smallest) < 0) {
        break;
      }
      if (seen_key_) {
        curr_key_boundary_switched_num++;
        grandparent_overlapped_bytes_ +=
            grandparents[grandparent_index_]->fd.GetFileSize();
        grandparent_boundary_switched_num_++;
      }
      being_grandparent_gap_ = false;
    } else {
      int cmp_result = sstableKeyCompare(
          ucmp, ikey, grandparents[grandparent_index_]->largest);
      // If it's same key, make sure grandparent_index_ is pointing to the last
      // one.
      if (cmp_result < 0 ||
          (cmp_result == 0 &&
           (grandparent_index_ == grandparents.size() - 1 ||
            sstableKeyCompare(ucmp, ikey,
                              grandparents[grandparent_index_ + 1]->smallest) <
                0))) {
        break;
      }
      if (seen_key_) {
        curr_key_boundary_switched_num++;
        grandparent_boundary_switched_num_++;
      }
      being_grandparent_gap_ = true;
      grandparent_index_++;
    }
  }

  // If the first key is in the middle of a grandparent file, adding it to the
  // overlap
  if (!seen_key_ && !being_grandparent_gap_) {
    assert(grandparent_overlapped_bytes_ == 0);
    grandparent_overlapped_bytes_ =
        GetCurrentKeyGrandparentOverlappedBytes(internal_key);
  }

  seen_key_ = true;
  return curr_key_boundary_switched_num;
}

uint64_t CompactionOutputs::GetCurrentKeyGrandparentOverlappedBytes(
    const Slice& internal_key) const {
  // no overlap with any grandparent file
  if (being_grandparent_gap_) {
    return 0;
  }
  uint64_t overlapped_bytes = 0;

  const std::vector<FileMetaData*>& grandparents = compaction_->grandparents();
  const Comparator* ucmp = compaction_->column_family_data()->user_comparator();
  InternalKey ikey;
  ikey.DecodeFrom(internal_key);
#ifndef NDEBUG
  // make sure the grandparent_index_ is pointing to the last files containing
  // the current key.
  int cmp_result =
      sstableKeyCompare(ucmp, ikey, grandparents[grandparent_index_]->largest);
  assert(
      cmp_result < 0 ||
      (cmp_result == 0 &&
       (grandparent_index_ == grandparents.size() - 1 ||
        sstableKeyCompare(
            ucmp, ikey, grandparents[grandparent_index_ + 1]->smallest) < 0)));
  assert(sstableKeyCompare(ucmp, ikey,
                           grandparents[grandparent_index_]->smallest) >= 0);
#endif
  overlapped_bytes += grandparents[grandparent_index_]->fd.GetFileSize();

  // go backwards to find all overlapped files, one key can overlap multiple
  // files. In the following example, if the current output key is `c`, and one
  // compaction file was cut before `c`, current `c` can overlap with 3 files:
  //  [a b]               [c...
  // [b, b] [c, c] [c, c] [c, d]
  for (int64_t i = static_cast<int64_t>(grandparent_index_) - 1;
       i >= 0 && sstableKeyCompare(ucmp, ikey, grandparents[i]->largest) == 0;
       i--) {
    overlapped_bytes += grandparents[i]->fd.GetFileSize();
  }

  return overlapped_bytes;
}

bool CompactionOutputs::ShouldStopBefore(const CompactionIterator& c_iter) {
  assert(c_iter.Valid());

  // always update grandparent information like overlapped file number, size
  // etc.
  const Slice& internal_key = c_iter.key();
  const uint64_t previous_overlapped_bytes = grandparent_overlapped_bytes_;
#ifndef NDEBUG
  bool should_stop = false;
  std::pair<bool*, const Slice> p{&should_stop, internal_key};
  TEST_SYNC_POINT_CALLBACK("CompactionOutputs::ShouldStopBefore", (void*)&p);
  if (should_stop) {
    return true;
  }
#endif  // NDEBUG
  size_t num_grandparent_boundaries_crossed =
      UpdateGrandparentBoundaryInfo(internal_key);

  if (!HasBuilder()) {
    return false;
  }

  // If there's user defined partitioner, check that first
  if (partitioner_ && partitioner_->ShouldPartition(PartitionerRequest(
                          last_key_for_partitioner_, c_iter.user_key(),
                          current_output_file_size_)) == kRequired) {
    return true;
  }

  // files output to Level 0 won't be split
  if (compaction_->output_level() == 0) {
    return false;
  }

  // reach the max file size
  if (current_output_file_size_ >= compaction_->max_output_file_size()) {
    return true;
  }

  const InternalKeyComparator* icmp =
      &compaction_->column_family_data()->internal_comparator();

  // Check if it needs to split for RoundRobin
  // Invalid local_output_split_key indicates that we do not need to split
  if (local_output_split_key_ != nullptr && !is_split_) {
    // Split occurs when the next key is larger than/equal to the cursor
    if (icmp->Compare(internal_key, local_output_split_key_->Encode()) >= 0) {
      is_split_ = true;
      return true;
    }
  }

  // only check if the current key is going to cross the grandparents file
  // boundary (either the file beginning or ending).
  if (num_grandparent_boundaries_crossed > 0) {
    // Cut the file before the current key if the size of the current output
    // file + its overlapped grandparent files is bigger than
    // max_compaction_bytes. Which is to prevent future bigger than
    // max_compaction_bytes compaction from the current output level.
    if (grandparent_overlapped_bytes_ + current_output_file_size_ >
        compaction_->max_compaction_bytes()) {
      return true;
    }

    // Cut the file if including the key is going to add a skippable file on
    // the grandparent level AND its size is reasonably big (1/8 of target file
    // size). For example, if it's compacting the files L0 + L1:
    //  L0:  [1,   21]
    //  L1:    [3,   23]
    //  L2: [2, 4] [11, 15] [22, 24]
    // Without this break, it will output as:
    //  L1: [1,3, 21,23]
    // With this break, it will output as (assuming [11, 15] at L2 is bigger
    // than 1/8 of target size):
    //  L1: [1,3] [21,23]
    // Then for the future compactions, [11,15] won't be included.
    // For random datasets (either evenly distributed or skewed), it rarely
    // triggers this condition, but if the user is adding 2 different datasets
    // without any overlap, it may likely happen.
    // More details, check PR #1963
    const size_t num_skippable_boundaries_crossed =
        being_grandparent_gap_ ? 2 : 3;
    if (compaction_->immutable_options()->compaction_style ==
            kCompactionStyleLevel &&
        compaction_->immutable_options()->level_compaction_dynamic_file_size &&
        num_grandparent_boundaries_crossed >=
            num_skippable_boundaries_crossed &&
        grandparent_overlapped_bytes_ - previous_overlapped_bytes >
            compaction_->target_output_file_size() / 8) {
      return true;
    }

    // Pre-cut the output file if it's reaching a certain size AND it's at the
    // boundary of a grandparent file. It can reduce the future compaction size,
    // the cost is having smaller files.
    // The pre-cut size threshold is based on how many grandparent boundaries
    // it has seen before. Basically, if it has seen no boundary at all, then it
    // will pre-cut at 50% target file size. Every boundary it has seen
    // increases the threshold by 5%, max at 90%, which it will always cut.
    // The idea is based on if it has seen more boundaries before, it will more
    // likely to see another boundary (file cutting opportunity) before the
    // target file size. The test shows it can generate larger files than a
    // static threshold like 75% and has a similar write amplification
    // improvement.
    if (compaction_->immutable_options()->compaction_style ==
            kCompactionStyleLevel &&
        compaction_->immutable_options()->level_compaction_dynamic_file_size &&
        current_output_file_size_ >=
            ((compaction_->target_output_file_size() + 99) / 100) *
                (50 + std::min(grandparent_boundary_switched_num_ * 5,
                               size_t{40}))) {
      return true;
    }
  }

  // check ttl file boundaries if there's any
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

Status CompactionOutputs::AddToOutput(
    const CompactionIterator& c_iter,
    const CompactionFileOpenFunc& open_file_func,
    const CompactionFileCloseFunc& close_file_func) {
  Status s;
  bool is_range_del = c_iter.IsDeleteRangeSentinelKey();
  if (is_range_del && compaction_->bottommost_level()) {
    // We don't consider range tombstone for bottommost level since:
    // 1. there is no grandparent and hence no overlap to consider
    // 2. range tombstone may be dropped at bottommost level.
    return s;
  }
  const Slice& key = c_iter.key();
  if (ShouldStopBefore(c_iter) && HasBuilder()) {
    s = close_file_func(*this, c_iter.InputStatus(), key);
    if (!s.ok()) {
      return s;
    }
    // reset grandparent information
    grandparent_boundary_switched_num_ = 0;
    grandparent_overlapped_bytes_ =
        GetCurrentKeyGrandparentOverlappedBytes(key);
    if (UNLIKELY(is_range_del)) {
      // lower bound for this new output file, this is needed as the lower bound
      // does not come from the smallest point key in this case.
      range_tombstone_lower_bound_.DecodeFrom(key);
    } else {
      range_tombstone_lower_bound_.Clear();
    }
  }

  // Open output file if necessary
  if (!HasBuilder()) {
    s = open_file_func(*this);
    if (!s.ok()) {
      return s;
    }
  }

  // c_iter may emit range deletion keys, so update `last_key_for_partitioner_`
  // here before returning below when `is_range_del` is true
  if (partitioner_) {
    last_key_for_partitioner_.assign(c_iter.user_key().data_,
                                     c_iter.user_key().size_);
  }

  if (UNLIKELY(is_range_del)) {
    return s;
  }

  assert(builder_ != nullptr);
  const Slice& value = c_iter.value();
  s = current_output().validator.Add(key, value);
  if (!s.ok()) {
    return s;
  }
  builder_->Add(key, value);

  stats_.num_output_records++;
  current_output_file_size_ = builder_->EstimatedFileSize();

  if (blob_garbage_meter_) {
    s = blob_garbage_meter_->ProcessOutFlow(key, value);
  }

  if (!s.ok()) {
    return s;
  }

  const ParsedInternalKey& ikey = c_iter.ikey();
  s = current_output().meta.UpdateBoundaries(key, value, ikey.sequence,
                                             ikey.type);

  return s;
}

Status CompactionOutputs::AddRangeDels(
    const Slice* comp_start_user_key, const Slice* comp_end_user_key,
    CompactionIterationStats& range_del_out_stats, bool bottommost_level,
    const InternalKeyComparator& icmp, SequenceNumber earliest_snapshot,
    const Slice& next_table_min_key, const std::string& full_history_ts_low) {
  assert(HasRangeDel());
  FileMetaData& meta = current_output().meta;
  const Comparator* ucmp = icmp.user_comparator();

  InternalKey lower_bound_buf, upper_bound_buf;
  Slice lower_bound_guard, upper_bound_guard;
  const Slice *lower_bound, *upper_bound;
  size_t output_size = outputs_.size();
  if (output_size == 1) {
    // This is the first file in the subcompaction.
    //
    // When outputting a range tombstone that spans a subcompaction boundary,
    // the files on either side of that boundary need to include that
    // boundary's user key. Otherwise, the spanning range tombstone would lose
    // coverage.
    //
    // To achieve this while preventing files from overlapping in internal key
    // (an LSM invariant violation), we allow the earlier file to include the
    // boundary user key up to `kMaxSequenceNumber,kTypeRangeDeletion`. The
    // later file can begin at the boundary user key at the newest key version
    // it contains. At this point that version number is unknown since we have
    // not processed the range tombstones yet, so permit any version. Same story
    // applies to timestamp, and a non-nullptr `comp_start_user_key` should have
    // `kMaxTs` here, which similarly permits any timestamp.
    if (comp_start_user_key != nullptr) {
      lower_bound_buf.Set(*comp_start_user_key, kMaxSequenceNumber,
                          kTypeRangeDeletion);
      lower_bound_guard = lower_bound_buf.Encode();
      lower_bound = &lower_bound_guard;
    } else {
      lower_bound = nullptr;
    }
  } else {
    // For subsequent output tables, only include range tombstones from min
    // key onwards since the previous file was extended to contain range
    // tombstones falling before min key.
    if (range_tombstone_lower_bound_.size() > 0) {
      assert(meta.smallest.size() == 0 ||
             icmp.Compare(range_tombstone_lower_bound_, meta.smallest) <= 0);
      lower_bound_guard = range_tombstone_lower_bound_.Encode();
    } else {
      assert(meta.smallest.size() > 0);
      lower_bound_guard = meta.smallest.Encode();
    }
    lower_bound = &lower_bound_guard;
  }

  if (next_table_min_key.empty()) {
    // This is the last file in the subcompaction. See above for the reason why
    // `kMaxSequenceNumber,kTypeRangeDeletion` is chosen.
    if (comp_end_user_key != nullptr) {
      upper_bound_buf.Set(*comp_end_user_key, kMaxSequenceNumber,
                          kTypeRangeDeletion);
      upper_bound_guard = upper_bound_buf.Encode();
      upper_bound = &upper_bound_guard;
    } else {
      upper_bound = nullptr;
    }
  } else {
    // There is another file coming whose coverage will begin at
    // `next_table_min_key`. The current file needs to extend range tombstone
    // coverage through its own keys (through `meta.largest`) and through user
    // keys preceding `next_table_min_key`'s user key.
    ParsedInternalKey next_table_min_key_parsed;
    ParseInternalKey(next_table_min_key, &next_table_min_key_parsed,
                     false /* log_err_key */)
        .PermitUncheckedError();
    assert(next_table_min_key_parsed.sequence < kMaxSequenceNumber);

    if (meta.largest.size() > 0 &&
        ucmp->EqualWithoutTimestamp(next_table_min_key_parsed.user_key,
                                    meta.largest.user_key())) {
      upper_bound_guard = meta.largest.Encode();
      upper_bound = &upper_bound_guard;
    } else if (lower_bound != nullptr &&
               ucmp->EqualWithoutTimestamp(
                   ExtractUserKey(*lower_bound),
                   next_table_min_key_parsed.user_key)) {
      // There are no in-bounds user keys preceding `next_table_min_key`, and no
      // point keys in this file either. Leave the file empty.
      return Status::OK();
    } else {
      upper_bound_buf.Set(next_table_min_key_parsed.user_key,
                          kMaxSequenceNumber, kTypeRangeDeletion);
      upper_bound_guard = upper_bound_buf.Encode();
      upper_bound = &upper_bound_guard;
    }
  }

  // The end key of the subcompaction must be bigger or equal to the upper
  // bound. If the end of subcompaction is null or the upper bound is null,
  // it means that this file is the last file in the compaction. So there
  // will be no overlapping between this file and others.
  assert(comp_end_user_key == nullptr || upper_bound == nullptr ||
         ucmp->CompareWithoutTimestamp(ExtractUserKey(*upper_bound),
                                       *comp_end_user_key) <= 0);
  auto it = range_del_agg_->NewIterator(lower_bound, upper_bound);
  bool reached_lower_bound = false;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    auto tombstone = it->Tombstone();
    auto kv = tombstone.Serialize();
    InternalKey tombstone_end = tombstone.SerializeEndKey();

    // TODO: the underlying iterator should support clamping the bounds.
    if (!reached_lower_bound && lower_bound != nullptr &&
        icmp.Compare(tombstone_end.Encode(), *lower_bound) <= 0) {
      continue;
    }
    reached_lower_bound = true;

    const size_t ts_sz = ucmp->timestamp_size();
    // Garbage collection for range tombstones.
    // If user-defined timestamp is enabled, range tombstones are dropped if
    // they are at bottommost_level, below full_history_ts_low and not visible
    // in any snapshot. trim_ts_ is passed to the constructor for
    // range_del_agg_, and range_del_agg_ internally drops tombstones above
    // trim_ts_.
    if (bottommost_level && tombstone.seq_ <= earliest_snapshot &&
        (ts_sz == 0 ||
         (!full_history_ts_low.empty() &&
          ucmp->CompareTimestamp(tombstone.ts_, full_history_ts_low) < 0))) {
      // TODO(andrewkr): tombstones that span multiple output files are
      // counted for each compaction output file, so lots of double
      // counting.
      range_del_out_stats.num_range_del_drop_obsolete++;
      range_del_out_stats.num_record_drop_obsolete++;
      continue;
    }

    assert(lower_bound == nullptr ||
           ucmp->CompareWithoutTimestamp(ExtractUserKey(*lower_bound),
                                         kv.second) <= 0);
    InternalKey tombstone_start = kv.first;
    if (lower_bound != nullptr &&
        ucmp->CompareWithoutTimestamp(ExtractUserKey(tombstone_start.Encode()),
                                      ExtractUserKey(*lower_bound)) < 0) {
      // This just updates the non-timestamp portion of `tombstone_start`'s user
      // key. Ideally there would be a simpler API usage
      ParsedInternalKey tombstone_start_parsed;
      ParseInternalKey(tombstone_start.Encode(), &tombstone_start_parsed,
                       false /* log_err_key */)
          .PermitUncheckedError();
      std::string ts =
          tombstone_start_parsed.GetTimestamp(ucmp->timestamp_size())
              .ToString();
      tombstone_start_parsed.user_key = ExtractUserKey(*lower_bound);
      tombstone_start_parsed.SetTimestamp(ts);
      tombstone_start.SetFrom(tombstone_start_parsed);
    }
    if (upper_bound != nullptr &&
        icmp.Compare(*upper_bound, tombstone_start.Encode()) < 0) {
      break;
    }

    // Range tombstone is not supported by output validator yet.
    builder_->Add(kv.first.Encode(), kv.second);

    if (lower_bound != nullptr &&
        icmp.Compare(tombstone_start.Encode(), *lower_bound) < 0) {
      tombstone_start.DecodeFrom(*lower_bound);
    }

    if (upper_bound != nullptr &&
        icmp.Compare(*upper_bound, tombstone_end.Encode()) < 0) {
      tombstone_end.DecodeFrom(*upper_bound);
    }

    assert(icmp.Compare(tombstone_start, tombstone_end) <= 0);
    meta.UpdateBoundariesForRange(tombstone_start, tombstone_end,
                                  tombstone.seq_, icmp);
    if (!bottommost_level) {
      SizeApproximationOptions approx_opts;
      approx_opts.files_size_error_margin = 0.1;
      auto approximate_covered_size =
          compaction_->input_version()->version_set()->ApproximateSize(
              approx_opts, compaction_->input_version(),
              tombstone_start.Encode(), tombstone_end.Encode(),
              compaction_->output_level() + 1 /* start_level */,
              -1 /* end_level */, kCompaction);
      meta.compensated_range_deletion_size += approximate_covered_size;
    }
  }
  return Status::OK();
}

void CompactionOutputs::FillFilesToCutForTtl() {
  if (compaction_->immutable_options()->compaction_style !=
          kCompactionStyleLevel ||
      compaction_->immutable_options()->compaction_pri !=
          kMinOverlappingRatio ||
      compaction_->mutable_cf_options()->ttl == 0 ||
      compaction_->num_input_levels() < 2 || compaction_->bottommost_level()) {
    return;
  }

  // We define new file with the oldest ancestor time to be younger than 1/4
  // TTL, and an old one to be older than 1/2 TTL time.
  int64_t temp_current_time;
  auto get_time_status =
      compaction_->immutable_options()->clock->GetCurrentTime(
          &temp_current_time);
  if (!get_time_status.ok()) {
    return;
  }

  auto current_time = static_cast<uint64_t>(temp_current_time);
  if (current_time < compaction_->mutable_cf_options()->ttl) {
    return;
  }

  uint64_t old_age_thres =
      current_time - compaction_->mutable_cf_options()->ttl / 2;
  const std::vector<FileMetaData*>& olevel =
      *(compaction_->inputs(compaction_->num_input_levels() - 1));
  for (FileMetaData* file : olevel) {
    // Worth filtering out by start and end?
    uint64_t oldest_ancester_time = file->TryGetOldestAncesterTime();
    // We put old files if they are not too small to prevent a flood
    // of small files.
    if (oldest_ancester_time < old_age_thres &&
        file->fd.GetFileSize() >
            compaction_->mutable_cf_options()->target_file_size_base / 2) {
      files_to_cut_for_ttl_.push_back(file);
    }
  }
}

CompactionOutputs::CompactionOutputs(const Compaction* compaction,
                                     const bool is_penultimate_level)
    : compaction_(compaction), is_penultimate_level_(is_penultimate_level) {
  partitioner_ = compaction->output_level() == 0
                     ? nullptr
                     : compaction->CreateSstPartitioner();

  if (compaction->output_level() != 0) {
    FillFilesToCutForTtl();
  }
}

}  // namespace ROCKSDB_NAMESPACE
