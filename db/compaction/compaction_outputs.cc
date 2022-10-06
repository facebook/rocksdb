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
  }

  // Open output file if necessary
  if (!HasBuilder()) {
    s = open_file_func(*this);
    if (!s.ok()) {
      return s;
    }
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

  if (partitioner_) {
    last_key_for_partitioner_.assign(c_iter.user_key().data_,
                                     c_iter.user_key().size_);
  }

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

  Slice lower_bound_guard, upper_bound_guard;
  std::string smallest_user_key;
  const Slice *lower_bound, *upper_bound;
  bool lower_bound_from_sub_compact = false;

  size_t output_size = outputs_.size();
  if (output_size == 1) {
    // For the first output table, include range tombstones before the min
    // key but after the subcompaction boundary.
    lower_bound = comp_start_user_key;
    lower_bound_from_sub_compact = true;
  } else if (meta.smallest.size() > 0) {
    // For subsequent output tables, only include range tombstones from min
    // key onwards since the previous file was extended to contain range
    // tombstones falling before min key.
    smallest_user_key = meta.smallest.user_key().ToString(false /*hex*/);
    lower_bound_guard = Slice(smallest_user_key);
    lower_bound = &lower_bound_guard;
  } else {
    lower_bound = nullptr;
  }
  if (!next_table_min_key.empty()) {
    // This may be the last file in the subcompaction in some cases, so we
    // need to compare the end key of subcompaction with the next file start
    // key. When the end key is chosen by the subcompaction, we know that
    // it must be the biggest key in output file. Therefore, it is safe to
    // use the smaller key as the upper bound of the output file, to ensure
    // that there is no overlapping between different output files.
    upper_bound_guard = ExtractUserKey(next_table_min_key);
    if (comp_end_user_key != nullptr &&
        ucmp->CompareWithoutTimestamp(upper_bound_guard, *comp_end_user_key) >=
            0) {
      upper_bound = comp_end_user_key;
    } else {
      upper_bound = &upper_bound_guard;
    }
  } else {
    // This is the last file in the subcompaction, so extend until the
    // subcompaction ends.
    upper_bound = comp_end_user_key;
  }
  bool has_overlapping_endpoints;
  if (upper_bound != nullptr && meta.largest.size() > 0) {
    has_overlapping_endpoints = ucmp->CompareWithoutTimestamp(
                                    meta.largest.user_key(), *upper_bound) == 0;
  } else {
    has_overlapping_endpoints = false;
  }

  // The end key of the subcompaction must be bigger or equal to the upper
  // bound. If the end of subcompaction is null or the upper bound is null,
  // it means that this file is the last file in the compaction. So there
  // will be no overlapping between this file and others.
  assert(comp_end_user_key == nullptr || upper_bound == nullptr ||
         ucmp->CompareWithoutTimestamp(*upper_bound, *comp_end_user_key) <= 0);
  auto it = range_del_agg_->NewIterator(lower_bound, upper_bound,
                                        has_overlapping_endpoints);
  // Position the range tombstone output iterator. There may be tombstone
  // fragments that are entirely out of range, so make sure that we do not
  // include those.
  if (lower_bound != nullptr) {
    it->Seek(*lower_bound);
  } else {
    it->SeekToFirst();
  }
  for (; it->Valid(); it->Next()) {
    auto tombstone = it->Tombstone();
    if (upper_bound != nullptr) {
      int cmp =
          ucmp->CompareWithoutTimestamp(*upper_bound, tombstone.start_key_);
      if ((has_overlapping_endpoints && cmp < 0) ||
          (!has_overlapping_endpoints && cmp <= 0)) {
        // Tombstones starting after upper_bound only need to be included in
        // the next table. If the current SST ends before upper_bound, i.e.,
        // `has_overlapping_endpoints == false`, we can also skip over range
        // tombstones that start exactly at upper_bound. Such range
        // tombstones will be included in the next file and are not relevant
        // to the point keys or endpoints of the current file.
        break;
      }
    }

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

    auto kv = tombstone.Serialize();
    assert(lower_bound == nullptr ||
           ucmp->CompareWithoutTimestamp(*lower_bound, kv.second) < 0);
    // Range tombstone is not supported by output validator yet.
    builder_->Add(kv.first.Encode(), kv.second);
    InternalKey smallest_candidate = std::move(kv.first);
    if (lower_bound != nullptr &&
        ucmp->CompareWithoutTimestamp(smallest_candidate.user_key(),
                                      *lower_bound) <= 0) {
      // Pretend the smallest key has the same user key as lower_bound
      // (the max key in the previous table or subcompaction) in order for
      // files to appear key-space partitioned.
      //
      // When lower_bound is chosen by a subcompaction, we know that
      // subcompactions over smaller keys cannot contain any keys at
      // lower_bound. We also know that smaller subcompactions exist,
      // because otherwise the subcompaction woud be unbounded on the left.
      // As a result, we know that no other files on the output level will
      // contain actual keys at lower_bound (an output file may have a
      // largest key of lower_bound@kMaxSequenceNumber, but this only
      // indicates a large range tombstone was truncated). Therefore, it is
      // safe to use the tombstone's sequence number, to ensure that keys at
      // lower_bound at lower levels are covered by truncated tombstones.
      //
      // If lower_bound was chosen by the smallest data key in the file,
      // choose lowest seqnum so this file's smallest internal key comes
      // after the previous file's largest. The fake seqnum is OK because
      // the read path's file-picking code only considers user key.
      if (lower_bound_from_sub_compact) {
        if (ts_sz) {
          assert(tombstone.ts_.size() == ts_sz);
          smallest_candidate = InternalKey(*lower_bound, tombstone.seq_,
                                           kTypeRangeDeletion, tombstone.ts_);
        } else {
          smallest_candidate =
              InternalKey(*lower_bound, tombstone.seq_, kTypeRangeDeletion);
        }
      } else {
        smallest_candidate = InternalKey(*lower_bound, 0, kTypeRangeDeletion);
      }
    }
    InternalKey largest_candidate = tombstone.SerializeEndKey();
    if (upper_bound != nullptr &&
        ucmp->CompareWithoutTimestamp(*upper_bound,
                                      largest_candidate.user_key()) <= 0) {
      // Pretend the largest key has the same user key as upper_bound (the
      // min key in the following table or subcompaction) in order for files
      // to appear key-space partitioned.
      //
      // Choose highest seqnum so this file's largest internal key comes
      // before the next file's/subcompaction's smallest. The fake seqnum is
      // OK because the read path's file-picking code only considers the
      // user key portion.
      //
      // Note Seek() also creates InternalKey with (user_key,
      // kMaxSequenceNumber), but with kTypeDeletion (0x7) instead of
      // kTypeRangeDeletion (0xF), so the range tombstone comes before the
      // Seek() key in InternalKey's ordering. So Seek() will look in the
      // next file for the user key
      if (ts_sz) {
        static constexpr char kTsMax[] = "\xff\xff\xff\xff\xff\xff\xff\xff\xff";
        if (ts_sz <= strlen(kTsMax)) {
          largest_candidate =
              InternalKey(*upper_bound, kMaxSequenceNumber, kTypeRangeDeletion,
                          Slice(kTsMax, ts_sz));
        } else {
          largest_candidate =
              InternalKey(*upper_bound, kMaxSequenceNumber, kTypeRangeDeletion,
                          std::string(ts_sz, '\xff'));
        }
      } else {
        largest_candidate =
            InternalKey(*upper_bound, kMaxSequenceNumber, kTypeRangeDeletion);
      }
    }
#ifndef NDEBUG
    SequenceNumber smallest_ikey_seqnum = kMaxSequenceNumber;
    if (meta.smallest.size() > 0) {
      smallest_ikey_seqnum = GetInternalKeySeqno(meta.smallest.Encode());
    }
#endif
    meta.UpdateBoundariesForRange(smallest_candidate, largest_candidate,
                                  tombstone.seq_, icmp);
    // The smallest key in a file is used for range tombstone truncation, so
    // it cannot have a seqnum of 0 (unless the smallest data key in a file
    // has a seqnum of 0). Otherwise, the truncated tombstone may expose
    // deleted keys at lower levels.
    assert(smallest_ikey_seqnum == 0 ||
           ExtractInternalKeyFooter(meta.smallest.Encode()) !=
               PackSequenceAndType(0, kTypeRangeDeletion));
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
