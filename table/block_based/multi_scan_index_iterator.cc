//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/block_based/multi_scan_index_iterator.h"

#include "monitoring/statistics_impl.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

MultiScanIndexIterator::MultiScanIndexIterator(
    std::vector<BlockHandle>&& block_handles,
    std::vector<std::string>&& data_block_separators,
    std::vector<std::tuple<size_t, size_t>>&& block_index_ranges_per_scan,
    const MultiScanArgs* scan_opts, std::shared_ptr<ReadSet> read_set,
    size_t prefetch_max_idx, const InternalKeyComparator& icomp,
    Statistics* statistics)
    : block_handles_(std::move(block_handles)),
      data_block_separators_(std::move(data_block_separators)),
      block_index_ranges_per_scan_(std::move(block_index_ranges_per_scan)),
      scan_opts_(scan_opts),
      read_set_(std::move(read_set)),
      prefetch_max_idx_(prefetch_max_idx),
      icomp_(icomp),
      user_comparator_(icomp.user_comparator()),
      statistics_(statistics) {}

MultiScanIndexIterator::~MultiScanIndexIterator() {
  if (statistics_ && wasted_blocks_count_ > 0) {
    RecordTick(statistics_, MULTISCAN_PREFETCH_BLOCKS_WASTED,
               wasted_blocks_count_);
  }
  // Release any remaining pinned blocks
  if (read_set_) {
    for (size_t i = cur_idx_; i < block_handles_.size(); ++i) {
      read_set_->ReleaseBlock(i);
    }
  }
}

void MultiScanIndexIterator::ReleaseBlocks(size_t from_idx, size_t to_idx) {
  for (size_t i = from_idx; i < to_idx; ++i) {
    if (i < prefetch_max_idx_) {
      wasted_blocks_count_++;
    }
    read_set_->ReleaseBlock(i);
  }
}

void MultiScanIndexIterator::Seek(const Slice& target) {
  if (!status_.ok()) {
    return;
  }

  // Reset scan range exhaustion flag on Seek
  scan_range_exhausted_ = false;

  // Check the case where there are no ranges prepared
  if (scan_opts_->size() == 0) {
    valid_ = false;
    return;
  }

  // Enforce forward-only seek
  if (!prev_seek_key_.empty() && icomp_.Compare(target, prev_seek_key_) <= 0) {
    // Seek key is not moving forward — keep current position
    return;
  }
  prev_seek_key_ = target.ToString();

  const auto& scan_ranges = scan_opts_->GetScanRanges();
  Slice user_seek_target = ExtractUserKey(target);

  // Allow reseek on the start of the last prepared range
  next_scan_idx_ =
      std::min(next_scan_idx_, block_index_ranges_per_scan_.size() - 1);

  auto compare_next_scan_start_result =
      user_comparator_.CompareWithoutTimestamp(
          user_seek_target, /*a_has_ts=*/true,
          scan_ranges[next_scan_idx_].range.start.value(),
          /*b_has_ts=*/false);

  // There are 3 different Cases we need to handle:
  // The following diagram explains different seek targets seeking at various
  // positions on the table, while the next_scan_idx_ points to PreparedRange 2.
  //
  // next_scan_idx_: ------------------┐
  //                                   ▼
  // table:     : __[PreparedRange 1]__[PreparedRange 2]__[PreparedRange 3]__
  // Seek target: <----- Case 1 ------>▲<------------- Case 2 -------------->
  //                                   │
  //                                 Case 3
  //
  // Case 1: seek before the start of next prepared range. This could happen
  //    due to too many delete tombstones triggering reseek or delete range.
  // Case 2: seek after the start of next prepared range.
  //    This could happen due to seek key adjustment from delete range file.
  // Case 3: seek at the beginning of a prepared range (expected case)

  if (compare_next_scan_start_result < 0) {
    // Case 1: Seek before the start of the next prepared range
    if (next_scan_idx_ == 0) {
      // Should not happen — seek before first prepared range
      assert(false && "Seek target before the first prepared range");
      valid_ = false;
      return;
    }
    auto seek_target_before_previous_prepared_range =
        user_comparator_.CompareWithoutTimestamp(
            user_seek_target, /*a_has_ts=*/true,
            scan_ranges[next_scan_idx_ - 1].range.start.value(),
            /*b_has_ts=*/false) < 0;
    assert(!seek_target_before_previous_prepared_range);
    if (seek_target_before_previous_prepared_range) {
      status_ = Status::InvalidArgument(
          "Seek target is before the previous prepared range at index " +
          std::to_string(next_scan_idx_));
      RecordTick(statistics_, MULTISCAN_SEEK_ERRORS);
      valid_ = false;
      return;
    }
    // Seek within a gap — advance to the right scan range and find block
    SeekToBlock(&user_seek_target);
  } else if (compare_next_scan_start_result > 0) {
    // Case 2: Seek after the start of the next prepared range
    SeekToBlock(&user_seek_target);
  } else {
    // Case 3: Seek at the beginning of a prepared range (expected case)
    assert(next_scan_idx_ < block_index_ranges_per_scan_.size());
    auto [cur_scan_start_idx, cur_scan_end_idx] =
        block_index_ranges_per_scan_[next_scan_idx_];
    ++next_scan_idx_;

    if (cur_scan_start_idx >= cur_scan_end_idx) {
      // No blocks are prepared for this range at current file
      SetExhausted();
      return;
    }

    // max_sequential_skip_in_iterations can trigger a reseek on the start
    // key of a scan range, even though we're already past cur_scan_start_idx
    size_t block_idx = std::max(cur_scan_start_idx, cur_idx_);
    SeekToBlockIdx(block_idx);
  }
}

void MultiScanIndexIterator::SeekToBlock(const Slice* user_seek_target) {
  const auto& scan_ranges = scan_opts_->GetScanRanges();

  // Advance next_scan_idx_ past ranges whose start key <= seek target
  while (next_scan_idx_ < block_index_ranges_per_scan_.size() &&
         user_comparator_.CompareWithoutTimestamp(
             *user_seek_target, /*a_has_ts=*/true,
             scan_ranges[next_scan_idx_].range.start.value(),
             /*b_has_ts=*/false) >= 0) {
    next_scan_idx_++;
  }

  assert(next_scan_idx_ > 0);
  auto cur_scan_idx = next_scan_idx_ - 1;
  auto [cur_scan_start_idx, cur_scan_end_idx] =
      block_index_ranges_per_scan_[cur_scan_idx];

  if (cur_scan_start_idx >= cur_scan_end_idx) {
    SetExhausted();
    return;
  }

  // Release blocks from current position to cur_scan_start_idx (wasted)
  ReleaseBlocks(cur_idx_, cur_scan_start_idx);

  // Find the correct block within the range using linear search on separators
  size_t block_idx = std::max(cur_scan_start_idx, cur_idx_);
  while (block_idx < data_block_separators_.size() &&
         user_comparator_.CompareWithoutTimestamp(
             *user_seek_target, /*a_has_ts=*/true,
             data_block_separators_[block_idx],
             /*b_has_ts=*/false) > 0) {
    if (block_idx < prefetch_max_idx_) {
      wasted_blocks_count_++;
    }
    read_set_->ReleaseBlock(block_idx);
    block_idx++;
  }

  if (block_idx >= data_block_separators_.size()) {
    // All remaining blocks were released above. Update cur_idx_ so the
    // destructor does not double-release them.
    cur_idx_ = block_handles_.size();
    SetExhausted();
    return;
  }

  // Update cur_idx_ before calling SeekToBlockIdx since we've already
  // released all blocks up to block_idx above. This prevents SeekToBlockIdx's
  // ReleaseBlocks(cur_idx_, block_idx) from double-releasing.
  cur_idx_ = block_idx;
  SeekToBlockIdx(block_idx);
}

void MultiScanIndexIterator::SeekToBlockIdx(size_t block_idx) {
  assert(cur_idx_ <= block_idx);

  // Release any blocks between cur_idx_ and block_idx (wasted)
  ReleaseBlocks(cur_idx_, block_idx);

  cur_idx_ = block_idx;
  valid_ = true;
}

void MultiScanIndexIterator::SetExhausted() {
  scan_range_exhausted_ = true;
  if (next_scan_idx_ < block_index_ranges_per_scan_.size()) {
    // More ranges remain — signal out-of-bound for current range.
    valid_ = true;
    // Position at the start of the next range so that the next Seek()
    // can find it. We need to be "valid" so that FindBlockForward sets
    // is_out_of_bound_ = true.
    auto [start, end] = block_index_ranges_per_scan_[next_scan_idx_];
    if (start < end) {
      cur_idx_ = start;
      return;
    }
    valid_ = false;
  } else {
    // Last range — natural EOF. Don't set out-of-bound so LevelIterator
    // advances to the next file.
    valid_ = false;
  }
}

void MultiScanIndexIterator::Next() {
  assert(valid_);

  // Release current block
  read_set_->ReleaseBlock(cur_idx_);
  ++cur_idx_;

  // Check if we've crossed a scan range boundary
  if (next_scan_idx_ > 0) {
    auto cur_scan_end_idx =
        std::get<1>(block_index_ranges_per_scan_[next_scan_idx_ - 1]);
    if (cur_idx_ >= cur_scan_end_idx) {
      // Current scan range is exhausted
      SetExhausted();
      return;
    }
  }

  // Check prefetch limit
  if (cur_idx_ >= prefetch_max_idx_) {
    valid_ = false;
    if (scan_opts_->max_prefetch_size > 0) {
      status_ = Status::PrefetchLimitReached();
    }
    return;
  }

  // Still within current range, valid
  valid_ = true;
}

void MultiScanIndexIterator::SeekToFirst() {
  if (block_index_ranges_per_scan_.empty()) {
    valid_ = false;
    return;
  }

  cur_idx_ = 0;
  next_scan_idx_ = 0;
  prev_seek_key_.clear();
  wasted_blocks_count_ = 0;
  status_ = Status::OK();

  auto [start, end] = block_index_ranges_per_scan_[0];
  if (start >= end) {
    valid_ = false;
    return;
  }
  cur_idx_ = start;
  next_scan_idx_ = 1;
  valid_ = true;
}

void MultiScanIndexIterator::SeekForPrev(const Slice& /*target*/) {
  valid_ = false;
}

void MultiScanIndexIterator::SeekToLast() { valid_ = false; }

void MultiScanIndexIterator::Prev() { valid_ = false; }

Slice MultiScanIndexIterator::key() const {
  assert(valid_);
  assert(cur_idx_ < data_block_separators_.size());

  // Build internal key: user_key + pack(kMaxSequenceNumber, kValueTypeForSeek)
  cur_key_buf_.clear();
  AppendInternalKey(&cur_key_buf_,
                    ParsedInternalKey(data_block_separators_[cur_idx_],
                                      kMaxSequenceNumber, kValueTypeForSeek));
  return Slice(cur_key_buf_);
}

Slice MultiScanIndexIterator::user_key() const {
  assert(valid_);
  assert(cur_idx_ < data_block_separators_.size());
  return Slice(data_block_separators_[cur_idx_]);
}

IndexValue MultiScanIndexIterator::value() const {
  assert(valid_);
  assert(cur_idx_ < block_handles_.size());
  // Return IndexValue with empty first_internal_key to disable
  // is_at_first_key_from_index_ optimization
  return IndexValue(block_handles_[cur_idx_], Slice());
}

uint64_t MultiScanIndexIterator::GetMaxPrefetchSize() const {
  return scan_opts_->max_prefetch_size;
}

}  // namespace ROCKSDB_NAMESPACE
