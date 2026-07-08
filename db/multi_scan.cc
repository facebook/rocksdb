//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <string>
#include <vector>

#include "file/file_util.h"
#include "rocksdb/db.h"
#include "rocksdb/file_system.h"
#include "rocksdb/iterator.h"

namespace ROCKSDB_NAMESPACE {

using MultiScanIterator = MultiScan::MultiScanIterator;

namespace {

Status ValidateScanOptionsForMultiScan(const MultiScanArgs& multiscan_opts) {
  if (multiscan_opts.empty()) {
    return Status::InvalidArgument("Empty MultiScanArgs");
  }

  const Comparator* comparator = multiscan_opts.GetComparator();
  if (comparator == nullptr) {
    return Status::InvalidArgument("MultiScanArgs requires comparator");
  }

  const std::vector<ScanOptions>& scan_opts = multiscan_opts.GetScanRanges();
  for (size_t i = 0; i < scan_opts.size(); ++i) {
    const auto& scan_range = scan_opts[i].range;
    if (!scan_range.start.has_value()) {
      return Status::InvalidArgument("Scan has no start key at index " +
                                     std::to_string(i));
    }
    if (multiscan_opts.reverse && !scan_range.limit.has_value()) {
      return Status::InvalidArgument(
          "Reverse MultiScan requires limit at index " + std::to_string(i));
    }
    if (scan_range.limit.has_value() &&
        comparator->CompareWithoutTimestamp(
            scan_range.start.value(), /*a_has_ts=*/false,
            scan_range.limit.value(), /*b_has_ts=*/false) >= 0) {
      return Status::InvalidArgument(
          "Scan start key is large or equal than limit at index " +
          std::to_string(i));
    }
  }

  return Status::OK();
}

}  // namespace

MultiScan::MultiScan(const ReadOptions& read_options,
                     const MultiScanArgs& scan_opts, DB* db,
                     ColumnFamilyHandle* cfh)
    : read_options_(read_options),
      scan_opts_([&] {
        // Disable async IO if the filesystem does not support it, consistent
        // with how ArenaWrappedDBIter::Init() handles ReadOptions::async_io.
        auto opts = scan_opts;
        if (opts.use_async_io &&
            !CheckFSFeatureSupport(db->GetFileSystem(),
                                   FSSupportedOps::kAsyncIO)) {
          opts.use_async_io = false;
        }
        return opts;
      }()),
      db_(db),
      cfh_(cfh) {
  Status validate_status = ValidateScanOptionsForMultiScan(scan_opts_);
  if (!validate_status.ok()) {
    db_iter_.reset(NewErrorIterator(validate_status));
    return;
  }

  bool slow_path = false;
  // Setup read_options bounds based on the first scan. Subsequent scans will
  // update the bounds and allocate a new DB iterator as necessary.
  {
    const ScanOptions& first_scan = scan_opts_.GetScanRanges()[0];
    if (scan_opts_.reverse) {
      lower_bound_ = *first_scan.range.start;
      read_options_.iterate_lower_bound = &lower_bound_;
      upper_bound_ = *first_scan.range.limit;
      read_options_.iterate_upper_bound = &upper_bound_;
    } else if (first_scan.range.limit) {
      upper_bound_ = *first_scan.range.limit;
      read_options_.iterate_upper_bound = &upper_bound_;
    } else {
      read_options_.iterate_upper_bound = nullptr;
    }
  }
  for (const auto& opts : scan_opts_.GetScanRanges()) {
    // Check that all the ScanOptions either specify an upper bound or not. If
    // its mixed we take the slow path which avoids calling Prepare: we have to
    // reallocate the Iterator with updated read_options everytime we switch
    // between upper bound or no upper bound, which complicates Prepare.
    if (opts.range.limit.has_value() !=
        scan_opts_.GetScanRanges()[0].range.limit.has_value()) {
      slow_path = true;
      break;
    }
  }
  db_iter_.reset(db->NewIterator(read_options_, cfh));
  if (!slow_path) {
    db_iter_->Prepare(scan_opts_);
  }
}

void MultiScanIterator::SeekCurrentRange() {
  const ScanOptions& scan_opt = scan_opts_[idx_];
  if (scan_opt.range.limit) {
    *upper_bound_ = *scan_opt.range.limit;
    read_options_.iterate_upper_bound = upper_bound_;
  } else {
    read_options_.iterate_upper_bound = nullptr;
  }

  if (reverse_) {
    *lower_bound_ = *scan_opt.range.start;
    read_options_.iterate_lower_bound = lower_bound_;
    assert(scan_opt.range.limit.has_value());
    db_iter_->SeekForPrev(*scan_opt.range.limit);
  } else {
    db_iter_->Seek(*scan_opt.range.start);
  }
}

MultiScanIterator& MultiScanIterator::operator++() {
  status_ = db_iter_->status();
  if (!status_.ok()) {
    throw MultiScanException(status_);
  }

  if (!valid_) {
    throw std::logic_error("Incrementing exhausted MultiScan iterator");
  }
  ++idx_;
  if (idx_ >= scan_opts_.size()) {
    valid_ = false;
    return *this;
  }

  // Check if we need to update read_options_
  if (scan_opts_[idx_].range.limit.has_value() !=
      (read_options_.iterate_upper_bound != nullptr)) {
    if (scan_opts_[idx_].range.limit) {
      read_options_.iterate_upper_bound = upper_bound_;
    } else {
      read_options_.iterate_upper_bound = nullptr;
    }
    db_iter_.reset(db_->NewIterator(read_options_, cfh_));
    scan_.Reset(db_iter_.get(), reverse_);
  }
  SeekCurrentRange();
  status_ = db_iter_->status();
  if (!status_.ok()) {
    throw MultiScanException(status_);
  }
  return *this;
}

}  // namespace ROCKSDB_NAMESPACE
