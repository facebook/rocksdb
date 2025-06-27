//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/db.h"

namespace ROCKSDB_NAMESPACE {

using MultiScanIterator = MultiScan::MultiScanIterator;

MultiScan::MultiScan(const ReadOptions& read_options,
                     const std::vector<ScanOptions>& scan_opts, DB* db,
                     ColumnFamilyHandle* cfh)
    : read_options_(read_options), scan_opts_(scan_opts), db_(db), cfh_(cfh) {
  bool slow_path = false;
  // Setup read_options with iterate_uuper_bound based on the first scan.
  // Subsequent scans will update and allocate a new DB iterator as necessary
  if (scan_opts[0].range.limit) {
    upper_bound_ = *scan_opts[0].range.limit;
    read_options_.iterate_upper_bound = &upper_bound_;
  } else {
    read_options_.iterate_upper_bound = nullptr;
  }
  for (auto opts : scan_opts) {
    // Check that all the ScanOptions either specify an upper bound or not. If
    // its mixed we take the slow path which avoids calling Prepare: we have to
    // reallocate the Iterator with updated read_options everytime we switch
    // between upper bound or no upper bound, which complicates Prepare.
    if (opts.range.limit.has_value() != scan_opts[0].range.limit.has_value()) {
      slow_path = true;
      break;
    }
  }
  db_iter_.reset(db->NewIterator(read_options_, cfh));
  if (!slow_path) {
    db_iter_->Prepare(scan_opts);
  }
}

MultiScanIterator& MultiScanIterator::operator++() {
  if (idx_ >= scan_opts_.size()) {
    throw std::logic_error("Index out of range");
  }
  idx_++;
  if (idx_ < scan_opts_.size()) {
    // Check if we need to update read_options_
    if (scan_opts_[idx_].range.limit.has_value() !=
        (read_options_.iterate_upper_bound != nullptr)) {
      if (scan_opts_[idx_].range.limit) {
        *upper_bound_ = *scan_opts_[idx_].range.limit;
        read_options_.iterate_upper_bound = upper_bound_;
      } else {
        read_options_.iterate_upper_bound = nullptr;
      }
      db_iter_.reset(db_->NewIterator(read_options_, cfh_));
      scan_.Reset(db_iter_.get());
    } else if (scan_opts_[idx_].range.limit) {
      *upper_bound_ = *scan_opts_[idx_].range.limit;
    }
    db_iter_->Seek(*scan_opts_[idx_].range.start);
    status_ = db_iter_->status();
    if (!status_.ok()) {
      throw MultiScanException(status_);
    }
  }
  return *this;
}

}  // namespace ROCKSDB_NAMESPACE
