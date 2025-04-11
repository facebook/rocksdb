//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/iterator.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

// EXPERIMENTAL
//
// An iterator that returns results from multiple scan ranges. The ranges are
// expected to be in increasing sorted order.
// The results are returned in nested container objects that can be iterated
// using an std::input_iterator.
//
// MultiScan
//     |
//     ---
//       |
//  MultiScanIterator  <-- std::input_iterator (returns a Scan object for each
//         |                                    scan range)
//         ---
//           |
//          Scan
//            |
//            ---
//              |
//          ScanIterator <-- std::input_iterator (returns the KVs of a single
//                                                scan range)
//
// The application on top of RocksDB
// would use this as follows -
//
//  std::vector<ScanOptions> scans{{.start = Slice("bar")},
//                              {.start = Slice("foo")}};
//  std::unique_ptr<MultiScanIterator> iter.reset(
//                                      db->NewMultiScanIterator());
//  try {
//    for (auto scan : *iter) {
//      for (auto it : scan) {
//        // Do something with key - it.first
//        // Do something with value - it.second
//      }
//    }
//  } catch (Status s) {
//  }

// A container object encapsulating a single scan range. It supports an
// std::input_iterator for a single pass iteration of the KVs in the range.
// A Status exception is thrown if there is an error in scanning the range.
class Scan {
 public:
  class ScanIterator;

  Scan(Iterator* db_iter) : db_iter_(db_iter) {}

  ScanIterator begin() { return ScanIterator(db_iter_); }

  std::nullptr_t end() { return nullptr; }

  class ScanIterator {
   public:
    using self_type = ScanIterator;
    using value_type = std::pair<Slice, Slice>;
    using reference = std::pair<Slice, Slice>&;
    using pointer = std::pair<Slice, Slice>*;
    using difference_type = int;
    using iterator_category = std::input_iterator_tag;

    explicit ScanIterator(Iterator* db_iter) : db_iter_(db_iter) {
      valid_ = db_iter_->Valid();
      if (valid_) {
        result_ = value_type(db_iter_->key(), db_iter_->value());
      }
    }

    ScanIterator() : db_iter_(nullptr), valid_(false) {}

    ~ScanIterator() { assert(status_.ok()); }

    ScanIterator& operator++() {
      if (!valid_) {
        throw Status::InvalidArgument("Trying to advance invalid iterator");
      } else {
        db_iter_->Next();
        status_ = db_iter_->status();
        if (!status_.ok()) {
          throw status_;
        } else {
          valid_ = db_iter_->Valid();
          if (valid_) {
            result_ = value_type(db_iter_->key(), db_iter_->value());
          }
        }
      }
      return *this;
    }

    bool operator==(std::nullptr_t /*other*/) const { return !valid_; }

    bool operator!=(std::nullptr_t /*other*/) const { return valid_; }

    reference operator*() {
      if (!valid_) {
        throw Status::InvalidArgument("Trying to deref invalid iterator");
      }
      return result_;
    }
    reference operator->() {
      if (!valid_) {
        throw Status::InvalidArgument("Trying to deref invalid iterator");
      }
      return result_;
    }

   private:
    Iterator* db_iter_;
    bool valid_;
    Status status_;
    value_type result_;
  };

 private:
  Iterator* db_iter_;
};

// A container object encapsulating the scan ranges for a multi scan.
// It supports an std::input_iterator for a single pass iteration of the
// ScanOptions in scan_opts, which can be dereferenced to get the container
// (Scan) for a single range.
// A Status exception is thrown if there is an error.
class MultiScan {
 public:
  MultiScan(const std::vector<ScanOptions>& scan_opts,
            std::unique_ptr<Iterator>&& db_iter)
      : scan_opts_(scan_opts), db_iter_(std::move(db_iter)) {}

  explicit MultiScan(std::unique_ptr<Iterator>&& db_iter)
      : db_iter_(std::move(db_iter)) {}

  class MultiScanIterator {
   public:
    MultiScanIterator(MultiScanIterator&) = delete;
    MultiScanIterator operator=(MultiScanIterator&) = delete;

    using self_type = MultiScanIterator;
    using value_type = Scan;
    using reference = Scan&;
    using pointer = Scan*;
    using difference_type = int;
    using iterator_category = std::input_iterator_tag;

    MultiScanIterator(const std::vector<ScanOptions>& scan_opts,
                      Iterator* db_iter)
        : scan_opts_(scan_opts), idx_(0), db_iter_(db_iter), scan_(db_iter_) {
      if (scan_opts_.empty()) {
        throw Status::InvalidArgument("Zero scans in multi-scan");
      }
      db_iter_->Seek(*scan_opts_[idx_].range.start);
      status_ = db_iter_->status();
      if (!status_.ok()) {
        throw status_;
      }
    }

    MultiScanIterator(const std::vector<ScanOptions>& scan_opts)
        : scan_opts_(scan_opts),
          idx_(scan_opts_.size()),
          db_iter_(nullptr),
          scan_(nullptr) {}

    ~MultiScanIterator() { assert(status_.ok()); }

    MultiScanIterator& operator++() {
      if (idx_ >= scan_opts_.size()) {
        throw Status::InvalidArgument("Index out of range");
      }
      idx_++;
      if (idx_ < scan_opts_.size()) {
        db_iter_->Seek(*scan_opts_[idx_].range.start);
        status_ = db_iter_->status();
        if (!status_.ok()) {
          throw status_;
        }
      }
      return *this;
    }

    bool operator==(std::nullptr_t /*other*/) const {
      return idx_ >= scan_opts_.size();
    }

    bool operator!=(std::nullptr_t /*other*/) const {
      return idx_ < scan_opts_.size();
    }

    reference operator*() { return scan_; }
    reference operator->() { return scan_; }

   private:
    const std::vector<ScanOptions>& scan_opts_;
    size_t idx_;
    Iterator* db_iter_;
    Status status_;
    Scan scan_;
  };

  MultiScanIterator begin() {
    return MultiScanIterator(scan_opts_, db_iter_.get());
  }

  std::nullptr_t end() { return nullptr; }

 private:
  const std::vector<ScanOptions> scan_opts_;
  std::unique_ptr<Iterator> db_iter_;
};

}  // namespace ROCKSDB_NAMESPACE
