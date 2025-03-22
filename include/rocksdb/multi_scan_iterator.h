#pragma once

#include "rocksdb/iterator.h"

namespace ROCKSDB_NAMESPACE {

// Descriptor for a RocksDB scan request. Only forward scans for now.
// We may add other options such as prefix scan in the future.
struct ScanDesc {
  Slice start;
  Slice upper_bound;
  std::optional<std::unordered_map<std::string, std::string>> property_bag;

  // An unbounded scan with a start key
  ScanDesc(const Slice& _start) : start(_start) {}

  // A bounded scan with a start key and upper bound
  ScanDesc(const Slice& _start, const Slice& _upper_bound)
      : start(_start), upper_bound(_upper_bound) {}
};

// An iterator that returns results from multiple scan ranges. The ranges are
// expected to be in increasing sorted order. The application on top of RocksDB
// would use this as follows -
//
//  std::vector<ScanDesc> scans{{.start = Slice("foo")},
//                              {.start = Slice("bar")}};
//  std::unique_ptr<MultiScanIterator> iter.reset(
//                                      db->NewMultiScanIterator());
//  for (auto& scan : scans) {
//    PinnableSlice val;
//    while (iter-status().ok() && !iter->empty()) {
//      Slice key;
//      std::string value;
//      iter->Dequeue(key, val);
//      if (val.IsPinned()) {
//        val_str = val.ToString();
//      } else {
//        val_str = std::move(*val.GetSelf());
//      }
//      val.Reset();
//      // Do something with key and val_str
//    }
//    if (!iter->status().ok()) {
//      break;
//    }
//    assert(iter->empty());
//    iter->SeekNext();
//  }
//  assert(!iter->status().ok() || iter->empty());
//
class MultiScanIterator {
 public:
  MultiScanIterator(const std::vector<ScanDesc>& scans,
                    std::unique_ptr<Iterator>&& iter)
      : scans_(scans), idx_(0), iter_(std::move(iter)) {
    // Position the iterator for the first scan
    NextScan();
  }

  void Dequeue(Slice& key, PinnableSlice& value) {
    key = iter_->key();
    value.PinSelf(iter_->value());
    iter_->Next();
    if (!iter_->Valid()) {
      empty_ = true;
    }
    status_ = iter_->status();
  }

  void SeekNext() {
    idx_++;
    if (idx_ < scans_.size()) {
      NextScan();
    } else {
      empty_ = true;
    }
  }

  bool empty() { return empty_; }

  Status status() { return status_; }

 private:
  const std::vector<ScanDesc>& scans_;
  size_t idx_;
  std::unique_ptr<Iterator> iter_;
  bool empty_;
  Status status_;

  void NextScan() {
    iter_->Seek(scans_[idx_].start);
    empty_ = !iter_->Valid();
    status_ = iter_->status();
  }
};

}  // namespace ROCKSDB_NAMESPACE
