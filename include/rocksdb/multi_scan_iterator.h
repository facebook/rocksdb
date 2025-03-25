#pragma once

#include "rocksdb/db.h"
#include "rocksdb/iterator.h"

namespace ROCKSDB_NAMESPACE {

// Descriptor for a RocksDB scan request. Only forward scans for now.
// We may add other options such as prefix scan in the future.
struct ScanDesc {
  RangeOpt range;
  std::optional<std::unordered_map<std::string, std::string>> property_bag;

  // An unbounded scan with a start key
  ScanDesc(const Slice& _start) : range(_start, OptSlice()) {}

  // A bounded scan with a start key and upper bound
  ScanDesc(const Slice& _start, const Slice& _upper_bound)
      : range(_start, _upper_bound) {}
};

#if 0
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
#endif

// An iterator that returns results from multiple scan ranges. The ranges are
// expected to be in increasing sorted order. The application on top of RocksDB
// would use this as follows -
//
//  std::vector<ScanDesc> scans{{.start = Slice("bar")},
//                              {.start = Slice("foo")}};
//  std::unique_ptr<MultiScanIterator> iter.reset(
//                                      db->NewMultiScanIterator());
//  try {
//    for (auto scan = iter.begin(); scan != iter.end(); ++scan) {
//      for (auto kv = scan.begin(); kv != scan.end(); ++kv) {
//        auto kvpair = *kv;
//        // Do something with key - kv.first
//        // Do something with value - kv.second
//      }
//    }
//  } catch {
//  }
class MultiScanIterator {
 public:
  MultiScanIterator(const std::vector<ScanDesc>& scans,
                    std::unique_ptr<Iterator>&& db_iter)
      : scans_(scans), db_iter_(std::move(db_iter)) {}

  class ScanIterator {
   public:
    class Scan;

    using self_type = ScanIterator;
    using value_type = Scan;
    using reference = Scan&;
    using pointer = Scan*;
    using difference_type = int;
    using iterator_category = std::input_iterator_tag;

    ScanIterator(const std::vector<ScanDesc>& scans, Iterator* db_iter)
        : scans_(scans), idx_(0), db_iter_(db_iter) {
      db_iter_->Seek(*scans_[idx_++].range.start);
      status_ = db_iter_->status();
      if (!status_.ok()) {
        throw status_;
      }
    }

    ScanIterator(const std::vector<ScanDesc>& scans)
        : scans_(scans), idx_(scans_.size()), db_iter_(nullptr) {}

    ScanIterator& operator++() {
      if (idx_ >= scans_.size()) {
        throw Status::InvalidArgument("Index out of range");
      }
      db_iter_->Seek(*scans_[idx_++].range.start);
      status_ = db_iter_->status();
      if (!status_.ok()) {
        throw status_;
      }
      return *this;
    }

    bool operator==(ScanIterator other) const { return idx_ == other.idx_; }

    bool operator!=(ScanIterator other) const { return idx_ != other.idx_; }

    value_type operator*() { return Scan(db_iter_); }

    class Scan {
     public:
      class SingleIterator;

      Scan(Iterator* db_iter) : db_iter_(db_iter) {}

      SingleIterator begin() { return SingleIterator(db_iter_); }

      SingleIterator end() { return SingleIterator(); }

      class SingleIterator {
       public:
        using self_type = SingleIterator;
        using value_type = std::pair<const Slice&, const Slice&>;
        using reference = std::pair<const Slice&, const Slice&>&;
        using pointer = std::pair<const Slice&, const Slice&>*;
        using difference_type = int;
        using iterator_category = std::input_iterator_tag;

        explicit SingleIterator(Iterator* db_iter) : db_iter_(db_iter) {
          valid_ = db_iter_->Valid();
        }

        SingleIterator() : db_iter_(nullptr), valid_(false) {}

        SingleIterator& operator++() {
          if (!valid_) {
            throw Status::InvalidArgument("Trying to advance invalid iterator");
          } else {
            db_iter_->Next();
            status_ = db_iter_->status();
            if (!status_.ok()) {
              throw status_;
            } else {
              valid_ = db_iter_->Valid();
            }
          }
          return *this;
        }

        bool operator==(SingleIterator other) const {
          return valid_ == other.valid_;
        }

        bool operator!=(SingleIterator other) const {
          return valid_ != other.valid_;
        }

        value_type operator*() {
          if (!valid_) {
            throw Status::InvalidArgument("Trying to deref invalid iterator");
          }
          return std::pair<const Slice&, const Slice&>(db_iter_->key(),
                                                       db_iter_->value());
        }

       private:
        Iterator* db_iter_;
        bool valid_;
        Status status_;
      };

     private:
      Iterator* db_iter_;
    };

   private:
    const std::vector<ScanDesc>& scans_;
    size_t idx_;
    Iterator* db_iter_;
    Status status_;
  };

  ScanIterator begin() { return ScanIterator(scans_, db_iter_.get()); }

  ScanIterator end() { return ScanIterator(scans_); }

 private:
  const std::vector<ScanDesc>& scans_;
  std::unique_ptr<Iterator> db_iter_;
};

}  // namespace ROCKSDB_NAMESPACE
