#pragma once

#include "rocksdb/range_delete_filter/range_delete_filter_wrapper.hpp"
#include "rocksdb/range_delete_filter/bucket/bucket.hpp"
#include "rocksdb/range_delete_filter/bucket/bucket_model.hpp"

namespace rangedelete_filter {

inline bool IsPointQuery(const uint64_t& a, const uint64_t& b) {
  return b == (a + 1);
}

class BucketWrapper : public RangeDeleteFliterWrapper {
 public:
  BucketWrapper() = default;
  BucketWrapper(bool is_int_bench) : RangeDeleteFliterWrapper() {}

  ~BucketWrapper() { delete filter_; }

  clock_t Construct(const rd_filter_opt & opts) override;

  bool InsertRange(const uint64_t &left, const uint64_t &right) override {
    return filter_->InsertRange(left, right);
  }

  /* [left, right) */
  bool Query(const uint64_t &key) override {
    return filter_->Query(key);
  }

  bool Query(const uint64_t &left, const uint64_t &right) override {
    if (IsPointQuery(left, right)) {
      return filter_->Query(left);
    }
    return filter_->Query(left, right - 1);
  }

  // auto size() const -> size_t override;

 private:
  bucket_filter::Bucket *filter_;
};

clock_t BucketWrapper::Construct(const rd_filter_opt & opts) {
    size_t bit_per_key = opts.bit_per_key;
    uint64_t num_keys =  opts.num_keys;
    uint64_t num_blocks = opts.num_blocks;
    uint64_t min_key = opts.min_key;
    uint64_t max_key = opts.max_key;
    
    clock_t begin_time = clock();
    filter_ = new bucket_filter::Bucket(bit_per_key, num_keys, num_blocks, min_key, max_key);
    return clock() - begin_time;
}

// auto BucketWrapper::size() const -> size_t { return filter_->size(); }
}  // namespace rdelete_filter