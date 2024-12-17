#pragma once

#include "rocksdb/range_delete_filter/range_delete_filter_wrapper.hpp"
#include "rocksdb/range_delete_filter/fast_snarf/fast_snarf.hpp"
#include "rocksdb/range_delete_filter/range_filter_util.hpp"

namespace rangedelete_filter {
class FastSNARFWrapper : public RangeDeleteFliterWrapper {
 public:
  FastSNARFWrapper() = default;
  FastSNARFWrapper(bool is_int_bench) : RangeDeleteFliterWrapper() {}

  ~FastSNARFWrapper() { delete filter_; }

  // void init(const std::vector<std::string> &argv) override {
  //   bpk_ = strtod(argv[0].c_str(), nullptr);
  //   block_sz_ = strtoul(argv[1].c_str(), nullptr, 10);
  //   element_per_cdf_ = strtoull(argv[2].c_str(), nullptr, 10);
  // }

  clock_t Construct(const rd_filter_opt & opts) override;

  auto InsertRange(const uint64_t &left, const uint64_t &right) -> bool override {
    return filter_->insertrange(left, right);
  }

  /* [left, right) */
  auto Query(const uint64_t &left, const uint64_t &right) -> bool override {
    if (isPointQuery(left, right)) {
      return filter_->query(left);
    }
    return filter_->query(left, right - 1);
  }

  // auto size() const -> size_t override;

 private:
  double bpk_;
  uint32_t block_sz_;
  size_t element_per_cdf_;

  fast_snarf::FastSNARF *filter_;
};

clock_t FastSNARFWrapper::Construct(const rd_filter_opt & opts) {
  clock_t begin_time = clock();
  std::vector<uint64_t> keys;
  filter_ = new fast_snarf::FastSNARF(bpk_, element_per_cdf_, block_sz_, keys);
  return clock() - begin_time;
}

// auto FastSNARFWrapper::size() const -> size_t { return filter_->size(); }
}  // namespace rdelete_filter