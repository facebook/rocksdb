#pragma once

#include <cstdint>
#include <set>
#include <string>
#include <tuple>
#include <vector>
#include <algorithm>

namespace rangedelete_filter {

struct rd_filter_opt {
  size_t bit_per_key;
  uint64_t num_keys;
  uint64_t num_blocks;
  uint64_t min_key;
  uint64_t max_key;
};

class RangeDeleteFliterWrapper {
 public:
  /** Set the int bench as default option */
  // RangeDeleteFliterWrapper() : is_int_bench_(true) {}
  RangeDeleteFliterWrapper() = default;
  // RangeDeleteFliterWrapper(bool is_int_bench) : is_int_bench_(is_int_bench) {}

  virtual ~RangeDeleteFliterWrapper() = default;

  // virtual void init(const std::vector<std::string> &argv) {}
  // virtual void init(const std::vector<std::pair<uint64_t, uint64_t>> &queries,
  //                   const std::vector<std::string> &argv) {}

  virtual clock_t Construct(const rd_filter_opt & opts) = 0;

  virtual bool InsertRange(const uint64_t &left, const uint64_t &right) = 0;

  /* [left, right) */
  virtual bool Query(const uint64_t &key) = 0;
  virtual bool Query(const uint64_t &left, const uint64_t &right) = 0;

  // virtual auto size() const -> size_t = 0;

 public:
  bool is_int_bench_;
};


}  // namespace rdelete_filter
