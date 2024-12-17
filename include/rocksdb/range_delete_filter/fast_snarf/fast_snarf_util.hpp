#ifndef FAST_SNARF_UTILL_HPP
#define FAST_SNARF_UTILL_HPP

#include <cstdint>
#include <vector>

namespace fast_snarf {

inline auto align_bit_to_byte(size_t size) -> size_t {
  return (size + 7U) >> 3;
}

inline auto min(uint64_t l, uint64_t r) -> uint64_t { return l > r ? r : l; }

inline auto max(uint64_t l, uint64_t r) -> uint64_t { return l > r ? l : r; }

inline auto align_bit2byte(uint64_t b_size) -> size_t {
  return (b_size + 7ULL) >> 3;
};

}  // namespace fast_snarf

#endif