#pragma once

#include <algorithm>
#include <cinttypes>
#include <cstdint>
#include <cassert>
#include <cstring>
#include <fstream>
#include <mutex>
#include <random>
#include <set>
#include <string>
#include <vector>
#include <tuple>

namespace rangedelete_filter {

struct rd_filter_opt {
  size_t bit_per_key;
  uint64_t num_keys;
  uint64_t num_blocks;
  uint64_t min_key;
  uint64_t max_key;
};

inline bool IsPointQuery(const uint64_t& a, const uint64_t& b) {
  return b == (a + 1);
}

inline bool IsPointQuery(const std::string& a, const std::string& b) {
  return b == a;
}

// For RocksDB
uint64_t sliceToUint64(const char* data) {
  uint64_t out = 0ULL;
  memcpy(&out, data, 8);
  return __builtin_bswap64(out);
}

std::string util_uint64ToString(const uint64_t& word) {
  uint64_t endian_swapped_word = __builtin_bswap64(word);
  return std::string(reinterpret_cast<const char*>(&endian_swapped_word), 8);
}

uint64_t util_stringToUint64(const std::string& str_word) {
  uint64_t int_word = 0;
  memcpy(reinterpret_cast<char*>(&int_word), str_word.data(), 8);
  return __builtin_bswap64(int_word);
}

}  // namespace rdelete_filter
