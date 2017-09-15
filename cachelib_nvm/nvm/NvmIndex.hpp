#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <vector>
#include <utility>

#include "cachelib_nvm/common/Buffer.hpp"
#include "cachelib_nvm/nvm/BTree.hpp"
#include "cachelib_nvm/nvm/Types.hpp"

namespace facebook {
namespace cachelib {
class NvmIndex {
 public:
  static constexpr size_t kStatTop = 10;

  explicit NvmIndex(HashFunction hashFn);
  // TODO: Copy/Move
  void insert(Buffer key, uint32_t offset);
  void insertHash(uint64_t hash, uint32_t offset);
  void remove(Buffer key);
  void removeHash(uint64_t hash);
  bool lookup(Buffer key, uint32_t& offset);
  uint64_t keyHash(Buffer key) const;
  size_t computeSize() const;

  struct Stats {
    size_t allocatedMemory{};
    size_t size{};
  };
  Stats collectStats() const;

 private:
  using Map = BTree<uint32_t, uint32_t>;

  static uint32_t bucket(uint64_t hash) {
    return hash >> (64 - 8);
  }

  static uint32_t subkey(uint64_t hash) {
    return hash & 0xffffffffu;
  }

  HashFunction hashFn_{};
  // Surprisingly, bucketing mutex doens't improve throughput benchmark.
  // Cache coherency issues?
  mutable std::mutex mutex_;
  std::vector<Map> buckets_;
};
}
}
