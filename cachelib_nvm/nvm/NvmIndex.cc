#include "NvmIndex.hpp"

#include <algorithm>

#include "cachelib_nvm/common/Logging.hpp"
#include "cachelib_nvm/nvm/Utils.hpp"

namespace facebook {
namespace cachelib {
NvmIndex::NvmIndex(HashFunction hashFn)
    : hashFn_{std::move(hashFn)} {
  for (uint32_t i = 0; i < 256; i++) {
    buckets_.emplace_back(Map{30, 60, 90});
  }
  assert(hashFn_);
}

void NvmIndex::insertHash(uint64_t hash, uint32_t offset) {
  // Overwrites existing if found
  auto b = bucket(hash);
  std::lock_guard<std::mutex> lock{mutex_};
  buckets_[b].insert(subkey(hash), offset);
}

void NvmIndex::insert(Buffer key, uint32_t offset) {
  insertHash(keyHash(std::move(key)), offset);
}

void NvmIndex::removeHash(uint64_t hash) {
  auto b = bucket(hash);
  std::lock_guard<std::mutex> lock{mutex_};
  buckets_[b].remove(subkey(hash));
}

void NvmIndex::remove(Buffer key) {
  removeHash(keyHash(std::move(key)));
}

bool NvmIndex::lookup(Buffer key, uint32_t& offset) {
  auto hash = keyHash(std::move(key));
  auto b = bucket(hash);
  std::lock_guard<std::mutex> lock{mutex_};
  return buckets_[b].lookup(subkey(hash), offset);
}

uint64_t NvmIndex::keyHash(Buffer key) const {
  return hashFn_(key.data(), key.size());
}

size_t NvmIndex::computeSize() const {
  size_t size = 0;
  for (size_t i = 0; i < buckets_.size(); i++) {
    std::lock_guard<std::mutex> lock{mutex_};
    size += buckets_[i].size();
  }
  return size;
}

NvmIndex::Stats NvmIndex::collectStats() const {
  Stats stats;
  stats.allocatedMemory = sizeof(*this) + util::memorySize(buckets_);
  for (size_t i = 0; i < buckets_.size(); i++) {
    std::lock_guard<std::mutex> lock{mutex_};
    stats.allocatedMemory += buckets_[i].getMemoryStats().totalAllocated();
    stats.size += buckets_[i].size();
  }
  return stats;
}
}
}
