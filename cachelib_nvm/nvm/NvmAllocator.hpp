#pragma once

#include <cstdlib>
#include <cstring>
#include <algorithm>
#include <memory>
#include <mutex>
#include <new>
#include <vector>

#include "cachelib_nvm/nvm/JobQueue.hpp"
#include "cachelib_nvm/nvm/NvmDevice.hpp"
#include "cachelib_nvm/nvm/RegionManager.hpp"
#include "cachelib_nvm/nvm/Types.hpp"
#include "cachelib_nvm/nvm/Utils.hpp"

namespace facebook {
namespace cachelib {

class RegionMetadata;

using RegionMetadataPtr = std::unique_ptr<
    RegionMetadata,
    util::DestroyDeleter<RegionMetadata>>;

// Every metadata may have variable number of entries, but maximum possible
// slots count allocated.
class CACHELIB_PACKED_ATTR RegionMetadata {
 public:
  static RegionMetadataPtr allocate(size_t bytes) {
    auto memory = std::malloc(bytes);
    // std::memset(memory, 0, bytes);
    return RegionMetadataPtr{new (memory) RegionMetadata()};
  }

  static size_t byteSize(uint32_t slots) {
    return sizeof(RegionMetadata) + slots * sizeof(uint64_t);
  }

  void destroy() {
    this->~RegionMetadata();
    std::free(this);
  }

  void add(uint64_t keyHash) {
    keyHashes_[size_] = keyHash;
    size_++;
  }

  uint64_t get(uint32_t i) const {
    return keyHashes_[i];
  }

  uint32_t size() const {
    return size_;
  }

 private:
  RegionMetadata() = default;
  RegionMetadata(const RegionMetadata&) = delete;
  RegionMetadata(RegionMetadata&&) = delete;
  RegionMetadata&& operator=(const RegionMetadata&) = delete;
  RegionMetadata&& operator=(RegionMetadata&&) = delete;
  ~RegionMetadata() = default;

  uint32_t size_{};
  uint64_t keyHashes_[];
};

// organizes the metadata on the file from the offset for every region.
class RegionMetadataManager {
 public:
  RegionMetadataManager(
      NvmDevice& device,
      uint64_t regionMetadataSize,
      uint64_t startOffset,
      uint64_t size);

  // read the current metadata block and convert it to a vector.
  void read(RegionId id, RegionMetadata& metadata);

  // write the metadata block for a region
  void write(RegionId rid, const RegionMetadata& metadata);

  RegionMetadataPtr newMetadata() const {
    return RegionMetadata::allocate(regionMetadataSize_);
  }

 private:
  NvmDevice& device_;
  const uint64_t regionMetadataSize_;
  const uint64_t startOffset_;
  const uint64_t size_;
};

// class to allocate a particular size from a region
class RegionAllocator {
 public:
  RegionAllocator(uint32_t klass, uint32_t regionSize)
    : klass_{klass},
      regionSize_{regionSize} {}
  RegionAllocator(const RegionAllocator&) = delete;
  RegionAllocator& operator=(const RegionAllocator&) = delete;
  RegionAllocator(RegionAllocator&&) = default;
  RegionAllocator& operator=(RegionAllocator&&) = default;

  bool allocate(uint32_t size, uint64_t keyHash, uint32_t& offset);

  void setRegion(RegionId rid, RegionMetadataPtr md) {
    rid_ = rid;
    allocOffset_ = 0;
    metadata_ = std::move(md);
  }

  RegionId getRegion() const {
    return rid_;
  }

  RegionMetadataPtr reset() {
    auto md = std::move(metadata_);
    rid_ = RegionId{};
    allocOffset_ = 0;
    return md;
  }

  uint32_t klass() const {
    return klass_;
  }

 private:
  // index in the parent's array
  const uint32_t klass_{};

  // region size (bytes)
  const uint32_t regionSize_{};

  // the current region id from which we are allocating
  RegionId rid_;

  // offset of the next allocation
  uint32_t allocOffset_{};

  // accumulated metadata for the current allocation
  RegionMetadataPtr metadata_;
};

// call back that is used to purge a given key hash
using RemoveIndexEntryCB = std::function<void(uint64_t)>;

class NvmAllocator {
 public:
  NvmAllocator(
      RegionManager& regionManager,
      RegionMetadataManager& regionMetadataManager,
      JobQueue& reclaimJobQueue,
      uint32_t cleanRegionsPool,
      std::vector<uint32_t> sizes,
      uint32_t deviceBlock,
      RemoveIndexEntryCB removeIndexEntry);

  OpenStatus allocate(uint32_t size, uint64_t keyHash, NvmSlot& slot);
  // Opens region for reading
  OpenStatus open(RegionId rid);
  void close(RegionId rid, OpenMode mode);

  uint32_t getSizeClass(RegionId rid) const {
    if (sizes_.empty()) {
      return 0;
    } else {
      return sizes_[regionManager_.getKlass(rid)];
    }
  }

 private:
  NvmAllocator(const NvmAllocator&) = delete;
  NvmAllocator(NvmAllocator&&) = delete;
  NvmAllocator& operator=(const NvmAllocator&) = delete;
  NvmAllocator& operator=(NvmAllocator&&) = delete;

  bool allocateWith(
      RegionAllocator& ra,
      uint32_t size,
      uint64_t keyHash,
      NvmSlot& slot,
      OpenStatus& status) noexcept;

  JobExitCode purgeRegion(RegionId rid, RegionMetadata* md);
  JobExitCode startReclaim();
  JobExitCode writeMetadata(RegionId rid, const RegionMetadata& md);
  void purgeEntries(const RegionMetadata& md);

  RegionManager& regionManager_;
  RegionMetadataManager& regionMetadataManager_;
  JobQueue& reclaimJobQueue_;

  const uint32_t deviceBlock_{};

  // sorted vector of sizes to binary search
  const std::vector<uint32_t> sizes_;

  // allocator mutex
  mutable std::mutex mutex_;

  // corresponding RegionAllocators
  std::vector<RegionAllocator> allocators_;

  std::vector<RegionId> clean_;

  // callback to remove cache entry from index
  RemoveIndexEntryCB removeIndexEntry_;
};
}
}
