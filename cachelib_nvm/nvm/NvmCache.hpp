#pragma once

#include <memory>
#include <stdexcept>
#include <vector>

#include "cachelib_nvm/common/Buffer.hpp"
#include "cachelib_nvm/nvm/JobQueue.hpp"
#include "cachelib_nvm/nvm/LruRegions.hpp"
#include "cachelib_nvm/nvm/NvmAllocator.hpp"
#include "cachelib_nvm/nvm/NvmIndex.hpp"
#include "cachelib_nvm/nvm/RegionManager.hpp"

namespace facebook {
namespace cachelib {
// Assumptions:
//  - Device block (Config::deviceBlock) is atomic
//  - Size of any key + 16 bytes must not be greater than device block size
//    for stack allocation (no size classes).
//  - Async insert/lookup/remove assumes Buffer lifetime is managed
//    by the client and valid until the operation is done.
class NvmCache {
 public:
  // NvmCache will move data from config
  struct Config {
    // Key hash function. Just a pointer is faster than std::function.
    HashFunction hash{};
    std::unique_ptr<NvmDevice> device;
    // Offset of start on the device.
    uint64_t offset{};
    // Size in bytes for allocations is given by @size, size that can be used
    // for metadata is given by @metadataSize. Total, device/file must have
    // at least @size + @metadataSize in capacity. NvmCache constructor
    // throws if there is not enough metadata space to serve given size (less
    // than getRequiredMetadataSize()).
    uint64_t metadataSize{};
    uint64_t userDataSize{};
    uint32_t regionSize{64 * 1024 * 1024};
    // @unit is granularity at which offsets and read/writes are measured.
    // Usually, it matches block size (minimal size of IO with the device),
    // but can be multiple of it.
    uint32_t unit{1024};
    uint32_t deviceBlock{1024};
    std::vector<uint32_t> sizeClasses;
    // Size of message queue thread pool.
    uint32_t threads{1};
    uint32_t maxQueueSize{25'000};
    // Regions reclaim threads.
    uint32_t reclaimThreads{1};
    // How many clean regions GC should (try to) maintain in the pool.
    uint32_t cleanRegionsPool{2};
    // If true, IO opeartions to file must be aligned on @deviceBlock. This
    // matters when operate on the raw device. For regular file, OS takes
    // care of it.
    bool alignedIO{false};
    // Do not do real IO to the device (only metadata).
    bool disableIO{false};

    uint64_t getMetadataSize() const;
    uint64_t getRegionMetadataSize() const;

    uint64_t getTotalSize() const {
      return metadataSize + userDataSize;
    }

    uint32_t getRegionCount() const {
      return userDataSize / regionSize;
    }
  };

  static uint32_t serializedSize(uint32_t keySize, uint32_t valueSize);

  NvmCache(Config config, RegionManager& regionManager);
  NvmCache(const NvmCache&) = delete;
  NvmCache& operator=(const NvmCache&) = delete;
  ~NvmCache() = default;

  // Asynchronously inserts entry into the cache. All buffers have lifetimes
  // to last till insert done (do Buffer::copy if needed).
  //
  // Use @exists to check if this entry appeared in the cache (for tests).
  //
  // TODO: In the future, we may have JobQueue where we will schedule callback
  // jobs (@insert will take optional Job* argument).
  void insert(Buffer key, Buffer value);

  // Synchronously looks entry up
  // TODO: For async mode, we may apply same idea of response job queue
  Buffer lookup(Buffer key);

  // Async remove
  void remove(Buffer key);

  // Checks if entry is present
  bool exists(Buffer key);

  size_t computeIndexSize() const {
    return index_.computeSize();
  }

  // For testing purposes only: pauses message queue
  void pause(bool stop) {
    jobQueue_.pause(stop);
  }

  // Stops message queue, but executes all outstanding messages before
  void drainJobQueue() {
    jobQueue_.drain();
    reclaimJobQueue_.drain();
  }

  struct Stats {
    NvmIndex::Stats index;
    RegionManager::Stats regionManager;
    uint64_t hit{};
    uint64_t miss{};
    uint64_t updatesIgnored{};
  };
  Stats collectStats() const;

 private:
  JobExitCode doInsert(Buffer key, Buffer value);
  JobExitCode doLookup(Buffer key, Buffer& value);
  JobExitCode doRemove(Buffer key);

  bool writeEntry(NvmSlot slot, Buffer key, Buffer value) const;
  bool readEntry(NvmSlot slot, Buffer key, Buffer& value) const;

  uint64_t getAbsOffset(uint64_t relOffset) const {
    return userDataOffset_ + relOffset;
  }

  void removeIndexEntry(uint64_t keyHash);
  bool isAligned(uint64_t offset) const;
  bool boundsCheck(NvmSlot slot) const;

  // index for the nvm
  NvmIndex index_;

  // TODO: Move inside NvmAllocator
  RegionManager& regionManager_;

  // manages the metadata for the regions
  RegionMetadataManager regionMetadataManager_;

  // async job queue
  JobQueue jobQueue_;
  JobQueue reclaimJobQueue_;

  // main allocator for the nvm
  NvmAllocator allocator_;

  std::unique_ptr<NvmDevice> device_;

  const uint64_t userDataOffset_{};
  const uint64_t unit_{};
  const uint32_t deviceBlock_{};
  const bool alignedIO_{};
  const bool disableIO_{};

  uint64_t hitCount_{};
  uint64_t missCount_{};
  uint64_t updatesIgnored_{};
};
}
}
