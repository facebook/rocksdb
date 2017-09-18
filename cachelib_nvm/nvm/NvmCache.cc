#include "NvmCache.hpp"

#include <cstring>
#include <future>

#include "cachelib_nvm/common/Logging.hpp"

namespace facebook {
namespace cachelib {
namespace {
// TODO: Endianness
struct CACHELIB_PACKED_ATTR EntryHeader {
  uint32_t slotSize;
  uint32_t keySize;
  uint32_t valueSize;
};
}

uint64_t NvmCache::Config::getRegionMetadataSize() const {
  // Smallest size needs the most metadata
  uint32_t minSlotSize = 0;
  if (sizeClasses.empty()) {
    minSlotSize = deviceBlock;
  } else {
    minSlotSize = *std::min_element(sizeClasses.begin(), sizeClasses.end());
  }
  const uint64_t rawSize = RegionMetadata::byteSize(regionSize / minSlotSize);
  return util::alignSize(rawSize, deviceBlock);
}

uint64_t NvmCache::Config::getMetadataSize() const {
  return getRegionMetadataSize() * getRegionCount();
}

NvmCache::NvmCache(Config config, RegionManager& regionManager)
    : index_{std::move(config.hash)},
      regionManager_{regionManager},
      regionMetadataManager_{
          *config.device,
          config.getRegionMetadataSize(),
          config.offset,
          config.metadataSize},
      jobQueue_{config.threads, config.maxQueueSize},
      reclaimJobQueue_{config.reclaimThreads, 0},
      allocator_{
          regionManager_,
          regionMetadataManager_,
          reclaimJobQueue_,
          config.cleanRegionsPool,
          config.sizeClasses,
          config.deviceBlock,
          [this](uint64_t keyHash) { removeIndexEntry(keyHash); }},
      device_{std::move(config.device)},
      userDataOffset_{config.offset + config.metadataSize},
      unit_{config.unit},
      deviceBlock_{config.deviceBlock},
      alignedIO_{config.alignedIO},
      disableIO_{config.disableIO} {
  assert(util::isPowTwo(config.deviceBlock));
  assert(config.deviceBlock % 8 == 0);
  assert(util::isPowTwo(config.unit));
  assert(config.unit > 0);
  assert(regionManager_.regionSize() % config.unit == 0);
  if (config.metadataSize < config.getMetadataSize()) {
    throw std::invalid_argument("metadata space is too small");
  }
  if (config.regionSize != regionManager_.regionSize()) {
    throw std::invalid_argument("region size mismatch");
  }
  for (auto& sc : config.sizeClasses) {
    if (sc % unit_ != 0 || sc > regionManager_.regionSize()) {
      throw std::invalid_argument("size class is not multiple of unit size");
    }
  }
}

uint32_t NvmCache::serializedSize(uint32_t keySize, uint32_t valueSize) {
  return sizeof(EntryHeader) + keySize + valueSize;
}

void NvmCache::insert(Buffer key, Buffer value) {
  jobQueue_.enqueue(makeJob([this, k = std::move(key), v = std::move(value)] {
    return doInsert(k.wrap(), v.wrap());
  }), "insert");
}

JobExitCode NvmCache::doInsert(Buffer key, Buffer value) {
  auto ss = serializedSize(key.size(), value.size());
  if (ss > regionManager_.regionSize()) {
    assert(!"value is larger then region");
    return JobExitCode::Done;
  }
  NvmSlot slot;
  switch (allocator_.allocate(ss, index_.keyHash(key.wrap()), slot)) {
    case OpenStatus::Ready:
      break;
    case OpenStatus::Retry:
      return JobExitCode::Reschedule;
  }
  if (disableIO_ || writeEntry(slot, key.wrap(), value.wrap())) {
    index_.insert(key.wrap(), slot.offset / unit_);
  }
  allocator_.close(slot.rid, OpenMode::Write);
  return JobExitCode::Done;
}

Buffer NvmCache::lookup(Buffer key) {
  Buffer buf;
  // TODO: Back to folly::Baton
  std::promise<void> done;
  jobQueue_.enqueue(makeJob([this, k = std::move(key), &buf, &done] {
    auto exitCode = doLookup(k.wrap(), buf);
    if (exitCode == JobExitCode::Done) {
      done.set_value();
    }
    return exitCode;
  }), "lookup");
  done.get_future().wait();
  return buf;
}

JobExitCode NvmCache::doLookup(Buffer key, Buffer& value) {
  uint32_t offset = 0;
  auto found = false;
  if (index_.lookup(key.wrap(), offset)) {
    NvmSlot slot;
    slot.offset = offset * unit_;
    slot.rid = regionManager_.offsetToRegion(slot.offset);
    switch (allocator_.open(slot.rid)) {
      case OpenStatus::Ready:
        slot.size = allocator_.getSizeClass(slot.rid);
        found = disableIO_ || readEntry(slot, key.wrap(), value);
        allocator_.close(slot.rid, OpenMode::Read);
        if (found) {
          regionManager_.recordHit(slot.rid);
        }
        break;
      case OpenStatus::Retry:
        return JobExitCode::Reschedule;
    }
  }
  if (allocator_.getReclaimCount() == 0) {
    if (found) {
      hitCount_.fetch_add(1, std::memory_order::memory_order_relaxed);
    } else {
      missCount_.fetch_add(1, std::memory_order::memory_order_relaxed);
    }
  }
  return JobExitCode::Done;
}

bool NvmCache::exists(Buffer key) {
  uint32_t dummy{};
  return index_.lookup(key.wrap(), dummy);
}

void NvmCache::remove(Buffer key) {
  jobQueue_.enqueue(makeJob([this, k = std::move(key)] {
    return doRemove(k.wrap());
  }), "remove");
}

JobExitCode NvmCache::doRemove(Buffer key) {
  index_.remove(key.wrap());
  return JobExitCode::Done;
}

// purge a region by removing all its keys from the index
void NvmCache::removeIndexEntry(uint64_t keyHash) {
  index_.removeHash(keyHash);
}

bool NvmCache::isAligned(uint64_t offset) const {
  return !alignedIO_ || offset % deviceBlock_ == 0;
}

bool NvmCache::boundsCheck(NvmSlot slot) const {
  uint64_t rs{regionManager_.regionSize()};
  auto i = slot.rid.index();
  return i * rs <= slot.offset && slot.offset + slot.size <= (i + 1) * rs;
}

bool NvmCache::writeEntry(NvmSlot slot, Buffer key, Buffer value) const {
  assert(serializedSize(key.size(), value.size()) <= slot.size);
  // Key must fit in one device block for mixed size region allocations
  // (see readEntry).
  assert(serializedSize(key.size(), 0) <= deviceBlock_);
  auto absOffset = getAbsOffset(slot.offset);
  assert(isAligned(absOffset));
  assert(boundsCheck(slot));
  assert(isAligned(slot.size));

  // TODO: Avoid copy if possible, store buffers?
  Buffer buf{slot.size};
  EntryHeader header;
  header.slotSize = slot.size;
  header.keySize = key.size();
  header.valueSize = value.size();
  auto p = buf.data();
  std::memcpy(p, &header, sizeof header);
  p += sizeof header;
  std::memcpy(p, key.data(), key.size());
  p += key.size();
  std::memcpy(p, value.data(), value.size());
  return device_->write(absOffset, buf.data(), buf.size());
}

bool NvmCache::readEntry(NvmSlot slot, Buffer key, Buffer& value) const {
  auto absOffset = getAbsOffset(slot.offset);
  assert(isAligned(absOffset));
  assert(boundsCheck(slot));

  uint32_t bufSize = slot.size == 0 ? deviceBlock_ : slot.size;
  assert(isAligned(bufSize));
  // If we read the smallest possible buffer (device block) then we don't check
  // region bounds. If the buffer is bigger, we have to clamp to the region's
  // boundary: std::min(bufSize, <regionEnd> - offset).
  Buffer buf{bufSize};
  if (!device_->read(absOffset, buf.data(), buf.size())) {
    return false;
  }
  uint32_t byteOfs = 0;
  auto header = *reinterpret_cast<EntryHeader*>(buf.data());
  byteOfs += sizeof header;
  if (header.keySize != key.size() ||
      std::memcmp(buf.data() + byteOfs, key.data(), key.size()) != 0) {
    return false;
  }
  byteOfs += header.keySize;
  if (slot.size == 0 && buf.size() < header.slotSize) {
    // TODO: Optimize this. Re-read now.
    buf = Buffer(header.slotSize);
    if (!device_->read(absOffset, buf.data(), buf.size())) {
      return false;
    }
  }
  // TODO: Direct read into @value
  value = buf.copy(byteOfs, header.valueSize);
  return true;
}

NvmCache::Stats NvmCache::collectStats() const {
  Stats stats;
  stats.index = index_.collectStats();
  stats.regionManager = regionManager_.collectStats();
  stats.hit = hitCount_.load(std::memory_order::memory_order_relaxed);
  stats.miss = missCount_.load(std::memory_order::memory_order_relaxed);
  return stats;
}
}
}
