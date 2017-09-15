#include "NvmAllocator.hpp"

#include <cassert>
#include <chrono>

#include "cachelib_nvm/common/Logging.hpp"
#include "cachelib_nvm/nvm/Utils.hpp"

using std::chrono::high_resolution_clock;
using std::chrono::duration_cast;
using std::chrono::microseconds;
using std::chrono::milliseconds;

namespace facebook {
namespace cachelib {
bool RegionAllocator::allocate(
    uint32_t size,
    uint64_t keyHash,
    uint32_t& offset) {
  assert(rid_.valid());
  if (allocOffset_ + size > regionSize_) {
    return false;
  }
  metadata_->add(keyHash);
  offset = allocOffset_;
  allocOffset_ += size;
  return true;
}

RegionMetadataManager::RegionMetadataManager(
    NvmDevice& device,
    uint64_t regionMetadataSize,
    uint64_t startOffset,
    uint64_t size)
    : device_(device),
      regionMetadataSize_{regionMetadataSize},
      startOffset_{startOffset},
      size_{size} {
#if CACHELIB_LOGGING
  VLOG(1) << "Metadata at offset " << startOffset_ << ", size " << size_
          << ", " << regionMetadataSize_ << " per region";
#endif
}

void RegionMetadataManager::write(RegionId rid, const RegionMetadata& header) {
  // TODO: Make async?
  assert(rid.valid());
  // metadata blocks are written indexed by the region index
  const uint64_t offset = rid.index() * regionMetadataSize_;
  assert(offset < size_);
  CHECK(device_.write(startOffset_ + offset, &header, regionMetadataSize_));
#if CACHELIB_LOGGING
  VLOG(2) << "Write metadata region " << rid.index() << ", " << header.size()
          << " entries";
#endif
}

void RegionMetadataManager::read(RegionId rid, RegionMetadata& header) {
  // TODO: Make async? Does purge costs much more than metadata read?
  assert(rid.valid());
  const uint64_t offset = rid.index() * regionMetadataSize_;
  assert(offset < size_);
  CHECK(device_.read(startOffset_ + offset, &header, regionMetadataSize_));
}

NvmAllocator::NvmAllocator(
    RegionManager& regionManager,
    RegionMetadataManager& regionMetadataManager,
    JobQueue& reclaimJobQueue,
    uint32_t cleanRegionsPool,
    std::vector<uint32_t> sizes,
    uint32_t deviceBlock,
    RemoveIndexEntryCB removeIndexEntry)
    : regionManager_{regionManager},
      regionMetadataManager_{regionMetadataManager},
      reclaimJobQueue_{reclaimJobQueue},
      deviceBlock_{deviceBlock},
      sizes_{std::move(sizes)},
      removeIndexEntry_{std::move(removeIndexEntry)} {
  if (sizes_.empty()) {
#if CACHELIB_LOGGING
    VLOG(1) << "Using stack allocation";
#endif
    allocators_.emplace_back(
        0 /* klass */,
        regionManager_.regionSize());
  } else {
#if CACHELIB_LOGGING
    VLOG(1) << "Using size buckets";
#endif
    for (size_t i = 0; i < sizes_.size(); i++) {
      allocators_.emplace_back(
          i /* klass */,
          regionManager_.regionSize());
    }
  }
  assert(cleanRegionsPool > 0);
  for (uint32_t i = 0; i < cleanRegionsPool; i++) {
    auto rid = regionManager_.getFree();
    CHECK(rid.valid()); // Not enough free regions
    if (rid.valid()) {
      clean_.push_back(rid);
    }
  }
}

OpenStatus NvmAllocator::allocate(
    uint32_t size,
    uint64_t keyHash,
    NvmSlot& slot) {
  uint32_t i = 0;
  if (sizes_.empty()) {
    slot.size = util::alignSize(size, deviceBlock_);
  } else {
    const auto it = std::lower_bound(sizes_.begin(), sizes_.end(), size);
    if (it == sizes_.end()) {
      assert(!"size greater than max size class");
      return {};
    }
    slot.size = *it;
    i = it - sizes_.begin();
  }
  auto status = OpenStatus::Ready;
  while (!allocateWith(allocators_[i], size, keyHash, slot, status)) {
    std::this_thread::yield();
  }
  assert(status != OpenStatus::Ready || slot.rid.valid());
  return status;
}

bool NvmAllocator::allocateWith(
    RegionAllocator& ra,
    uint32_t size,
    uint64_t keyHash,
    NvmSlot& slot,
    OpenStatus& status) noexcept {
  // TODO: Measure this lock
  // Manual lock/unlock generates smaller (so, faster) code. For this fat
  // block this is important.
  mutex_.lock();
  auto rid = ra.getRegion();
  if (!rid.valid()) {
    if (clean_.empty()) {
      mutex_.unlock();
      status = OpenStatus::Retry;
      return true;
    }
    rid = clean_.back();
    clean_.pop_back();
    regionManager_.setKlass(rid, ra.klass());
    // TODO: Is allocation under lock too bad?
    ra.setRegion(rid, regionMetadataManager_.newMetadata());
    mutex_.unlock();
    regionManager_.trackHits(rid);
    // Replace with a reclaimed page
    reclaimJobQueue_.enqueue(
        makeJob([this] { return startReclaim(); }),
        "reclaim");
    return false;
  }
  uint32_t offset{};
  if (!ra.allocate(slot.size, keyHash, offset)) {
    auto md = ra.reset();
    mutex_.unlock();
    // TODO: Account metadata memory in flight
    reclaimJobQueue_.enqueue(makeJob([this, rid, md = std::move(md)] {
      return writeMetadata(rid, *md);
    }), "write_metadata");
    status = OpenStatus::Retry;
    return true;
  }
  status = regionManager_.open(rid, OpenMode::Write);
  mutex_.unlock();
  if (status == OpenStatus::Ready) {
    // Do not close region here - close after write is complete
    slot.rid = rid;
    slot.offset = regionManager_.absoluteOffset(rid, offset);
    regionManager_.traceAllocation(rid, size);
  }
  return true;
}

JobExitCode NvmAllocator::startReclaim() {
  RegionId rid;
  RegionMetadataPtr md;
  {
    std::lock_guard<std::mutex> lock{mutex_};
    rid = regionManager_.getFree();
    if (rid.valid()) {
      clean_.push_back(rid);
      return JobExitCode::Done;
    }
    rid = regionManager_.evict();
    auto& ra = allocators_[regionManager_.getKlass(rid)];
    if (rid == ra.getRegion()) {
      md = ra.reset();
    }
  }
#if CACHELIB_LOGGING
  VLOG(3) << "Evict " << rid.index();
#endif
  reclaimJobQueue_.enqueue(makeJob([this, rid, md = std::move(md)] {
    return purgeRegion(rid, md.get());
  }), "purge");
  return JobExitCode::Done;
}

JobExitCode NvmAllocator::writeMetadata(
    RegionId rid,
    const RegionMetadata& md) {
  {
    std::lock_guard<std::mutex> lock{mutex_};
    if (!regionManager_.tryLock(rid, LockMode::AllowReaders)) {
      return JobExitCode::Reschedule;
    }
  }
  regionMetadataManager_.write(rid, md);
  {
    std::lock_guard<std::mutex> lock{mutex_};
    regionManager_.unlock(rid);
  }
  return JobExitCode::Done;
}

JobExitCode NvmAllocator::purgeRegion(RegionId rid, RegionMetadata* md) {
  // Temporary metadata to read from disk (most likely)
#if CACHELIB_LOGGING
  VLOG(2) << "Purge region " << rid.index();
#endif
  {
    std::lock_guard<std::mutex> lock{mutex_};
    if (!regionManager_.tryLock(rid, LockMode::Exclusive)) {
      return JobExitCode::Reschedule;
    }
  }
#if CACHELIB_LOGGING
  auto start = high_resolution_clock::now();
#endif
  uint32_t entries = 0;
  if (md == nullptr) {
    auto newMD = regionMetadataManager_.newMetadata();
    // TODO: Read by 4k blocks
    regionMetadataManager_.read(rid, *newMD);
    purgeEntries(*newMD);
    entries = newMD->size();
  } else {
    purgeEntries(*md);
    entries = md->size();
  }
  {
    std::lock_guard<std::mutex> lock{mutex_};
    regionManager_.unlock(rid);
    clean_.push_back(rid);
  }
  (void)entries;
#if CACHELIB_LOGGING
  auto dur = duration_cast<microseconds>(high_resolution_clock::now() - start);
  VLOG(2) << "Purge: " << entries << " entries in " << dur.count()
          << " us";
#endif
  return JobExitCode::Done;
}

void NvmAllocator::purgeEntries(const RegionMetadata& md) {
  for (uint32_t i = 0; i < md.size(); i++) {
    // TODO: Send the offset here and verify that we are purging only the index
    // for this offset.
    removeIndexEntry_(md.get(i));
  }
}

OpenStatus NvmAllocator::open(RegionId rid) {
  std::lock_guard<std::mutex> lock{mutex_};
  return regionManager_.open(rid, OpenMode::Read);
}

void NvmAllocator::close(RegionId rid, OpenMode mode) {
  std::lock_guard<std::mutex> lock{mutex_};
  regionManager_.close(rid, mode);
}
}
}
