// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(LIBZBD)

#include "env/zbd_zenfs.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <libzbd/zbd.h>
#include <linux/blkzoned.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <string>
#include <utility>
#include <vector>

#include "env/io_zenfs.h"
#include "rocksdb/env.h"

/* Number of reserved zones for metadata
 * Two non-offline meta zones are needed to be able
 * to roll the metadata log safely. One extra
 * is allocated to cover for one zone going offline.
 */
#define ZENFS_META_ZONES (3)

/* Minimum of number of zones that makes sense */
#define ZENFS_MIN_ZONES (32)

namespace ROCKSDB_NAMESPACE {

Zone::Zone(ZonedBlockDevice *zbd, struct zbd_zone *z)
    : zbd_(zbd),
      start_(zbd_zone_start(z)),
      max_capacity_(zbd_zone_capacity(z)),
      wp_(zbd_zone_wp(z)),
      open_for_write_(false) {
  lifetime_ = Env::WLTH_NOT_SET;
  used_capacity_ = 0;
  capacity_ = 0;
  if (!(zbd_zone_full(z) || zbd_zone_offline(z) || zbd_zone_rdonly(z)))
    capacity_ = zbd_zone_capacity(z) - (zbd_zone_wp(z) - zbd_zone_start(z));
}

bool Zone::IsUsed() { return (used_capacity_ > 0) || open_for_write_; }
uint64_t Zone::GetCapacityLeft() { return capacity_; }
bool Zone::IsFull() { return (capacity_ == 0); }
bool Zone::IsEmpty() { return (wp_ == start_); }
uint64_t Zone::GetZoneNr() { return start_ / zbd_->GetZoneSize(); }

/* Close a zone that has been written to, to decrease the open zone
   count in the zoned block device and to allow other files to write
   to it */
void Zone::CloseWR() {
  assert(open_for_write_);
  open_for_write_ = false;

  if (Close().ok()) {
    zbd_->NotifyIOZoneClosed();
  }

  if (capacity_ == 0) zbd_->NotifyIOZoneFull();
}

/* Reset a zone and check the state after the reset - the capacity
   might have changed or the zone gone bad */
IOStatus Zone::Reset() {
  size_t zone_sz = zbd_->GetZoneSize();
  int fd = zbd_->GetWriteFD();
  unsigned int report = 1;
  struct zbd_zone z;
  int ret;

  assert(!IsUsed());

  ret = zbd_reset_zones(fd, start_, zone_sz);
  if (ret) return IOStatus::IOError("Zone reset failed\n");

  ret = zbd_report_zones(fd, start_, zone_sz, ZBD_RO_ALL, &z, &report);

  if (ret || (report != 1)) return IOStatus::IOError("Zone report failed\n");

  if (zbd_zone_offline(&z))
    capacity_ = 0;
  else
    max_capacity_ = capacity_ = zbd_zone_capacity(&z);

  wp_ = start_;
  lifetime_ = Env::WLTH_NOT_SET;

  return IOStatus::OK();
}

/* Finish a zone and set the available write capacity to 0 */
IOStatus Zone::Finish() {
  size_t zone_sz = zbd_->GetZoneSize();
  int fd = zbd_->GetWriteFD();
  int ret;

  assert(!open_for_write_);

  ret = zbd_finish_zones(fd, start_, zone_sz);
  if (ret) return IOStatus::IOError("Zone finish failed\n");

  capacity_ = 0;
  wp_ = start_ + zone_sz;

  return IOStatus::OK();
}

/* Close a zone */
IOStatus Zone::Close() {
  size_t zone_sz = zbd_->GetZoneSize();
  int fd = zbd_->GetWriteFD();
  int ret;

  assert(!open_for_write_);

  if (!(IsEmpty() || IsFull())) {
    ret = zbd_close_zones(fd, start_, zone_sz);
    if (ret) return IOStatus::IOError("Zone close failed\n");
  }

  return IOStatus::OK();
}

/* Write a number of logical blocks at the zone write pointer*/
IOStatus Zone::Append(char *data, uint32_t size) {
  char *ptr = data;
  uint32_t left = size;
  int fd = zbd_->GetWriteFD();
  int ret;

  if (capacity_ < size)
    return IOStatus::NoSpace("Not enough capacity for append");

  assert((size % zbd_->GetBlockSize()) == 0);

  while (left) {
    ret = pwrite(fd, ptr, size, wp_);
    if (ret < 0) return IOStatus::IOError("Write failed");

    ptr += ret;
    wp_ += ret;
    capacity_ -= ret;
    left -= ret;
  }

  return IOStatus::OK();
}

ZoneExtent::ZoneExtent(uint64_t start, uint32_t length, Zone *zone)
    : start_(start), length_(length), zone_(zone) {}

/* Look up the zone which the adress belongs to */
Zone *ZonedBlockDevice::GetIOZone(uint64_t offset) {
  for (const auto z : io_zones)
    if (z->start_ <= offset && offset < (z->start_ + zone_sz_)) return z;
  return nullptr;
}

ZonedBlockDevice::ZonedBlockDevice(std::string bdevname,
                                   std::shared_ptr<Logger> logger)
    : filename_("/dev/" + bdevname), logger_(logger) {
  read_f_ = read_direct_f_ = write_f_ = -1;
  Info(logger_, "New Zoned Block Device: %s", filename_.c_str());
};

void ZonedBlockDevice::CloseFDs() {
  zbd_close(read_f_);
  zbd_close(read_direct_f_);
  zbd_close(write_f_);
  read_f_ = read_direct_f_ = write_f_ = -1;
}

/* Open the zoned block device, set up read and write file descriptors
   and populate the metadata and io zone lists */
IOStatus ZonedBlockDevice::Open(bool readonly) {
  struct zbd_zone *zone_rep = nullptr;
  unsigned int reported_zones;
  size_t addr_space_sz;
  zbd_info info;
  Status s;
  uint64_t i = 0;
  uint64_t m = 0;
  int ret;

  read_f_ = zbd_open(filename_.c_str(), O_RDONLY, &info);
  if (read_f_ < 0) {
    return IOStatus::InvalidArgument("Failed to open zoned block device");
  }

  read_direct_f_ = zbd_open(filename_.c_str(), O_RDONLY, &info);
  if (read_direct_f_ < 0) {
    CloseFDs();
    return IOStatus::InvalidArgument("Failed to open zoned block device");
  }

  if (!readonly) {
    write_f_ = zbd_open(filename_.c_str(), O_WRONLY | O_DIRECT, &info);
    if (write_f_ < 0) {
      CloseFDs();
      return IOStatus::InvalidArgument("Failed to open zoned block device");
    }
  }

  if (info.model != ZBD_DM_HOST_MANAGED) {
    CloseFDs();
    return IOStatus::NotSupported("Not a host managed block device");
  }

  if (info.nr_zones < ZENFS_MIN_ZONES) {
    CloseFDs();
    return IOStatus::NotSupported(
        "Too few zones on the zoned block device (32 required)");
  }

  block_sz_ = info.pblock_size;
  zone_sz_ = info.zone_size;
  nr_zones_ = info.nr_zones;

  /* We need one open zone for meta data writes, the rest can be used for files
   */
  if (info.max_nr_active_zones == 0)
    max_nr_active_io_zones_ = info.nr_zones;
  else
    max_nr_active_io_zones_ = info.max_nr_active_zones - 1;

  if (info.max_nr_open_zones == 0)
    max_nr_open_io_zones_ = info.nr_zones;
  else
    max_nr_open_io_zones_ = info.max_nr_open_zones - 1;

  Info(logger_, "Zone block device nr zones: %u max active: %u max open: %u \n",
       info.nr_zones, info.max_nr_active_zones, info.max_nr_open_zones);

  addr_space_sz = (uint64_t)nr_zones_ * zone_sz_;

  ret = zbd_list_zones(read_f_, 0, addr_space_sz, ZBD_RO_ALL, &zone_rep,
                       &reported_zones);

  if (ret || reported_zones != nr_zones_) {
    CloseFDs();
    free(zone_rep);
    Error(logger_, "Failed to list zones, err: %d", ret);
    return IOStatus::IOError("Failed to list zones");
  }

  while (m < ZENFS_META_ZONES && i < reported_zones) {
    struct zbd_zone *z = &zone_rep[i++];
    /* Only use sequential write required zones */
    if (zbd_zone_type(z) == ZBD_ZONE_TYPE_SWR) {
      if (!zbd_zone_offline(z)) {
        meta_zones.push_back(new Zone(this, z));
      }
      m++;
    }
  }

  active_io_zones_ = 0;
  open_io_zones_ = 0;

  for (; i < reported_zones; i++) {
    struct zbd_zone *z = &zone_rep[i];
    /* Only use sequential write required zones */
    if (zbd_zone_type(z) == ZBD_ZONE_TYPE_SWR) {
      if (!zbd_zone_offline(z)) {
        Zone *newZone = new Zone(this, z);
        io_zones.push_back(newZone);
        if (zbd_zone_imp_open(z) || zbd_zone_exp_open(z) ||
            zbd_zone_closed(z)) {
          active_io_zones_++;
          if (zbd_zone_imp_open(z) || zbd_zone_exp_open(z)) {
            if (!readonly) {
              newZone->Close();
            }
          }
        }
      }
    }
  }

  free(zone_rep);
  start_time_ = time(NULL);

  return IOStatus::OK();
}

/* Notify that a zone has been filled, this will allow the zone
   allocator to allocate a new zone if the maximum number of active
   zones was previously reached */
void ZonedBlockDevice::NotifyIOZoneFull() {
  const std::lock_guard<std::mutex> lock(zone_resources_mtx_);
  active_io_zones_--;
  zone_resources_.notify_one();
}

/* Notify that a zone has been closed, this will allow the zone
   allocator to allocate a zone if the max number of open zones
   was previously reached */
void ZonedBlockDevice::NotifyIOZoneClosed() {
  const std::lock_guard<std::mutex> lock(zone_resources_mtx_);
  open_io_zones_--;
  zone_resources_.notify_one();
}

/* Return the number of free bytes in the filesystem  -
   the sum of available io zone capacities */
uint64_t ZonedBlockDevice::GetFreeSpace() {
  uint64_t free = 0;
  for (const auto z : io_zones) {
    free += z->capacity_;
  }

  return free;
}

/* Calculate and log disk and zone statistics */
void ZonedBlockDevice::LogZoneStats() {
  uint64_t used_capacity = 0;
  uint64_t reclaimable_capacity = 0;
  uint64_t reclaimables_max_capacity = 0;
  uint64_t active = 0;
  io_zones_mtx.lock();

  for (const auto z : io_zones) {
    used_capacity += z->used_capacity_;

    if (z->used_capacity_) {
      reclaimable_capacity += z->max_capacity_ - z->used_capacity_;
      reclaimables_max_capacity += z->max_capacity_;
    }

    if (!(z->IsFull() || z->IsEmpty())) active++;
  }

  if (reclaimables_max_capacity == 0) reclaimables_max_capacity = 1;

  Info(logger_,
       "[Zonestats:time(s),used_cap(MB),reclaimable_cap(MB), "
       "avg_reclaimable(%%), active(#), active_zones(#), open_zones(#)] %ld "
       "%lu %lu %lu %lu %ld %ld\n",
       time(NULL) - start_time_, used_capacity / (1024 * 1024),
       reclaimable_capacity / (1024 * 1024),
       100 * reclaimable_capacity / reclaimables_max_capacity, active,
       active_io_zones_.load(), open_io_zones_.load());

  io_zones_mtx.unlock();
}

/* Log used capacity across all zones */
void ZonedBlockDevice::LogZoneUsage() {
  for (const auto z : io_zones) {
    int64_t used = z->used_capacity_;

    if (used > 0) {
      Debug(logger_, "Zone 0x%lX used capacity: %ld bytes (%ld MB)\n",
            z->start_, used, used / (1024 * 1024));
    }
  }
}

ZonedBlockDevice::~ZonedBlockDevice() {
  for (const auto z : meta_zones) {
    delete z;
  }

  for (const auto z : io_zones) {
    delete z;
  }

  CloseFDs();
}

#define LIFETIME_DIFF_NOT_GOOD (100)

/* Calculate life time diff for a zone vs a file - file life time
   should be smaller not to prolong the life of the data already written */
unsigned int GetLifeTimeDiff(Env::WriteLifeTimeHint zone_lifetime,
                             Env::WriteLifeTimeHint file_lifetime) {
  assert(file_lifetime <= Env::WLTH_EXTREME);

  if ((file_lifetime == Env::WLTH_NOT_SET) ||
      (file_lifetime == Env::WLTH_NONE)) {
    if (file_lifetime == zone_lifetime) {
      return 0;
    } else {
      return LIFETIME_DIFF_NOT_GOOD;
    }
  }

  if (zone_lifetime > file_lifetime) return zone_lifetime - file_lifetime;

  return LIFETIME_DIFF_NOT_GOOD;
}

/* Allocate an unused metadata zone */
Zone *ZonedBlockDevice::AllocateMetaZone() {
  for (const auto z : meta_zones) {
    /* If the zone is not used, reset and use it */
    if (!z->IsUsed()) {
      if (!z->IsEmpty()) {
        if (!z->Reset().ok()) {
          Warn(logger_, "Failed resetting zone!");
          continue;
        }
      }
      return z;
    }
  }
  return nullptr;
}

/* Reset any IO zones are not empty and does not have any valid data */
void ZonedBlockDevice::ResetUnusedIOZones() {
  const std::lock_guard<std::mutex> lock(zone_resources_mtx_);
  /* Reset any unused zones */
  for (const auto z : io_zones) {
    if (!z->IsUsed() && !z->IsEmpty()) {
      if (!z->IsFull()) active_io_zones_--;
      if (!z->Reset().ok()) Warn(logger_, "Failed reseting zone");
    }
  }
}

/* Allocate the best fitting zone while keeping within the open and active
   zone limits */
Zone *ZonedBlockDevice::AllocateZone(Env::WriteLifeTimeHint file_lifetime) {
  Zone *allocated_zone = nullptr;
  Zone *finish_victim = nullptr;
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  int new_zone = 0;
  Status s;

  io_zones_mtx.lock();

  /* Make sure we are below the zone open limit */
  {
    std::unique_lock<std::mutex> lk(zone_resources_mtx_);
    zone_resources_.wait(lk, [this] {
      if (open_io_zones_.load() < max_nr_open_io_zones_) return true;
      return false;
    });
  }

  /* Reset any unused zones and finish used zones under capacity treshold*/
  for (const auto z : io_zones) {
    if (z->open_for_write_ || z->IsEmpty() || (z->IsFull() && z->IsUsed()))
      continue;

    if (!z->IsUsed()) {
      if (!z->IsFull()) active_io_zones_--;
      s = z->Reset();
      if (!s.ok()) {
        Debug(logger_, "Failed resetting zone !");
      }
      continue;
    }

    if ((z->capacity_ < (z->max_capacity_ * finish_threshold_ / 100))) {
      /* If there is less than finish_threshold_% remaining capacity in a
       * non-open-zone, finish the zone */
      s = z->Finish();
      if (!s.ok()) {
        Debug(logger_, "Failed finishing zone");
      }
      active_io_zones_--;
    }

    if (!z->IsFull()) {
      if (finish_victim == nullptr) {
        finish_victim = z;
      } else if (finish_victim->capacity_ > z->capacity_) {
        finish_victim = z;
      }
    }
  }

  /* Try to fill an already open zone(with the best life time diff) */
  for (const auto z : io_zones) {
    if ((!z->open_for_write_) && (z->used_capacity_ > 0) && !z->IsFull()) {
      unsigned int diff = GetLifeTimeDiff(z->lifetime_, file_lifetime);
      if (diff <= best_diff) {
        allocated_zone = z;
        best_diff = diff;
      }
    }
  }

  /* If we did not find a good match, allocate an empty one */
  if (best_diff >= LIFETIME_DIFF_NOT_GOOD) {
    /* If we at the active io zone limit, finish an open zone(if available) with
     * least capacity left */
    if (active_io_zones_.load() == max_nr_active_io_zones_ &&
        finish_victim != nullptr) {
      s = finish_victim->Finish();
      if (!s.ok()) {
        Debug(logger_, "Failed finishing zone");
      }
      active_io_zones_--;
    }

    if (active_io_zones_.load() < max_nr_active_io_zones_) {
      for (const auto z : io_zones) {
        if ((!z->open_for_write_) && z->IsEmpty()) {
          z->lifetime_ = file_lifetime;
          allocated_zone = z;
          active_io_zones_++;
          new_zone = 1;
          break;
        }
      }
    }
  }

  if (allocated_zone) {
    assert(!allocated_zone->open_for_write_);
    allocated_zone->open_for_write_ = true;
    open_io_zones_++;
    Debug(logger_,
          "Allocating zone(new=%d) start: 0x%lx wp: 0x%lx lt: %d file lt: %d\n",
          new_zone, allocated_zone->start_, allocated_zone->wp_,
          allocated_zone->lifetime_, file_lifetime);
  }

  io_zones_mtx.unlock();
  LogZoneStats();

  return allocated_zone;
}

std::string ZonedBlockDevice::GetFilename() { return filename_; }
uint32_t ZonedBlockDevice::GetBlockSize() { return block_sz_; }

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(LIBZBD)
