//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifdef ROCKSDB_LIB_IO_POSIX

#include "util/io_posix.h"
#include <errno.h>
#include <fcntl.h>
#if defined(OS_LINUX)
#include <linux/fs.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#ifdef OS_LINUX
#include <sys/statfs.h>
#include <sys/syscall.h>
#endif
#include "port/port.h"
#include "rocksdb/slice.h"
#include "util/coding.h"
#include "util/iostats_context_imp.h"
#include "util/posix_logger.h"
#include "util/string_util.h"
#include "util/sync_point.h"

namespace rocksdb {

// A wrapper for fadvise, if the platform doesn't support fadvise,
// it will simply return Status::NotSupport.
int Fadvise(int fd, off_t offset, size_t len, int advice) {
#ifdef OS_LINUX
  return posix_fadvise(fd, offset, len, advice);
#else
  return 0;  // simply do nothing.
#endif
}

/*
 * PosixSequentialFile
 */
PosixSequentialFile::PosixSequentialFile(const std::string& fname, FILE* f,
                                         const EnvOptions& options)
    : filename_(fname),
      file_(f),
      fd_(fileno(f)),
      use_os_buffer_(options.use_os_buffer) {}

PosixSequentialFile::~PosixSequentialFile() { fclose(file_); }

Status PosixSequentialFile::Read(size_t n, Slice* result, char* scratch) {
  Status s;
  size_t r = 0;
  do {
    r = fread_unlocked(scratch, 1, n, file_);
  } while (r == 0 && ferror(file_) && errno == EINTR);
  *result = Slice(scratch, r);
  if (r < n) {
    if (feof(file_)) {
      // We leave status as ok if we hit the end of the file
      // We also clear the error so that the reads can continue
      // if a new data is written to the file
      clearerr(file_);
    } else {
      // A partial read with an error: return a non-ok status
      s = IOError(filename_, errno);
    }
  }
  if (!use_os_buffer_) {
    // we need to fadvise away the entire range of pages because
    // we do not want readahead pages to be cached.
    Fadvise(fd_, 0, 0, POSIX_FADV_DONTNEED);  // free OS pages
  }
  return s;
}

Status PosixSequentialFile::Skip(uint64_t n) {
  if (fseek(file_, static_cast<long int>(n), SEEK_CUR)) {
    return IOError(filename_, errno);
  }
  return Status::OK();
}

Status PosixSequentialFile::InvalidateCache(size_t offset, size_t length) {
#ifndef OS_LINUX
  return Status::OK();
#else
  // free OS pages
  int ret = Fadvise(fd_, offset, length, POSIX_FADV_DONTNEED);
  if (ret == 0) {
    return Status::OK();
  }
  return IOError(filename_, errno);
#endif
}

#if defined(OS_LINUX)
namespace {
static size_t GetUniqueIdFromFile(int fd, char* id, size_t max_size) {
  if (max_size < kMaxVarint64Length * 3) {
    return 0;
  }

  struct stat buf;
  int result = fstat(fd, &buf);
  if (result == -1) {
    return 0;
  }

  long version = 0;
  result = ioctl(fd, FS_IOC_GETVERSION, &version);
  if (result == -1) {
    return 0;
  }
  uint64_t uversion = (uint64_t)version;

  char* rid = id;
  rid = EncodeVarint64(rid, buf.st_dev);
  rid = EncodeVarint64(rid, buf.st_ino);
  rid = EncodeVarint64(rid, uversion);
  assert(rid >= id);
  return static_cast<size_t>(rid - id);
}
}
#endif

/*
 * PosixRandomAccessFile
 *
 * pread() based random-access
 */
PosixRandomAccessFile::PosixRandomAccessFile(const std::string& fname, int fd,
                                             const EnvOptions& options)
    : filename_(fname), fd_(fd), use_os_buffer_(options.use_os_buffer) {
  assert(!options.use_mmap_reads || sizeof(void*) < 8);
}

PosixRandomAccessFile::~PosixRandomAccessFile() { close(fd_); }

Status PosixRandomAccessFile::Read(uint64_t offset, size_t n, Slice* result,
                                   char* scratch) const {
  Status s;
  ssize_t r = -1;
  size_t left = n;
  char* ptr = scratch;
  while (left > 0) {
    r = pread(fd_, ptr, left, static_cast<off_t>(offset));

    if (r <= 0) {
      if (errno == EINTR) {
        continue;
      }
      break;
    }
    ptr += r;
    offset += r;
    left -= r;
  }

  *result = Slice(scratch, (r < 0) ? 0 : n - left);
  if (r < 0) {
    // An error: return a non-ok status
    s = IOError(filename_, errno);
  }
  if (!use_os_buffer_) {
    // we need to fadvise away the entire range of pages because
    // we do not want readahead pages to be cached.
    Fadvise(fd_, 0, 0, POSIX_FADV_DONTNEED);  // free OS pages
  }
  return s;
}

#ifdef OS_LINUX
size_t PosixRandomAccessFile::GetUniqueId(char* id, size_t max_size) const {
  return GetUniqueIdFromFile(fd_, id, max_size);
}
#endif

void PosixRandomAccessFile::Hint(AccessPattern pattern) {
  switch (pattern) {
    case NORMAL:
      Fadvise(fd_, 0, 0, POSIX_FADV_NORMAL);
      break;
    case RANDOM:
      Fadvise(fd_, 0, 0, POSIX_FADV_RANDOM);
      break;
    case SEQUENTIAL:
      Fadvise(fd_, 0, 0, POSIX_FADV_SEQUENTIAL);
      break;
    case WILLNEED:
      Fadvise(fd_, 0, 0, POSIX_FADV_WILLNEED);
      break;
    case DONTNEED:
      Fadvise(fd_, 0, 0, POSIX_FADV_DONTNEED);
      break;
    default:
      assert(false);
      break;
  }
}

Status PosixRandomAccessFile::InvalidateCache(size_t offset, size_t length) {
#ifndef OS_LINUX
  return Status::OK();
#else
  // free OS pages
  int ret = Fadvise(fd_, offset, length, POSIX_FADV_DONTNEED);
  if (ret == 0) {
    return Status::OK();
  }
  return IOError(filename_, errno);
#endif
}

/*
 * PosixMmapReadableFile
 *
 * mmap() based random-access
 */
// base[0,length-1] contains the mmapped contents of the file.
PosixMmapReadableFile::PosixMmapReadableFile(const int fd,
                                             const std::string& fname,
                                             void* base, size_t length,
                                             const EnvOptions& options)
    : fd_(fd), filename_(fname), mmapped_region_(base), length_(length) {
  fd_ = fd_ + 0;  // suppress the warning for used variables
  assert(options.use_mmap_reads);
  assert(options.use_os_buffer);
}

PosixMmapReadableFile::~PosixMmapReadableFile() {
  int ret = munmap(mmapped_region_, length_);
  if (ret != 0) {
    fprintf(stdout, "failed to munmap %p length %" ROCKSDB_PRIszt " \n",
            mmapped_region_, length_);
  }
}

Status PosixMmapReadableFile::Read(uint64_t offset, size_t n, Slice* result,
                                   char* scratch) const {
  Status s;
  if (offset > length_) {
    *result = Slice();
    return IOError(filename_, EINVAL);
  } else if (offset + n > length_) {
    n = static_cast<size_t>(length_ - offset);
  }
  *result = Slice(reinterpret_cast<char*>(mmapped_region_) + offset, n);
  return s;
}

Status PosixMmapReadableFile::InvalidateCache(size_t offset, size_t length) {
#ifndef OS_LINUX
  return Status::OK();
#else
  // free OS pages
  int ret = Fadvise(fd_, offset, length, POSIX_FADV_DONTNEED);
  if (ret == 0) {
    return Status::OK();
  }
  return IOError(filename_, errno);
#endif
}

/*
 * PosixMmapFile
 *
 * We preallocate up to an extra megabyte and use memcpy to append new
 * data to the file.  This is safe since we either properly close the
 * file before reading from it, or for log files, the reading code
 * knows enough to skip zero suffixes.
 */
Status PosixMmapFile::UnmapCurrentRegion() {
  TEST_KILL_RANDOM("PosixMmapFile::UnmapCurrentRegion:0", rocksdb_kill_odds);
  if (base_ != nullptr) {
    int munmap_status = munmap(base_, limit_ - base_);
    if (munmap_status != 0) {
      return IOError(filename_, munmap_status);
    }
    file_offset_ += limit_ - base_;
    base_ = nullptr;
    limit_ = nullptr;
    last_sync_ = nullptr;
    dst_ = nullptr;

    // Increase the amount we map the next time, but capped at 1MB
    if (map_size_ < (1 << 20)) {
      map_size_ *= 2;
    }
  }
  return Status::OK();
}

Status PosixMmapFile::MapNewRegion() {
#ifdef ROCKSDB_FALLOCATE_PRESENT
  assert(base_ == nullptr);

  TEST_KILL_RANDOM("PosixMmapFile::UnmapCurrentRegion:0", rocksdb_kill_odds);
  // we can't fallocate with FALLOC_FL_KEEP_SIZE here
  if (allow_fallocate_) {
    IOSTATS_TIMER_GUARD(allocate_nanos);
    int alloc_status = fallocate(fd_, 0, file_offset_, map_size_);
    if (alloc_status != 0) {
      // fallback to posix_fallocate
      alloc_status = posix_fallocate(fd_, file_offset_, map_size_);
    }
    if (alloc_status != 0) {
      return Status::IOError("Error allocating space to file : " + filename_ +
                             "Error : " + strerror(alloc_status));
    }
  }

  TEST_KILL_RANDOM("PosixMmapFile::Append:1", rocksdb_kill_odds);
  void* ptr = mmap(nullptr, map_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_,
                   file_offset_);
  if (ptr == MAP_FAILED) {
    return Status::IOError("MMap failed on " + filename_);
  }
  TEST_KILL_RANDOM("PosixMmapFile::Append:2", rocksdb_kill_odds);

  base_ = reinterpret_cast<char*>(ptr);
  limit_ = base_ + map_size_;
  dst_ = base_;
  last_sync_ = base_;
  return Status::OK();
#else
  return Status::NotSupported("This platform doesn't support fallocate()");
#endif
}

Status PosixMmapFile::Msync() {
  if (dst_ == last_sync_) {
    return Status::OK();
  }
  // Find the beginnings of the pages that contain the first and last
  // bytes to be synced.
  size_t p1 = TruncateToPageBoundary(last_sync_ - base_);
  size_t p2 = TruncateToPageBoundary(dst_ - base_ - 1);
  last_sync_ = dst_;
  TEST_KILL_RANDOM("PosixMmapFile::Msync:0", rocksdb_kill_odds);
  if (msync(base_ + p1, p2 - p1 + page_size_, MS_SYNC) < 0) {
    return IOError(filename_, errno);
  }
  return Status::OK();
}

PosixMmapFile::PosixMmapFile(const std::string& fname, int fd, size_t page_size,
                             const EnvOptions& options)
    : filename_(fname),
      fd_(fd),
      page_size_(page_size),
      map_size_(Roundup(65536, page_size)),
      base_(nullptr),
      limit_(nullptr),
      dst_(nullptr),
      last_sync_(nullptr),
      file_offset_(0) {
#ifdef ROCKSDB_FALLOCATE_PRESENT
  allow_fallocate_ = options.allow_fallocate;
  fallocate_with_keep_size_ = options.fallocate_with_keep_size;
#endif
  assert((page_size & (page_size - 1)) == 0);
  assert(options.use_mmap_writes);
}

PosixMmapFile::~PosixMmapFile() {
  if (fd_ >= 0) {
    PosixMmapFile::Close();
  }
}

Status PosixMmapFile::Append(const Slice& data) {
  const char* src = data.data();
  size_t left = data.size();
  while (left > 0) {
    assert(base_ <= dst_);
    assert(dst_ <= limit_);
    size_t avail = limit_ - dst_;
    if (avail == 0) {
      Status s = UnmapCurrentRegion();
      if (!s.ok()) {
        return s;
      }
      s = MapNewRegion();
      if (!s.ok()) {
        return s;
      }
      TEST_KILL_RANDOM("PosixMmapFile::Append:0", rocksdb_kill_odds);
    }

    size_t n = (left <= avail) ? left : avail;
    memcpy(dst_, src, n);
    dst_ += n;
    src += n;
    left -= n;
  }
  return Status::OK();
}

Status PosixMmapFile::Close() {
  Status s;
  size_t unused = limit_ - dst_;

  s = UnmapCurrentRegion();
  if (!s.ok()) {
    s = IOError(filename_, errno);
  } else if (unused > 0) {
    // Trim the extra space at the end of the file
    if (ftruncate(fd_, file_offset_ - unused) < 0) {
      s = IOError(filename_, errno);
    }
  }

  if (close(fd_) < 0) {
    if (s.ok()) {
      s = IOError(filename_, errno);
    }
  }

  fd_ = -1;
  base_ = nullptr;
  limit_ = nullptr;
  return s;
}

Status PosixMmapFile::Flush() { return Status::OK(); }

Status PosixMmapFile::Sync() {
  if (fdatasync(fd_) < 0) {
    return IOError(filename_, errno);
  }

  return Msync();
}

/**
 * Flush data as well as metadata to stable storage.
 */
Status PosixMmapFile::Fsync() {
  if (fsync(fd_) < 0) {
    return IOError(filename_, errno);
  }

  return Msync();
}

/**
 * Get the size of valid data in the file. This will not match the
 * size that is returned from the filesystem because we use mmap
 * to extend file by map_size every time.
 */
uint64_t PosixMmapFile::GetFileSize() {
  size_t used = dst_ - base_;
  return file_offset_ + used;
}

Status PosixMmapFile::InvalidateCache(size_t offset, size_t length) {
#ifndef OS_LINUX
  return Status::OK();
#else
  // free OS pages
  int ret = Fadvise(fd_, offset, length, POSIX_FADV_DONTNEED);
  if (ret == 0) {
    return Status::OK();
  }
  return IOError(filename_, errno);
#endif
}

#ifdef ROCKSDB_FALLOCATE_PRESENT
Status PosixMmapFile::Allocate(off_t offset, off_t len) {
  TEST_KILL_RANDOM("PosixMmapFile::Allocate:0", rocksdb_kill_odds);
  int alloc_status = 0;
  if (allow_fallocate_) {
    alloc_status = fallocate(
        fd_, fallocate_with_keep_size_ ? FALLOC_FL_KEEP_SIZE : 0, offset, len);
  }
  if (alloc_status == 0) {
    return Status::OK();
  } else {
    return IOError(filename_, errno);
  }
}
#endif

/*
 * PosixWritableFile
 *
 * Use posix write to write data to a file.
 */
PosixWritableFile::PosixWritableFile(const std::string& fname, int fd,
                                     const EnvOptions& options)
    : filename_(fname), fd_(fd), filesize_(0) {
#ifdef ROCKSDB_FALLOCATE_PRESENT
  allow_fallocate_ = options.allow_fallocate;
  fallocate_with_keep_size_ = options.fallocate_with_keep_size;
#endif
  assert(!options.use_mmap_writes);
}

PosixWritableFile::~PosixWritableFile() {
  if (fd_ >= 0) {
    PosixWritableFile::Close();
  }
}

Status PosixWritableFile::Append(const Slice& data) {
  const char* src = data.data();
  size_t left = data.size();
  while (left != 0) {
    ssize_t done = write(fd_, src, left);
    if (done < 0) {
      if (errno == EINTR) {
        continue;
      }
      return IOError(filename_, errno);
    }
    left -= done;
    src += done;
  }
  filesize_ += data.size();
  return Status::OK();
}

Status PosixWritableFile::Close() {
  Status s;

  size_t block_size;
  size_t last_allocated_block;
  GetPreallocationStatus(&block_size, &last_allocated_block);
  if (last_allocated_block > 0) {
    // trim the extra space preallocated at the end of the file
    // NOTE(ljin): we probably don't want to surface failure as an IOError,
    // but it will be nice to log these errors.
    int dummy __attribute__((unused));
    dummy = ftruncate(fd_, filesize_);
#ifdef ROCKSDB_FALLOCATE_PRESENT
    // in some file systems, ftruncate only trims trailing space if the
    // new file size is smaller than the current size. Calling fallocate
    // with FALLOC_FL_PUNCH_HOLE flag to explicitly release these unused
    // blocks. FALLOC_FL_PUNCH_HOLE is supported on at least the following
    // filesystems:
    //   XFS (since Linux 2.6.38)
    //   ext4 (since Linux 3.0)
    //   Btrfs (since Linux 3.7)
    //   tmpfs (since Linux 3.5)
    // We ignore error since failure of this operation does not affect
    // correctness.
    IOSTATS_TIMER_GUARD(allocate_nanos);
    if (allow_fallocate_) {
      fallocate(fd_, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE, filesize_,
                block_size * last_allocated_block - filesize_);
    }
#endif
  }

  if (close(fd_) < 0) {
    s = IOError(filename_, errno);
  }
  fd_ = -1;
  return s;
}

// write out the cached data to the OS cache
Status PosixWritableFile::Flush() { return Status::OK(); }

Status PosixWritableFile::Sync() {
  if (fdatasync(fd_) < 0) {
    return IOError(filename_, errno);
  }
  return Status::OK();
}

Status PosixWritableFile::Fsync() {
  if (fsync(fd_) < 0) {
    return IOError(filename_, errno);
  }
  return Status::OK();
}

bool PosixWritableFile::IsSyncThreadSafe() const { return true; }

uint64_t PosixWritableFile::GetFileSize() { return filesize_; }

Status PosixWritableFile::InvalidateCache(size_t offset, size_t length) {
#ifndef OS_LINUX
  return Status::OK();
#else
  // free OS pages
  int ret = Fadvise(fd_, offset, length, POSIX_FADV_DONTNEED);
  if (ret == 0) {
    return Status::OK();
  }
  return IOError(filename_, errno);
#endif
}

#ifdef ROCKSDB_FALLOCATE_PRESENT
Status PosixWritableFile::Allocate(off_t offset, off_t len) {
  TEST_KILL_RANDOM("PosixWritableFile::Allocate:0", rocksdb_kill_odds);
  IOSTATS_TIMER_GUARD(allocate_nanos);
  int alloc_status = 0;
  if (allow_fallocate_) {
    alloc_status = fallocate(
        fd_, fallocate_with_keep_size_ ? FALLOC_FL_KEEP_SIZE : 0, offset, len);
  }
  if (alloc_status == 0) {
    return Status::OK();
  } else {
    return IOError(filename_, errno);
  }
}

Status PosixWritableFile::RangeSync(off_t offset, off_t nbytes) {
  if (sync_file_range(fd_, offset, nbytes, SYNC_FILE_RANGE_WRITE) == 0) {
    return Status::OK();
  } else {
    return IOError(filename_, errno);
  }
}

size_t PosixWritableFile::GetUniqueId(char* id, size_t max_size) const {
  return GetUniqueIdFromFile(fd_, id, max_size);
}
#endif

PosixDirectory::~PosixDirectory() { close(fd_); }

Status PosixDirectory::Fsync() {
  if (fsync(fd_) == -1) {
    return IOError("directory", errno);
  }
  return Status::OK();
}
}  // namespace rocksdb
#endif
