//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#include <errno.h>
#if defined(ROCKSDB_IOURING_PRESENT)
#include <liburing.h>
#include <sys/uio.h>
#endif
#include <unistd.h>
#include <atomic>
#include <functional>
#include <map>
#include <string>
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "util/mutexlock.h"
#include "util/thread_local.h"

// For non linux platform, the following macros are used only as place
// holder.
#if !(defined OS_LINUX) && !(defined CYGWIN) && !(defined OS_AIX)
#define POSIX_FADV_NORMAL 0     /* [MC1] no further special treatment */
#define POSIX_FADV_RANDOM 1     /* [MC1] expect random page refs */
#define POSIX_FADV_SEQUENTIAL 2 /* [MC1] expect sequential page refs */
#define POSIX_FADV_WILLNEED 3   /* [MC1] will need these pages */
#define POSIX_FADV_DONTNEED 4   /* [MC1] don't need these pages */
#endif

namespace ROCKSDB_NAMESPACE {
std::string IOErrorMsg(const std::string& context,
                       const std::string& file_name);
// file_name can be left empty if it is not unkown.
IOStatus IOError(const std::string& context, const std::string& file_name,
                 int err_number);

class PosixHelper {
 public:
  static size_t GetUniqueIdFromFile(int fd, char* id, size_t max_size);
  static size_t GetLogicalBlockSizeOfFd(int fd);
  static Status GetLogicalBlockSizeOfDirectory(const std::string& directory,
                                               size_t* size);
};

#ifdef OS_LINUX
// Files under a specific directory have the same logical block size.
// This class caches the logical block size for the specified directories to
// save the CPU cost of computing the size.
// Safe for concurrent access from multiple threads without any external
// synchronization.
class LogicalBlockSizeCache {
 public:
  LogicalBlockSizeCache(
      std::function<size_t(int)> get_logical_block_size_of_fd =
          PosixHelper::GetLogicalBlockSizeOfFd,
      std::function<Status(const std::string&, size_t*)>
          get_logical_block_size_of_directory =
              PosixHelper::GetLogicalBlockSizeOfDirectory)
      : get_logical_block_size_of_fd_(get_logical_block_size_of_fd),
        get_logical_block_size_of_directory_(
            get_logical_block_size_of_directory) {}

  // Takes the following actions:
  // 1. Increases reference count of the directories;
  // 2. If the directory's logical block size is not cached,
  //    compute the buffer size and cache the result.
  Status RefAndCacheLogicalBlockSize(
      const std::vector<std::string>& directories);

  // Takes the following actions:
  // 1. Decreases reference count of the directories;
  // 2. If the reference count of a directory reaches 0, remove the directory
  //    from the cache.
  void UnrefAndTryRemoveCachedLogicalBlockSize(
      const std::vector<std::string>& directories);

  // Returns the logical block size for the file.
  //
  // If the file is under a cached directory, return the cached size.
  // Otherwise, the size is computed.
  size_t GetLogicalBlockSize(const std::string& fname, int fd);

  int GetRefCount(const std::string& dir) {
    ReadLock lock(&cache_mutex_);
    auto it = cache_.find(dir);
    if (it == cache_.end()) {
      return 0;
    }
    return it->second.ref;
  }

  size_t Size() const { return cache_.size(); }

  bool Contains(const std::string& dir) {
    ReadLock lock(&cache_mutex_);
    return cache_.find(dir) != cache_.end();
  }

 private:
  struct CacheValue {
    CacheValue() : size(0), ref(0) {}

    // Logical block size of the directory.
    size_t size;
    // Reference count of the directory.
    int ref;
  };

  std::function<size_t(int)> get_logical_block_size_of_fd_;
  std::function<Status(const std::string&, size_t*)>
      get_logical_block_size_of_directory_;

  std::map<std::string, CacheValue> cache_;
  port::RWMutex cache_mutex_;
};
#endif

class PosixSequentialFile : public FSSequentialFile {
 private:
  std::string filename_;
  FILE* file_;
  int fd_;
  bool use_direct_io_;
  size_t logical_sector_size_;

 public:
  PosixSequentialFile(const std::string& fname, FILE* file, int fd,
                      size_t logical_block_size,
                      const EnvOptions& options);
  virtual ~PosixSequentialFile();

  virtual IOStatus Read(size_t n, const IOOptions& opts, Slice* result,
                        char* scratch, IODebugContext* dbg) override;
  virtual IOStatus PositionedRead(uint64_t offset, size_t n,
                                  const IOOptions& opts, Slice* result,
                                  char* scratch, IODebugContext* dbg) override;
  virtual IOStatus Skip(uint64_t n) override;
  virtual IOStatus InvalidateCache(size_t offset, size_t length) override;
  virtual bool use_direct_io() const override { return use_direct_io_; }
  virtual size_t GetRequiredBufferAlignment() const override {
    return logical_sector_size_;
  }
};

#if defined(ROCKSDB_IOURING_PRESENT)
// io_uring instance queue depth
const unsigned int kIoUringDepth = 256;

inline void DeleteIOUring(void* p) {
  struct io_uring* iu = static_cast<struct io_uring*>(p);
  delete iu;
}

inline struct io_uring* CreateIOUring() {
  struct io_uring* new_io_uring = new struct io_uring;
  int ret = io_uring_queue_init(kIoUringDepth, new_io_uring, 0);
  if (ret) {
    delete new_io_uring;
    new_io_uring = nullptr;
  }
  return new_io_uring;
}
#endif  // defined(ROCKSDB_IOURING_PRESENT)

class PosixRandomAccessFile : public FSRandomAccessFile {
 protected:
  std::string filename_;
  int fd_;
  bool use_direct_io_;
  size_t logical_sector_size_;
#if defined(ROCKSDB_IOURING_PRESENT)
  ThreadLocalPtr* thread_local_io_urings_;
#endif

 public:
  PosixRandomAccessFile(const std::string& fname, int fd,
                        size_t logical_block_size,
                        const EnvOptions& options
#if defined(ROCKSDB_IOURING_PRESENT)
                        ,
                        ThreadLocalPtr* thread_local_io_urings
#endif
  );
  virtual ~PosixRandomAccessFile();

  virtual IOStatus Read(uint64_t offset, size_t n, const IOOptions& opts,
                        Slice* result, char* scratch,
                        IODebugContext* dbg) const override;

  virtual IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
                             const IOOptions& options,
                             IODebugContext* dbg) override;

  virtual IOStatus Prefetch(uint64_t offset, size_t n, const IOOptions& opts,
                            IODebugContext* dbg) override;

#if defined(OS_LINUX) || defined(OS_MACOSX) || defined(OS_AIX)
  virtual size_t GetUniqueId(char* id, size_t max_size) const override;
#endif
  virtual void Hint(AccessPattern pattern) override;
  virtual IOStatus InvalidateCache(size_t offset, size_t length) override;
  virtual bool use_direct_io() const override { return use_direct_io_; }
  virtual size_t GetRequiredBufferAlignment() const override {
    return logical_sector_size_;
  }
};

class PosixWritableFile : public FSWritableFile {
 protected:
  const std::string filename_;
  const bool use_direct_io_;
  int fd_;
  uint64_t filesize_;
  size_t logical_sector_size_;
#ifdef ROCKSDB_FALLOCATE_PRESENT
  bool allow_fallocate_;
  bool fallocate_with_keep_size_;
#endif
#ifdef ROCKSDB_RANGESYNC_PRESENT
  // Even if the syscall is present, the filesystem may still not properly
  // support it, so we need to do a dynamic check too.
  bool sync_file_range_supported_;
#endif  // ROCKSDB_RANGESYNC_PRESENT

 public:
  explicit PosixWritableFile(const std::string& fname, int fd,
                             size_t logical_block_size,
                             const EnvOptions& options);
  virtual ~PosixWritableFile();

  // Need to implement this so the file is truncated correctly
  // with direct I/O
  virtual IOStatus Truncate(uint64_t size, const IOOptions& opts,
                            IODebugContext* dbg) override;
  virtual IOStatus Close(const IOOptions& opts, IODebugContext* dbg) override;
  virtual IOStatus Append(const Slice& data, const IOOptions& opts,
                          IODebugContext* dbg) override;
  virtual IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                                    const IOOptions& opts,
                                    IODebugContext* dbg) override;
  virtual IOStatus Flush(const IOOptions& opts, IODebugContext* dbg) override;
  virtual IOStatus Sync(const IOOptions& opts, IODebugContext* dbg) override;
  virtual IOStatus Fsync(const IOOptions& opts, IODebugContext* dbg) override;
  virtual bool IsSyncThreadSafe() const override;
  virtual bool use_direct_io() const override { return use_direct_io_; }
  virtual void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override;
  virtual uint64_t GetFileSize(const IOOptions& opts,
                               IODebugContext* dbg) override;
  virtual IOStatus InvalidateCache(size_t offset, size_t length) override;
  virtual size_t GetRequiredBufferAlignment() const override {
    return logical_sector_size_;
  }
#ifdef ROCKSDB_FALLOCATE_PRESENT
  virtual IOStatus Allocate(uint64_t offset, uint64_t len,
                            const IOOptions& opts,
                            IODebugContext* dbg) override;
#endif
  virtual IOStatus RangeSync(uint64_t offset, uint64_t nbytes,
                             const IOOptions& opts,
                             IODebugContext* dbg) override;
#ifdef OS_LINUX
  virtual size_t GetUniqueId(char* id, size_t max_size) const override;
#endif
};

// mmap() based random-access
class PosixMmapReadableFile : public FSRandomAccessFile {
 private:
  int fd_;
  std::string filename_;
  void* mmapped_region_;
  size_t length_;

 public:
  PosixMmapReadableFile(const int fd, const std::string& fname, void* base,
                        size_t length, const EnvOptions& options);
  virtual ~PosixMmapReadableFile();
  virtual IOStatus Read(uint64_t offset, size_t n, const IOOptions& opts,
                        Slice* result, char* scratch,
                        IODebugContext* dbg) const override;
  virtual IOStatus InvalidateCache(size_t offset, size_t length) override;
};

class PosixMmapFile : public FSWritableFile {
 private:
  std::string filename_;
  int fd_;
  size_t page_size_;
  size_t map_size_;       // How much extra memory to map at a time
  char* base_;            // The mapped region
  char* limit_;           // Limit of the mapped region
  char* dst_;             // Where to write next  (in range [base_,limit_])
  char* last_sync_;       // Where have we synced up to
  uint64_t file_offset_;  // Offset of base_ in file
#ifdef ROCKSDB_FALLOCATE_PRESENT
  bool allow_fallocate_;  // If false, fallocate calls are bypassed
  bool fallocate_with_keep_size_;
#endif

  // Roundup x to a multiple of y
  static size_t Roundup(size_t x, size_t y) { return ((x + y - 1) / y) * y; }

  size_t TruncateToPageBoundary(size_t s) {
    s -= (s & (page_size_ - 1));
    assert((s % page_size_) == 0);
    return s;
  }

  IOStatus MapNewRegion();
  IOStatus UnmapCurrentRegion();
  IOStatus Msync();

 public:
  PosixMmapFile(const std::string& fname, int fd, size_t page_size,
                const EnvOptions& options);
  ~PosixMmapFile();

  // Means Close() will properly take care of truncate
  // and it does not need any additional information
  virtual IOStatus Truncate(uint64_t /*size*/, const IOOptions& /*opts*/,
                            IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }
  virtual IOStatus Close(const IOOptions& opts, IODebugContext* dbg) override;
  virtual IOStatus Append(const Slice& data, const IOOptions& opts,
                          IODebugContext* dbg) override;
  virtual IOStatus Flush(const IOOptions& opts, IODebugContext* dbg) override;
  virtual IOStatus Sync(const IOOptions& opts, IODebugContext* dbg) override;
  virtual IOStatus Fsync(const IOOptions& opts, IODebugContext* dbg) override;
  virtual uint64_t GetFileSize(const IOOptions& opts,
                               IODebugContext* dbg) override;
  virtual IOStatus InvalidateCache(size_t offset, size_t length) override;
#ifdef ROCKSDB_FALLOCATE_PRESENT
  virtual IOStatus Allocate(uint64_t offset, uint64_t len,
                            const IOOptions& opts,
                            IODebugContext* dbg) override;
#endif
};

class PosixRandomRWFile : public FSRandomRWFile {
 public:
  explicit PosixRandomRWFile(const std::string& fname, int fd,
                             const EnvOptions& options);
  virtual ~PosixRandomRWFile();

  virtual IOStatus Write(uint64_t offset, const Slice& data,
                         const IOOptions& opts, IODebugContext* dbg) override;

  virtual IOStatus Read(uint64_t offset, size_t n, const IOOptions& opts,
                        Slice* result, char* scratch,
                        IODebugContext* dbg) const override;

  virtual IOStatus Flush(const IOOptions& opts, IODebugContext* dbg) override;
  virtual IOStatus Sync(const IOOptions& opts, IODebugContext* dbg) override;
  virtual IOStatus Fsync(const IOOptions& opts, IODebugContext* dbg) override;
  virtual IOStatus Close(const IOOptions& opts, IODebugContext* dbg) override;

 private:
  const std::string filename_;
  int fd_;
};

struct PosixMemoryMappedFileBuffer : public MemoryMappedFileBuffer {
  PosixMemoryMappedFileBuffer(void* _base, size_t _length)
      : MemoryMappedFileBuffer(_base, _length) {}
  virtual ~PosixMemoryMappedFileBuffer();
};

class PosixDirectory : public FSDirectory {
 public:
  explicit PosixDirectory(int fd) : fd_(fd) {}
  ~PosixDirectory();
  virtual IOStatus Fsync(const IOOptions& opts, IODebugContext* dbg) override;

 private:
  int fd_;
};

}  // namespace ROCKSDB_NAMESPACE
