//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifdef ROCKSDB_LIB_IO_POSIX
#include "env/io_posix.h"
#include <errno.h>
#include <fcntl.h>
#include <algorithm>
#if defined(OS_LINUX)
#include <linux/fs.h>
#ifndef FALLOC_FL_KEEP_SIZE
#include <linux/falloc.h>
#endif
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
#include <sys/sysmacros.h>
#endif
#include "monitoring/iostats_context_imp.h"
#include "port/port.h"
#include "rocksdb/slice.h"
#include "test_util/sync_point.h"
#include "util/autovector.h"
#include "util/coding.h"
#include "util/string_util.h"

#if defined(OS_LINUX) && !defined(F_SET_RW_HINT)
#define F_LINUX_SPECIFIC_BASE 1024
#define F_SET_RW_HINT (F_LINUX_SPECIFIC_BASE + 12)
#endif

namespace ROCKSDB_NAMESPACE {

std::string IOErrorMsg(const std::string& context,
                       const std::string& file_name) {
  if (file_name.empty()) {
    return context;
  }
  return context + ": " + file_name;
}

// file_name can be left empty if it is not unkown.
IOStatus IOError(const std::string& context, const std::string& file_name,
                 int err_number) {
  switch (err_number) {
    case ENOSPC: {
      IOStatus s = IOStatus::NoSpace(IOErrorMsg(context, file_name),
                                     strerror(err_number));
      s.SetRetryable(true);
      return s;
    }
    case ESTALE:
      return IOStatus::IOError(IOStatus::kStaleFile);
    case ENOENT:
      return IOStatus::PathNotFound(IOErrorMsg(context, file_name),
                                    strerror(err_number));
    default:
      return IOStatus::IOError(IOErrorMsg(context, file_name),
                               strerror(err_number));
  }
}

// A wrapper for fadvise, if the platform doesn't support fadvise,
// it will simply return 0.
int Fadvise(int fd, off_t offset, size_t len, int advice) {
#ifdef OS_LINUX
  return posix_fadvise(fd, offset, len, advice);
#else
  (void)fd;
  (void)offset;
  (void)len;
  (void)advice;
  return 0;  // simply do nothing.
#endif
}

namespace {

// On MacOS (and probably *BSD), the posix write and pwrite calls do not support
// buffers larger than 2^31-1 bytes. These two wrappers fix this issue by
// cutting the buffer in 1GB chunks. We use this chunk size to be sure to keep
// the writes aligned.

bool PosixWrite(int fd, const char* buf, size_t nbyte) {
  const size_t kLimit1Gb = 1UL << 30;

  const char* src = buf;
  size_t left = nbyte;

  while (left != 0) {
    size_t bytes_to_write = std::min(left, kLimit1Gb);

    ssize_t done = write(fd, src, bytes_to_write);
    if (done < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    left -= done;
    src += done;
  }
  return true;
}

bool PosixPositionedWrite(int fd, const char* buf, size_t nbyte, off_t offset) {
  const size_t kLimit1Gb = 1UL << 30;

  const char* src = buf;
  size_t left = nbyte;

  while (left != 0) {
    size_t bytes_to_write = std::min(left, kLimit1Gb);

    ssize_t done = pwrite(fd, src, bytes_to_write, offset);
    if (done < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    left -= done;
    offset += done;
    src += done;
  }

  return true;
}

#ifdef ROCKSDB_RANGESYNC_PRESENT

#if !defined(ZFS_SUPER_MAGIC)
// The magic number for ZFS was not exposed until recently. It should be fixed
// forever so we can just copy the magic number here.
#define ZFS_SUPER_MAGIC 0x2fc12fc1
#endif

bool IsSyncFileRangeSupported(int fd) {
  // This function tracks and checks for cases where we know `sync_file_range`
  // definitely will not work properly despite passing the compile-time check
  // (`ROCKSDB_RANGESYNC_PRESENT`). If we are unsure, or if any of the checks
  // fail in unexpected ways, we allow `sync_file_range` to be used. This way
  // should minimize risk of impacting existing use cases.
  struct statfs buf;
  int ret = fstatfs(fd, &buf);
  assert(ret == 0);
  if (ret == 0 && buf.f_type == ZFS_SUPER_MAGIC) {
    // Testing on ZFS showed the writeback did not happen asynchronously when
    // `sync_file_range` was called, even though it returned success. Avoid it
    // and use `fdatasync` instead to preserve the contract of `bytes_per_sync`,
    // even though this'll incur extra I/O for metadata.
    return false;
  }

  ret = sync_file_range(fd, 0 /* offset */, 0 /* nbytes */, 0 /* flags */);
  assert(!(ret == -1 && errno != ENOSYS));
  if (ret == -1 && errno == ENOSYS) {
    // `sync_file_range` is not implemented on all platforms even if
    // compile-time checks pass and a supported filesystem is in-use. For
    // example, using ext4 on WSL (Windows Subsystem for Linux),
    // `sync_file_range()` returns `ENOSYS`
    // ("Function not implemented").
    return false;
  }
  // None of the known cases matched, so allow `sync_file_range` use.
  return true;
}

#undef ZFS_SUPER_MAGIC

#endif  // ROCKSDB_RANGESYNC_PRESENT

}  // anonymous namespace

/*
 * DirectIOHelper
 */
namespace {

bool IsSectorAligned(const size_t off, size_t sector_size) {
  assert((sector_size & (sector_size - 1)) == 0);
  return (off & (sector_size - 1)) == 0;
}

#ifndef NDEBUG
bool IsSectorAligned(const void* ptr, size_t sector_size) {
  return uintptr_t(ptr) % sector_size == 0;
}
#endif
}  // namespace

/*
 * PosixSequentialFile
 */
PosixSequentialFile::PosixSequentialFile(const std::string& fname, FILE* file,
                                         int fd, size_t logical_block_size,
                                         const EnvOptions& options)
    : filename_(fname),
      file_(file),
      fd_(fd),
      use_direct_io_(options.use_direct_reads),
      logical_sector_size_(logical_block_size) {
  assert(!options.use_direct_reads || !options.use_mmap_reads);
}

PosixSequentialFile::~PosixSequentialFile() {
  if (!use_direct_io()) {
    assert(file_);
    fclose(file_);
  } else {
    assert(fd_);
    close(fd_);
  }
}

IOStatus PosixSequentialFile::Read(size_t n, const IOOptions& /*opts*/,
                                   Slice* result, char* scratch,
                                   IODebugContext* /*dbg*/) {
  assert(result != nullptr && !use_direct_io());
  IOStatus s;
  size_t r = 0;
  do {
    clearerr(file_);
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
      s = IOError("While reading file sequentially", filename_, errno);
    }
  }
  return s;
}

IOStatus PosixSequentialFile::PositionedRead(uint64_t offset, size_t n,
                                             const IOOptions& /*opts*/,
                                             Slice* result, char* scratch,
                                             IODebugContext* /*dbg*/) {
  assert(use_direct_io());
  assert(IsSectorAligned(offset, GetRequiredBufferAlignment()));
  assert(IsSectorAligned(n, GetRequiredBufferAlignment()));
  assert(IsSectorAligned(scratch, GetRequiredBufferAlignment()));

  IOStatus s;
  ssize_t r = -1;
  size_t left = n;
  char* ptr = scratch;
  while (left > 0) {
    r = pread(fd_, ptr, left, static_cast<off_t>(offset));
    if (r <= 0) {
      if (r == -1 && errno == EINTR) {
        continue;
      }
      break;
    }
    ptr += r;
    offset += r;
    left -= r;
    if (!IsSectorAligned(r, GetRequiredBufferAlignment())) {
      // Bytes reads don't fill sectors. Should only happen at the end
      // of the file.
      break;
    }
  }
  if (r < 0) {
    // An error: return a non-ok status
    s = IOError(
        "While pread " + ToString(n) + " bytes from offset " + ToString(offset),
        filename_, errno);
  }
  *result = Slice(scratch, (r < 0) ? 0 : n - left);
  return s;
}

IOStatus PosixSequentialFile::Skip(uint64_t n) {
  if (fseek(file_, static_cast<long int>(n), SEEK_CUR)) {
    return IOError("While fseek to skip " + ToString(n) + " bytes", filename_,
                   errno);
  }
  return IOStatus::OK();
}

IOStatus PosixSequentialFile::InvalidateCache(size_t offset, size_t length) {
#ifndef OS_LINUX
  (void)offset;
  (void)length;
  return IOStatus::OK();
#else
  if (!use_direct_io()) {
    // free OS pages
    int ret = Fadvise(fd_, offset, length, POSIX_FADV_DONTNEED);
    if (ret != 0) {
      return IOError("While fadvise NotNeeded offset " + ToString(offset) +
                         " len " + ToString(length),
                     filename_, errno);
    }
  }
  return IOStatus::OK();
#endif
}

/*
 * PosixRandomAccessFile
 */
#if defined(OS_LINUX)
size_t PosixHelper::GetUniqueIdFromFile(int fd, char* id, size_t max_size) {
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
  TEST_SYNC_POINT_CALLBACK("GetUniqueIdFromFile:FS_IOC_GETVERSION", &result);
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
#endif

#if defined(OS_MACOSX) || defined(OS_AIX)
size_t PosixHelper::GetUniqueIdFromFile(int fd, char* id, size_t max_size) {
  if (max_size < kMaxVarint64Length * 3) {
    return 0;
  }

  struct stat buf;
  int result = fstat(fd, &buf);
  if (result == -1) {
    return 0;
  }

  char* rid = id;
  rid = EncodeVarint64(rid, buf.st_dev);
  rid = EncodeVarint64(rid, buf.st_ino);
  rid = EncodeVarint64(rid, buf.st_gen);
  assert(rid >= id);
  return static_cast<size_t>(rid - id);
}
#endif

#ifdef OS_LINUX
std::string RemoveTrailingSlash(const std::string& path) {
  std::string p = path;
  if (p.size() > 1 && p.back() == '/') {
    p.pop_back();
  }
  return p;
}

Status LogicalBlockSizeCache::RefAndCacheLogicalBlockSize(
    const std::vector<std::string>& directories) {
  std::vector<std::string> dirs;
  dirs.reserve(directories.size());
  for (auto& d : directories) {
    dirs.emplace_back(RemoveTrailingSlash(d));
  }

  std::map<std::string, size_t> dir_sizes;
  {
    ReadLock lock(&cache_mutex_);
    for (const auto& dir : dirs) {
      if (cache_.find(dir) == cache_.end()) {
        dir_sizes.emplace(dir, 0);
      }
    }
  }

  Status s;
  for (auto& dir_size : dir_sizes) {
    s = get_logical_block_size_of_directory_(dir_size.first, &dir_size.second);
    if (!s.ok()) {
      return s;
    }
  }

  WriteLock lock(&cache_mutex_);
  for (const auto& dir : dirs) {
    auto& v = cache_[dir];
    v.ref++;
    auto dir_size = dir_sizes.find(dir);
    if (dir_size != dir_sizes.end()) {
      v.size = dir_size->second;
    }
  }
  return s;
}

void LogicalBlockSizeCache::UnrefAndTryRemoveCachedLogicalBlockSize(
    const std::vector<std::string>& directories) {
  std::vector<std::string> dirs;
  dirs.reserve(directories.size());
  for (auto& dir : directories) {
    dirs.emplace_back(RemoveTrailingSlash(dir));
  }

  WriteLock lock(&cache_mutex_);
  for (const auto& dir : dirs) {
    auto it = cache_.find(dir);
    if (it != cache_.end() && !(--(it->second.ref))) {
      cache_.erase(it);
    }
  }
}

size_t LogicalBlockSizeCache::GetLogicalBlockSize(const std::string& fname,
                                                  int fd) {
  std::string dir = fname.substr(0, fname.find_last_of("/"));
  if (dir.empty()) {
    dir = "/";
  }
  {
    ReadLock lock(&cache_mutex_);
    auto it = cache_.find(dir);
    if (it != cache_.end()) {
      return it->second.size;
    }
  }
  return get_logical_block_size_of_fd_(fd);
}
#endif

Status PosixHelper::GetLogicalBlockSizeOfDirectory(const std::string& directory,
                                                   size_t* size) {
  int fd = open(directory.c_str(), O_DIRECTORY | O_RDONLY);
  if (fd == -1) {
    close(fd);
    return Status::IOError("Cannot open directory " + directory);
  }
  *size = PosixHelper::GetLogicalBlockSizeOfFd(fd);
  close(fd);
  return Status::OK();
}

size_t PosixHelper::GetLogicalBlockSizeOfFd(int fd) {
#ifdef OS_LINUX
  struct stat buf;
  int result = fstat(fd, &buf);
  if (result == -1) {
    return kDefaultPageSize;
  }
  if (major(buf.st_dev) == 0) {
    // Unnamed devices (e.g. non-device mounts), reserved as null device number.
    // These don't have an entry in /sys/dev/block/. Return a sensible default.
    return kDefaultPageSize;
  }

  // Reading queue/logical_block_size does not require special permissions.
  const int kBufferSize = 100;
  char path[kBufferSize];
  char real_path[PATH_MAX + 1];
  snprintf(path, kBufferSize, "/sys/dev/block/%u:%u", major(buf.st_dev),
           minor(buf.st_dev));
  if (realpath(path, real_path) == nullptr) {
    return kDefaultPageSize;
  }
  std::string device_dir(real_path);
  if (!device_dir.empty() && device_dir.back() == '/') {
    device_dir.pop_back();
  }
  // NOTE: sda3 and nvme0n1p1 do not have a `queue/` subdir, only the parent sda
  // and nvme0n1 have it.
  // $ ls -al '/sys/dev/block/8:3'
  // lrwxrwxrwx. 1 root root 0 Jun 26 01:38 /sys/dev/block/8:3 ->
  // ../../block/sda/sda3
  // $ ls -al '/sys/dev/block/259:4'
  // lrwxrwxrwx 1 root root 0 Jan 31 16:04 /sys/dev/block/259:4 ->
  // ../../devices/pci0000:17/0000:17:00.0/0000:18:00.0/nvme/nvme0/nvme0n1/nvme0n1p1
  size_t parent_end = device_dir.rfind('/', device_dir.length() - 1);
  if (parent_end == std::string::npos) {
    return kDefaultPageSize;
  }
  size_t parent_begin = device_dir.rfind('/', parent_end - 1);
  if (parent_begin == std::string::npos) {
    return kDefaultPageSize;
  }
  std::string parent =
      device_dir.substr(parent_begin + 1, parent_end - parent_begin - 1);
  std::string child = device_dir.substr(parent_end + 1, std::string::npos);
  if (parent != "block" &&
      (child.compare(0, 4, "nvme") || child.find('p') != std::string::npos)) {
    device_dir = device_dir.substr(0, parent_end);
  }
  std::string fname = device_dir + "/queue/logical_block_size";
  FILE* fp;
  size_t size = 0;
  fp = fopen(fname.c_str(), "r");
  if (fp != nullptr) {
    char* line = nullptr;
    size_t len = 0;
    if (getline(&line, &len, fp) != -1) {
      sscanf(line, "%zu", &size);
    }
    free(line);
    fclose(fp);
  }
  if (size != 0 && (size & (size - 1)) == 0) {
    return size;
  }
#endif
  (void)fd;
  return kDefaultPageSize;
}

/*
 * PosixRandomAccessFile
 *
 * pread() based random-access
 */
PosixRandomAccessFile::PosixRandomAccessFile(
    const std::string& fname, int fd, size_t logical_block_size,
    const EnvOptions& options
#if defined(ROCKSDB_IOURING_PRESENT)
    ,
    ThreadLocalPtr* thread_local_io_urings
#endif
    )
    : filename_(fname),
      fd_(fd),
      use_direct_io_(options.use_direct_reads),
      logical_sector_size_(logical_block_size)
#if defined(ROCKSDB_IOURING_PRESENT)
      ,
      thread_local_io_urings_(thread_local_io_urings)
#endif
{
  assert(!options.use_direct_reads || !options.use_mmap_reads);
  assert(!options.use_mmap_reads || sizeof(void*) < 8);
}

PosixRandomAccessFile::~PosixRandomAccessFile() { close(fd_); }

IOStatus PosixRandomAccessFile::Read(uint64_t offset, size_t n,
                                     const IOOptions& /*opts*/, Slice* result,
                                     char* scratch,
                                     IODebugContext* /*dbg*/) const {
  if (use_direct_io()) {
    assert(IsSectorAligned(offset, GetRequiredBufferAlignment()));
    assert(IsSectorAligned(n, GetRequiredBufferAlignment()));
    assert(IsSectorAligned(scratch, GetRequiredBufferAlignment()));
  }
  IOStatus s;
  ssize_t r = -1;
  size_t left = n;
  char* ptr = scratch;
  while (left > 0) {
    r = pread(fd_, ptr, left, static_cast<off_t>(offset));
    if (r <= 0) {
      if (r == -1 && errno == EINTR) {
        continue;
      }
      break;
    }
    ptr += r;
    offset += r;
    left -= r;
    if (use_direct_io() &&
        r % static_cast<ssize_t>(GetRequiredBufferAlignment()) != 0) {
      // Bytes reads don't fill sectors. Should only happen at the end
      // of the file.
      break;
    }
  }
  if (r < 0) {
    // An error: return a non-ok status
    s = IOError(
        "While pread offset " + ToString(offset) + " len " + ToString(n),
        filename_, errno);
  }
  *result = Slice(scratch, (r < 0) ? 0 : n - left);
  return s;
}

IOStatus PosixRandomAccessFile::MultiRead(FSReadRequest* reqs,
                                          size_t num_reqs,
                                          const IOOptions& options,
                                          IODebugContext* dbg) {
  if (use_direct_io()) {
    for (size_t i = 0; i < num_reqs; i++) {
      assert(IsSectorAligned(reqs[i].offset, GetRequiredBufferAlignment()));
      assert(IsSectorAligned(reqs[i].len, GetRequiredBufferAlignment()));
      assert(IsSectorAligned(reqs[i].scratch, GetRequiredBufferAlignment()));
    }
  }

#if defined(ROCKSDB_IOURING_PRESENT)
  struct io_uring* iu = nullptr;
  if (thread_local_io_urings_) {
    iu = static_cast<struct io_uring*>(thread_local_io_urings_->Get());
    if (iu == nullptr) {
      iu = CreateIOUring();
      if (iu != nullptr) {
        thread_local_io_urings_->Reset(iu);
      }
    }
  }

  // Init failed, platform doesn't support io_uring. Fall back to
  // serialized reads
  if (iu == nullptr) {
    return FSRandomAccessFile::MultiRead(reqs, num_reqs, options, dbg);
  }

  struct WrappedReadRequest {
    FSReadRequest* req;
    struct iovec iov;
    size_t finished_len;
    explicit WrappedReadRequest(FSReadRequest* r) : req(r), finished_len(0) {}
  };

  autovector<WrappedReadRequest, 32> req_wraps;
  autovector<WrappedReadRequest*, 4> incomplete_rq_list;

  for (size_t i = 0; i < num_reqs; i++) {
    req_wraps.emplace_back(&reqs[i]);
  }

  size_t reqs_off = 0;
  while (num_reqs > reqs_off || !incomplete_rq_list.empty()) {
    size_t this_reqs = (num_reqs - reqs_off) + incomplete_rq_list.size();

    // If requests exceed depth, split it into batches
    if (this_reqs > kIoUringDepth) this_reqs = kIoUringDepth;

    assert(incomplete_rq_list.size() <= this_reqs);
    for (size_t i = 0; i < this_reqs; i++) {
      WrappedReadRequest* rep_to_submit;
      if (i < incomplete_rq_list.size()) {
        rep_to_submit = incomplete_rq_list[i];
      } else {
        rep_to_submit = &req_wraps[reqs_off++];
      }
      assert(rep_to_submit->req->len > rep_to_submit->finished_len);
      rep_to_submit->iov.iov_base =
          rep_to_submit->req->scratch + rep_to_submit->finished_len;
      rep_to_submit->iov.iov_len =
          rep_to_submit->req->len - rep_to_submit->finished_len;

      struct io_uring_sqe* sqe;
      sqe = io_uring_get_sqe(iu);
      io_uring_prep_readv(
          sqe, fd_, &rep_to_submit->iov, 1,
          rep_to_submit->req->offset + rep_to_submit->finished_len);
      io_uring_sqe_set_data(sqe, rep_to_submit);
    }
    incomplete_rq_list.clear();

    ssize_t ret =
        io_uring_submit_and_wait(iu, static_cast<unsigned int>(this_reqs));
    if (static_cast<size_t>(ret) != this_reqs) {
      fprintf(stderr, "ret = %ld this_reqs: %ld\n", (long)ret, (long)this_reqs);
    }
    assert(static_cast<size_t>(ret) == this_reqs);

    for (size_t i = 0; i < this_reqs; i++) {
      struct io_uring_cqe* cqe;
      WrappedReadRequest* req_wrap;

      // We could use the peek variant here, but this seems safer in terms
      // of our initial wait not reaping all completions
      ret = io_uring_wait_cqe(iu, &cqe);
      assert(!ret);

      req_wrap = static_cast<WrappedReadRequest*>(io_uring_cqe_get_data(cqe));
      FSReadRequest* req = req_wrap->req;
      if (cqe->res < 0) {
        req->result = Slice(req->scratch, 0);
        req->status = IOError("Req failed", filename_, cqe->res);
      } else {
        size_t bytes_read = static_cast<size_t>(cqe->res);
        TEST_SYNC_POINT_CALLBACK(
            "PosixRandomAccessFile::MultiRead:io_uring_result", &bytes_read);
        if (bytes_read == req_wrap->iov.iov_len) {
          req->result = Slice(req->scratch, req->len);
          req->status = IOStatus::OK();
        } else if (bytes_read == 0) {
          // cqe->res == 0 can means EOF, or can mean partial results. See
          // comment
          // https://github.com/facebook/rocksdb/pull/6441#issuecomment-589843435
          // Fall back to pread in this case.
          if (use_direct_io() &&
              !IsSectorAligned(req_wrap->finished_len,
                               GetRequiredBufferAlignment())) {
            // Bytes reads don't fill sectors. Should only happen at the end
            // of the file.
            req->result = Slice(req->scratch, req_wrap->finished_len);
            req->status = IOStatus::OK();
          } else {
            Slice tmp_slice;
            req->status =
                Read(req->offset + req_wrap->finished_len,
                     req->len - req_wrap->finished_len, options, &tmp_slice,
                     req->scratch + req_wrap->finished_len, dbg);
            req->result =
                Slice(req->scratch, req_wrap->finished_len + tmp_slice.size());
          }
        } else if (bytes_read < req_wrap->iov.iov_len) {
          assert(bytes_read > 0);
          assert(bytes_read + req_wrap->finished_len < req->len);
          req_wrap->finished_len += bytes_read;
          incomplete_rq_list.push_back(req_wrap);
        } else {
          req->result = Slice(req->scratch, 0);
          req->status = IOError("Req returned more bytes than requested",
                                filename_, cqe->res);
        }
      }
      io_uring_cqe_seen(iu, cqe);
    }
  }
  return IOStatus::OK();
#else
  return FSRandomAccessFile::MultiRead(reqs, num_reqs, options, dbg);
#endif
}

IOStatus PosixRandomAccessFile::Prefetch(uint64_t offset, size_t n,
                                         const IOOptions& /*opts*/,
                                         IODebugContext* /*dbg*/) {
  IOStatus s;
  if (!use_direct_io()) {
    ssize_t r = 0;
#ifdef OS_LINUX
    r = readahead(fd_, offset, n);
#endif
#ifdef OS_MACOSX
    radvisory advice;
    advice.ra_offset = static_cast<off_t>(offset);
    advice.ra_count = static_cast<int>(n);
    r = fcntl(fd_, F_RDADVISE, &advice);
#endif
    if (r == -1) {
      s = IOError("While prefetching offset " + ToString(offset) + " len " +
                      ToString(n),
                  filename_, errno);
    }
  }
  return s;
}

#if defined(OS_LINUX) || defined(OS_MACOSX) || defined(OS_AIX)
size_t PosixRandomAccessFile::GetUniqueId(char* id, size_t max_size) const {
  return PosixHelper::GetUniqueIdFromFile(fd_, id, max_size);
}
#endif

void PosixRandomAccessFile::Hint(AccessPattern pattern) {
  if (use_direct_io()) {
    return;
  }
  switch (pattern) {
    case kNormal:
      Fadvise(fd_, 0, 0, POSIX_FADV_NORMAL);
      break;
    case kRandom:
      Fadvise(fd_, 0, 0, POSIX_FADV_RANDOM);
      break;
    case kSequential:
      Fadvise(fd_, 0, 0, POSIX_FADV_SEQUENTIAL);
      break;
    case kWillNeed:
      Fadvise(fd_, 0, 0, POSIX_FADV_WILLNEED);
      break;
    case kWontNeed:
      Fadvise(fd_, 0, 0, POSIX_FADV_DONTNEED);
      break;
    default:
      assert(false);
      break;
  }
}

IOStatus PosixRandomAccessFile::InvalidateCache(size_t offset, size_t length) {
  if (use_direct_io()) {
    return IOStatus::OK();
  }
#ifndef OS_LINUX
  (void)offset;
  (void)length;
  return IOStatus::OK();
#else
  // free OS pages
  int ret = Fadvise(fd_, offset, length, POSIX_FADV_DONTNEED);
  if (ret == 0) {
    return IOStatus::OK();
  }
  return IOError("While fadvise NotNeeded offset " + ToString(offset) +
                     " len " + ToString(length),
                 filename_, errno);
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
#ifdef NDEBUG
  (void)options;
#endif
  fd_ = fd_ + 0;  // suppress the warning for used variables
  assert(options.use_mmap_reads);
  assert(!options.use_direct_reads);
}

PosixMmapReadableFile::~PosixMmapReadableFile() {
  int ret = munmap(mmapped_region_, length_);
  if (ret != 0) {
    fprintf(stdout, "failed to munmap %p length %" ROCKSDB_PRIszt " \n",
            mmapped_region_, length_);
  }
  close(fd_);
}

IOStatus PosixMmapReadableFile::Read(uint64_t offset, size_t n,
                                     const IOOptions& /*opts*/, Slice* result,
                                     char* /*scratch*/,
                                     IODebugContext* /*dbg*/) const {
  IOStatus s;
  if (offset > length_) {
    *result = Slice();
    return IOError("While mmap read offset " + ToString(offset) +
                       " larger than file length " + ToString(length_),
                   filename_, EINVAL);
  } else if (offset + n > length_) {
    n = static_cast<size_t>(length_ - offset);
  }
  *result = Slice(reinterpret_cast<char*>(mmapped_region_) + offset, n);
  return s;
}

IOStatus PosixMmapReadableFile::InvalidateCache(size_t offset, size_t length) {
#ifndef OS_LINUX
  (void)offset;
  (void)length;
  return IOStatus::OK();
#else
  // free OS pages
  int ret = Fadvise(fd_, offset, length, POSIX_FADV_DONTNEED);
  if (ret == 0) {
    return IOStatus::OK();
  }
  return IOError("While fadvise not needed. Offset " + ToString(offset) +
                     " len" + ToString(length),
                 filename_, errno);
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
IOStatus PosixMmapFile::UnmapCurrentRegion() {
  TEST_KILL_RANDOM("PosixMmapFile::UnmapCurrentRegion:0", rocksdb_kill_odds);
  if (base_ != nullptr) {
    int munmap_status = munmap(base_, limit_ - base_);
    if (munmap_status != 0) {
      return IOError("While munmap", filename_, munmap_status);
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
  return IOStatus::OK();
}

IOStatus PosixMmapFile::MapNewRegion() {
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
      return IOStatus::IOError("Error allocating space to file : " + filename_ +
                               "Error : " + strerror(alloc_status));
    }
  }

  TEST_KILL_RANDOM("PosixMmapFile::Append:1", rocksdb_kill_odds);
  void* ptr = mmap(nullptr, map_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_,
                   file_offset_);
  if (ptr == MAP_FAILED) {
    return IOStatus::IOError("MMap failed on " + filename_);
  }
  TEST_KILL_RANDOM("PosixMmapFile::Append:2", rocksdb_kill_odds);

  base_ = reinterpret_cast<char*>(ptr);
  limit_ = base_ + map_size_;
  dst_ = base_;
  last_sync_ = base_;
  return IOStatus::OK();
#else
  return IOStatus::NotSupported("This platform doesn't support fallocate()");
#endif
}

IOStatus PosixMmapFile::Msync() {
  if (dst_ == last_sync_) {
    return IOStatus::OK();
  }
  // Find the beginnings of the pages that contain the first and last
  // bytes to be synced.
  size_t p1 = TruncateToPageBoundary(last_sync_ - base_);
  size_t p2 = TruncateToPageBoundary(dst_ - base_ - 1);
  last_sync_ = dst_;
  TEST_KILL_RANDOM("PosixMmapFile::Msync:0", rocksdb_kill_odds);
  if (msync(base_ + p1, p2 - p1 + page_size_, MS_SYNC) < 0) {
    return IOError("While msync", filename_, errno);
  }
  return IOStatus::OK();
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
#else
  (void)options;
#endif
  assert((page_size & (page_size - 1)) == 0);
  assert(options.use_mmap_writes);
  assert(!options.use_direct_writes);
}

PosixMmapFile::~PosixMmapFile() {
  if (fd_ >= 0) {
    IOStatus s = PosixMmapFile::Close(IOOptions(), nullptr);
    s.PermitUncheckedError();
  }
}

IOStatus PosixMmapFile::Append(const Slice& data, const IOOptions& /*opts*/,
                               IODebugContext* /*dbg*/) {
  const char* src = data.data();
  size_t left = data.size();
  while (left > 0) {
    assert(base_ <= dst_);
    assert(dst_ <= limit_);
    size_t avail = limit_ - dst_;
    if (avail == 0) {
      IOStatus s = UnmapCurrentRegion();
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
    assert(dst_);
    memcpy(dst_, src, n);
    dst_ += n;
    src += n;
    left -= n;
  }
  return IOStatus::OK();
}

IOStatus PosixMmapFile::Close(const IOOptions& /*opts*/,
                              IODebugContext* /*dbg*/) {
  IOStatus s;
  size_t unused = limit_ - dst_;

  s = UnmapCurrentRegion();
  if (!s.ok()) {
    s = IOError("While closing mmapped file", filename_, errno);
  } else if (unused > 0) {
    // Trim the extra space at the end of the file
    if (ftruncate(fd_, file_offset_ - unused) < 0) {
      s = IOError("While ftruncating mmaped file", filename_, errno);
    }
  }

  if (close(fd_) < 0) {
    if (s.ok()) {
      s = IOError("While closing mmapped file", filename_, errno);
    }
  }

  fd_ = -1;
  base_ = nullptr;
  limit_ = nullptr;
  return s;
}

IOStatus PosixMmapFile::Flush(const IOOptions& /*opts*/,
                              IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

IOStatus PosixMmapFile::Sync(const IOOptions& /*opts*/,
                             IODebugContext* /*dbg*/) {
  if (fdatasync(fd_) < 0) {
    return IOError("While fdatasync mmapped file", filename_, errno);
  }

  return Msync();
}

/**
 * Flush data as well as metadata to stable storage.
 */
IOStatus PosixMmapFile::Fsync(const IOOptions& /*opts*/,
                              IODebugContext* /*dbg*/) {
  if (fsync(fd_) < 0) {
    return IOError("While fsync mmaped file", filename_, errno);
  }

  return Msync();
}

/**
 * Get the size of valid data in the file. This will not match the
 * size that is returned from the filesystem because we use mmap
 * to extend file by map_size every time.
 */
uint64_t PosixMmapFile::GetFileSize(const IOOptions& /*opts*/,
                                    IODebugContext* /*dbg*/) {
  size_t used = dst_ - base_;
  return file_offset_ + used;
}

IOStatus PosixMmapFile::InvalidateCache(size_t offset, size_t length) {
#ifndef OS_LINUX
  (void)offset;
  (void)length;
  return IOStatus::OK();
#else
  // free OS pages
  int ret = Fadvise(fd_, offset, length, POSIX_FADV_DONTNEED);
  if (ret == 0) {
    return IOStatus::OK();
  }
  return IOError("While fadvise NotNeeded mmapped file", filename_, errno);
#endif
}

#ifdef ROCKSDB_FALLOCATE_PRESENT
IOStatus PosixMmapFile::Allocate(uint64_t offset, uint64_t len,
                                 const IOOptions& /*opts*/,
                                 IODebugContext* /*dbg*/) {
  assert(offset <= static_cast<uint64_t>(std::numeric_limits<off_t>::max()));
  assert(len <= static_cast<uint64_t>(std::numeric_limits<off_t>::max()));
  TEST_KILL_RANDOM("PosixMmapFile::Allocate:0", rocksdb_kill_odds);
  int alloc_status = 0;
  if (allow_fallocate_) {
    alloc_status =
        fallocate(fd_, fallocate_with_keep_size_ ? FALLOC_FL_KEEP_SIZE : 0,
                  static_cast<off_t>(offset), static_cast<off_t>(len));
  }
  if (alloc_status == 0) {
    return IOStatus::OK();
  } else {
    return IOError(
        "While fallocate offset " + ToString(offset) + " len " + ToString(len),
        filename_, errno);
  }
}
#endif

/*
 * PosixWritableFile
 *
 * Use posix write to write data to a file.
 */
PosixWritableFile::PosixWritableFile(const std::string& fname, int fd,
                                     size_t logical_block_size,
                                     const EnvOptions& options)
    : FSWritableFile(options),
      filename_(fname),
      use_direct_io_(options.use_direct_writes),
      fd_(fd),
      filesize_(0),
      logical_sector_size_(logical_block_size) {
#ifdef ROCKSDB_FALLOCATE_PRESENT
  allow_fallocate_ = options.allow_fallocate;
  fallocate_with_keep_size_ = options.fallocate_with_keep_size;
#endif
#ifdef ROCKSDB_RANGESYNC_PRESENT
  sync_file_range_supported_ = IsSyncFileRangeSupported(fd_);
#endif  // ROCKSDB_RANGESYNC_PRESENT
  assert(!options.use_mmap_writes);
}

PosixWritableFile::~PosixWritableFile() {
  if (fd_ >= 0) {
    IOStatus s = PosixWritableFile::Close(IOOptions(), nullptr);
    s.PermitUncheckedError();
  }
}

IOStatus PosixWritableFile::Append(const Slice& data, const IOOptions& /*opts*/,
                                   IODebugContext* /*dbg*/) {
  if (use_direct_io()) {
    assert(IsSectorAligned(data.size(), GetRequiredBufferAlignment()));
    assert(IsSectorAligned(data.data(), GetRequiredBufferAlignment()));
  }
  const char* src = data.data();
  size_t nbytes = data.size();

  if (!PosixWrite(fd_, src, nbytes)) {
    return IOError("While appending to file", filename_, errno);
  }

  filesize_ += nbytes;
  return IOStatus::OK();
}

IOStatus PosixWritableFile::PositionedAppend(const Slice& data, uint64_t offset,
                                             const IOOptions& /*opts*/,
                                             IODebugContext* /*dbg*/) {
  if (use_direct_io()) {
    assert(IsSectorAligned(offset, GetRequiredBufferAlignment()));
    assert(IsSectorAligned(data.size(), GetRequiredBufferAlignment()));
    assert(IsSectorAligned(data.data(), GetRequiredBufferAlignment()));
  }
  assert(offset <= static_cast<uint64_t>(std::numeric_limits<off_t>::max()));
  const char* src = data.data();
  size_t nbytes = data.size();
  if (!PosixPositionedWrite(fd_, src, nbytes, static_cast<off_t>(offset))) {
    return IOError("While pwrite to file at offset " + ToString(offset),
                   filename_, errno);
  }
  filesize_ = offset + nbytes;
  return IOStatus::OK();
}

IOStatus PosixWritableFile::Truncate(uint64_t size, const IOOptions& /*opts*/,
                                     IODebugContext* /*dbg*/) {
  IOStatus s;
  int r = ftruncate(fd_, size);
  if (r < 0) {
    s = IOError("While ftruncate file to size " + ToString(size), filename_,
                errno);
  } else {
    filesize_ = size;
  }
  return s;
}

IOStatus PosixWritableFile::Close(const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) {
  IOStatus s;

  size_t block_size;
  size_t last_allocated_block;
  GetPreallocationStatus(&block_size, &last_allocated_block);
  if (last_allocated_block > 0) {
    // trim the extra space preallocated at the end of the file
    // NOTE(ljin): we probably don't want to surface failure as an IOError,
    // but it will be nice to log these errors.
    int dummy __attribute__((__unused__));
    dummy = ftruncate(fd_, filesize_);
#if defined(ROCKSDB_FALLOCATE_PRESENT) && defined(FALLOC_FL_PUNCH_HOLE) && \
    !defined(TRAVIS)
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
    // TRAVIS - this code does not work on TRAVIS filesystems.
    // the FALLOC_FL_KEEP_SIZE option is expected to not change the size
    // of the file, but it does. Simple strace report will show that.
    // While we work with Travis-CI team to figure out if this is a
    // quirk of Docker/AUFS, we will comment this out.
    struct stat file_stats;
    int result = fstat(fd_, &file_stats);
    // After ftruncate, we check whether ftruncate has the correct behavior.
    // If not, we should hack it with FALLOC_FL_PUNCH_HOLE
    if (result == 0 &&
        (file_stats.st_size + file_stats.st_blksize - 1) /
                file_stats.st_blksize !=
            file_stats.st_blocks / (file_stats.st_blksize / 512)) {
      IOSTATS_TIMER_GUARD(allocate_nanos);
      if (allow_fallocate_) {
        fallocate(fd_, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE, filesize_,
                  block_size * last_allocated_block - filesize_);
      }
    }
#endif
  }

  if (close(fd_) < 0) {
    s = IOError("While closing file after writing", filename_, errno);
  }
  fd_ = -1;
  return s;
}

// write out the cached data to the OS cache
IOStatus PosixWritableFile::Flush(const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

IOStatus PosixWritableFile::Sync(const IOOptions& /*opts*/,
                                 IODebugContext* /*dbg*/) {
  if (fdatasync(fd_) < 0) {
    return IOError("While fdatasync", filename_, errno);
  }
  return IOStatus::OK();
}

IOStatus PosixWritableFile::Fsync(const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) {
  if (fsync(fd_) < 0) {
    return IOError("While fsync", filename_, errno);
  }
  return IOStatus::OK();
}

bool PosixWritableFile::IsSyncThreadSafe() const { return true; }

uint64_t PosixWritableFile::GetFileSize(const IOOptions& /*opts*/,
                                        IODebugContext* /*dbg*/) {
  return filesize_;
}

void PosixWritableFile::SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) {
#ifdef OS_LINUX
// Suppress Valgrind "Unimplemented functionality" error.
#ifndef ROCKSDB_VALGRIND_RUN
  if (hint == write_hint_) {
    return;
  }
  if (fcntl(fd_, F_SET_RW_HINT, &hint) == 0) {
    write_hint_ = hint;
  }
#else
  (void)hint;
#endif  // ROCKSDB_VALGRIND_RUN
#else
  (void)hint;
#endif  // OS_LINUX
}

IOStatus PosixWritableFile::InvalidateCache(size_t offset, size_t length) {
  if (use_direct_io()) {
    return IOStatus::OK();
  }
#ifndef OS_LINUX
  (void)offset;
  (void)length;
  return IOStatus::OK();
#else
  // free OS pages
  int ret = Fadvise(fd_, offset, length, POSIX_FADV_DONTNEED);
  if (ret == 0) {
    return IOStatus::OK();
  }
  return IOError("While fadvise NotNeeded", filename_, errno);
#endif
}

#ifdef ROCKSDB_FALLOCATE_PRESENT
IOStatus PosixWritableFile::Allocate(uint64_t offset, uint64_t len,
                                     const IOOptions& /*opts*/,
                                     IODebugContext* /*dbg*/) {
  assert(offset <= static_cast<uint64_t>(std::numeric_limits<off_t>::max()));
  assert(len <= static_cast<uint64_t>(std::numeric_limits<off_t>::max()));
  TEST_KILL_RANDOM("PosixWritableFile::Allocate:0", rocksdb_kill_odds);
  IOSTATS_TIMER_GUARD(allocate_nanos);
  int alloc_status = 0;
  if (allow_fallocate_) {
    alloc_status =
        fallocate(fd_, fallocate_with_keep_size_ ? FALLOC_FL_KEEP_SIZE : 0,
                  static_cast<off_t>(offset), static_cast<off_t>(len));
  }
  if (alloc_status == 0) {
    return IOStatus::OK();
  } else {
    return IOError(
        "While fallocate offset " + ToString(offset) + " len " + ToString(len),
        filename_, errno);
  }
}
#endif

IOStatus PosixWritableFile::RangeSync(uint64_t offset, uint64_t nbytes,
                                      const IOOptions& opts,
                                      IODebugContext* dbg) {
#ifdef ROCKSDB_RANGESYNC_PRESENT
  assert(offset <= static_cast<uint64_t>(std::numeric_limits<off_t>::max()));
  assert(nbytes <= static_cast<uint64_t>(std::numeric_limits<off_t>::max()));
  if (sync_file_range_supported_) {
    int ret;
    if (strict_bytes_per_sync_) {
      // Specifying `SYNC_FILE_RANGE_WAIT_BEFORE` together with an offset/length
      // that spans all bytes written so far tells `sync_file_range` to wait for
      // any outstanding writeback requests to finish before issuing a new one.
      ret =
          sync_file_range(fd_, 0, static_cast<off_t>(offset + nbytes),
                          SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE);
    } else {
      ret = sync_file_range(fd_, static_cast<off_t>(offset),
                            static_cast<off_t>(nbytes), SYNC_FILE_RANGE_WRITE);
    }
    if (ret != 0) {
      return IOError("While sync_file_range returned " + ToString(ret),
                     filename_, errno);
    }
    return IOStatus::OK();
  }
#endif  // ROCKSDB_RANGESYNC_PRESENT
  return FSWritableFile::RangeSync(offset, nbytes, opts, dbg);
}

#ifdef OS_LINUX
size_t PosixWritableFile::GetUniqueId(char* id, size_t max_size) const {
  return PosixHelper::GetUniqueIdFromFile(fd_, id, max_size);
}
#endif

/*
 * PosixRandomRWFile
 */

PosixRandomRWFile::PosixRandomRWFile(const std::string& fname, int fd,
                                     const EnvOptions& /*options*/)
    : filename_(fname), fd_(fd) {}

PosixRandomRWFile::~PosixRandomRWFile() {
  if (fd_ >= 0) {
    IOStatus s = Close(IOOptions(), nullptr);
    s.PermitUncheckedError();
  }
}

IOStatus PosixRandomRWFile::Write(uint64_t offset, const Slice& data,
                                  const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) {
  const char* src = data.data();
  size_t nbytes = data.size();
  if (!PosixPositionedWrite(fd_, src, nbytes, static_cast<off_t>(offset))) {
    return IOError(
        "While write random read/write file at offset " + ToString(offset),
        filename_, errno);
  }

  return IOStatus::OK();
}

IOStatus PosixRandomRWFile::Read(uint64_t offset, size_t n,
                                 const IOOptions& /*opts*/, Slice* result,
                                 char* scratch, IODebugContext* /*dbg*/) const {
  size_t left = n;
  char* ptr = scratch;
  while (left > 0) {
    ssize_t done = pread(fd_, ptr, left, offset);
    if (done < 0) {
      // error while reading from file
      if (errno == EINTR) {
        // read was interrupted, try again.
        continue;
      }
      return IOError("While reading random read/write file offset " +
                         ToString(offset) + " len " + ToString(n),
                     filename_, errno);
    } else if (done == 0) {
      // Nothing more to read
      break;
    }

    // Read `done` bytes
    ptr += done;
    offset += done;
    left -= done;
  }

  *result = Slice(scratch, n - left);
  return IOStatus::OK();
}

IOStatus PosixRandomRWFile::Flush(const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

IOStatus PosixRandomRWFile::Sync(const IOOptions& /*opts*/,
                                 IODebugContext* /*dbg*/) {
  if (fdatasync(fd_) < 0) {
    return IOError("While fdatasync random read/write file", filename_, errno);
  }
  return IOStatus::OK();
}

IOStatus PosixRandomRWFile::Fsync(const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) {
  if (fsync(fd_) < 0) {
    return IOError("While fsync random read/write file", filename_, errno);
  }
  return IOStatus::OK();
}

IOStatus PosixRandomRWFile::Close(const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) {
  if (close(fd_) < 0) {
    return IOError("While close random read/write file", filename_, errno);
  }
  fd_ = -1;
  return IOStatus::OK();
}

PosixMemoryMappedFileBuffer::~PosixMemoryMappedFileBuffer() {
  // TODO should have error handling though not much we can do...
  munmap(this->base_, length_);
}

/*
 * PosixDirectory
 */

PosixDirectory::~PosixDirectory() { close(fd_); }

IOStatus PosixDirectory::Fsync(const IOOptions& /*opts*/,
                               IODebugContext* /*dbg*/) {
#ifndef OS_AIX
  if (fsync(fd_) == -1) {
    return IOError("While fsync", "a directory", errno);
  }
#endif
  return IOStatus::OK();
}
}  // namespace ROCKSDB_NAMESPACE
#endif
