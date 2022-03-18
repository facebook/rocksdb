//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors

#if !defined(OS_WIN)

#include <dirent.h>
#ifndef ROCKSDB_NO_DYNAMIC_EXTENSION
#include <dlfcn.h>
#endif
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#if defined(OS_LINUX) || defined(OS_SOLARIS) || defined(OS_ANDROID)
#include <sys/statfs.h>
#include <sys/sysmacros.h>
#endif
#include <sys/statvfs.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>

#include <algorithm>
// Get nano time includes
#if defined(OS_LINUX) || defined(OS_FREEBSD)
#elif defined(__MACH__)
#include <Availability.h>
#include <mach/clock.h>
#include <mach/mach.h>
#else
#include <chrono>
#endif
#include <deque>
#include <set>
#include <vector>

#include "env/composite_env_wrapper.h"
#include "env/io_posix.h"
#include "logging/posix_logger.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/thread_status_updater.h"
#include "port/port.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/object_registry.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/compression_context_cache.h"
#include "util/random.h"
#include "util/string_util.h"
#include "util/thread_local.h"
#include "util/threadpool_imp.h"

#if !defined(TMPFS_MAGIC)
#define TMPFS_MAGIC 0x01021994
#endif
#if !defined(XFS_SUPER_MAGIC)
#define XFS_SUPER_MAGIC 0x58465342
#endif
#if !defined(EXT4_SUPER_MAGIC)
#define EXT4_SUPER_MAGIC 0xEF53
#endif

extern "C" bool RocksDbIOUringEnable() __attribute__((__weak__));

namespace ROCKSDB_NAMESPACE {

namespace {

inline mode_t GetDBFileMode(bool allow_non_owner_access) {
  return allow_non_owner_access ? 0644 : 0600;
}

static uint64_t gettid() { return Env::Default()->GetThreadID(); }

// list of pathnames that are locked
// Only used for error message.
struct LockHoldingInfo {
  int64_t acquire_time;
  uint64_t acquiring_thread;
};
static std::map<std::string, LockHoldingInfo> locked_files;
static port::Mutex mutex_locked_files;

static int LockOrUnlock(int fd, bool lock) {
  errno = 0;
  struct flock f;
  memset(&f, 0, sizeof(f));
  f.l_type = (lock ? F_WRLCK : F_UNLCK);
  f.l_whence = SEEK_SET;
  f.l_start = 0;
  f.l_len = 0;  // Lock/unlock entire file
  int value = fcntl(fd, F_SETLK, &f);

  return value;
}

class PosixFileLock : public FileLock {
 public:
  int fd_ = /*invalid*/ -1;
  std::string filename;

  void Clear() {
    fd_ = -1;
    filename.clear();
  }

  virtual ~PosixFileLock() override {
    // Check for destruction without UnlockFile
    assert(fd_ == -1);
  }
};

int cloexec_flags(int flags, const EnvOptions* options) {
  // If the system supports opening the file with cloexec enabled,
  // do so, as this avoids a race condition if a db is opened around
  // the same time that a child process is forked
#ifdef O_CLOEXEC
  if (options == nullptr || options->set_fd_cloexec) {
    flags |= O_CLOEXEC;
  }
#else
  (void)options;
#endif
  return flags;
}

class PosixFileSystem : public FileSystem {
 public:
  PosixFileSystem();

  static const char* kClassName() { return "PosixFileSystem"; }
  const char* Name() const override { return kClassName(); }
  const char* NickName() const override { return kDefaultName(); }

  ~PosixFileSystem() override {}

  void SetFD_CLOEXEC(int fd, const EnvOptions* options) {
    if ((options == nullptr || options->set_fd_cloexec) && fd > 0) {
      fcntl(fd, F_SETFD, fcntl(fd, F_GETFD) | FD_CLOEXEC);
    }
  }

  IOStatus NewSequentialFile(const std::string& fname,
                             const FileOptions& options,
                             std::unique_ptr<FSSequentialFile>* result,
                             IODebugContext* /*dbg*/) override {
    result->reset();
    int fd = -1;
    int flags = cloexec_flags(O_RDONLY, &options);
    FILE* file = nullptr;

    if (options.use_direct_reads && !options.use_mmap_reads) {
#ifdef ROCKSDB_LITE
      return IOStatus::IOError(fname,
                               "Direct I/O not supported in RocksDB lite");
#endif  // !ROCKSDB_LITE
#if !defined(OS_MACOSX) && !defined(OS_OPENBSD) && !defined(OS_SOLARIS)
      flags |= O_DIRECT;
      TEST_SYNC_POINT_CALLBACK("NewSequentialFile:O_DIRECT", &flags);
#endif
    }

    do {
      IOSTATS_TIMER_GUARD(open_nanos);
      fd = open(fname.c_str(), flags, GetDBFileMode(allow_non_owner_access_));
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
      return IOError("While opening a file for sequentially reading", fname,
                     errno);
    }

    SetFD_CLOEXEC(fd, &options);

    if (options.use_direct_reads && !options.use_mmap_reads) {
#ifdef OS_MACOSX
      if (fcntl(fd, F_NOCACHE, 1) == -1) {
        close(fd);
        return IOError("While fcntl NoCache", fname, errno);
      }
#endif
    } else {
      do {
        IOSTATS_TIMER_GUARD(open_nanos);
        file = fdopen(fd, "r");
      } while (file == nullptr && errno == EINTR);
      if (file == nullptr) {
        close(fd);
        return IOError("While opening file for sequentially read", fname,
                       errno);
      }
    }
    result->reset(new PosixSequentialFile(
        fname, file, fd, GetLogicalBlockSizeForReadIfNeeded(options, fname, fd),
        options));
    return IOStatus::OK();
  }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& options,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* /*dbg*/) override {
    result->reset();
    IOStatus s = IOStatus::OK();
    int fd;
    int flags = cloexec_flags(O_RDONLY, &options);

    if (options.use_direct_reads && !options.use_mmap_reads) {
#ifdef ROCKSDB_LITE
      return IOStatus::IOError(fname,
                               "Direct I/O not supported in RocksDB lite");
#endif  // !ROCKSDB_LITE
#if !defined(OS_MACOSX) && !defined(OS_OPENBSD) && !defined(OS_SOLARIS)
      flags |= O_DIRECT;
      TEST_SYNC_POINT_CALLBACK("NewRandomAccessFile:O_DIRECT", &flags);
#endif
    }

    do {
      IOSTATS_TIMER_GUARD(open_nanos);
      fd = open(fname.c_str(), flags, GetDBFileMode(allow_non_owner_access_));
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
      s = IOError("While open a file for random read", fname, errno);
      return s;
    }
    SetFD_CLOEXEC(fd, &options);

    if (options.use_mmap_reads) {
      // Use of mmap for random reads has been removed because it
      // kills performance when storage is fast.
      // Use mmap when virtual address-space is plentiful.
      uint64_t size;
      IOOptions opts;
      s = GetFileSize(fname, opts, &size, nullptr);
      if (s.ok()) {
        void* base = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (base != MAP_FAILED) {
          result->reset(
              new PosixMmapReadableFile(fd, fname, base, size, options));
        } else {
          s = IOError("while mmap file for read", fname, errno);
          close(fd);
        }
      } else {
        close(fd);
      }
    } else {
      if (options.use_direct_reads && !options.use_mmap_reads) {
#ifdef OS_MACOSX
        if (fcntl(fd, F_NOCACHE, 1) == -1) {
          close(fd);
          return IOError("while fcntl NoCache", fname, errno);
        }
#endif
      }
      result->reset(new PosixRandomAccessFile(
          fname, fd, GetLogicalBlockSizeForReadIfNeeded(options, fname, fd),
          options
#if defined(ROCKSDB_IOURING_PRESENT)
          ,
          !IsIOUringEnabled() ? nullptr : thread_local_io_urings_.get()
#endif
              ));
    }
    return s;
  }

  virtual IOStatus OpenWritableFile(const std::string& fname,
                                    const FileOptions& options, bool reopen,
                                    std::unique_ptr<FSWritableFile>* result,
                                    IODebugContext* /*dbg*/) {
    result->reset();
    IOStatus s;
    int fd = -1;
    int flags = (reopen) ? (O_CREAT | O_APPEND) : (O_CREAT | O_TRUNC);
    // Direct IO mode with O_DIRECT flag or F_NOCAHCE (MAC OSX)
    if (options.use_direct_writes && !options.use_mmap_writes) {
      // Note: we should avoid O_APPEND here due to ta the following bug:
      // POSIX requires that opening a file with the O_APPEND flag should
      // have no affect on the location at which pwrite() writes data.
      // However, on Linux, if a file is opened with O_APPEND, pwrite()
      // appends data to the end of the file, regardless of the value of
      // offset.
      // More info here: https://linux.die.net/man/2/pwrite
#ifdef ROCKSDB_LITE
      return IOStatus::IOError(fname,
                               "Direct I/O not supported in RocksDB lite");
#endif  // ROCKSDB_LITE
      flags |= O_WRONLY;
#if !defined(OS_MACOSX) && !defined(OS_OPENBSD) && !defined(OS_SOLARIS)
      flags |= O_DIRECT;
#endif
      TEST_SYNC_POINT_CALLBACK("NewWritableFile:O_DIRECT", &flags);
    } else if (options.use_mmap_writes) {
      // non-direct I/O
      flags |= O_RDWR;
    } else {
      flags |= O_WRONLY;
    }

    flags = cloexec_flags(flags, &options);

    do {
      IOSTATS_TIMER_GUARD(open_nanos);
      fd = open(fname.c_str(), flags, GetDBFileMode(allow_non_owner_access_));
    } while (fd < 0 && errno == EINTR);

    if (fd < 0) {
      s = IOError("While open a file for appending", fname, errno);
      return s;
    }
    SetFD_CLOEXEC(fd, &options);

    if (options.use_mmap_writes) {
      MaybeForceDisableMmap(fd);
    }
    if (options.use_mmap_writes && !forceMmapOff_) {
      result->reset(new PosixMmapFile(fname, fd, page_size_, options));
    } else if (options.use_direct_writes && !options.use_mmap_writes) {
#ifdef OS_MACOSX
      if (fcntl(fd, F_NOCACHE, 1) == -1) {
        close(fd);
        s = IOError("While fcntl NoCache an opened file for appending", fname,
                    errno);
        return s;
      }
#elif defined(OS_SOLARIS)
      if (directio(fd, DIRECTIO_ON) == -1) {
        if (errno != ENOTTY) {  // ZFS filesystems don't support DIRECTIO_ON
          close(fd);
          s = IOError("While calling directio()", fname, errno);
          return s;
        }
      }
#endif
      result->reset(new PosixWritableFile(
          fname, fd, GetLogicalBlockSizeForWriteIfNeeded(options, fname, fd),
          options));
    } else {
      // disable mmap writes
      EnvOptions no_mmap_writes_options = options;
      no_mmap_writes_options.use_mmap_writes = false;
      result->reset(
          new PosixWritableFile(fname, fd,
                                GetLogicalBlockSizeForWriteIfNeeded(
                                    no_mmap_writes_options, fname, fd),
                                no_mmap_writes_options));
    }
    return s;
  }

  IOStatus NewWritableFile(const std::string& fname, const FileOptions& options,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override {
    return OpenWritableFile(fname, options, false, result, dbg);
  }

  IOStatus ReopenWritableFile(const std::string& fname,
                              const FileOptions& options,
                              std::unique_ptr<FSWritableFile>* result,
                              IODebugContext* dbg) override {
    return OpenWritableFile(fname, options, true, result, dbg);
  }

  IOStatus ReuseWritableFile(const std::string& fname,
                             const std::string& old_fname,
                             const FileOptions& options,
                             std::unique_ptr<FSWritableFile>* result,
                             IODebugContext* /*dbg*/) override {
    result->reset();
    IOStatus s;
    int fd = -1;

    int flags = 0;
    // Direct IO mode with O_DIRECT flag or F_NOCAHCE (MAC OSX)
    if (options.use_direct_writes && !options.use_mmap_writes) {
#ifdef ROCKSDB_LITE
      return IOStatus::IOError(fname,
                               "Direct I/O not supported in RocksDB lite");
#endif  // !ROCKSDB_LITE
      flags |= O_WRONLY;
#if !defined(OS_MACOSX) && !defined(OS_OPENBSD) && !defined(OS_SOLARIS)
      flags |= O_DIRECT;
#endif
      TEST_SYNC_POINT_CALLBACK("NewWritableFile:O_DIRECT", &flags);
    } else if (options.use_mmap_writes) {
      // mmap needs O_RDWR mode
      flags |= O_RDWR;
    } else {
      flags |= O_WRONLY;
    }

    flags = cloexec_flags(flags, &options);

    do {
      IOSTATS_TIMER_GUARD(open_nanos);
      fd = open(old_fname.c_str(), flags,
                GetDBFileMode(allow_non_owner_access_));
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
      s = IOError("while reopen file for write", fname, errno);
      return s;
    }

    SetFD_CLOEXEC(fd, &options);
    // rename into place
    if (rename(old_fname.c_str(), fname.c_str()) != 0) {
      s = IOError("while rename file to " + fname, old_fname, errno);
      close(fd);
      return s;
    }

    if (options.use_mmap_writes) {
      MaybeForceDisableMmap(fd);
    }
    if (options.use_mmap_writes && !forceMmapOff_) {
      result->reset(new PosixMmapFile(fname, fd, page_size_, options));
    } else if (options.use_direct_writes && !options.use_mmap_writes) {
#ifdef OS_MACOSX
      if (fcntl(fd, F_NOCACHE, 1) == -1) {
        close(fd);
        s = IOError("while fcntl NoCache for reopened file for append", fname,
                    errno);
        return s;
      }
#elif defined(OS_SOLARIS)
      if (directio(fd, DIRECTIO_ON) == -1) {
        if (errno != ENOTTY) {  // ZFS filesystems don't support DIRECTIO_ON
          close(fd);
          s = IOError("while calling directio()", fname, errno);
          return s;
        }
      }
#endif
      result->reset(new PosixWritableFile(
          fname, fd, GetLogicalBlockSizeForWriteIfNeeded(options, fname, fd),
          options));
    } else {
      // disable mmap writes
      FileOptions no_mmap_writes_options = options;
      no_mmap_writes_options.use_mmap_writes = false;
      result->reset(
          new PosixWritableFile(fname, fd,
                                GetLogicalBlockSizeForWriteIfNeeded(
                                    no_mmap_writes_options, fname, fd),
                                no_mmap_writes_options));
    }
    return s;
  }

  IOStatus NewRandomRWFile(const std::string& fname, const FileOptions& options,
                           std::unique_ptr<FSRandomRWFile>* result,
                           IODebugContext* /*dbg*/) override {
    int fd = -1;
    int flags = cloexec_flags(O_RDWR, &options);

    while (fd < 0) {
      IOSTATS_TIMER_GUARD(open_nanos);

      fd = open(fname.c_str(), flags, GetDBFileMode(allow_non_owner_access_));
      if (fd < 0) {
        // Error while opening the file
        if (errno == EINTR) {
          continue;
        }
        return IOError("While open file for random read/write", fname, errno);
      }
    }

    SetFD_CLOEXEC(fd, &options);
    result->reset(new PosixRandomRWFile(fname, fd, options));
    return IOStatus::OK();
  }

  IOStatus NewMemoryMappedFileBuffer(
      const std::string& fname,
      std::unique_ptr<MemoryMappedFileBuffer>* result) override {
    int fd = -1;
    IOStatus status;
    int flags = cloexec_flags(O_RDWR, nullptr);

    while (fd < 0) {
      IOSTATS_TIMER_GUARD(open_nanos);
      fd = open(fname.c_str(), flags, 0644);
      if (fd < 0) {
        // Error while opening the file
        if (errno == EINTR) {
          continue;
        }
        status =
            IOError("While open file for raw mmap buffer access", fname, errno);
        break;
      }
    }
    uint64_t size;
    if (status.ok()) {
      IOOptions opts;
      status = GetFileSize(fname, opts, &size, nullptr);
    }
    void* base = nullptr;
    if (status.ok()) {
      base = mmap(nullptr, static_cast<size_t>(size), PROT_READ | PROT_WRITE,
                  MAP_SHARED, fd, 0);
      if (base == MAP_FAILED) {
        status = IOError("while mmap file for read", fname, errno);
      }
    }
    if (status.ok()) {
      result->reset(
          new PosixMemoryMappedFileBuffer(base, static_cast<size_t>(size)));
    }
    if (fd >= 0) {
      // don't need to keep it open after mmap has been called
      close(fd);
    }
    return status;
  }

  IOStatus NewDirectory(const std::string& name, const IOOptions& /*opts*/,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* /*dbg*/) override {
    result->reset();
    int fd;
    int flags = cloexec_flags(0, nullptr);
    {
      IOSTATS_TIMER_GUARD(open_nanos);
      fd = open(name.c_str(), flags);
    }
    if (fd < 0) {
      return IOError("While open directory", name, errno);
    } else {
      result->reset(new PosixDirectory(fd));
    }
    return IOStatus::OK();
  }

  IOStatus NewLogger(const std::string& fname, const IOOptions& /*opts*/,
                     std::shared_ptr<Logger>* result,
                     IODebugContext* /*dbg*/) override {
    FILE* f = nullptr;
    int fd;
    {
      IOSTATS_TIMER_GUARD(open_nanos);
      fd = open(fname.c_str(),
                cloexec_flags(O_WRONLY | O_CREAT | O_TRUNC, nullptr),
                GetDBFileMode(allow_non_owner_access_));
      if (fd != -1) {
        f = fdopen(fd,
                   "w"
#ifdef __GLIBC_PREREQ
#if __GLIBC_PREREQ(2, 7)
                   "e"  // glibc extension to enable O_CLOEXEC
#endif
#endif
        );
      }
    }
    if (fd == -1) {
      result->reset();
      return status_to_io_status(
          IOError("when open a file for new logger", fname, errno));
    }
    if (f == nullptr) {
      close(fd);
      result->reset();
      return status_to_io_status(
          IOError("when fdopen a file for new logger", fname, errno));
    } else {
#ifdef ROCKSDB_FALLOCATE_PRESENT
      fallocate(fd, FALLOC_FL_KEEP_SIZE, 0, 4 * 1024);
#endif
      SetFD_CLOEXEC(fd, nullptr);
      result->reset(new PosixLogger(f, &gettid, Env::Default()));
      return IOStatus::OK();
    }
  }

  IOStatus FileExists(const std::string& fname, const IOOptions& /*opts*/,
                      IODebugContext* /*dbg*/) override {
    int result = access(fname.c_str(), F_OK);

    if (result == 0) {
      return IOStatus::OK();
    }

    int err = errno;
    switch (err) {
      case EACCES:
      case ELOOP:
      case ENAMETOOLONG:
      case ENOENT:
      case ENOTDIR:
        return IOStatus::NotFound();
      default:
        assert(err == EIO || err == ENOMEM);
        return IOStatus::IOError("Unexpected error(" +
                                 ROCKSDB_NAMESPACE::ToString(err) +
                                 ") accessing file `" + fname + "' ");
    }
  }

  IOStatus GetChildren(const std::string& dir, const IOOptions& /*opts*/,
                       std::vector<std::string>* result,
                       IODebugContext* /*dbg*/) override {
    result->clear();

    DIR* d = opendir(dir.c_str());
    if (d == nullptr) {
      switch (errno) {
        case EACCES:
        case ENOENT:
        case ENOTDIR:
          return IOStatus::NotFound();
        default:
          return IOError("While opendir", dir, errno);
      }
    }

    // reset errno before calling readdir()
    errno = 0;
    struct dirent* entry;
    while ((entry = readdir(d)) != nullptr) {
      // filter out '.' and '..' directory entries
      // which appear only on some platforms
      const bool ignore =
          entry->d_type == DT_DIR &&
          (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0);
      if (!ignore) {
        result->push_back(entry->d_name);
      }
      errno = 0;  // reset errno if readdir() success
    }

    // always attempt to close the dir
    const auto pre_close_errno = errno;  // errno may be modified by closedir
    const int close_result = closedir(d);

    if (pre_close_errno != 0) {
      // error occurred during readdir
      return IOError("While readdir", dir, pre_close_errno);
    }

    if (close_result != 0) {
      // error occurred during closedir
      return IOError("While closedir", dir, errno);
    }

    return IOStatus::OK();
  }

  IOStatus DeleteFile(const std::string& fname, const IOOptions& /*opts*/,
                      IODebugContext* /*dbg*/) override {
    IOStatus result;
    if (unlink(fname.c_str()) != 0) {
      result = IOError("while unlink() file", fname, errno);
    }
    return result;
  }

  IOStatus CreateDir(const std::string& name, const IOOptions& /*opts*/,
                     IODebugContext* /*dbg*/) override {
    if (mkdir(name.c_str(), 0755) != 0) {
      return IOError("While mkdir", name, errno);
    }
    return IOStatus::OK();
  }

  IOStatus CreateDirIfMissing(const std::string& name,
                              const IOOptions& /*opts*/,
                              IODebugContext* /*dbg*/) override {
    if (mkdir(name.c_str(), 0755) != 0) {
      if (errno != EEXIST) {
        return IOError("While mkdir if missing", name, errno);
      } else if (!DirExists(name)) {  // Check that name is actually a
                                      // directory.
        // Message is taken from mkdir
        return IOStatus::IOError("`" + name +
                                 "' exists but is not a directory");
      }
    }
    return IOStatus::OK();
  }

  IOStatus DeleteDir(const std::string& name, const IOOptions& /*opts*/,
                     IODebugContext* /*dbg*/) override {
    if (rmdir(name.c_str()) != 0) {
      return IOError("file rmdir", name, errno);
    }
    return IOStatus::OK();
  }

  IOStatus GetFileSize(const std::string& fname, const IOOptions& /*opts*/,
                       uint64_t* size, IODebugContext* /*dbg*/) override {
    struct stat sbuf;
    if (stat(fname.c_str(), &sbuf) != 0) {
      *size = 0;
      return IOError("while stat a file for size", fname, errno);
    } else {
      *size = sbuf.st_size;
    }
    return IOStatus::OK();
  }

  IOStatus GetFileModificationTime(const std::string& fname,
                                   const IOOptions& /*opts*/,
                                   uint64_t* file_mtime,
                                   IODebugContext* /*dbg*/) override {
    struct stat s;
    if (stat(fname.c_str(), &s) != 0) {
      return IOError("while stat a file for modification time", fname, errno);
    }
    *file_mtime = static_cast<uint64_t>(s.st_mtime);
    return IOStatus::OK();
  }

  IOStatus RenameFile(const std::string& src, const std::string& target,
                      const IOOptions& /*opts*/,
                      IODebugContext* /*dbg*/) override {
    if (rename(src.c_str(), target.c_str()) != 0) {
      return IOError("While renaming a file to " + target, src, errno);
    }
    return IOStatus::OK();
  }

  IOStatus LinkFile(const std::string& src, const std::string& target,
                    const IOOptions& /*opts*/,
                    IODebugContext* /*dbg*/) override {
    if (link(src.c_str(), target.c_str()) != 0) {
      if (errno == EXDEV || errno == ENOTSUP) {
        return IOStatus::NotSupported(errno == EXDEV
                                          ? "No cross FS links allowed"
                                          : "Links not supported by FS");
      }
      return IOError("while link file to " + target, src, errno);
    }
    return IOStatus::OK();
  }

  IOStatus NumFileLinks(const std::string& fname, const IOOptions& /*opts*/,
                        uint64_t* count, IODebugContext* /*dbg*/) override {
    struct stat s;
    if (stat(fname.c_str(), &s) != 0) {
      return IOError("while stat a file for num file links", fname, errno);
    }
    *count = static_cast<uint64_t>(s.st_nlink);
    return IOStatus::OK();
  }

  IOStatus AreFilesSame(const std::string& first, const std::string& second,
                        const IOOptions& /*opts*/, bool* res,
                        IODebugContext* /*dbg*/) override {
    struct stat statbuf[2];
    if (stat(first.c_str(), &statbuf[0]) != 0) {
      return IOError("stat file", first, errno);
    }
    if (stat(second.c_str(), &statbuf[1]) != 0) {
      return IOError("stat file", second, errno);
    }

    if (major(statbuf[0].st_dev) != major(statbuf[1].st_dev) ||
        minor(statbuf[0].st_dev) != minor(statbuf[1].st_dev) ||
        statbuf[0].st_ino != statbuf[1].st_ino) {
      *res = false;
    } else {
      *res = true;
    }
    return IOStatus::OK();
  }

  IOStatus LockFile(const std::string& fname, const IOOptions& /*opts*/,
                    FileLock** lock, IODebugContext* /*dbg*/) override {
    *lock = nullptr;

    LockHoldingInfo lhi;
    int64_t current_time = 0;
    // Ignore status code as the time is only used for error message.
    SystemClock::Default()
        ->GetCurrentTime(&current_time)
        .PermitUncheckedError();
    lhi.acquire_time = current_time;
    lhi.acquiring_thread = Env::Default()->GetThreadID();

    mutex_locked_files.Lock();
    // If it already exists in the locked_files set, then it is already locked,
    // and fail this lock attempt. Otherwise, insert it into locked_files.
    // This check is needed because fcntl() does not detect lock conflict
    // if the fcntl is issued by the same thread that earlier acquired
    // this lock.
    // We must do this check *before* opening the file:
    // Otherwise, we will open a new file descriptor. Locks are associated with
    // a process, not a file descriptor and when *any* file descriptor is
    // closed, all locks the process holds for that *file* are released
    const auto it_success = locked_files.insert({fname, lhi});
    if (it_success.second == false) {
      LockHoldingInfo prev_info = it_success.first->second;
      mutex_locked_files.Unlock();
      errno = ENOLCK;
      // Note that the thread ID printed is the same one as the one in
      // posix logger, but posix logger prints it hex format.
      return IOError(
          "lock hold by current process, acquire time " +
              ROCKSDB_NAMESPACE::ToString(prev_info.acquire_time) +
              " acquiring thread " +
              ROCKSDB_NAMESPACE::ToString(prev_info.acquiring_thread),
          fname, errno);
    }

    IOStatus result = IOStatus::OK();
    int fd;
    int flags = cloexec_flags(O_RDWR | O_CREAT, nullptr);

    {
      IOSTATS_TIMER_GUARD(open_nanos);
      fd = open(fname.c_str(), flags, 0644);
    }
    if (fd < 0) {
      result = IOError("while open a file for lock", fname, errno);
    } else if (LockOrUnlock(fd, true) == -1) {
      result = IOError("While lock file", fname, errno);
      close(fd);
    } else {
      SetFD_CLOEXEC(fd, nullptr);
      PosixFileLock* my_lock = new PosixFileLock;
      my_lock->fd_ = fd;
      my_lock->filename = fname;
      *lock = my_lock;
    }
    if (!result.ok()) {
      // If there is an error in locking, then remove the pathname from
      // locked_files. (If we got this far, it did not exist in locked_files
      // before this call.)
      locked_files.erase(fname);
    }

    mutex_locked_files.Unlock();
    return result;
  }

  IOStatus UnlockFile(FileLock* lock, const IOOptions& /*opts*/,
                      IODebugContext* /*dbg*/) override {
    PosixFileLock* my_lock = reinterpret_cast<PosixFileLock*>(lock);
    IOStatus result;
    mutex_locked_files.Lock();
    // If we are unlocking, then verify that we had locked it earlier,
    // it should already exist in locked_files. Remove it from locked_files.
    if (locked_files.erase(my_lock->filename) != 1) {
      errno = ENOLCK;
      result = IOError("unlock", my_lock->filename, errno);
    } else if (LockOrUnlock(my_lock->fd_, false) == -1) {
      result = IOError("unlock", my_lock->filename, errno);
    }
    close(my_lock->fd_);
    my_lock->Clear();
    delete my_lock;
    mutex_locked_files.Unlock();
    return result;
  }

  IOStatus GetAbsolutePath(const std::string& db_path,
                           const IOOptions& /*opts*/, std::string* output_path,
                           IODebugContext* /*dbg*/) override {
    if (!db_path.empty() && db_path[0] == '/') {
      *output_path = db_path;
      return IOStatus::OK();
    }

    char the_path[256];
    char* ret = getcwd(the_path, 256);
    if (ret == nullptr) {
      return IOStatus::IOError(errnoStr(errno).c_str());
    }

    *output_path = ret;
    return IOStatus::OK();
  }

  IOStatus GetTestDirectory(const IOOptions& /*opts*/, std::string* result,
                            IODebugContext* /*dbg*/) override {
    const char* env = getenv("TEST_TMPDIR");
    if (env && env[0] != '\0') {
      *result = env;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "/tmp/rocksdbtest-%d", int(geteuid()));
      *result = buf;
    }
    // Directory may already exist
    {
      IOOptions opts;
      return CreateDirIfMissing(*result, opts, nullptr);
    }
    return IOStatus::OK();
  }

  IOStatus GetFreeSpace(const std::string& fname, const IOOptions& /*opts*/,
                        uint64_t* free_space,
                        IODebugContext* /*dbg*/) override {
    struct statvfs sbuf;

    if (statvfs(fname.c_str(), &sbuf) < 0) {
      return IOError("While doing statvfs", fname, errno);
    }

    // sbuf.bfree is total free space available to root
    // sbuf.bavail is total free space available to unprivileged user
    //  sbuf.bavail <= sbuf.bfree ... pick correct based upon effective user id
    if (geteuid()) {
      // non-zero user is unprivileged, or -1 if error.  take more conservative
      // size
      *free_space = ((uint64_t)sbuf.f_bsize * sbuf.f_bavail);
    } else {
      // root user can access all disk space
      *free_space = ((uint64_t)sbuf.f_bsize * sbuf.f_bfree);
    }
    return IOStatus::OK();
  }

  IOStatus IsDirectory(const std::string& path, const IOOptions& /*opts*/,
                       bool* is_dir, IODebugContext* /*dbg*/) override {
    // First open
    int fd = -1;
    int flags = cloexec_flags(O_RDONLY, nullptr);
    {
      IOSTATS_TIMER_GUARD(open_nanos);
      fd = open(path.c_str(), flags);
    }
    if (fd < 0) {
      return IOError("While open for IsDirectory()", path, errno);
    }
    IOStatus io_s;
    struct stat sbuf;
    if (fstat(fd, &sbuf) < 0) {
      io_s = IOError("While doing stat for IsDirectory()", path, errno);
    }
    close(fd);
    if (io_s.ok() && nullptr != is_dir) {
      *is_dir = S_ISDIR(sbuf.st_mode);
    }
    return io_s;
  }

  FileOptions OptimizeForLogWrite(const FileOptions& file_options,
                                  const DBOptions& db_options) const override {
    FileOptions optimized = file_options;
    optimized.use_mmap_writes = false;
    optimized.use_direct_writes = false;
    optimized.bytes_per_sync = db_options.wal_bytes_per_sync;
    // TODO(icanadi) it's faster if fallocate_with_keep_size is false, but it
    // breaks TransactionLogIteratorStallAtLastRecord unit test. Fix the unit
    // test and make this false
    optimized.fallocate_with_keep_size = true;
    optimized.writable_file_max_buffer_size =
        db_options.writable_file_max_buffer_size;
    return optimized;
  }

  FileOptions OptimizeForManifestWrite(
      const FileOptions& file_options) const override {
    FileOptions optimized = file_options;
    optimized.use_mmap_writes = false;
    optimized.use_direct_writes = false;
    optimized.fallocate_with_keep_size = true;
    return optimized;
  }
#ifdef OS_LINUX
  Status RegisterDbPaths(const std::vector<std::string>& paths) override {
    return logical_block_size_cache_.RefAndCacheLogicalBlockSize(paths);
  }
  Status UnregisterDbPaths(const std::vector<std::string>& paths) override {
    logical_block_size_cache_.UnrefAndTryRemoveCachedLogicalBlockSize(paths);
    return Status::OK();
  }
#endif
 private:
  bool forceMmapOff_ = false;  // do we override Env options?

  // Returns true iff the named directory exists and is a directory.
  virtual bool DirExists(const std::string& dname) {
    struct stat statbuf;
    if (stat(dname.c_str(), &statbuf) == 0) {
      return S_ISDIR(statbuf.st_mode);
    }
    return false;  // stat() failed return false
  }

  bool SupportsFastAllocate(int fd) {
#ifdef ROCKSDB_FALLOCATE_PRESENT
    struct statfs s;
    if (fstatfs(fd, &s)) {
      return false;
    }
    switch (s.f_type) {
      case EXT4_SUPER_MAGIC:
        return true;
      case XFS_SUPER_MAGIC:
        return true;
      case TMPFS_MAGIC:
        return true;
      default:
        return false;
    }
#else
    (void)fd;
    return false;
#endif
  }

  void MaybeForceDisableMmap(int fd) {
    static std::once_flag s_check_disk_for_mmap_once;
    assert(this == FileSystem::Default().get());
    std::call_once(
        s_check_disk_for_mmap_once,
        [this](int fdesc) {
          // this will be executed once in the program's lifetime.
          // do not use mmapWrite on non ext-3/xfs/tmpfs systems.
          if (!SupportsFastAllocate(fdesc)) {
            forceMmapOff_ = true;
          }
        },
        fd);
  }

#ifdef ROCKSDB_IOURING_PRESENT
  bool IsIOUringEnabled() {
    if (RocksDbIOUringEnable && RocksDbIOUringEnable()) {
      return true;
    } else {
      return false;
    }
  }
#endif  // ROCKSDB_IOURING_PRESENT

#if defined(ROCKSDB_IOURING_PRESENT)
  // io_uring instance
  std::unique_ptr<ThreadLocalPtr> thread_local_io_urings_;
#endif

  size_t page_size_;

  // If true, allow non owner read access for db files. Otherwise, non-owner
  //  has no access to db files.
  bool allow_non_owner_access_;

#ifdef OS_LINUX
  static LogicalBlockSizeCache logical_block_size_cache_;
#endif
  static size_t GetLogicalBlockSize(const std::string& fname, int fd);
  // In non-direct IO mode, this directly returns kDefaultPageSize.
  // Otherwise call GetLogicalBlockSize.
  static size_t GetLogicalBlockSizeForReadIfNeeded(const EnvOptions& options,
                                                   const std::string& fname,
                                                   int fd);
  static size_t GetLogicalBlockSizeForWriteIfNeeded(const EnvOptions& options,
                                                    const std::string& fname,
                                                    int fd);
};

#ifdef OS_LINUX
LogicalBlockSizeCache PosixFileSystem::logical_block_size_cache_;
#endif

size_t PosixFileSystem::GetLogicalBlockSize(const std::string& fname, int fd) {
#ifdef OS_LINUX
  return logical_block_size_cache_.GetLogicalBlockSize(fname, fd);
#else
  (void)fname;
  return PosixHelper::GetLogicalBlockSizeOfFd(fd);
#endif
}

size_t PosixFileSystem::GetLogicalBlockSizeForReadIfNeeded(
    const EnvOptions& options, const std::string& fname, int fd) {
  return options.use_direct_reads
             ? PosixFileSystem::GetLogicalBlockSize(fname, fd)
             : kDefaultPageSize;
}

size_t PosixFileSystem::GetLogicalBlockSizeForWriteIfNeeded(
    const EnvOptions& options, const std::string& fname, int fd) {
  return options.use_direct_writes
             ? PosixFileSystem::GetLogicalBlockSize(fname, fd)
             : kDefaultPageSize;
}

PosixFileSystem::PosixFileSystem()
    : forceMmapOff_(false),
      page_size_(getpagesize()),
      allow_non_owner_access_(true) {
#if defined(ROCKSDB_IOURING_PRESENT)
  // Test whether IOUring is supported, and if it does, create a managing
  // object for thread local point so that in the future thread-local
  // io_uring can be created.
  struct io_uring* new_io_uring = CreateIOUring();
  if (new_io_uring != nullptr) {
    thread_local_io_urings_.reset(new ThreadLocalPtr(DeleteIOUring));
    delete new_io_uring;
  }
#endif
}

}  // namespace

//
// Default Posix FileSystem
//
std::shared_ptr<FileSystem> FileSystem::Default() {
  static PosixFileSystem default_fs;
  static std::shared_ptr<PosixFileSystem> default_fs_ptr(
      &default_fs, [](PosixFileSystem*) {});
  return default_fs_ptr;
}

#ifndef ROCKSDB_LITE
static FactoryFunc<FileSystem> posix_filesystem_reg =
    ObjectLibrary::Default()->AddFactory<FileSystem>(
        ObjectLibrary::PatternEntry("posix").AddSeparator("://", false),
        [](const std::string& /* uri */, std::unique_ptr<FileSystem>* f,
           std::string* /* errmsg */) {
          f->reset(new PosixFileSystem());
          return f->get();
        });
#endif

}  // namespace ROCKSDB_NAMESPACE

#endif
