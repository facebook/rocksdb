//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// This test uses a custom FileSystem to keep track of the state of a file
// system the last "Sync". The data being written is cached in a "buffer".
// Only when "Sync" is called, the data will be persistent. It can similate
// file data loss (or entire files) not protected by a "Sync". For any of the
// FileSystem related operations, by specify the "IOStatus Error", a specific
// error can be returned when file system is not activated.

#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdarg>
#include <functional>
#include <map>
#include <set>
#include <string>
#include <thread>

#ifndef OS_WIN
#include <fcntl.h>
#include <limits.h>
#include <unistd.h>
#endif

// PATH_MAX may not be defined on all platforms
#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#include "file/filename.h"
#include "port/lang.h"
#include "rocksdb/file_system.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/thread_local.h"

namespace ROCKSDB_NAMESPACE {

// A fixed-size circular buffer that records recently injected errors.
// Thread-safe for concurrent writes. Designed to be safe to read from a
// signal handler (PrintAll uses only fprintf to stderr).
class InjectedErrorLog {
 public:
  static constexpr size_t kMaxEntries = 1000;
  static constexpr size_t kMaxMessageLen = 256;

  struct Entry {
    uint64_t timestamp_us;
    uint64_t thread_id;
    char context[kMaxMessageLen];
  };

  InjectedErrorLog() : head_(0), entries_{} { log_path_[0] = '\0'; }

  // Set the file path for PrintAll() output. Must be called before any
  // signal handler invocation (not async-signal-safe itself due to string
  // copy, but called once at setup time). If not set, PrintAll() falls
  // back to writing to stderr.
  void SetLogFilePath(const std::string& path) {
    size_t len = std::min(path.size(), sizeof(log_path_) - 1);
    memcpy(log_path_, path.data(), len);
    log_path_[len] = '\0';
  }

  TSAN_SUPPRESSION void Record(const char* fmt, ...)
#if defined(__GNUC__) || defined(__clang__)
      __attribute__((format(printf, 2, 3)))
#endif
  {
    size_t idx = head_.fetch_add(1, std::memory_order_relaxed) % kMaxEntries;
    Entry& e = entries_[idx];
    e.thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());
    auto now = std::chrono::system_clock::now();
    e.timestamp_us = std::chrono::duration_cast<std::chrono::microseconds>(
                         now.time_since_epoch())
                         .count();
    // Format into a local buffer first, then copy into the shared entry.
    // This avoids calling the TSAN-intercepted vsnprintf directly on shared
    // memory. We use a byte-by-byte loop instead of memcpy because
    // TSAN_SUPPRESSION (no_sanitize("thread")) only suppresses
    // compiler-inserted instrumentation -- it does NOT suppress TSAN's
    // runtime interceptors for libc functions like memcpy, vsnprintf, and
    // snprintf. Plain store instructions are always suppressed regardless
    // of optimization level. The volatile source pointer prevents the
    // compiler from recognizing this as a memcpy idiom and replacing it
    // with a memcpy call.
    char local_buf[kMaxMessageLen];
    va_list args;
    va_start(args, fmt);
    vsnprintf(local_buf, kMaxMessageLen, fmt, args);
    va_end(args);
    const volatile char* src = local_buf;
    for (size_t i = 0; i < kMaxMessageLen; i++) {
      e.context[i] = src[i];
    }
  }

  // Format the first few bytes of a buffer as hex for logging.
  // Returns a string like "ab cd ef 01 02 ..."
  static std::string HexHead(const char* data, size_t size,
                             size_t max_bytes = 8) {
    std::string result;
    size_t n = std::min(size, max_bytes);
    char buf[4];
    for (size_t i = 0; i < n; i++) {
      snprintf(buf, sizeof(buf), "%02x ", (unsigned char)data[i]);
      result += buf;
    }
    if (size > max_bytes) result += "...";
    if (!result.empty() && result.back() == ' ') result.pop_back();
    return result;
  }

  // Print all recorded entries to a log file (or stderr as fallback).
  // Async-signal-safe: uses only open/write/close/snprintf (no fprintf,
  // no malloc). Safe to call from a signal handler.
  //
  // Note: entries may be read while being written by another thread.
  // This is a benign race -- at worst, one entry may appear garbled.
  // We accept this trade-off to keep PrintAll() free of locks and safe
  // for use in signal handlers.
  TSAN_SUPPRESSION void PrintAll() const {
#ifndef OS_WIN
    int fd = -1;
    if (log_path_[0] != '\0') {
      fd = open(log_path_, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    }
    // Fall back to stdout if open failed or no path was set.
    // We avoid stderr because db_crashtest.py treats any stderr output
    // as a test failure.
    if (fd < 0) {
      fd = STDOUT_FILENO;
    }

    auto write_str = [fd](const char* buf, int len) {
      if (len > 0) {
        // Ignore return value in signal handler -- nothing we can do
        auto unused __attribute__((unused)) = write(fd, buf, len);
      }
    };

    char buf[512];
    int len = snprintf(buf, sizeof(buf),
                       "\n=== Recently Injected Fault Injection Errors "
                       "(most recent last) ===\n");
    write_str(buf, len);

    size_t total = head_.load(std::memory_order_relaxed);
    if (total == 0) {
      len = snprintf(buf, sizeof(buf), "(none)\n");
      write_str(buf, len);
      if (fd != STDOUT_FILENO) close(fd);
      return;
    }
    size_t count = std::min(total, kMaxEntries);
    size_t start = (total >= kMaxEntries) ? (total % kMaxEntries) : 0;
    for (size_t i = 0; i < count; i++) {
      size_t idx = (start + i) % kMaxEntries;
      // Copy entry fields to locals to avoid passing shared memory through
      // TSAN-intercepted snprintf. See comment in Record() for why we use a
      // volatile pointer to prevent loop-to-memcpy optimization.
      const Entry& e = entries_[idx];
      uint64_t local_ts = e.timestamp_us;
      uint64_t local_tid = e.thread_id;
      char local_ctx[kMaxMessageLen];
      const volatile char* ctx_src = e.context;
      for (size_t j = 0; j < kMaxMessageLen; j++) {
        local_ctx[j] = ctx_src[j];
      }
      if (local_ts == 0) continue;
      uint64_t secs = local_ts / 1000000;
      uint64_t usecs = local_ts % 1000000;
      len = snprintf(buf, sizeof(buf), "[%llu.%06llu] thread=%llu: %s\n",
                     (unsigned long long)secs, (unsigned long long)usecs,
                     (unsigned long long)local_tid, local_ctx);
      write_str(buf, len);
    }
    len = snprintf(buf, sizeof(buf),
                   "=== End of injected error log (%zu entries) ===\n", count);
    write_str(buf, len);
    if (fd != STDOUT_FILENO) close(fd);
#else
    // On Windows, crash callbacks via signal handlers are not used,
    // so PrintAll() is a no-op.
#endif
  }

 private:
  std::atomic<size_t> head_;
  Entry entries_[kMaxEntries];
  char log_path_[PATH_MAX];
};

class TestFSWritableFile;
class FaultInjectionTestFS;

// Deferred detail builders for injected error logging.
// These return lambdas that are only evaluated when a fault is actually
// injected, avoiding string formatting overhead on the common (no-fault) path.
// Captured references are safe because the lambda is called synchronously
// within MaybeInjectThreadLocalError before the caller returns.
namespace fault_injection_detail {

inline std::function<std::string()> NoDetail() { return {}; }

inline std::function<std::string()> TwoFiles(const std::string& /*f1*/,
                                             const std::string& f2) {
  return [&f2]() -> std::string {
    char buf[160];
    snprintf(buf, sizeof(buf), "\"%.128s\"", f2.c_str());
    return std::string(buf);
  };
}

inline std::function<std::string()> SizeAndHead(const Slice& data) {
  return [data]() -> std::string {
    char buf[128];
    snprintf(buf, sizeof(buf), "size=%zu, head=[%s]", data.size(),
             InjectedErrorLog::HexHead(data.data(), data.size()).c_str());
    return std::string(buf);
  };
}

inline std::function<std::string()> OffsetSizeAndHead(uint64_t offset,
                                                      const Slice& data) {
  return [offset, data]() -> std::string {
    char buf[160];
    snprintf(buf, sizeof(buf), "offset=%llu, size=%zu, head=[%s]",
             (unsigned long long)offset, data.size(),
             InjectedErrorLog::HexHead(data.data(), data.size()).c_str());
    return std::string(buf);
  };
}

inline std::function<std::string()> OffsetAndSize(uint64_t offset, size_t n) {
  return [offset, n]() -> std::string {
    char buf[64];
    snprintf(buf, sizeof(buf), "offset=%llu, size=%zu",
             (unsigned long long)offset, n);
    return std::string(buf);
  };
}

inline std::function<std::string()> Size(uint64_t size) {
  return [size]() -> std::string {
    char buf[32];
    snprintf(buf, sizeof(buf), "size=%llu", (unsigned long long)size);
    return std::string(buf);
  };
}

inline std::function<std::string()> Count(size_t count) {
  return [count]() -> std::string {
    char buf[32];
    snprintf(buf, sizeof(buf), "num_reqs=%zu", count);
    return std::string(buf);
  };
}

inline std::function<std::string()> ReqOffsetAndSize(size_t req_idx,
                                                     uint64_t offset,
                                                     size_t n) {
  return [req_idx, offset, n]() -> std::string {
    char buf[96];
    snprintf(buf, sizeof(buf), "req[%zu], offset=%llu, size=%zu", req_idx,
             (unsigned long long)offset, n);
    return std::string(buf);
  };
}

}  // namespace fault_injection_detail

enum class FaultInjectionIOType {
  kRead = 0,
  kWrite,
  kMetadataRead,
  kMetadataWrite,
};

struct FSFileState {
  std::string filename_;
  uint64_t pos_at_last_append_ = 0;
  uint64_t pos_at_last_sync_ = 0;
  std::string buffer_;

  explicit FSFileState(const std::string& filename = {})
      : filename_(filename) {}

  bool IsFullySynced() const {
    return pos_at_last_append_ == pos_at_last_sync_;
  }

  IOStatus DropUnsyncedData();

  IOStatus DropRandomUnsyncedData(Random* rand);
};

// A wrapper around WritableFileWriter* file
// is written to or sync'ed.
class TestFSWritableFile : public FSWritableFile {
 public:
  explicit TestFSWritableFile(const std::string& fname,
                              const FileOptions& file_opts,
                              std::unique_ptr<FSWritableFile>&& f,
                              FaultInjectionTestFS* fs);
  virtual ~TestFSWritableFile();
  IOStatus Append(const Slice& data, const IOOptions&,
                  IODebugContext*) override;
  IOStatus Append(const Slice& data, const IOOptions& options,
                  const DataVerificationInfo& verification_info,
                  IODebugContext* dbg) override;
  IOStatus Truncate(uint64_t size, const IOOptions& options,
                    IODebugContext* dbg) override;
  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override;
  IOStatus Flush(const IOOptions&, IODebugContext*) override;
  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override;
  IOStatus RangeSync(uint64_t /*offset*/, uint64_t /*nbytes*/,
                     const IOOptions& options, IODebugContext* dbg) override;
  bool IsSyncThreadSafe() const override { return true; }
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            IODebugContext* dbg) override;
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            const DataVerificationInfo& verification_info,
                            IODebugContext* dbg) override;
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  bool use_direct_io() const override { return target_->use_direct_io(); }

  uint64_t GetFileSize(const IOOptions& options, IODebugContext* dbg) override {
    MutexLock l(&mutex_);
    return target_->GetFileSize(options, dbg);
  }

 private:
  FSFileState state_;  // Need protection by mutex_
  FileOptions file_opts_;
  std::unique_ptr<FSWritableFile> target_;
  bool writable_file_opened_;
  FaultInjectionTestFS* fs_;
  port::Mutex mutex_;
  const bool unsync_data_loss_;
};

// A wrapper around FSRandomRWFile* file
// is read from/write to or sync'ed.
class TestFSRandomRWFile : public FSRandomRWFile {
 public:
  explicit TestFSRandomRWFile(const std::string& fname,
                              std::unique_ptr<FSRandomRWFile>&& f,
                              FaultInjectionTestFS* fs);
  virtual ~TestFSRandomRWFile();
  IOStatus Write(uint64_t offset, const Slice& data, const IOOptions& options,
                 IODebugContext* dbg) override;
  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;
  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override;
  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override;
  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override;
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  bool use_direct_io() const override { return target_->use_direct_io(); }

 private:
  // keep a copy of file name, so we can untrack it in File system, when it is
  // closed
  std::string fname_;
  std::unique_ptr<FSRandomRWFile> target_;
  bool file_opened_;
  FaultInjectionTestFS* fs_;
};

class TestFSRandomAccessFile : public FSRandomAccessFile {
 public:
  explicit TestFSRandomAccessFile(const std::string& fname,
                                  std::unique_ptr<FSRandomAccessFile>&& f,
                                  FaultInjectionTestFS* fs);
  ~TestFSRandomAccessFile() override {}
  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;
  IOStatus ReadAsync(FSReadRequest& req, const IOOptions& opts,
                     std::function<void(FSReadRequest&, void*)> cb,
                     void* cb_arg, void** io_handle, IOHandleDeleter* del_fn,
                     IODebugContext* dbg) override;
  IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
                     const IOOptions& options, IODebugContext* dbg) override;
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  bool use_direct_io() const override { return target_->use_direct_io(); }

  size_t GetUniqueId(char* id, size_t max_size) const override;

  IOStatus GetFileSize(uint64_t* file_size) override;

 private:
  std::string fname_;
  std::unique_ptr<FSRandomAccessFile> target_;
  FaultInjectionTestFS* fs_;
  const bool is_sst_;
};

class TestFSSequentialFile : public FSSequentialFileOwnerWrapper {
 public:
  explicit TestFSSequentialFile(std::unique_ptr<FSSequentialFile>&& f,
                                FaultInjectionTestFS* fs, std::string fname)
      : FSSequentialFileOwnerWrapper(std::move(f)),
        fs_(fs),
        fname_(std::move(fname)) {}
  IOStatus Read(size_t n, const IOOptions& options, Slice* result,
                char* scratch, IODebugContext* dbg) override;
  IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions& options,
                          Slice* result, char* scratch,
                          IODebugContext* dbg) override;

 private:
  FaultInjectionTestFS* fs_;
  std::string fname_;
  uint64_t read_pos_ = 0;
  uint64_t target_read_pos_ = 0;
};

class TestFSDirectory : public FSDirectory {
 public:
  explicit TestFSDirectory(FaultInjectionTestFS* fs, std::string dirname,
                           FSDirectory* dir)
      : fs_(fs), dirname_(std::move(dirname)), dir_(dir) {}
  ~TestFSDirectory() {}

  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus FsyncWithDirOptions(
      const IOOptions& options, IODebugContext* dbg,
      const DirFsyncOptions& dir_fsync_options) override;

 private:
  FaultInjectionTestFS* fs_;
  std::string dirname_;
  std::unique_ptr<FSDirectory> dir_;
};

class FaultInjectionTestFS : public FileSystemWrapper {
 public:
  explicit FaultInjectionTestFS(const std::shared_ptr<FileSystem>& base)
      : FileSystemWrapper(base),
        filesystem_active_(true),
        filesystem_writable_(false),
        inject_unsynced_data_loss_(false),
        read_unsynced_data_(true),
        allow_link_open_file_(false),
        injected_thread_local_read_error_(DeleteThreadLocalErrorContext),
        injected_thread_local_write_error_(DeleteThreadLocalErrorContext),
        injected_thread_local_metadata_read_error_(
            DeleteThreadLocalErrorContext),
        injected_thread_local_metadata_write_error_(
            DeleteThreadLocalErrorContext),
        ingest_data_corruption_before_write_(false),
        checksum_handoff_func_type_(kCRC32c) {}
  virtual ~FaultInjectionTestFS() override { fs_error_.PermitUncheckedError(); }

  static const char* kClassName() { return "FaultInjectionTestFS"; }
  const char* Name() const override { return kClassName(); }

  static bool IsInjectedError(const Status& s,
                              const std::string& specific_error_marker = "") {
    if (s.ok()) {
      return false;
    }
    const char* state = s.getState();
    if (state == nullptr) {
      return false;
    }
    bool is_injected_error = std::strstr(state, kInjected.c_str()) != nullptr;
    bool is_specific_error =
        specific_error_marker.empty() ||
        std::strstr(state, specific_error_marker.c_str()) != nullptr;

    return is_injected_error && is_specific_error;
  }

  static bool IsFailedToWriteToWALError(const Status& s) {
    return IsInjectedError(s, kFailedToWriteToWAL);
  }

  IOStatus NewDirectory(const std::string& name, const IOOptions& options,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override;

  IOStatus FileExists(const std::string& fname, const IOOptions& options,
                      IODebugContext* dbg) override;

  IOStatus GetChildren(const std::string& dir, const IOOptions& options,
                       std::vector<std::string>* result,
                       IODebugContext* dbg) override;

  IOStatus GetChildrenFileAttributes(const std::string& dir,
                                     const IOOptions& options,
                                     std::vector<FileAttributes>* result,
                                     IODebugContext* dbg) override;

  IOStatus NewWritableFile(const std::string& fname,
                           const FileOptions& file_opts,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override;

  IOStatus ReopenWritableFile(const std::string& fname,
                              const FileOptions& file_opts,
                              std::unique_ptr<FSWritableFile>* result,
                              IODebugContext* dbg) override;

  IOStatus ReuseWritableFile(const std::string& fname,
                             const std::string& old_fname,
                             const FileOptions& file_opts,
                             std::unique_ptr<FSWritableFile>* result,
                             IODebugContext* dbg) override;

  IOStatus NewRandomRWFile(const std::string& fname,
                           const FileOptions& file_opts,
                           std::unique_ptr<FSRandomRWFile>* result,
                           IODebugContext* dbg) override;

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& file_opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override;
  IOStatus NewSequentialFile(const std::string& f, const FileOptions& file_opts,
                             std::unique_ptr<FSSequentialFile>* r,
                             IODebugContext* dbg) override;

  IOStatus DeleteFile(const std::string& f, const IOOptions& options,
                      IODebugContext* dbg) override;

  IOStatus GetFileSize(const std::string& f, const IOOptions& options,
                       uint64_t* file_size, IODebugContext* dbg) override;

  IOStatus GetFileModificationTime(const std::string& fname,
                                   const IOOptions& options,
                                   uint64_t* file_mtime,
                                   IODebugContext* dbg) override;

  IOStatus RenameFile(const std::string& s, const std::string& t,
                      const IOOptions& options, IODebugContext* dbg) override;

  IOStatus LinkFile(const std::string& src, const std::string& target,
                    const IOOptions& options, IODebugContext* dbg) override;

  IOStatus NumFileLinks(const std::string& fname, const IOOptions& options,
                        uint64_t* count, IODebugContext* dbg) override;

  IOStatus AreFilesSame(const std::string& first, const std::string& second,
                        const IOOptions& options, bool* res,
                        IODebugContext* dbg) override;
  IOStatus GetAbsolutePath(const std::string& db_path, const IOOptions& options,
                           std::string* output_path,
                           IODebugContext* dbg) override;

// Undef to eliminate clash on Windows
#undef GetFreeSpace
  IOStatus GetFreeSpace(const std::string& path, const IOOptions& options,
                        uint64_t* disk_free, IODebugContext* dbg) override {
    IOStatus io_s;
    if (!IsFilesystemActive() &&
        fs_error_.subcode() == IOStatus::SubCode::kNoSpace) {
      *disk_free = 0;
    } else {
      io_s = MaybeInjectThreadLocalError(FaultInjectionIOType::kMetadataRead,
                                         options, "GetFreeSpace", path);
      if (io_s.ok()) {
        io_s = target()->GetFreeSpace(path, options, disk_free, dbg);
      }
    }
    return io_s;
  }

  IOStatus IsDirectory(const std::string& path, const IOOptions& options,
                       bool* is_dir, IODebugContext* dgb) override;

  IOStatus Poll(std::vector<void*>& io_handles,
                size_t min_completions) override;

  IOStatus AbortIO(std::vector<void*>& io_handles) override;

  void WritableFileClosed(const FSFileState& state);

  void WritableFileSynced(const FSFileState& state);

  void WritableFileAppended(const FSFileState& state);

  void RandomRWFileClosed(const std::string& fname);

  IOStatus DropUnsyncedFileData();

  IOStatus DropRandomUnsyncedFileData(Random* rnd);

  IOStatus DeleteFilesCreatedAfterLastDirSync(const IOOptions& options,
                                              IODebugContext* dbg);

  void ResetState();

  void UntrackFile(const std::string& f);

  void SyncDir(const std::string& dirname) {
    MutexLock l(&mutex_);
    dir_to_new_files_since_last_sync_.erase(dirname);
  }

  // Setting the filesystem to inactive is the test equivalent to simulating a
  // system reset. Setting to inactive will freeze our saved filesystem state so
  // that it will stop being recorded. It can then be reset back to the state at
  // the time of the reset.
  bool IsFilesystemActive() {
    MutexLock l(&mutex_);
    return filesystem_active_;
  }

  // Setting filesystem_writable_ makes NewWritableFile. ReopenWritableFile,
  // and NewRandomRWFile bypass FaultInjectionTestFS and go directly to the
  // target FS
  bool IsFilesystemDirectWritable() {
    MutexLock l(&mutex_);
    return filesystem_writable_;
  }
  void SetFilesystemActiveNoLock(
      bool active, IOStatus error = IOStatus::Corruption("Not active")) {
    error.PermitUncheckedError();
    filesystem_active_ = active;
    if (!active) {
      fs_error_ = error;
    }
  }
  void SetFilesystemActive(
      bool active, IOStatus error = IOStatus::Corruption("Not active")) {
    MutexLock l(&mutex_);
    error.PermitUncheckedError();
    SetFilesystemActiveNoLock(active, error);
  }
  void SetFilesystemDirectWritable(bool writable) {
    MutexLock l(&mutex_);
    filesystem_writable_ = writable;
  }

  // If true, we buffer write data in memory to simulate data loss upon system
  // crash by only having process crashes
  void SetInjectUnsyncedDataLoss(bool inject) {
    MutexLock l(&mutex_);
    inject_unsynced_data_loss_ = inject;
  }

  bool InjectUnsyncedDataLoss() {
    MutexLock l(&mutex_);
    return inject_unsynced_data_loss_;
  }

  // In places (e.g. GetSortedWals()) RocksDB relies on querying the file size
  // or even reading the contents of files currently open for writing, and
  // as in POSIX semantics, expects to see the flushed size and contents
  // regardless of what has been synced. FaultInjectionTestFS historically
  // did not emulate this behavior, only showing synced data from such read
  // operations. (Different from FaultInjectionTestEnv--sigh.) Calling this
  // function with false restores this historical behavior for testing
  // stability, but use of this semantics must be phased out as it is
  // inconsistent with expected FileSystem semantics. In other words, this
  // functionality is DEPRECATED. Intended to be set after construction and
  // unchanged (not thread safe).
  void SetReadUnsyncedData(bool read_unsynced_data) {
    read_unsynced_data_ = read_unsynced_data;
  }
  bool ReadUnsyncedData() const { return read_unsynced_data_; }

  // FaultInjectionTestFS normally includes a hygiene check for FileSystem
  // implementations that only support LinkFile() on closed files (not open
  // for write). Setting this to true bypasses the check.
  void SetAllowLinkOpenFile(bool allow_link_open_file = true) {
    allow_link_open_file_ = allow_link_open_file;
  }

  bool ShouldIOActivitiesExcludedFromFaultInjection(
      Env::IOActivity io_activity) {
    MutexLock l(&mutex_);
    return io_activities_excluded_from_fault_injection.find(io_activity) !=
           io_activities_excluded_from_fault_injection.end();
  }

  void AssertNoOpenFile() { assert(open_managed_files_.empty()); }

  IOStatus GetError() { return fs_error_; }

  void SetFileSystemIOError(IOStatus io_error) {
    MutexLock l(&mutex_);
    io_error.PermitUncheckedError();
    fs_error_ = io_error;
  }

  // To simulate the data corruption before data is written in FS
  void IngestDataCorruptionBeforeWrite() {
    MutexLock l(&mutex_);
    ingest_data_corruption_before_write_ = true;
  }

  void NoDataCorruptionBeforeWrite() {
    MutexLock l(&mutex_);
    ingest_data_corruption_before_write_ = false;
  }

  bool ShouldDataCorruptionBeforeWrite() {
    MutexLock l(&mutex_);
    return ingest_data_corruption_before_write_;
  }

  void SetChecksumHandoffFuncType(const ChecksumType& func_type) {
    MutexLock l(&mutex_);
    checksum_handoff_func_type_ = func_type;
  }

  const ChecksumType& GetChecksumHandoffFuncType() {
    MutexLock l(&mutex_);
    return checksum_handoff_func_type_;
  }

  void SetFailGetUniqueId(bool flag) {
    MutexLock l(&mutex_);
    fail_get_file_unique_id_ = flag;
  }

  bool ShouldFailGetUniqueId() {
    MutexLock l(&mutex_);
    return fail_get_file_unique_id_;
  }

  void SetFailRandomAccessGetFileSizeSst(bool flag) {
    MutexLock l(&mutex_);
    fail_random_access_get_file_size_sst_ = flag;
  }

  bool ShouldFailRandomAccessGetFileSizeSst() {
    MutexLock l(&mutex_);
    return fail_random_access_get_file_size_sst_;
  }

  void SetFailFilesystemGetFileSizeSst(bool flag) {
    MutexLock l(&mutex_);
    fail_fs_get_file_size_sst_ = flag;
  }

  bool ShouldFailFilesystemGetFileSizeSst() {
    MutexLock l(&mutex_);
    return fail_fs_get_file_size_sst_;
  }

  // Specify what the operation, so we can inject the right type of error
  enum ErrorOperation : char {
    kRead = 0,
    kMultiReadSingleReq = 1,
    kMultiRead = 2,
    kOpen,
    kAppend,
    kPositionedAppend,
    kUnknown,
  };

  void SetThreadLocalErrorContext(FaultInjectionIOType type, uint32_t seed,
                                  int one_in, bool retryable,
                                  bool has_data_loss) {
    struct ErrorContext* new_ctx = new ErrorContext(seed);
    new_ctx->one_in = one_in;
    new_ctx->count = 0;
    new_ctx->retryable = retryable;
    new_ctx->has_data_loss = has_data_loss;

    SetErrorContextOfFaultInjectionIOType(type, new_ctx);
  }

  static void DeleteThreadLocalErrorContext(void* p) {
    ErrorContext* ctx = static_cast<ErrorContext*>(p);
    delete ctx;
  }

  IOStatus MaybeInjectThreadLocalError(
      FaultInjectionIOType type, const IOOptions& io_options,
      const char* op_name, const std::string& file_name,
      std::function<std::string()> detail_fn = {}, ErrorOperation op = kUnknown,
      Slice* slice = nullptr, bool direct_io = false, char* scratch = nullptr,
      bool need_count_increase = false, bool* fault_injected = nullptr);

  int GetAndResetInjectedThreadLocalErrorCount(FaultInjectionIOType type) {
    ErrorContext* ctx = GetErrorContextFromFaultInjectionIOType(type);
    int count = 0;
    if (ctx) {
      count = ctx->count;
      ctx->count = 0;
    }
    return count;
  }

  void SetIOActivitiesExcludedFromFaultInjection(
      const std::set<Env::IOActivity>& io_activities) {
    MutexLock l(&mutex_);
    io_activities_excluded_from_fault_injection = io_activities;
  }

  void SetFileTypesExcludedFromWriteFaultInjection(
      const std::set<FileType>& types) {
    MutexLock l(&mutex_);
    file_types_excluded_from_write_fault_injection_ = types;
  }

  void EnableThreadLocalErrorInjection(FaultInjectionIOType type) {
    ErrorContext* ctx = GetErrorContextFromFaultInjectionIOType(type);
    if (ctx) {
      ctx->enable_error_injection = true;
    }
  }

  void EnableAllThreadLocalErrorInjection() {
    EnableThreadLocalErrorInjection(FaultInjectionIOType::kRead);
    EnableThreadLocalErrorInjection(FaultInjectionIOType::kWrite);
    EnableThreadLocalErrorInjection(FaultInjectionIOType::kMetadataRead);
    EnableThreadLocalErrorInjection(FaultInjectionIOType::kMetadataWrite);
  }

  void DisableThreadLocalErrorInjection(FaultInjectionIOType type) {
    ErrorContext* ctx = GetErrorContextFromFaultInjectionIOType(type);
    if (ctx) {
      ctx->enable_error_injection = false;
    }
  }

  void DisableAllThreadLocalErrorInjection() {
    DisableThreadLocalErrorInjection(FaultInjectionIOType::kRead);
    DisableThreadLocalErrorInjection(FaultInjectionIOType::kWrite);
    DisableThreadLocalErrorInjection(FaultInjectionIOType::kMetadataRead);
    DisableThreadLocalErrorInjection(FaultInjectionIOType::kMetadataWrite);
  }

  void PrintInjectedThreadLocalErrorBacktrace(FaultInjectionIOType type);

  // If there is unsynced data in the specified file within the specified
  // range [offset, offset + n), return the unsynced data overlapping with
  // that range, in a corresponding range of scratch. When known, also return
  // the position of the last sync, so that the caller can determine whether
  // more data is available from the target file when not available from
  // unsynced.
  void ReadUnsynced(const std::string& fname, uint64_t offset, size_t n,
                    Slice* result, char* scratch, int64_t* pos_at_last_sync);

  // Access the injected error log for printing on crash or test failure.
  InjectedErrorLog& GetInjectedErrorLog() { return injected_error_log_; }
  const InjectedErrorLog& GetInjectedErrorLog() const {
    return injected_error_log_;
  }

  // Print recently injected errors to stderr. Call this on test failure
  // to see what errors were injected leading up to the failure.
  void PrintRecentInjectedErrors() const { injected_error_log_.PrintAll(); }

  // Set the file path where PrintAll() will write its output.
  // Must be called before any signal handler invocation.
  void SetInjectedErrorLogPath(const std::string& path) {
    injected_error_log_.SetLogFilePath(path);
  }

  inline static const std::string kInjected = "injected";

 private:
  inline static const std::string kFailedToWriteToWAL =
      "failed to write to WAL";
  port::Mutex mutex_;
  std::map<std::string, FSFileState> db_file_state_;
  std::set<std::string> open_managed_files_;
  // directory -> (file name -> file contents to recover)
  // When data is recovered from unsyned parent directory, the files with
  // empty file contents to recover is deleted. Those with non-empty ones
  // will be recovered to content accordingly.
  std::unordered_map<std::string, std::map<std::string, std::string>>
      dir_to_new_files_since_last_sync_;
  bool filesystem_active_;    // Record flushes, syncs, writes
  bool filesystem_writable_;  // Bypass FaultInjectionTestFS and go directly
                              // to underlying FS for writable files
  bool inject_unsynced_data_loss_;  // See InjectUnsyncedDataLoss()
  bool read_unsynced_data_;         // See SetReadUnsyncedData()
  bool allow_link_open_file_;       // See SetAllowLinkOpenFile()
  IOStatus fs_error_;

  enum ErrorType : int {
    kErrorTypeStatus = 0,
    kErrorTypeCorruption,
    kErrorTypeTruncated,
    kErrorTypeMax
  };

  struct ErrorContext {
    Random rand;
    int one_in;
    int count;
    bool enable_error_injection;
    void* callstack;
    std::string message;
    int frames;
    ErrorType type;
    bool retryable;
    bool has_data_loss;

    explicit ErrorContext(uint32_t seed)
        : rand(seed),
          enable_error_injection(false),
          callstack(nullptr),
          frames(0),
          retryable(false),
          has_data_loss(false) {}
    ~ErrorContext() {
      if (callstack) {
        free(callstack);
      }
    }
  };

  std::set<FileType> file_types_excluded_from_write_fault_injection_;
  std::set<Env::IOActivity> io_activities_excluded_from_fault_injection;
  ThreadLocalPtr injected_thread_local_read_error_;
  ThreadLocalPtr injected_thread_local_write_error_;
  ThreadLocalPtr injected_thread_local_metadata_read_error_;
  ThreadLocalPtr injected_thread_local_metadata_write_error_;
  bool ingest_data_corruption_before_write_;
  ChecksumType checksum_handoff_func_type_;
  bool fail_get_file_unique_id_ = false;
  bool fail_random_access_get_file_size_sst_ = false;
  bool fail_fs_get_file_size_sst_ = false;
  InjectedErrorLog injected_error_log_;

  // Inject an error. For a READ operation, a status of IOError(), a
  // corruption in the contents of scratch, or truncation of slice
  // are the types of error with equal probability. For OPEN,
  // its always an IOError.
  // fault_injected returns whether a fault is injected. It is needed
  // because some fault is inected with IOStatus to be OK.
  IOStatus MaybeInjectThreadLocalReadError(
      const IOOptions& io_options, const char* op_name,
      const std::string& file_name, std::function<std::string()> detail_fn,
      ErrorOperation op, Slice* slice, bool direct_io, char* scratch,
      bool need_count_increase, bool* fault_injected);

  bool ShouldExcludeFromWriteFaultInjection(const std::string& file_name) {
    MutexLock l(&mutex_);
    FileType file_type = kTempFile;
    uint64_t file_number = 0;
    if (!TryParseFileName(file_name, &file_number, &file_type)) {
      return false;
    }
    return file_types_excluded_from_write_fault_injection_.find(file_type) !=
           file_types_excluded_from_write_fault_injection_.end();
  }

  // Extract number of type from file name. Return false if failing to fine
  // them.
  bool TryParseFileName(const std::string& file_name, uint64_t* number,
                        FileType* type);

  ErrorContext* GetErrorContextFromFaultInjectionIOType(
      FaultInjectionIOType type) {
    ErrorContext* ctx = nullptr;
    switch (type) {
      case FaultInjectionIOType::kRead:
        ctx = static_cast<struct ErrorContext*>(
            injected_thread_local_read_error_.Get());
        break;
      case FaultInjectionIOType::kWrite:
        ctx = static_cast<struct ErrorContext*>(
            injected_thread_local_write_error_.Get());
        break;
      case FaultInjectionIOType::kMetadataRead:
        ctx = static_cast<struct ErrorContext*>(
            injected_thread_local_metadata_read_error_.Get());
        break;
      case FaultInjectionIOType::kMetadataWrite:
        ctx = static_cast<struct ErrorContext*>(
            injected_thread_local_metadata_write_error_.Get());
        break;
      default:
        assert(false);
        break;
    }
    return ctx;
  }

  void SetErrorContextOfFaultInjectionIOType(FaultInjectionIOType type,
                                             ErrorContext* new_ctx) {
    ErrorContext* old_ctx = nullptr;
    switch (type) {
      case FaultInjectionIOType::kRead:
        old_ctx = static_cast<struct ErrorContext*>(
            injected_thread_local_read_error_.Swap(new_ctx));
        break;
      case FaultInjectionIOType::kWrite:
        old_ctx = static_cast<struct ErrorContext*>(
            injected_thread_local_write_error_.Swap(new_ctx));
        break;
      case FaultInjectionIOType::kMetadataRead:
        old_ctx = static_cast<struct ErrorContext*>(
            injected_thread_local_metadata_read_error_.Swap(new_ctx));
        break;
      case FaultInjectionIOType::kMetadataWrite:
        old_ctx = static_cast<struct ErrorContext*>(
            injected_thread_local_metadata_write_error_.Swap(new_ctx));
        break;
      default:
        assert(false);
        break;
    }

    if (old_ctx) {
      DeleteThreadLocalErrorContext(old_ctx);
    }
  }

  std::string GetErrorMessage(FaultInjectionIOType type,
                              const std::string& file_name, ErrorOperation op) {
    std::ostringstream msg;
    msg << kInjected << " ";
    switch (type) {
      case FaultInjectionIOType::kRead:
        msg << "read error";
        break;
      case FaultInjectionIOType::kWrite:
        msg << "write error";
        break;
      case FaultInjectionIOType::kMetadataRead:
        msg << "metadata read error";
        break;
      case FaultInjectionIOType::kMetadataWrite:
        msg << "metadata write error";
        break;
      default:
        assert(false);
        break;
    }

    if (type == FaultInjectionIOType::kWrite &&
        (op == ErrorOperation::kOpen || op == ErrorOperation::kAppend ||
         op == ErrorOperation::kPositionedAppend)) {
      FileType file_type = kTempFile;
      uint64_t ignore = 0;
      if (TryParseFileName(file_name, &ignore, &file_type) &&
          file_type == FileType::kWalFile) {
        msg << " " << kFailedToWriteToWAL;
      }
    }
    return msg.str();
  }
};

}  // namespace ROCKSDB_NAMESPACE
