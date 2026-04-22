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
#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <functional>
#include <limits>
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

// A binary log that records injected errors directly to a file.
// Thread-safe for concurrent writes. The log file can be flushed on exit or
// from a signal handler so records survive clean exits and crash paths.
class InjectedErrorLog {
 public:
  static constexpr size_t kMaxOpNameLen = 32;
  static constexpr size_t kMaxFileNameLen = 72;
  static constexpr size_t kMaxStatusMessageLen = 56;
  static constexpr size_t kMaxDetailPayloadLen = 48;
  static constexpr uint32_t kFileVersion = 3;
  static constexpr std::array<char, 8> kFileMagic = {'F', 'I', 'N', 'J',
                                                     'L', 'O', 'G', '1'};

  enum class DetailKind : uint8_t {
    kNone = 0,
    kTwoFiles = 1,
    kSizeAndHead = 2,
    kOffsetSizeAndHead = 3,
    kOffsetAndSize = 4,
    kSize = 5,
    kCount = 6,
    kReqOffsetAndSize = 7,
  };

  // Borrowed raw detail payload. The referenced Slice is consumed
  // synchronously when a fault is actually injected.
  struct DetailRef {
    DetailKind kind = DetailKind::kNone;
    uint64_t offset = 0;
    uint64_t size = 0;
    uint32_t count = 0;
    uint32_t req_idx = 0;
    Slice payload;
  };

  struct Entry {
    uint64_t timestamp_us;
    uint64_t thread_id;
    uint64_t offset;
    uint64_t size;
    uint32_t count;
    uint32_t req_idx;
    uint8_t detail_kind;
    uint8_t detail_payload_size;
    uint8_t retryable;
    uint8_t data_loss;
    char op_name[kMaxOpNameLen];
    char file_name[kMaxFileNameLen];
    char status_message[kMaxStatusMessageLen];
    char detail_payload[kMaxDetailPayloadLen];
  };

  static_assert(sizeof(Entry) == 256,
                "Injected error log entry size must stay stable");

  struct RawFileHeader {
    char magic[8];
    uint64_t total_entries;
    uint32_t version;
    uint32_t header_size;
    uint32_t entry_size;
    uint32_t max_entries;
    uint32_t dumped_entries;
    uint32_t reserved;
  };

  static_assert(sizeof(RawFileHeader) == 40,
                "Injected error log file header size must stay stable");

  InjectedErrorLog()
      : total_entries_(0),
        next_write_offset_(sizeof(RawFileHeader)),
        finalized_(0),
        log_fd_(-1) {
    log_path_[0] = '\0';
  }

  ~InjectedErrorLog() { Finalize(); }

  // Set the file path for raw binary output. Must be called before any signal
  // handler invocation. The file starts with a fixed header followed by a
  // stream of fixed-size entries.
  void SetLogFilePath(const std::string& path) {
#ifndef OS_WIN
    Finalize();
#endif
    total_entries_.store(0, std::memory_order_relaxed);
    next_write_offset_.store(sizeof(RawFileHeader), std::memory_order_relaxed);
    finalized_.store(0, std::memory_order_relaxed);
    log_fd_.store(-1, std::memory_order_relaxed);
    size_t len = std::min(path.size(), sizeof(log_path_) - 1);
    memcpy(log_path_, path.data(), len);
    log_path_[len] = '\0';

#ifndef OS_WIN
    if (log_path_[0] == '\0') {
      return;
    }
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
    int fd = open(log_path_, flags, 0644);
    if (fd < 0) {
      return;
    }
    RawFileHeader header = MakeHeader(/*total_entries=*/0);
    if (!PwriteAll(fd, reinterpret_cast<const char*>(&header), sizeof(header),
                   /*offset=*/0)) {
      close(fd);
      auto unused __attribute__((unused)) = unlink(log_path_);
      return;
    }
    log_fd_.store(fd, std::memory_order_release);
#endif
  }

  void Record(const Slice& op_name, const Slice& file_name,
              const DetailRef& detail, const Slice& status_message,
              bool retryable, bool data_loss) {
#ifndef OS_WIN
    if (finalized_.load(std::memory_order_acquire) != 0) {
      return;
    }
    int fd = log_fd_.load(std::memory_order_acquire);
    if (fd < 0) {
      return;
    }

    Entry local{};
    local.timestamp_us = NowMicros();
    local.thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());
    local.offset = detail.offset;
    local.size = detail.size;
    local.count = detail.count;
    local.req_idx = detail.req_idx;
    local.detail_kind = static_cast<uint8_t>(detail.kind);
    local.detail_payload_size = static_cast<uint8_t>(CopySliceBytes(
        detail.payload, local.detail_payload, sizeof(local.detail_payload)));
    local.retryable = retryable ? 1 : 0;
    local.data_loss = data_loss ? 1 : 0;
    CopyStringSample(op_name, local.op_name, sizeof(local.op_name));
    CopyStringSample(file_name, local.file_name, sizeof(local.file_name));
    CopyStringSample(status_message, local.status_message,
                     sizeof(local.status_message));
    uint64_t offset =
        next_write_offset_.fetch_add(sizeof(local), std::memory_order_relaxed);
    if (PwriteAll(fd, reinterpret_cast<const char*>(&local), sizeof(local),
                  static_cast<off_t>(offset))) {
      total_entries_.fetch_add(1, std::memory_order_relaxed);
    }
#else
    (void)op_name;
    (void)file_name;
    (void)detail;
    (void)status_message;
    (void)retryable;
    (void)data_loss;
#endif
  }

  // Format the first few bytes of a buffer as hex for logging.
  // Returns a string like "ab cd ef 01 02 ..."
  static std::string HexHead(const char* data, size_t size,
                             size_t max_bytes = 8) {
    std::string result;
    size_t n = std::min(size, max_bytes);
    static const char kHexDigits[] = "0123456789abcdef";
    result.reserve(n * 3 + ((size > max_bytes) ? 4 : 0));
    for (size_t i = 0; i < n; i++) {
      if (i > 0) {
        result.push_back(' ');
      }
      uint8_t byte = static_cast<uint8_t>(data[i]);
      result.push_back(kHexDigits[byte >> 4]);
      result.push_back(kHexDigits[byte & 0x0f]);
    }
    if (size > max_bytes) {
      result.append(" ...");
    }
    return result;
  }

  // Flush the already-appended log file so crash/termination paths preserve
  // the most recent records.
  void Flush() const {
#ifndef OS_WIN
    int fd = log_fd_.load(std::memory_order_acquire);
    if (fd < 0) {
      return;
    }
    auto unused __attribute__((unused)) = fsync(fd);
#else
    // On Windows, crash callbacks via signal handlers are not used.
#endif
  }

  // Finalize the log on a clean exit by rewriting the header with the final
  // entry count and syncing the file one last time.
  void Finalize() {
#ifndef OS_WIN
    uint32_t expected = 0;
    if (!finalized_.compare_exchange_strong(expected, 1,
                                            std::memory_order_acq_rel)) {
      return;
    }
    int fd = log_fd_.exchange(-1, std::memory_order_acq_rel);
    if (fd < 0) {
      return;
    }
    uint64_t total = total_entries_.load(std::memory_order_relaxed);
    if (total == 0) {
      close(fd);
      if (log_path_[0] != '\0') {
        auto unused __attribute__((unused)) = unlink(log_path_);
      }
      return;
    }
    RawFileHeader header = MakeHeader(total);
    PwriteAll(fd, reinterpret_cast<const char*>(&header), sizeof(header),
              /*offset=*/0);
    auto unused __attribute__((unused)) = fsync(fd);
    close(fd);
#endif
  }

 private:
  static uint64_t NowMicros() {
    auto now = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(
               now.time_since_epoch())
        .count();
  }

  static void CopyStringSample(const Slice& src, char* dst, size_t dst_len) {
    if (dst_len == 0) {
      return;
    }
    size_t copied = std::min(src.size(), dst_len - 1);
    for (size_t i = 0; i < copied; ++i) {
      dst[i] = src[i];
    }
    dst[copied] = '\0';
  }

  static size_t CopySliceBytes(const Slice& src, char* dst, size_t dst_len) {
    size_t copied = std::min(src.size(), dst_len);
    for (size_t i = 0; i < copied; ++i) {
      dst[i] = src[i];
    }
    return copied;
  }

  static bool WriteAll(int fd, const char* data, size_t len) {
#ifndef OS_WIN
    while (len > 0) {
      ssize_t written = write(fd, data, len);
      if (written <= 0) {
        return false;
      }
      data += static_cast<size_t>(written);
      len -= static_cast<size_t>(written);
    }
    return true;
#else
    (void)fd;
    (void)data;
    (void)len;
    return false;
#endif
  }

  static bool PwriteAll(int fd, const char* data, size_t len, off_t offset) {
#ifndef OS_WIN
    while (len > 0) {
      ssize_t written = pwrite(fd, data, len, offset);
      if (written <= 0) {
        return false;
      }
      data += static_cast<size_t>(written);
      len -= static_cast<size_t>(written);
      offset += written;
    }
    return true;
#else
    (void)fd;
    (void)data;
    (void)len;
    (void)offset;
    return false;
#endif
  }

  static RawFileHeader MakeHeader(uint64_t total_entries) {
    RawFileHeader header{};
    for (size_t i = 0; i < kFileMagic.size(); ++i) {
      header.magic[i] = kFileMagic[i];
    }
    header.total_entries = total_entries;
    header.version = kFileVersion;
    header.header_size = static_cast<uint32_t>(sizeof(header));
    header.entry_size = static_cast<uint32_t>(sizeof(Entry));
    header.max_entries = 0;
    header.dumped_entries = static_cast<uint32_t>(std::min<uint64_t>(
        total_entries, std::numeric_limits<uint32_t>::max()));
    header.reserved = 0;
    return header;
  }

  std::atomic<uint64_t> total_entries_;
  std::atomic<uint64_t> next_write_offset_;
  std::atomic<uint32_t> finalized_;
  std::atomic<int> log_fd_;
  char log_path_[PATH_MAX];
};

class TestFSWritableFile;
class FaultInjectionTestFS;

// Borrowed raw detail builders for injected error logging.
// These avoid hot-path string formatting and are consumed synchronously
// when a fault is actually injected.
namespace fault_injection_detail {

using DetailRef = InjectedErrorLog::DetailRef;

inline DetailRef NoDetail() { return DetailRef(); }

inline DetailRef TwoFiles(const std::string& /*f1*/, const std::string& f2) {
  DetailRef detail;
  detail.kind = InjectedErrorLog::DetailKind::kTwoFiles;
  detail.size = static_cast<uint64_t>(f2.size());
  detail.payload = Slice(f2);
  return detail;
}

inline DetailRef SizeAndHead(const Slice& data) {
  DetailRef detail;
  detail.kind = InjectedErrorLog::DetailKind::kSizeAndHead;
  detail.size = static_cast<uint64_t>(data.size());
  detail.payload = data;
  return detail;
}

inline DetailRef OffsetSizeAndHead(uint64_t offset, const Slice& data) {
  DetailRef detail;
  detail.kind = InjectedErrorLog::DetailKind::kOffsetSizeAndHead;
  detail.offset = offset;
  detail.size = static_cast<uint64_t>(data.size());
  detail.payload = data;
  return detail;
}

inline DetailRef OffsetAndSize(uint64_t offset, size_t n) {
  DetailRef detail;
  detail.kind = InjectedErrorLog::DetailKind::kOffsetAndSize;
  detail.offset = offset;
  detail.size = static_cast<uint64_t>(n);
  return detail;
}

inline DetailRef Size(uint64_t size) {
  DetailRef detail;
  detail.kind = InjectedErrorLog::DetailKind::kSize;
  detail.size = size;
  return detail;
}

inline DetailRef Count(size_t count) {
  assert(count <= std::numeric_limits<uint32_t>::max());
  DetailRef detail;
  detail.kind = InjectedErrorLog::DetailKind::kCount;
  detail.count = static_cast<uint32_t>(count);
  return detail;
}

inline DetailRef ReqOffsetAndSize(size_t req_idx, uint64_t offset, size_t n) {
  assert(req_idx <= std::numeric_limits<uint32_t>::max());
  DetailRef detail;
  detail.kind = InjectedErrorLog::DetailKind::kReqOffsetAndSize;
  detail.req_idx = static_cast<uint32_t>(req_idx);
  detail.offset = offset;
  detail.size = static_cast<uint64_t>(n);
  return detail;
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
      InjectedErrorLog::DetailRef detail = {}, ErrorOperation op = kUnknown,
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

  void SetFileTypesExcludedFromFaultInjection(const std::set<FileType>& types) {
    MutexLock l(&mutex_);
    file_types_excluded_from_fault_injection_ = types;
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

  // Access the injected error log for flush/finalize on crash or test failure.
  InjectedErrorLog& GetInjectedErrorLog() { return injected_error_log_; }
  const InjectedErrorLog& GetInjectedErrorLog() const {
    return injected_error_log_;
  }

  // Flush recently injected errors to the configured binary log file.
  void FlushRecentInjectedErrors() const { injected_error_log_.Flush(); }

  // Finalize the binary log on a clean exit so the header records the
  // complete entry count.
  void FinalizeRecentInjectedErrors() { injected_error_log_.Finalize(); }

  // Set the file path where the binary log will be written.
  // Must be called before any signal handler invocation.
  void SetInjectedErrorLogPath(const std::string& path) {
    injected_error_log_.SetLogFilePath(path);
  }

  inline static const std::string kInjected = "injected";

 private:
  inline static const std::string kFailedToWriteToWAL =
      "failed to write to WAL";
  inline static const std::string kInjectedReadError = "injected read error";
  inline static const std::string kInjectedEmptyResult =
      "injected empty result";
  inline static const std::string kInjectedCorruptLastByte =
      "injected corrupt last byte";
  inline static const std::string kInjectedErrorResultMultiGetSingle =
      "injected error result multiget single";
  inline static const std::string kInjectedWriteError = "injected write error";
  inline static const std::string kInjectedWriteErrorFailedToWriteToWAL =
      "injected write error failed to write to WAL";
  inline static const std::string kInjectedMetadataReadError =
      "injected metadata read error";
  inline static const std::string kInjectedMetadataWriteError =
      "injected metadata write error";
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

  std::set<FileType> file_types_excluded_from_fault_injection_;
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
      const std::string& file_name, InjectedErrorLog::DetailRef detail,
      ErrorOperation op, Slice* slice, bool direct_io, char* scratch,
      bool need_count_increase, bool* fault_injected);

  bool ShouldExcludeFromFaultInjection(const std::string& file_name,
                                       FaultInjectionIOType type) {
    MutexLock l(&mutex_);
    FileType file_type = kTempFile;
    uint64_t file_number = 0;
    if (!TryParseFileName(file_name, &file_number, &file_type)) {
      return false;
    }
    if (file_types_excluded_from_fault_injection_.count(file_type) > 0) {
      return true;
    }
    switch (type) {
      case FaultInjectionIOType::kWrite:
        return file_types_excluded_from_write_fault_injection_.count(
                   file_type) > 0;
      case FaultInjectionIOType::kRead:
      case FaultInjectionIOType::kMetadataRead:
      case FaultInjectionIOType::kMetadataWrite:
        return false;
    }
    return false;
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
    switch (type) {
      case FaultInjectionIOType::kRead:
        return kInjectedReadError;
      case FaultInjectionIOType::kWrite: {
        if (op == ErrorOperation::kOpen || op == ErrorOperation::kAppend ||
            op == ErrorOperation::kPositionedAppend) {
          FileType file_type = kTempFile;
          uint64_t ignore = 0;
          if (TryParseFileName(file_name, &ignore, &file_type) &&
              file_type == FileType::kWalFile) {
            return kInjectedWriteErrorFailedToWriteToWAL;
          }
        }
        return kInjectedWriteError;
      }
      case FaultInjectionIOType::kMetadataRead:
        return kInjectedMetadataReadError;
      case FaultInjectionIOType::kMetadataWrite:
        return kInjectedMetadataWriteError;
    }
    assert(false);
    return kInjectedReadError;
  }
};

}  // namespace ROCKSDB_NAMESPACE
