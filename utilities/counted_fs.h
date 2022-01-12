//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <memory>

#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
struct FileOpCounters {
  std::atomic<int> ops;
  std::atomic<uint64_t> bytes;

  FileOpCounters() : ops(0), bytes(0) {}

  void Reset() {
    ops = 0;
    bytes = 0;
  }
  void RecordOp(const IOStatus& io_s, size_t added_bytes) {
    if (!io_s.IsNotSupported()) {
      ops.fetch_add(1, std::memory_order_relaxed);
    }
    if (io_s.ok()) {
      bytes.fetch_add(added_bytes, std::memory_order_relaxed);
    }
  }
};

struct FileCounters {
  static const char* kName() { return "FileCounters"; }

  std::atomic<int> opens;
  std::atomic<int> deletes;
  std::atomic<int> renames;
  std::atomic<int> flushes;
  std::atomic<int> syncs;
  std::atomic<int> fsyncs;
  std::atomic<int> closes;
  FileOpCounters reads;
  FileOpCounters writes;

  FileCounters() { Reset(); }
  void Reset() {
    opens = 0;
    deletes = 0;
    closes = 0;
    renames = 0;
    flushes = 0;
    syncs = 0;
    fsyncs = 0;
    reads.Reset();
    writes.Reset();
  }
};

// A FileSystem class that counts operations (reads, writes, opens, closes, etc)
class CountedFileSystem : public FileSystemWrapper {
 private:
  FileCounters counters_;
  bool skip_fsync_;

 public:
  explicit CountedFileSystem(const std::shared_ptr<FileSystem>& base,
                             bool skip_fsync = false);
  static const char* kClassName() { return "CountedFileSystem"; }
  const char* Name() const override { return kClassName(); }

  IOStatus NewSequentialFile(const std::string& f, const FileOptions& options,
                             std::unique_ptr<FSSequentialFile>* r,
                             IODebugContext* dbg) override;

  IOStatus NewRandomAccessFile(const std::string& f,
                               const FileOptions& file_opts,
                               std::unique_ptr<FSRandomAccessFile>* r,
                               IODebugContext* dbg) override;

  IOStatus NewWritableFile(const std::string& f, const FileOptions& options,
                           std::unique_ptr<FSWritableFile>* r,
                           IODebugContext* dbg) override;
  IOStatus ReopenWritableFile(const std::string& fname,
                              const FileOptions& options,
                              std::unique_ptr<FSWritableFile>* result,
                              IODebugContext* dbg) override;

  IOStatus ReuseWritableFile(const std::string& fname,
                             const std::string& old_fname,
                             const FileOptions& file_opts,
                             std::unique_ptr<FSWritableFile>* result,
                             IODebugContext* dbg) override;
  IOStatus NewRandomRWFile(const std::string& name, const FileOptions& options,
                           std::unique_ptr<FSRandomRWFile>* result,
                           IODebugContext* dbg) override;

  IOStatus DeleteFile(const std::string& fname, const IOOptions& options,
                      IODebugContext* dbg) override {
    IOStatus s = target()->DeleteFile(fname, options, dbg);
    if (s.ok()) {
      counters_.deletes++;
    }
    return s;
  }

  IOStatus RenameFile(const std::string& s, const std::string& t,
                      const IOOptions& options, IODebugContext* dbg) override {
    IOStatus st = target()->RenameFile(s, t, options, dbg);
    if (st.ok()) {
      counters_.renames++;
    }
    return st;
  }

  const FileCounters* counters() const { return &counters_; }

  FileCounters* counters() { return &counters_; }

  const void* GetOptionsPtr(const std::string& name) const override {
    if (name == FileCounters::kName()) {
      return counters();
    } else {
      return FileSystemWrapper::GetOptionsPtr(name);
    }
  }
};
}  // namespace ROCKSDB_NAMESPACE
