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
#include "utilities/injection_fs.h"

namespace ROCKSDB_NAMESPACE {
class Logger;

struct OpCounter {
  std::atomic<int> ops;
  std::atomic<uint64_t> bytes;

  OpCounter() : ops(0), bytes(0) {}

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

struct FileOpCounters {
  static const char* kName() { return "FileOpCounters"; }

  std::atomic<int> opens;
  std::atomic<int> closes;
  std::atomic<int> deletes;
  std::atomic<int> renames;
  std::atomic<int> flushes;
  std::atomic<int> syncs;
  std::atomic<int> dsyncs;
  std::atomic<int> fsyncs;
  std::atomic<int> dir_opens;
  std::atomic<int> dir_closes;
  OpCounter reads;
  OpCounter writes;

  FileOpCounters()
      : opens(0),
        closes(0),
        deletes(0),
        renames(0),
        flushes(0),
        syncs(0),
        dsyncs(0),
        fsyncs(0),
        dir_opens(0),
        dir_closes(0) {}

  void Reset() {
    opens = 0;
    closes = 0;
    deletes = 0;
    renames = 0;
    flushes = 0;
    syncs = 0;
    dsyncs = 0;
    fsyncs = 0;
    dir_opens = 0;
    dir_closes = 0;
    reads.Reset();
    writes.Reset();
  }
  std::string PrintCounters() const;
};

// A FileSystem class that counts operations (reads, writes, opens, closes, etc)
class CountedFileSystem : public InjectionFileSystem {
 public:
 private:
  FileOpCounters counters_;

 public:
  explicit CountedFileSystem(const std::shared_ptr<FileSystem>& base);
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

  IOStatus NewDirectory(const std::string& name, const IOOptions& io_opts,
                        std::unique_ptr<FSDirectory>* result,
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

  const FileOpCounters* counters() const { return &counters_; }

  FileOpCounters* counters() { return &counters_; }

  const void* GetOptionsPtr(const std::string& name) const override {
    if (name == FileOpCounters::kName()) {
      return counters();
    } else {
      return FileSystemWrapper::GetOptionsPtr(name);
    }
  }

  // Prints the counters to a string
  std::string PrintCounters() const { return counters_.PrintCounters(); }
  void ResetCounters() { counters_.Reset(); }

 protected:
  IOStatus DoRead(FSSequentialFile* file, size_t n, const IOOptions& options,
                  Slice* result, char* scratch, IODebugContext* dbg) override {
    auto rv =
        InjectionFileSystem::DoRead(file, n, options, result, scratch, dbg);
    counters_.reads.RecordOp(rv, result->size());
    return rv;
  }

  IOStatus DoPositionedRead(FSSequentialFile* file, uint64_t offset, size_t n,
                            const IOOptions& options, Slice* result,
                            char* scratch, IODebugContext* dbg) override {
    auto rv = InjectionFileSystem::DoPositionedRead(file, offset, n, options,
                                                    result, scratch, dbg);
    counters_.reads.RecordOp(rv, result->size());
    return rv;
  }
  void DoClose(FSSequentialFile* file) override {
    InjectionFileSystem::DoClose(file);
    counters_.closes++;
  }

  IOStatus DoRead(FSRandomAccessFile* file, uint64_t offset, size_t n,
                  const IOOptions& options, Slice* result, char* scratch,
                  IODebugContext* dbg) override {
    IOStatus rv = InjectionFileSystem::DoRead(file, offset, n, options, result,
                                              scratch, dbg);
    counters_.reads.RecordOp(rv, result->size());
    return rv;
  }

  IOStatus DoMultiRead(FSRandomAccessFile* file, FSReadRequest* reqs,
                       size_t num_reqs, const IOOptions& options,
                       IODebugContext* dbg) override {
    IOStatus rv =
        InjectionFileSystem::DoMultiRead(file, reqs, num_reqs, options, dbg);
    for (size_t r = 0; r < num_reqs; r++) {
      counters_.reads.RecordOp(reqs[r].status, reqs[r].result.size());
    }
    return rv;
  }

  void DoClose(FSRandomAccessFile* file) override {
    InjectionFileSystem::DoClose(file);
    counters_.closes++;
  }

  IOStatus DoAppend(FSWritableFile* file, const Slice& data,
                    const IOOptions& options, IODebugContext* dbg) override {
    IOStatus rv = InjectionFileSystem::DoAppend(file, data, options, dbg);
    counters_.writes.RecordOp(rv, data.size());
    return rv;
  }

  IOStatus DoAppend(FSWritableFile* file, const Slice& data,
                    const IOOptions& options, const DataVerificationInfo& info,
                    IODebugContext* dbg) override {
    IOStatus rv = InjectionFileSystem::DoAppend(file, data, options, info, dbg);
    counters_.writes.RecordOp(rv, data.size());
    return rv;
  }

  IOStatus DoPositionedAppend(FSWritableFile* file, const Slice& data,
                              uint64_t offset, const IOOptions& options,
                              IODebugContext* dbg) override {
    IOStatus rv = InjectionFileSystem::DoPositionedAppend(file, data, offset,
                                                          options, dbg);
    counters_.writes.RecordOp(rv, data.size());
    return rv;
  }

  IOStatus DoPositionedAppend(FSWritableFile* file, const Slice& data,
                              uint64_t offset, const IOOptions& options,
                              const DataVerificationInfo& info,
                              IODebugContext* dbg) override {
    IOStatus rv = InjectionFileSystem::DoPositionedAppend(file, data, offset,
                                                          options, info, dbg);
    counters_.writes.RecordOp(rv, data.size());
    return rv;
  }

  IOStatus DoClose(FSWritableFile* file, const IOOptions& options,
                   IODebugContext* dbg) override {
    IOStatus rv = InjectionFileSystem::DoClose(file, options, dbg);
    if (rv.ok()) {
      counters_.closes++;
    }
    return rv;
  }

  IOStatus DoFlush(FSWritableFile* file, const IOOptions& options,
                   IODebugContext* dbg) override {
    IOStatus rv = InjectionFileSystem::DoFlush(file, options, dbg);
    if (rv.ok()) {
      counters_.flushes++;
    }
    return rv;
  }

  IOStatus DoSync(FSWritableFile* file, const IOOptions& options,
                  IODebugContext* dbg) override {
    IOStatus rv = InjectionFileSystem::DoSync(file, options, dbg);
    if (rv.ok()) {
      counters_.syncs++;
    }
    return rv;
  }

  IOStatus DoFsync(FSWritableFile* file, const IOOptions& options,
                   IODebugContext* dbg) override {
    IOStatus rv = InjectionFileSystem::DoFsync(file, options, dbg);
    if (rv.ok()) {
      counters_.fsyncs++;
    }
    return rv;
  }

  IOStatus DoRangeSync(FSWritableFile* file, uint64_t offset, uint64_t nbytes,
                       const IOOptions& options, IODebugContext* dbg) override {
    IOStatus rv =
        InjectionFileSystem::DoRangeSync(file, offset, nbytes, options, dbg);
    if (rv.ok()) {
      counters_.syncs++;
    }
    return rv;
  }

  IOStatus DoWrite(FSRandomRWFile* file, uint64_t offset, const Slice& data,
                   const IOOptions& options, IODebugContext* dbg) override {
    IOStatus rv =
        InjectionFileSystem::DoWrite(file, offset, data, options, dbg);
    counters_.writes.RecordOp(rv, data.size());
    return rv;
  }

  IOStatus DoRead(FSRandomRWFile* file, uint64_t offset, size_t n,
                  const IOOptions& options, Slice* result, char* scratch,
                  IODebugContext* dbg) override {
    IOStatus rv = InjectionFileSystem::DoRead(file, offset, n, options, result,
                                              scratch, dbg);
    counters_.reads.RecordOp(rv, result->size());
    return rv;
  }

  IOStatus DoFlush(FSRandomRWFile* file, const IOOptions& options,
                   IODebugContext* dbg) override {
    IOStatus rv = InjectionFileSystem::DoFlush(file, options, dbg);
    if (rv.ok()) {
      counters_.flushes++;
    }
    return rv;
  }

  IOStatus DoSync(FSRandomRWFile* file, const IOOptions& options,
                  IODebugContext* dbg) override {
    IOStatus rv = InjectionFileSystem::DoSync(file, options, dbg);
    if (rv.ok()) {
      counters_.syncs++;
    }
    return rv;
  }

  IOStatus DoFsync(FSRandomRWFile* file, const IOOptions& options,
                   IODebugContext* dbg) override {
    IOStatus rv = InjectionFileSystem::DoFsync(file, options, dbg);
    if (rv.ok()) {
      counters_.fsyncs++;
    }
    return rv;
  }

  IOStatus DoClose(FSRandomRWFile* file, const IOOptions& options,
                   IODebugContext* dbg) override {
    IOStatus rv = InjectionFileSystem::DoClose(file, options, dbg);
    if (rv.ok()) {
      counters_.closes++;
    }
    return rv;
  }

  IOStatus DoFsync(FSDirectory* dir, const IOOptions& options,
                   IODebugContext* dbg) override {
    IOStatus rv = InjectionFileSystem::DoFsync(dir, options, dbg);
    if (rv.ok()) {
      counters_.dsyncs++;
    }
    return rv;
  }

  IOStatus DoFsyncWithDirOptions(FSDirectory* dir, const IOOptions& options,
                                 IODebugContext* dbg,
                                 const DirFsyncOptions& dir_options) override {
    IOStatus rv = InjectionFileSystem::DoFsyncWithDirOptions(dir, options, dbg,
                                                             dir_options);
    if (rv.ok()) {
      counters_.dsyncs++;
    }
    return rv;
  }

  IOStatus DoClose(FSDirectory* dir, const IOOptions& options,
                   IODebugContext* dbg) override {
    IOStatus rv = InjectionFileSystem::DoClose(dir, options, dbg);
    if (rv.ok()) {
      counters_.closes++;
      counters_.dir_closes++;
    }
    return rv;
  }
};
}  // namespace ROCKSDB_NAMESPACE
