//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/counted_fs.h"

#include "rocksdb/file_system.h"

namespace ROCKSDB_NAMESPACE {
namespace {
class CountedSequentialFile : public FSSequentialFileOwnerWrapper {
 private:
  CountedFileSystem* fs_;

 public:
  CountedSequentialFile(std::unique_ptr<FSSequentialFile>&& target,
                        CountedFileSystem* fs)
      : FSSequentialFileOwnerWrapper(std::move(target)), fs_(fs) {}

  ~CountedSequentialFile() override { fs_->counters()->closes++; }

  IOStatus Read(size_t n, const IOOptions& options, Slice* result,
                char* scratch, IODebugContext* dbg) override {
    IOStatus rv = target()->Read(n, options, result, scratch, dbg);
    fs_->counters()->reads.RecordOp(rv, result->size());
    return rv;
  }

  IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions& options,
                          Slice* result, char* scratch,
                          IODebugContext* dbg) override {
    IOStatus rv =
        target()->PositionedRead(offset, n, options, result, scratch, dbg);
    fs_->counters()->reads.RecordOp(rv, result->size());
    return rv;
  }
};

class CountedRandomAccessFile : public FSRandomAccessFileOwnerWrapper {
 private:
  CountedFileSystem* fs_;

 public:
  CountedRandomAccessFile(std::unique_ptr<FSRandomAccessFile>&& target,
                          CountedFileSystem* fs)
      : FSRandomAccessFileOwnerWrapper(std::move(target)), fs_(fs) {}

  ~CountedRandomAccessFile() override { fs_->counters()->closes++; }

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override {
    IOStatus rv = target()->Read(offset, n, options, result, scratch, dbg);
    fs_->counters()->reads.RecordOp(rv, result->size());
    return rv;
  }

  IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
                     const IOOptions& options, IODebugContext* dbg) override {
    IOStatus rv = target()->MultiRead(reqs, num_reqs, options, dbg);
    for (size_t r = 0; r < num_reqs; r++) {
      fs_->counters()->reads.RecordOp(reqs[r].status, reqs[r].result.size());
    }
    return rv;
  }
};

class CountedWritableFile : public FSWritableFileOwnerWrapper {
 private:
  CountedFileSystem* fs_;
  bool skip_fsync_;

 public:
  CountedWritableFile(std::unique_ptr<FSWritableFile>&& target,
                      CountedFileSystem* fs, bool skip_fsync)
      : FSWritableFileOwnerWrapper(std::move(target)),
        fs_(fs),
        skip_fsync_(skip_fsync) {}

  IOStatus Append(const Slice& data, const IOOptions& options,
                  IODebugContext* dbg) override {
    IOStatus rv = target()->Append(data, options, dbg);
    fs_->counters()->writes.RecordOp(rv, data.size());
    return rv;
  }

  IOStatus Append(const Slice& data, const IOOptions& options,
                  const DataVerificationInfo& info,
                  IODebugContext* dbg) override {
    IOStatus rv = target()->Append(data, options, info, dbg);
    fs_->counters()->writes.RecordOp(rv, data.size());
    return rv;
  }

  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            IODebugContext* dbg) override {
    IOStatus rv = target()->PositionedAppend(data, offset, options, dbg);
    fs_->counters()->writes.RecordOp(rv, data.size());
    return rv;
  }

  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            const DataVerificationInfo& info,
                            IODebugContext* dbg) override {
    IOStatus rv = target()->PositionedAppend(data, offset, options, info, dbg);
    fs_->counters()->writes.RecordOp(rv, data.size());
    return rv;
  }

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
    IOStatus rv = target()->Close(options, dbg);
    if (rv.ok()) {
      fs_->counters()->closes++;
    }
    return rv;
  }

  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override {
    IOStatus rv = target()->Flush(options, dbg);
    if (rv.ok()) {
      fs_->counters()->flushes++;
    }
    return rv;
  }

  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override {
    IOStatus rv;
    if (!skip_fsync_) {
      rv = target()->Sync(options, dbg);
    }
    if (rv.ok()) {
      fs_->counters()->syncs++;
    }
    return rv;
  }

  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
    IOStatus rv;
    if (!skip_fsync_) {
      rv = target()->Fsync(options, dbg);
    }
    if (rv.ok()) {
      fs_->counters()->fsyncs++;
    }
    return rv;
  }

  IOStatus RangeSync(uint64_t offset, uint64_t nbytes, const IOOptions& options,
                     IODebugContext* dbg) override {
    IOStatus rv = target()->RangeSync(offset, nbytes, options, dbg);
    if (rv.ok()) {
      fs_->counters()->syncs++;
    }
    return rv;
  }
};

class CountedRandomRWFile : public FSRandomRWFileOwnerWrapper {
 private:
  mutable CountedFileSystem* fs_;
  bool skip_fsync_;

 public:
  CountedRandomRWFile(std::unique_ptr<FSRandomRWFile>&& target,
                      CountedFileSystem* fs, bool skip_fsync)
      : FSRandomRWFileOwnerWrapper(std::move(target)),
        fs_(fs),
        skip_fsync_(skip_fsync) {}
  IOStatus Write(uint64_t offset, const Slice& data, const IOOptions& options,
                 IODebugContext* dbg) override {
    IOStatus rv = target()->Write(offset, data, options, dbg);
    fs_->counters()->writes.RecordOp(rv, data.size());
    return rv;
  }

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override {
    IOStatus rv = target()->Read(offset, n, options, result, scratch, dbg);
    fs_->counters()->reads.RecordOp(rv, result->size());
    return rv;
  }

  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override {
    IOStatus rv = target()->Flush(options, dbg);
    if (rv.ok()) {
      fs_->counters()->flushes++;
    }
    return rv;
  }

  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override {
    IOStatus rv;
    if (!skip_fsync_) {
      rv = target()->Sync(options, dbg);
    }
    if (rv.ok()) {
      fs_->counters()->syncs++;
    }
    return rv;
  }

  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
    IOStatus rv;
    if (!skip_fsync_) {
      rv = target()->Fsync(options, dbg);
    }
    if (rv.ok()) {
      fs_->counters()->fsyncs++;
    }
    return rv;
  }

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
    IOStatus rv = target()->Close(options, dbg);
    if (rv.ok()) {
      fs_->counters()->closes++;
    }
    return rv;
  }
};
}  // namespace

CountedFileSystem::CountedFileSystem(const std::shared_ptr<FileSystem>& base,
                                     bool skip_fsync)
    : FileSystemWrapper(base), skip_fsync_(skip_fsync) {}

IOStatus CountedFileSystem::NewSequentialFile(
    const std::string& f, const FileOptions& options,
    std::unique_ptr<FSSequentialFile>* r, IODebugContext* dbg) {
  std::unique_ptr<FSSequentialFile> base;
  IOStatus s = target()->NewSequentialFile(f, options, &base, dbg);
  if (s.ok()) {
    counters_.opens++;
    r->reset(new CountedSequentialFile(std::move(base), this));
  }
  return s;
}

IOStatus CountedFileSystem::NewRandomAccessFile(
    const std::string& f, const FileOptions& options,
    std::unique_ptr<FSRandomAccessFile>* r, IODebugContext* dbg) {
  std::unique_ptr<FSRandomAccessFile> base;
  IOStatus s = target()->NewRandomAccessFile(f, options, &base, dbg);
  if (s.ok()) {
    counters_.opens++;
    r->reset(new CountedRandomAccessFile(std::move(base), this));
  }
  return s;
}

IOStatus CountedFileSystem::NewWritableFile(const std::string& f,
                                            const FileOptions& options,
                                            std::unique_ptr<FSWritableFile>* r,
                                            IODebugContext* dbg) {
  std::unique_ptr<FSWritableFile> base;
  IOStatus s = target()->NewWritableFile(f, options, &base, dbg);
  if (s.ok()) {
    counters_.opens++;
    r->reset(new CountedWritableFile(std::move(base), this, skip_fsync_));
  }
  return s;
}

IOStatus CountedFileSystem::ReopenWritableFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSWritableFile>* result, IODebugContext* dbg) {
  std::unique_ptr<FSWritableFile> base;
  IOStatus s = target()->ReopenWritableFile(fname, options, &base, dbg);
  if (s.ok()) {
    counters_.opens++;
    result->reset(new CountedWritableFile(std::move(base), this, skip_fsync_));
  }
  return s;
}

IOStatus CountedFileSystem::ReuseWritableFile(
    const std::string& fname, const std::string& old_fname,
    const FileOptions& options, std::unique_ptr<FSWritableFile>* result,
    IODebugContext* dbg) {
  std::unique_ptr<FSWritableFile> base;
  IOStatus s =
      target()->ReuseWritableFile(fname, old_fname, options, &base, dbg);
  if (s.ok()) {
    counters_.opens++;
    result->reset(new CountedWritableFile(std::move(base), this, skip_fsync_));
  }
  return s;
}

IOStatus CountedFileSystem::NewRandomRWFile(
    const std::string& name, const FileOptions& options,
    std::unique_ptr<FSRandomRWFile>* result, IODebugContext* dbg) {
  std::unique_ptr<FSRandomRWFile> base;
  IOStatus s = target()->NewRandomRWFile(name, options, &base, dbg);
  if (s.ok()) {
    counters_.opens++;
    result->reset(new CountedRandomRWFile(std::move(base), this, skip_fsync_));
  }
  return s;
}
}  // namespace ROCKSDB_NAMESPACE
