//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifdef GFLAGS
#pragma once
#include "db_stress_tool/db_stress_common.h"
#include "monitoring/thread_status_util.h"

namespace ROCKSDB_NAMESPACE {
class DbStressRandomAccessFileWrapper : public FSRandomAccessFileOwnerWrapper {
 public:
  explicit DbStressRandomAccessFileWrapper(
      std::unique_ptr<FSRandomAccessFile>&& target)
      : FSRandomAccessFileOwnerWrapper(std::move(target)) {}

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override {
#ifndef NDEBUG
    const ThreadStatus::OperationType thread_op =
        ThreadStatusUtil::GetThreadOperation();
    Env::IOActivity io_activity =
        ThreadStatusUtil::TEST_GetExpectedIOActivity(thread_op);
    assert(io_activity == Env::IOActivity::kUnknown ||
           io_activity == options.io_activity);
#endif
    return target()->Read(offset, n, options, result, scratch, dbg);
  }

  IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
                     const IOOptions& options, IODebugContext* dbg) override {
#ifndef NDEBUG
    const ThreadStatus::OperationType thread_op =
        ThreadStatusUtil::GetThreadOperation();
    Env::IOActivity io_activity =
        ThreadStatusUtil::TEST_GetExpectedIOActivity(thread_op);
    assert(io_activity == Env::IOActivity::kUnknown ||
           io_activity == options.io_activity);
#endif
    return target()->MultiRead(reqs, num_reqs, options, dbg);
  }

  IOStatus Prefetch(uint64_t offset, size_t n, const IOOptions& options,
                    IODebugContext* dbg) override {
#ifndef NDEBUG
    const ThreadStatus::OperationType thread_op =
        ThreadStatusUtil::GetThreadOperation();
    Env::IOActivity io_activity =
        ThreadStatusUtil::TEST_GetExpectedIOActivity(thread_op);
    assert(io_activity == Env::IOActivity::kUnknown ||
           io_activity == options.io_activity);
#endif
    return target()->Prefetch(offset, n, options, dbg);
  }

  IOStatus ReadAsync(FSReadRequest& req, const IOOptions& options,
                     std::function<void(FSReadRequest&, void*)> cb,
                     void* cb_arg, void** io_handle, IOHandleDeleter* del_fn,
                     IODebugContext* dbg) override {
#ifndef NDEBUG
    const ThreadStatus::OperationType thread_op =
        ThreadStatusUtil::GetThreadOperation();
    Env::IOActivity io_activity =
        ThreadStatusUtil::TEST_GetExpectedIOActivity(thread_op);
    assert(io_activity == Env::IOActivity::kUnknown ||
           io_activity == options.io_activity);
#endif
    return target()->ReadAsync(req, options, cb, cb_arg, io_handle, del_fn,
                               dbg);
  }
};

class DbStressWritableFileWrapper : public FSWritableFileOwnerWrapper {
 public:
  explicit DbStressWritableFileWrapper(std::unique_ptr<FSWritableFile>&& target)
      : FSWritableFileOwnerWrapper(std::move(target)) {}

  IOStatus Append(const Slice& data, const IOOptions& options,
                  IODebugContext* dbg) override {
#ifndef NDEBUG
    const ThreadStatus::OperationType thread_op =
        ThreadStatusUtil::GetThreadOperation();
    Env::IOActivity io_activity =
        ThreadStatusUtil::TEST_GetExpectedIOActivity(thread_op);
    assert(io_activity == Env::IOActivity::kUnknown ||
           io_activity == options.io_activity);
#endif
    return target()->Append(data, options, dbg);
  }
  IOStatus Append(const Slice& data, const IOOptions& options,
                  const DataVerificationInfo& verification_info,
                  IODebugContext* dbg) override {
#ifndef NDEBUG
    const ThreadStatus::OperationType thread_op =
        ThreadStatusUtil::GetThreadOperation();
    Env::IOActivity io_activity =
        ThreadStatusUtil::TEST_GetExpectedIOActivity(thread_op);
    assert(io_activity == Env::IOActivity::kUnknown ||
           io_activity == options.io_activity);
#endif
    return target()->Append(data, options, verification_info, dbg);
  }
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            IODebugContext* dbg) override {
#ifndef NDEBUG
    const ThreadStatus::OperationType thread_op =
        ThreadStatusUtil::GetThreadOperation();
    Env::IOActivity io_activity =
        ThreadStatusUtil::TEST_GetExpectedIOActivity(thread_op);
    assert(io_activity == Env::IOActivity::kUnknown ||
           io_activity == options.io_activity);
#endif
    return target()->PositionedAppend(data, offset, options, dbg);
  }
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            const DataVerificationInfo& verification_info,
                            IODebugContext* dbg) override {
#ifndef NDEBUG
    const ThreadStatus::OperationType thread_op =
        ThreadStatusUtil::GetThreadOperation();
    Env::IOActivity io_activity =
        ThreadStatusUtil::TEST_GetExpectedIOActivity(thread_op);
    assert(io_activity == Env::IOActivity::kUnknown ||
           io_activity == options.io_activity);
#endif
    return target()->PositionedAppend(data, offset, options, verification_info,
                                      dbg);
  }

  IOStatus Truncate(uint64_t size, const IOOptions& options,
                    IODebugContext* dbg) override {
#ifndef NDEBUG
    const ThreadStatus::OperationType thread_op =
        ThreadStatusUtil::GetThreadOperation();
    Env::IOActivity io_activity =
        ThreadStatusUtil::TEST_GetExpectedIOActivity(thread_op);
    assert(io_activity == Env::IOActivity::kUnknown ||
           io_activity == options.io_activity);
#endif
    return target()->Truncate(size, options, dbg);
  }

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
#ifndef NDEBUG
    const ThreadStatus::OperationType thread_op =
        ThreadStatusUtil::GetThreadOperation();
    Env::IOActivity io_activity =
        ThreadStatusUtil::TEST_GetExpectedIOActivity(thread_op);
    assert(io_activity == Env::IOActivity::kUnknown ||
           io_activity == options.io_activity);
#endif
    return target()->Close(options, dbg);
  }

  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override {
#ifndef NDEBUG
    const ThreadStatus::OperationType thread_op =
        ThreadStatusUtil::GetThreadOperation();
    Env::IOActivity io_activity =
        ThreadStatusUtil::TEST_GetExpectedIOActivity(thread_op);
    assert(io_activity == Env::IOActivity::kUnknown ||
           io_activity == options.io_activity);
#endif
    return target()->Flush(options, dbg);
  }

  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override {
#ifndef NDEBUG
    const ThreadStatus::OperationType thread_op =
        ThreadStatusUtil::GetThreadOperation();
    Env::IOActivity io_activity =
        ThreadStatusUtil::TEST_GetExpectedIOActivity(thread_op);
    assert(io_activity == Env::IOActivity::kUnknown ||
           io_activity == options.io_activity);
#endif
    return target()->Sync(options, dbg);
  }

  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
#ifndef NDEBUG
    const ThreadStatus::OperationType thread_op =
        ThreadStatusUtil::GetThreadOperation();
    Env::IOActivity io_activity =
        ThreadStatusUtil::TEST_GetExpectedIOActivity(thread_op);
    assert(io_activity == Env::IOActivity::kUnknown ||
           io_activity == options.io_activity);
#endif
    return target()->Fsync(options, dbg);
  }

#ifdef ROCKSDB_FALLOCATE_PRESENT
  IOStatus Allocate(uint64_t offset, uint64_t len, const IOOptions& options,
                    IODebugContext* dbg) override {
#ifndef NDEBUG
    const ThreadStatus::OperationType thread_op =
        ThreadStatusUtil::GetThreadOperation();
    Env::IOActivity io_activity =
        ThreadStatusUtil::TEST_GetExpectedIOActivity(thread_op);
    assert(io_activity == Env::IOActivity::kUnknown ||
           io_activity == options.io_activity);
#endif
    return target()->Allocate(offset, len, options, dbg);
  }
#endif

  IOStatus RangeSync(uint64_t offset, uint64_t nbytes, const IOOptions& options,
                     IODebugContext* dbg) override {
#ifndef NDEBUG
    const ThreadStatus::OperationType thread_op =
        ThreadStatusUtil::GetThreadOperation();
    Env::IOActivity io_activity =
        ThreadStatusUtil::TEST_GetExpectedIOActivity(thread_op);
    assert(io_activity == Env::IOActivity::kUnknown ||
           io_activity == options.io_activity);
#endif
    return target()->RangeSync(offset, nbytes, options, dbg);
  }
};

class DbStressFSWrapper : public FileSystemWrapper {
 public:
  explicit DbStressFSWrapper(const std::shared_ptr<FileSystem>& t)
      : FileSystemWrapper(t) {}
  static const char* kClassName() { return "DbStressFS"; }
  const char* Name() const override { return kClassName(); }

  IOStatus NewRandomAccessFile(const std::string& f,
                               const FileOptions& file_opts,
                               std::unique_ptr<FSRandomAccessFile>* r,
                               IODebugContext* dbg) override {
    std::unique_ptr<FSRandomAccessFile> file;
    IOStatus s = target()->NewRandomAccessFile(f, file_opts, &file, dbg);
    if (s.ok()) {
      r->reset(new DbStressRandomAccessFileWrapper(std::move(file)));
    }
    return s;
  }

  IOStatus NewWritableFile(const std::string& f, const FileOptions& file_opts,
                           std::unique_ptr<FSWritableFile>* r,
                           IODebugContext* dbg) override {
    std::unique_ptr<FSWritableFile> file;
    IOStatus s = target()->NewWritableFile(f, file_opts, &file, dbg);
    if (s.ok()) {
      r->reset(new DbStressWritableFileWrapper(std::move(file)));
    }
    return s;
  }

  IOStatus DeleteFile(const std::string& f, const IOOptions& opts,
                      IODebugContext* dbg) override {
    // We determine whether it is a manifest file by searching a strong,
    // so that there will be false positive if the directory path contains the
    // keyword but it is unlikely.
    // Checkpoint, backup, and restore directories needs to be exempted.
    if (!if_preserve_all_manifests ||
        f.find("MANIFEST-") == std::string::npos ||
        f.find("checkpoint") != std::string::npos ||
        f.find(".backup") != std::string::npos ||
        f.find(".restore") != std::string::npos) {
      return target()->DeleteFile(f, opts, dbg);
    }
    // Rename the file instead of deletion to keep the history, and
    // at the same time it is not visible to RocksDB.
    return target()->RenameFile(f, f + "_renamed_", opts, dbg);
  }

  // If true, all manifest files will not be delted in DeleteFile().
  bool if_preserve_all_manifests = true;
};
}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
