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
struct NoSyncOptions {
  static const char* kName() { return "NoSyncOptions"; }
  explicit NoSyncOptions(bool enabled = false)
      : do_sync(enabled),
        do_fsync(enabled),
        do_rsync(enabled),
        do_dsync(enabled) {}

  bool do_sync = false;
  bool do_fsync = false;
  bool do_rsync = false;
  bool do_dsync = false;
};

// A FileSystem that allows the sync operations to be skipped
// By default, the NoSyncFileSystem will skip all sync (Sync, Fsync,
// RangeSync, and Fsync for directories) operations.
//
class NoSyncFileSystem : public InjectionFileSystem {
 private:
  NoSyncOptions sync_opts_;

 public:
  // Creates a new NoSyncFileSystem wrapping the input base.
  // If enabled=false, all sync operations are skipped (e.g. disabled).
  // Sync operations can also be turned on or off by their type individually
  // through the configuration or methods.
  explicit NoSyncFileSystem(const std::shared_ptr<FileSystem>& base,
                            bool enabled = false);
  static const char* kClassName() { return "NoSyncFileSystem"; }
  const char* Name() const override { return kClassName(); }

  void SetSyncEnabled(bool b) { sync_opts_.do_sync = b; }
  void SetFSyncEnabled(bool b) { sync_opts_.do_fsync = b; }
  void SetRangeSyncEnabled(bool b) { sync_opts_.do_rsync = b; }
  void SetDirSyncEnabled(bool b) { sync_opts_.do_dsync = b; }
  bool IsSyncEnabled() const { return sync_opts_.do_sync; }
  bool IsFSyncEnabled() const { return sync_opts_.do_fsync; }
  bool IsRangeSyncEnabled() const { return sync_opts_.do_rsync; }
  bool IsDirSyncEnabled() const { return sync_opts_.do_dsync; }

 protected:
  IOStatus DoSync(FSWritableFile* file, const IOOptions& options,
                  IODebugContext* dbg) override {
    if (sync_opts_.do_sync) {
      return InjectionFileSystem::DoSync(file, options, dbg);
    } else {
      return IOStatus::OK();
    }
  }

  IOStatus DoFsync(FSWritableFile* file, const IOOptions& options,
                   IODebugContext* dbg) override {
    if (sync_opts_.do_fsync) {
      return InjectionFileSystem::DoFsync(file, options, dbg);
    } else {
      return IOStatus::OK();
    }
  }

  IOStatus DoRangeSync(FSWritableFile* file, uint64_t offset, uint64_t nbytes,
                       const IOOptions& options, IODebugContext* dbg) override {
    if (sync_opts_.do_rsync) {
      return InjectionFileSystem::DoRangeSync(file, offset, nbytes, options,
                                              dbg);
    } else {
      return IOStatus::OK();
    }
  }

  IOStatus DoSync(FSRandomRWFile* file, const IOOptions& options,
                  IODebugContext* dbg) override {
    if (sync_opts_.do_sync) {
      return InjectionFileSystem::DoSync(file, options, dbg);
    } else {
      return IOStatus::OK();
    }
  }

  IOStatus DoFsync(FSRandomRWFile* file, const IOOptions& options,
                   IODebugContext* dbg) override {
    if (sync_opts_.do_fsync) {
      return InjectionFileSystem::DoFsync(file, options, dbg);
    } else {
      return IOStatus::OK();
    }
  }

  IOStatus DoFsync(FSDirectory* dir, const IOOptions& options,
                   IODebugContext* dbg) override {
    if (sync_opts_.do_dsync) {
      return InjectionFileSystem::DoFsync(dir, options, dbg);
    } else {
      return IOStatus::OK();
    }
  }

  IOStatus DoFsyncWithDirOptions(FSDirectory* dir, const IOOptions& options,
                                 IODebugContext* dbg,
                                 const DirFsyncOptions& dir_options) override {
    if (sync_opts_.do_dsync) {
      return InjectionFileSystem::DoFsyncWithDirOptions(dir, options, dbg,
                                                        dir_options);
    } else {
      return IOStatus::OK();
    }
  }
};
}  // namespace ROCKSDB_NAMESPACE
