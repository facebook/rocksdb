//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#pragma once
#include <atomic>
#include <thread>

#include "rocksdb/configurable.h"
#include "rocksdb/file_system.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class CloudFileSystem;
class Env;

// Creates a new file, appends data to a file or delete an existing file via
// logging into a cloud stream (such as Kinesis).
//
class CloudLogWritableFile : public FSWritableFile {
 public:
  CloudLogWritableFile(Env* env, CloudFileSystem* cloud_fs,
                       const std::string& fname, const FileOptions& options);

  IOStatus Flush(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    assert(status_.ok());
    return status_;
  }

  IOStatus Sync(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    assert(status_.ok());
    return status_;
  }

  virtual IOStatus status() { return status_; }

  // Delete a file by logging a delete operation to the Cloud stream.
  virtual IOStatus LogDelete() = 0;

 protected:
  Env* env_;
  CloudFileSystem* cloud_fs_;
  std::string fname_;
  IOStatus status_;
};

class CloudLogController : public Configurable {
 public:
  virtual ~CloudLogController();
  static const char* Type() { return "CloudStorageProvider"; }
  // Creates and configures a new CloudLogController from the input options and
  // id.
  static Status CreateFromString(
      const ConfigOptions& config_options, const std::string& id,
      std::shared_ptr<CloudLogController>* controller);

  // Returns name of the cloud log type (Kinesis, etc.).
  virtual const char* Name() const = 0;

  // Create a stream to store all log files.
  virtual IOStatus CreateStream(const std::string& topic) = 0;

  // Waits for stream to be ready (blocking).
  virtual IOStatus WaitForStreamReady(const std::string& topic) = 0;

  // Continuously tail the cloud log stream and apply changes to
  // the local file system (blocking).
  virtual IOStatus TailStream() = 0;

  // Creates a new cloud log writable file.
  virtual CloudLogWritableFile* CreateWritableFile(const std::string& fname,
                                                   const FileOptions& options,
                                                   IODebugContext* dbg) = 0;

  // Directory where files are cached locally.
  virtual const std::string& GetCacheDir() const = 0;
  virtual Status status() const = 0;

  virtual IOStatus StartTailingStream(const std::string& topic) = 0;
  virtual void StopTailingStream() = 0;
  virtual IOStatus GetFileModificationTime(const std::string& fname,
                                           uint64_t* time) = 0;
  virtual IOStatus NewSequentialFile(const std::string& fname,
                                     const FileOptions& file_opts,
                                     std::unique_ptr<FSSequentialFile>* result,
                                     IODebugContext* dbg) = 0;
  virtual IOStatus NewRandomAccessFile(
      const std::string& fname, const FileOptions& file_opts,
      std::unique_ptr<FSRandomAccessFile>* result, IODebugContext* dbg) = 0;
  virtual IOStatus FileExists(const std::string& fname) = 0;
  virtual IOStatus GetFileSize(const std::string& logical_fname,
                               uint64_t* size) = 0;
};

}  // namespace ROCKSDB_NAMESPACE
