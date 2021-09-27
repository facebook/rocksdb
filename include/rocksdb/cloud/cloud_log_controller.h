//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#pragma once
#include <atomic>
#include <thread>

#include "rocksdb/configurable.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class CloudEnv;
class CloudEnvOptions;

// Creates a new file, appends data to a file or delete an existing file via
// logging into a cloud stream (such as Kinesis).
//
class CloudLogWritableFile : public WritableFile {
 public:
  CloudLogWritableFile(CloudEnv* env, const std::string& fname,
                       const EnvOptions& options);
  virtual ~CloudLogWritableFile();

  virtual Status Flush() {
    assert(status_.ok());
    return status_;
  }

  virtual Status Sync() {
    assert(status_.ok());
    return status_;
  }

  virtual Status status() { return status_; }

  // Appends data to the file. If the file doesn't exist, it'll get created.
  using WritableFile::Append;
  virtual Status Append(const Slice& data) = 0;

  // Closes a file by writing an EOF marker to the Cloud stream.
  virtual Status Close() = 0;

  // Delete a file by logging a delete operation to the Cloud stream.
  virtual Status LogDelete() = 0;

 protected:
  CloudEnv* env_;
  const std::string fname_;
  Status status_;
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
  virtual Status CreateStream(const std::string& topic) = 0;

  // Waits for stream to be ready (blocking).
  virtual Status WaitForStreamReady(const std::string& topic) = 0;

  // Continuously tail the cloud log stream and apply changes to
  // the local file system (blocking).
  virtual Status TailStream() = 0;

  // Creates a new cloud log writable file.
  virtual CloudLogWritableFile* CreateWritableFile(
      const std::string& fname, const EnvOptions& options) = 0;


  // Directory where files are cached locally.
  virtual const std::string& GetCacheDir() const = 0;
  virtual Status const status() const = 0;

  virtual Status StartTailingStream(const std::string& topic) = 0;
  virtual void StopTailingStream() = 0;
  virtual Status GetFileModificationTime(const std::string& fname,
                                         uint64_t* time) = 0;
  virtual Status NewSequentialFile(const std::string& fname,
                                   std::unique_ptr<SequentialFile>* result,
                                   const EnvOptions& options) = 0;
  virtual Status NewRandomAccessFile(const std::string& fname,
                                     std::unique_ptr<RandomAccessFile>* result,
                                     const EnvOptions& options) = 0;
  virtual Status FileExists(const std::string& fname) = 0;
  virtual Status GetFileSize(const std::string& logical_fname,
                             uint64_t* size) = 0;
};

}  // namespace ROCKSDB_NAMESPACE
