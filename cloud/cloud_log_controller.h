//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#pragma once
#include <atomic>
#include <thread>

#include "rocksdb/env.h"
#include "rocksdb/status.h"

namespace rocksdb {
class CloudEnv;
class CloudEnvOptions;

// Creates a new file, appends data to a file or delete an existing file via
// logging into a cloud stream (such as Kinesis).
//
class CloudLogWritableFile : public WritableFile {
 public:
  CloudLogWritableFile(CloudEnv* env, const std::string& fname, const EnvOptions& options);
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

class CloudLogController {
 public:
  static constexpr const char* kCacheDir = "/tmp/ROCKSET";

  // Delay in Cloud Log stream: writes to read visibility
  static const std::chrono::microseconds kRetryPeriod;

  static const uint32_t kAppend = 0x1;  // add a new record to a logfile
  static const uint32_t kDelete = 0x2;  // delete a log file
  static const uint32_t kClosed = 0x4;  // closing a file

  CloudLogController(CloudEnv* env);
  virtual ~CloudLogController();

  // Create a stream to store all log files.
  virtual Status CreateStream(const std::string& topic) = 0;

  // Waits for stream to be ready (blocking).
  virtual Status WaitForStreamReady(const std::string& topic) = 0;

  // Continuously tail the cloud log stream and apply changes to
  // the local file system (blocking).
  virtual Status TailStream() = 0;

  // Creates a new cloud log writable file.
  virtual CloudLogWritableFile* CreateWritableFile(const std::string& fname,
                                                   const EnvOptions& options) = 0;

  // Returns name of the cloud log type (Kinesis, etc.).
  virtual const char *Name() const { return "cloudlog"; }

  // Directory where files are cached locally.
  const std::string & GetCacheDir() const { return cache_dir_; }

  Status const status() { return status_; }

  // Converts an original pathname to a pathname in the cache.
  std::string GetCachePath(const Slice& original_pathname) const;

  static void SerializeLogRecordAppend(const Slice& filename, const Slice& data,
                                       uint64_t offset, std::string* out);
  static void SerializeLogRecordClosed(const Slice& filename,
                                       uint64_t file_size, std::string* out);
  static void SerializeLogRecordDelete(const std::string& filename,
                                       std::string* out);

  // Retries fnc until success or timeout has expired.
  typedef std::function<Status()> RetryType;
  Status Retry(RetryType func);
  Status StartTailingStream(const std::string & topic);
  void StopTailingStream();
 protected:
  CloudEnv* env_;
  Status status_;
  std::string cache_dir_;

  // A cache of pathnames to their open file _escriptors
  std::map<std::string, std::unique_ptr<RandomRWFile>> cache_fds_;

  Status Apply(const Slice& data);
  static bool ExtractLogRecord(const Slice& input, uint32_t* operation,
                               Slice* filename, uint64_t* offset_in_file,
                               uint64_t* file_size, Slice* data);
  bool IsRunning() const { return running_; }
private:
  // Background thread to tail stream
  std::unique_ptr<std::thread> tid_;
  std::atomic<bool> running_;
};
Status CreateKinesisController(CloudEnv* env, std::unique_ptr<CloudLogController> * result);
Status CreateKafkaController(CloudEnv* env, std::unique_ptr<CloudLogController> * result);
} // namespace rocksdb

