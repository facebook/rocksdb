//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#pragma once

#include <atomic>
#include <thread>

#include "rocksdb/cloud/cloud_log_controller.h"

namespace ROCKSDB_NAMESPACE {
class CloudEnv;
class CloudEnvOptions;

class CloudLogControllerImpl : public CloudLogController {
 public:
  static const char* kKafka() { return "kafka"; }
  static const char* kKinesis() { return "kinesis"; }

  static constexpr const char* kCacheDir = "/tmp/ROCKSET";
  // Delay in Cloud Log stream: writes to read visibility
  static const std::chrono::microseconds kRetryPeriod;
  static Status CreateKinesisController(
      std::unique_ptr<CloudLogController>* result);
  static Status CreateKafkaController(
      std::unique_ptr<CloudLogController>* result);

  static const uint32_t kAppend = 0x1;  // add a new record to a logfile
  static const uint32_t kDelete = 0x2;  // delete a log file
  static const uint32_t kClosed = 0x4;  // closing a file

  CloudLogControllerImpl();
  virtual ~CloudLogControllerImpl();
  static Status CreateKinesisController(
      CloudEnv* env, std::shared_ptr<CloudLogController>* result);
  static Status CreateKafkaController(
      CloudEnv* env, std::shared_ptr<CloudLogController>* result);

  // Directory where files are cached locally.
  const std::string& GetCacheDir() const override { return cache_dir_; }
  Status const status() const override { return status_; }
  virtual Status StartTailingStream(const std::string& topic) override;
  void StopTailingStream() override;

  static void SerializeLogRecordAppend(const Slice& filename, const Slice& data,
                                       uint64_t offset, std::string* out);
  static void SerializeLogRecordClosed(const Slice& filename,
                                       uint64_t file_size, std::string* out);
  static void SerializeLogRecordDelete(const std::string& filename,
                                       std::string* out);
  Status GetFileModificationTime(const std::string& fname,
                                 uint64_t* time) override;
  Status NewSequentialFile(const std::string& fname,
                           std::unique_ptr<SequentialFile>* result,
                           const EnvOptions& options) override;
  Status NewRandomAccessFile(const std::string& fname,
                             std::unique_ptr<RandomAccessFile>* result,
                             const EnvOptions& options) override;
  Status FileExists(const std::string& fname) override;
  Status GetFileSize(const std::string& logical_fname, uint64_t* size) override;
  Status PrepareOptions(const ConfigOptions& options) override;

 protected:
  // Converts an original pathname to a pathname in the cache.
  std::string GetCachePath(const Slice& original_pathname) const;

  // Retries fnc until success or timeout has expired.
  typedef std::function<Status()> RetryType;
  Status Retry(RetryType func);

  static bool ExtractLogRecord(const Slice& input, uint32_t* operation,
                               Slice* filename, uint64_t* offset_in_file,
                               uint64_t* file_size, Slice* data);
  CloudEnv* env_;
  Status status_;
  std::string cache_dir_;
  // A cache of pathnames to their open file _escriptors
  std::map<std::string, std::unique_ptr<RandomRWFile>> cache_fds_;

  Status Apply(const Slice& data);
  bool IsRunning() const { return running_; }

 private:
  // Background thread to tail stream
  std::unique_ptr<std::thread> tid_;
  std::atomic<bool> running_;
};
}  // namespace ROCKSDB_NAMESPACE
