//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#pragma once
#ifdef USE_AWS

#include <chrono>
#include <fstream>
#include <iostream>
#include "cloud/aws/aws_env.h"
#include "cloud/filename.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "util/filename.h"

#include <aws/core/Aws.h>
#include <aws/core/utils/DateTime.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/crypto/CryptoStream.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/CreateBucketConfiguration.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/CreateBucketResult.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectResult.h>
#include <aws/s3/model/GetBucketVersioningRequest.h>
#include <aws/s3/model/GetBucketVersioningResult.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/ListObjectsResult.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/PutObjectResult.h>

#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/KinesisErrors.h>
#include <aws/kinesis/model/CreateStreamRequest.h>
#include <aws/kinesis/model/DescribeStreamRequest.h>
#include <aws/kinesis/model/DescribeStreamResult.h>
#include <aws/kinesis/model/GetRecordsRequest.h>
#include <aws/kinesis/model/GetRecordsResult.h>
#include <aws/kinesis/model/GetShardIteratorRequest.h>
#include <aws/kinesis/model/GetShardIteratorResult.h>
#include <aws/kinesis/model/PutRecordRequest.h>
#include <aws/kinesis/model/PutRecordResult.h>
#include <aws/kinesis/model/PutRecordsRequest.h>
#include <aws/kinesis/model/PutRecordsRequestEntry.h>
#include <aws/kinesis/model/Record.h>
#include <aws/kinesis/model/ShardIteratorType.h>
#include <aws/kinesis/model/StreamDescription.h>

using Aws::Kinesis::Model::PutRecordRequest;
using Aws::Kinesis::Model::PutRecordsRequest;
using Aws::Kinesis::Model::PutRecordsRequestEntry;
using Aws::Kinesis::Model::PutRecordOutcome;
using Aws::Kinesis::Model::PutRecordsOutcome;
using Aws::Kinesis::Model::PutRecordResult;
using Aws::Kinesis::Model::PutRecordsResult;
using Aws::Kinesis::Model::PutRecordsResultEntry;
using Aws::Kinesis::Model::CreateStreamOutcome;
using Aws::Kinesis::Model::CreateStreamRequest;
using Aws::Kinesis::Model::Shard;
using Aws::Kinesis::Model::DescribeStreamRequest;
using Aws::Kinesis::Model::DescribeStreamOutcome;
using Aws::Kinesis::Model::DescribeStreamResult;
using Aws::Kinesis::Model::StreamDescription;
using Aws::Kinesis::Model::GetShardIteratorRequest;
using Aws::Kinesis::Model::GetShardIteratorResult;
using Aws::Kinesis::Model::ShardIteratorType;
using Aws::Kinesis::Model::GetShardIteratorOutcome;
using Aws::Kinesis::Model::GetRecordsRequest;
using Aws::Kinesis::Model::GetRecordsOutcome;
using Aws::Kinesis::Model::GetRecordsResult;
using Aws::Kinesis::Model::Record;
using Aws::Kinesis::KinesisClient;
using Aws::Kinesis::KinesisErrors;

// A few local defintions
namespace {

// pathaname seperator
const std::string pathsep = "/";

// types of rocksdb files
const std::string sst = ".sst";
const std::string ldb = ".ldb";
const std::string log = ".log";

// Is this a sst file, i.e. ends in ".sst" or ".ldb"
inline bool IsSstFile(const std::string& pathname) {
  if (pathname.size() < sst.size()) {
    return false;
  }
  const char* ptr = pathname.c_str() + pathname.size() - sst.size();
  if ((memcmp(ptr, sst.c_str(), sst.size()) == 0) ||
      (memcmp(ptr, ldb.c_str(), ldb.size()) == 0)) {
    return true;
  }
  return false;
}

// A log file has ".log" suffix
inline bool IsWalFile(const std::string& pathname) {
  if (pathname.size() < log.size()) {
    return false;
  }
  const char* ptr = pathname.c_str() + pathname.size() - log.size();
  if (memcmp(ptr, log.c_str(), log.size()) == 0) {
    return true;
  }
  return false;
}

bool IsManifestFile(const std::string& pathname) {
  // extract last component of the path
  std::string fname;
  size_t offset = pathname.find_last_of(pathsep);
  if (offset != std::string::npos) {
    fname = pathname.substr(offset + 1, pathname.size());
  } else {
    fname = pathname;
  }
  if (fname.find("MANIFEST") == 0) {
    return true;
  }
  return false;
}

bool __attribute__((unused)) IsIdentityFile(const std::string& pathname) {
  // extract last component of the path
  std::string fname;
  size_t offset = pathname.find_last_of(pathsep);
  if (offset != std::string::npos) {
    fname = pathname.substr(offset + 1, pathname.size());
  } else {
    fname = pathname;
  }
  if (fname.find("IDENTITY") == 0) {
    return true;
  }
  return false;
}

// A log file has ".log" suffix or starts with 'MANIFEST"
inline bool IsLogFile(const std::string& pathname) {
  return IsWalFile(pathname) || IsManifestFile(pathname);
}

// Get my bucket name
inline Aws::String GetBucket(const std::string& bucket_prefix) {
  std::string dd = "rockset." + bucket_prefix;
  return Aws::String(dd.c_str(), dd.size());
}

// Get my stream name
inline Aws::String GetStreamName(const std::string& bucket_prefix) {
  std::string dd = "rockset." + bucket_prefix;
  return Aws::String(dd.c_str(), dd.size());
}

}  // namespace

namespace rocksdb {

class S3ReadableFile : virtual public SequentialFile,
                       virtual public RandomAccessFile {
 public:
  S3ReadableFile(AwsEnv* env, const std::string& bucket_prefix,
                 const std::string& fname, bool is_file = true);
  virtual ~S3ReadableFile();

  // sequential access, read data at current offset in file
  virtual Status Read(size_t n, Slice* result, char* scratch);

  // random access, read data from specified offset in file
  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const;

  virtual Status Skip(uint64_t n);

  uint64_t GetSize() const { return file_size_; }
  uint64_t GetLastModTime() const { return last_mod_time_; }
  virtual Status status() { return status_; }
  virtual size_t GetUniqueId(char* id, size_t max_size) const override;

 private:
  AwsEnv* env_;
  std::string fname_;
  uint64_t file_number_;
  FileType file_type_;
  WalFileType log_type_;
  Status status_;
  Aws::String s3_bucket_;
  Aws::String s3_object_;
  uint64_t offset_;
  mutable uint64_t file_size_;
  mutable uint64_t last_mod_time_;
  bool is_file_;  // is this a file or dir?
  Status GetFileInfo();
};

// Appends to a file in S3.
class S3WritableFile : public WritableFile {
 private:
  AwsEnv* env_;
  std::string fname_;
  Status status_;
  unique_ptr<WritableFile> temp_file_;  // handle to the temporary file
  Aws::String s3_bucket_;
  Aws::String s3_object_;
  bool is_manifest_;
  const uint64_t manifest_durable_periodicity_millis_;
  uint64_t manifest_last_sync_time_;  // last time when manifest made duarbale

 public:
  // create S3 bucket
  static Status CreateBucketInS3(
      std::shared_ptr<Aws::S3::S3Client> client,
      const std::string& bucket_prefix,
      const Aws::S3::Model::BucketLocationConstraint& location);

  S3WritableFile(AwsEnv* env, const std::string& local_fname,
                 const std::string& bucket_prefix,
                 const std::string& cloud_fname, const EnvOptions& options,
                 const CloudEnvOptions cloud_env_options);

  virtual ~S3WritableFile();

  virtual Status Append(const Slice& data) {
    assert(status_.ok());
    // write to temporary file
    return temp_file_->Append(data);
  }

  virtual Status Flush() {
    assert(status_.ok());
    return temp_file_->Flush();
  }

  virtual Status Sync();

  virtual Status status() { return status_; }

  virtual Status Close();

  virtual Status CopyManifestToS3(bool force = false);
  static Status CopyToS3(const AwsEnv* env, const std::string& fname,
                         const Aws::String& s3_bucket,
                         const Aws::String& destination_object);
  static Status CopyFromS3(AwsEnv* env, const std::string& bucket_prefix,
                           const std::string& source_object,
                           const std::string& destination_pathname,
                           uint64_t size = 0,  // entire file
                           bool do_sync = 1);  // sync
};

// Creates a new file, appends data to a file or delete an existing file via
// logging into a Kinesis stream
//
class KinesisWritableFile : public WritableFile {
 public:
  KinesisWritableFile(AwsEnv* env, const std::string& fname,
                      const EnvOptions& options);

  virtual ~KinesisWritableFile();

  // Appends data to a file. The file is crested if it does not already exists.
  virtual Status Append(const Slice& data);

  virtual Status Flush() {
    assert(status_.ok());
    return status_;
  }

  virtual Status Sync() {
    assert(status_.ok());
    return status_;
  }

  virtual Status status() { return status_; }

  // Closes a file by writing an eof marker to Kinesis stream
  virtual Status Close();

  // Delete a file by logging a delete operation to the Kinesis stream
  virtual Status LogDelete();

 private:
  AwsEnv* env_;
  std::string fname_;
  Status status_;
  unique_ptr<WritableFile> temp_file_;  // handle to the temporary file
  Aws::String topic_;
  uint64_t current_offset_;
};

//
// Intricacies of reading a Kinesis stream
//
class KinesisSystem {
 public:
  static const uint32_t Append = 0x1;  // add a new record to a logfile
  static const uint32_t Delete = 0x2;  // delete a log file
  static const uint32_t Closed = 0x4;  // closing a file

  KinesisSystem(AwsEnv* env, std::shared_ptr<Logger> info_log);
  virtual ~KinesisSystem();

  // Continuously tail the Kinesis stream and apply to local file system
  Status TailStream();

  // The directory where files are cached
  std::string const GetCacheDir() { return cache_dir_; }

  Status const status() { return status_; }

  // convert a original pathname to a pathname in the cache
  static std::string GetCachePath(const std::string& cache_dir,
                                  const Slice& original_pathname);

  static void SerializeLogRecordAppend(const Slice& filename, const Slice& data,
                                       uint64_t offset, std::string* out);
  static void SerializeLogRecordClosed(const Slice& filename,
                                       uint64_t file_size, std::string* out);
  static void SerializeLogRecordDelete(const std::string& filename,
                                       std::string* out);

  // create stream to store all log files
  static Status CreateStream(
      AwsEnv* env, std::shared_ptr<Logger> info_log,
      std::shared_ptr<Aws::Kinesis::KinesisClient> client,
      const std::string& bucket_prefix);
  // wait for stream to be ready
  static Status WaitForStreamReady(
      AwsEnv* env, std::shared_ptr<Logger> info_log,
      std::shared_ptr<Aws::Kinesis::KinesisClient> client,
      const std::string& bucket_prefix);

  // delay in Kinesis stream: writes to read visibility
  static const uint64_t retry_period_micros = 30 * 1000000L;  // 30 seconds

  // Retry this till success or timeout has expired
  typedef std::function<Status()> RetryType;
  static Status Retry(Env* env, RetryType func);

 private:
  AwsEnv* env_;
  std::shared_ptr<Logger> info_log_;
  Aws::String topic_;
  Status status_;
  std::string cache_dir_;

  // list of shards and their positions
  Aws::Vector<Shard> shards_;
  Aws::Vector<Aws::String> shards_iterator_;
  std::vector<Aws::String> shards_position_;

  // A cache of pathnames to their open file _escriptors
  std::map<std::string, std::unique_ptr<RandomRWFile>> cache_fds_;

  Status InitializeShards();
  Status Apply(const Slice& data);

  // Set shard iterator for every shard to position specified by
  // shards_position_
  void SeekShards();
  static bool ExtractLogRecord(const Slice& input, uint32_t* operation,
                               Slice* filename, uint64_t* offset_in_file,
                               uint64_t* file_size, Slice* data);
};

}  // namepace rocksdb

#endif /* USE_AWS */
