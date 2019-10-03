//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
// This file defines an AWS-Kinesis environment for rocksdb.
// A log file maps to a stream in Kinesis.
//
#ifdef USE_AWS

#include <fstream>
#include <iostream>

#include "cloud/aws/aws_env.h"
#include "cloud/aws/aws_file.h"
#include "cloud/aws/aws_kinesis.h"
#include "rocksdb/status.h"
#include "util/coding.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"

namespace rocksdb {

CloudLogWritableFile::CloudLogWritableFile(
    AwsEnv* env, const std::string& fname, const EnvOptions& /*options*/)
  : env_(env), fname_(fname) {}

CloudLogWritableFile::~CloudLogWritableFile() {}

const std::chrono::microseconds CloudLogController::kRetryPeriod =
  std::chrono::seconds(30);

CloudLogController::CloudLogController(
    AwsEnv* env, std::shared_ptr<Logger> info_log)
  : env_(env), info_log_(info_log) {

  // Create a random number for the cache directory.
  const std::string uid = trim(env_->GetBaseEnv()->GenerateUniqueId());

  // Temporary directory for cache.
  const std::string bucket_dir = kCacheDir + pathsep + env_->GetSrcBucketName();
  cache_dir_ = bucket_dir + pathsep + uid;

  // Create temporary directories.
  status_ = env_->GetBaseEnv()->CreateDirIfMissing(kCacheDir);
  if (status_.ok()) {
    status_ = env_->GetBaseEnv()->CreateDirIfMissing(bucket_dir);
  }
  if (status_.ok()) {
    status_ = env_->GetBaseEnv()->CreateDirIfMissing(cache_dir_);
  }
}

CloudLogController::~CloudLogController() {
  Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
      "[%s] CloudLogController closed.", GetTypeName().c_str());
}

std::string CloudLogController::GetCachePath(
    const std::string& cache_dir, const Slice& original_pathname) {
  return cache_dir + pathsep + basename(original_pathname.ToString());
}

Status CloudLogController::Apply(const Slice& in) {
  uint32_t operation;
  uint64_t offset_in_file;
  uint64_t file_size;
  Slice original_pathname;
  Slice payload;
  Status st;
  bool ret = ExtractLogRecord(in, &operation, &original_pathname,
                              &offset_in_file, &file_size, &payload);
  if (!ret) {
    return Status::IOError("Unable to parse payload from stream");
  }

  // Convert original pathname to a local file path.
  std::string pathname = GetCachePath(cache_dir_, original_pathname);

  // Apply operation on cache file.
  if (operation == kAppend) {
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[%s] Tailer: Appending %ld bytes to %s at offset %ld",
        GetTypeName().c_str(), payload.size(), pathname.c_str(),
        offset_in_file);

    auto iter = cache_fds_.find(pathname);

    // If this file is not yet open, open it and store it in cache.
    if (iter == cache_fds_.end()) {
      unique_ptr<RandomRWFile> result;
      st = env_->GetBaseEnv()->NewRandomRWFile(
          pathname, &result, EnvOptions());

      if (!st.ok()) {
          // create the file
          unique_ptr<WritableFile> tmp_writable_file;
          env_->GetBaseEnv()->NewWritableFile(pathname, &tmp_writable_file,
                                               EnvOptions());
          tmp_writable_file.reset();
          // Try again.
          st = env_->GetBaseEnv()->NewRandomRWFile(
                  pathname, &result, EnvOptions());
      }

      if (st.ok()) {
        cache_fds_[pathname] = std::move(result);
        Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
            "[%s] Tailer: Successfully opened file %s and cached",
            GetTypeName().c_str(), pathname.c_str());
      } else {
          return st;
      }
    }

    RandomRWFile* fd = cache_fds_[pathname].get();
    st = fd->Write(offset_in_file, payload);
    if (!st.ok()) {
      Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
          "[%s] Tailer: Error writing to cached file: %s", pathname.c_str(),
          GetTypeName().c_str(), st.ToString().c_str());
    }
  } else if (operation == kDelete) {
    // Delete file from cache directory.
    auto iter = cache_fds_.find(pathname);
    if (iter != cache_fds_.end()) {
      Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
          "[%s] Tailer: Delete file %s, but it is still open."
          " Closing it now..", GetTypeName().c_str(), pathname.c_str());
      RandomRWFile* fd = iter->second.get();
      fd->Close();
      cache_fds_.erase(iter);
    }

    st = env_->GetBaseEnv()->DeleteFile(pathname);
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[%s] Tailer: Deleted file: %s %s",
        GetTypeName().c_str(), pathname.c_str(), st.ToString().c_str());

    if (st.IsNotFound()) {
      st = Status::OK();
    }
  } else if (operation == kClosed) {
    auto iter = cache_fds_.find(pathname);
    if (iter != cache_fds_.end()) {
      RandomRWFile* fd = iter->second.get();
      st = fd->Close();
      cache_fds_.erase(iter);
    }
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[%s] Tailer: Closed file %s %s",
        GetTypeName().c_str(), pathname.c_str(), st.ToString().c_str());
  } else {
    st = Status::IOError("Unknown operation");
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[%s] Tailer: Unknown operation '%x': File %s %s",
        GetTypeName().c_str(), operation, pathname.c_str(),
        st.ToString().c_str());
  }

  return st;
}

void CloudLogController::SerializeLogRecordAppend(const Slice& filename,
    const Slice& data, uint64_t offset, std::string* out) {
  // write the operation type
  PutVarint32(out, kAppend);

  // write out the offset in file where the data needs to be written
  PutFixed64(out, offset);

  // write out the filename
  PutLengthPrefixedSlice(out, filename);

  // write out the data
  PutLengthPrefixedSlice(out, data);
}

void CloudLogController::SerializeLogRecordClosed(
    const Slice& filename, uint64_t file_size, std::string* out) {
  // write the operation type
  PutVarint32(out, kClosed);

  // write out the file size
  PutFixed64(out, file_size);

  // write out the filename
  PutLengthPrefixedSlice(out, filename);
}

void CloudLogController::SerializeLogRecordDelete(
    const std::string& filename, std::string* out) {
  // write the operation type
  PutVarint32(out, kDelete);

  // write out the filename
  PutLengthPrefixedSlice(out, filename);
}

bool CloudLogController::ExtractLogRecord(const Slice& input,
    uint32_t* operation, Slice* filename,
    uint64_t* offset_in_file,
    uint64_t* file_size, Slice* data) {

  Slice in = input;
  if (in.size() < 1) {
    return false;
  }

  // extract operation
  if (!GetVarint32(&in, operation)) {
    return false;
  }
  if (*operation == kAppend) {
    *file_size = 0;
    if (!GetFixed64(&in, offset_in_file) ||        // extract offset in file
        !GetLengthPrefixedSlice(&in, filename) ||  // extract filename
        !GetLengthPrefixedSlice(&in, data)) {      // extract file contents
      return false;
    }
  } else if (*operation == kDelete) {
    *file_size = 0;
    *offset_in_file = 0;
    if (!GetLengthPrefixedSlice(&in, filename)) {  // extract filename
      return false;
    }
  } else if (*operation == kClosed) {
    *offset_in_file = 0;
    if (!GetFixed64(&in, file_size) ||             // extract filesize
        !GetLengthPrefixedSlice(&in, filename)) {  // extract filename
      return false;
    }
  } else {
    return false;
  }
  return true;
}

}  // namespace

#endif /* USE_AWS */
