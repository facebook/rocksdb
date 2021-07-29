//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
// This file defines an AWS-Kinesis environment for rocksdb.
// A log file maps to a stream in Kinesis.
//

#include "rocksdb/cloud/cloud_log_controller.h"

#include <cinttypes>
#include <fstream>
#include <iostream>

#include "cloud/cloud_log_controller_impl.h"
#include "cloud/filename.h"
#include "rocksdb/cloud/cloud_env_options.h"
#include "rocksdb/convenience.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/object_registry.h"
#include "util/coding.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
CloudLogWritableFile::CloudLogWritableFile(CloudEnv* env,
                                           const std::string& fname,
                                           const EnvOptions& /*options*/)
    : env_(env), fname_(fname) {}

CloudLogWritableFile::~CloudLogWritableFile() {}

const std::chrono::microseconds CloudLogControllerImpl::kRetryPeriod =
    std::chrono::seconds(30);

CloudLogController::~CloudLogController() {}

Status CloudLogController::CreateFromString(
    const ConfigOptions& /*config_options*/, const std::string& id,
    std::shared_ptr<CloudLogController>* controller) {
  if (id.empty()) {
    controller->reset();
    return Status::OK();
  } else {
    return ObjectRegistry::NewInstance()->NewSharedObject<CloudLogController>(id, controller);
  }
}

CloudLogControllerImpl::CloudLogControllerImpl() : running_(false) {}

CloudLogControllerImpl::~CloudLogControllerImpl() {
  if (running_) {
    // This is probably not a good situation as the derived class is partially
    // destroyed but the tailer might still be active.
    Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
        "CloudLogController closing.  Stopping stream.");
    StopTailingStream();
  }
  if (env_ != nullptr) {
    Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
        "CloudLogController closed.");
  }
}

Status CloudLogControllerImpl::PrepareOptions(const ConfigOptions& options) {
  env_ = static_cast<CloudEnv*>(options.env);
  // Create a random number for the cache directory.
  const std::string uid = trim(env_->GetBaseEnv()->GenerateUniqueId());

  // Temporary directory for cache.
  const std::string bucket_dir = kCacheDir + pathsep + env_->GetSrcBucketName();
  cache_dir_ = bucket_dir + pathsep + uid;

  Env* base = env_->GetBaseEnv();
  // Create temporary directories.
  status_ = base->CreateDirIfMissing(kCacheDir);
  if (status_.ok()) {
    status_ = base->CreateDirIfMissing(bucket_dir);
  }
  if (status_.ok()) {
    status_ = base->CreateDirIfMissing(cache_dir_);
  }
  if (status_.ok()) {
    status_ = StartTailingStream(env_->GetSrcBucketName());
  }
  if (status_.ok()) {
    status_ = CloudLogController::PrepareOptions(options);
  }
  return status_;
}

std::string CloudLogControllerImpl::GetCachePath(
    const Slice& original_pathname) const {
  const std::string& cache_dir = GetCacheDir();
  return cache_dir + pathsep + basename(original_pathname.ToString());
}

bool CloudLogControllerImpl::ExtractLogRecord(
    const Slice& input, uint32_t* operation, Slice* filename,
    uint64_t* offset_in_file, uint64_t* file_size, Slice* data) {
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

Status CloudLogControllerImpl::Apply(const Slice& in) {
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
  std::string pathname = GetCachePath(original_pathname);

  // Apply operation on cache file.
  if (operation == kAppend) {
    Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
        "[%s] Tailer: Appending %ld bytes to %s at offset %" PRIu64, Name(),
        payload.size(), pathname.c_str(), offset_in_file);

    auto iter = cache_fds_.find(pathname);

    // If this file is not yet open, open it and store it in cache.
    if (iter == cache_fds_.end()) {
      std::unique_ptr<RandomRWFile> result;
      st = env_->GetBaseEnv()->NewRandomRWFile(pathname, &result, EnvOptions());

      if (!st.ok()) {
        // create the file
        std::unique_ptr<WritableFile> tmp_writable_file;
        env_->GetBaseEnv()->NewWritableFile(pathname, &tmp_writable_file,
                                            EnvOptions());
        tmp_writable_file.reset();
        // Try again.
        st = env_->GetBaseEnv()->NewRandomRWFile(pathname, &result,
                                                 EnvOptions());
      }

      if (st.ok()) {
        cache_fds_[pathname] = std::move(result);
        Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
            "[%s] Tailer: Successfully opened file %s and cached", Name(),
            pathname.c_str());
      } else {
        return st;
      }
    }

    RandomRWFile* fd = cache_fds_[pathname].get();
    st = fd->Write(offset_in_file, payload);
    if (!st.ok()) {
      Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
          "[%s] Tailer: Error writing to cached file %s: %s", pathname.c_str(),
          Name(), st.ToString().c_str());
    }
  } else if (operation == kDelete) {
    // Delete file from cache directory.
    auto iter = cache_fds_.find(pathname);
    if (iter != cache_fds_.end()) {
      Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
          "[%s] Tailer: Delete file %s, but it is still open."
          " Closing it now..",
          Name(), pathname.c_str());
      RandomRWFile* fd = iter->second.get();
      fd->Close();
      cache_fds_.erase(iter);
    }

    st = env_->GetBaseEnv()->DeleteFile(pathname);
    Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
        "[%s] Tailer: Deleted file: %s %s", Name(), pathname.c_str(),
        st.ToString().c_str());

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
    Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
        "[%s] Tailer: Closed file %s %s", Name(), pathname.c_str(),
        st.ToString().c_str());
  } else {
    st = Status::IOError("Unknown operation");
    Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
        "[%s] Tailer: Unknown operation '%x': File %s %s", Name(), operation,
        pathname.c_str(), st.ToString().c_str());
  }

  return st;
}

void CloudLogControllerImpl::SerializeLogRecordAppend(const Slice& filename,
                                                      const Slice& data,
                                                      uint64_t offset,
                                                      std::string* out) {
  // write the operation type
  PutVarint32(out, kAppend);

  // write out the offset in file where the data needs to be written
  PutFixed64(out, offset);

  // write out the filename
  PutLengthPrefixedSlice(out, filename);

  // write out the data
  PutLengthPrefixedSlice(out, data);
}

void CloudLogControllerImpl::SerializeLogRecordClosed(const Slice& filename,
                                                      uint64_t file_size,
                                                      std::string* out) {
  // write the operation type
  PutVarint32(out, kClosed);

  // write out the file size
  PutFixed64(out, file_size);

  // write out the filename
  PutLengthPrefixedSlice(out, filename);
}

void CloudLogControllerImpl::SerializeLogRecordDelete(
    const std::string& filename, std::string* out) {
  // write the operation type
  PutVarint32(out, kDelete);

  // write out the filename
  PutLengthPrefixedSlice(out, filename);
}

Status CloudLogControllerImpl::StartTailingStream(const std::string& topic) {
  if (tid_) {
    return Status::Busy("Tailer already started");
  }

  Status st = CreateStream(topic);
  if (st.ok()) {
    running_ = true;
    // create tailer thread
    auto lambda = [this]() { TailStream(); };
    tid_.reset(new std::thread(lambda));
  }
  return st;
}

void CloudLogControllerImpl::StopTailingStream() {
  running_ = false;
  if (tid_ && tid_->joinable()) {
    tid_->join();
  }
  tid_.reset();
}
//
// Keep retrying the command until it is successful or the timeout has expired
//
Status CloudLogControllerImpl::Retry(RetryType func) {
  Status stat;
  std::chrono::microseconds start(env_->NowMicros());

  while (true) {
    // If command is successful, return immediately
    stat = func();
    if (stat.ok()) {
      break;
    }
    // sleep for some time
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // If timeout has expired, return error
    std::chrono::microseconds now(env_->NowMicros());
    if (start + CloudLogControllerImpl::kRetryPeriod < now) {
      stat = Status::TimedOut();
      break;
    }
  }
  return stat;
}

Status CloudLogControllerImpl::GetFileModificationTime(const std::string& fname,
                                                       uint64_t* time) {
  Status st = status();
  if (st.ok()) {
    // map  pathname to cache dir
    std::string pathname = GetCachePath(Slice(fname));
    Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
        "[kinesis] GetFileModificationTime logfile %s %s", pathname.c_str(),
        "ok");

    auto lambda = [this, pathname, time]() -> Status {
      return env_->GetBaseEnv()->GetFileModificationTime(pathname, time);
    };
    st = Retry(lambda);
  }
  return st;
}

Status CloudLogControllerImpl::NewSequentialFile(
    const std::string& fname, std::unique_ptr<SequentialFile>* result,
    const EnvOptions& options) {
  // read from Kinesis
  Status st = status();
  if (st.ok()) {
    // map  pathname to cache dir
    std::string pathname = GetCachePath(Slice(fname));
    Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
        "[%s] NewSequentialFile logfile %s %s", Name(), pathname.c_str(), "ok");

    auto lambda = [this, pathname, &result, options]() -> Status {
      return env_->GetBaseEnv()->NewSequentialFile(pathname, result, options);
    };
    st = Retry(lambda);
  }
  return st;
}

Status CloudLogControllerImpl::NewRandomAccessFile(
    const std::string& fname, std::unique_ptr<RandomAccessFile>* result,
    const EnvOptions& options) {
  Status st = status();
  if (st.ok()) {
    // map  pathname to cache dir
    std::string pathname = GetCachePath(Slice(fname));
    Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
        "[%s] NewRandomAccessFile logfile %s %s", Name(), pathname.c_str(),
        "ok");

    auto lambda = [this, pathname, &result, options]() -> Status {
      return env_->GetBaseEnv()->NewRandomAccessFile(pathname, result, options);
    };
    st = Retry(lambda);
  }
  return st;
}

Status CloudLogControllerImpl::FileExists(const std::string& fname) {
  Status st = status();
  if (st.ok()) {
    // map  pathname to cache dir
    std::string pathname = GetCachePath(Slice(fname));
    Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
        "[%s] FileExists logfile %s %s", Name(), pathname.c_str(), "ok");

    auto lambda = [this, pathname]() -> Status {
      return env_->GetBaseEnv()->FileExists(pathname);
    };
    st = Retry(lambda);
  }
  return st;
}

Status CloudLogControllerImpl::GetFileSize(const std::string& fname,
                                           uint64_t* size) {
  Status st = status();
  if (st.ok()) {
    // map  pathname to cache dir
    std::string pathname = GetCachePath(Slice(fname));
    Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
        "[%s] GetFileSize logfile %s %s", Name(), pathname.c_str(), "ok");

    auto lambda = [this, pathname, size]() -> Status {
      return env_->GetBaseEnv()->GetFileSize(pathname, size);
    };
    st = Retry(lambda);
  }
  return st;
}

}  // namespace ROCKSDB_NAMESPACE
