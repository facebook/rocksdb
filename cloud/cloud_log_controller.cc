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
#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/convenience.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/object_registry.h"
#include "util/coding.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
CloudLogWritableFile::CloudLogWritableFile(Env* env, CloudFileSystem* cloud_fs,
                                           const std::string& fname,
                                           const FileOptions& /*options*/)
    : env_(env), cloud_fs_(cloud_fs), fname_(fname) {}

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
    Log(InfoLogLevel::DEBUG_LEVEL, cloud_fs_->GetLogger(),
        "CloudLogController closing.  Stopping stream.");
    StopTailingStream();
  }
  if (env_ != nullptr) {
    Log(InfoLogLevel::DEBUG_LEVEL, cloud_fs_->GetLogger(),
        "CloudLogController closed.");
  }
}

Status CloudLogControllerImpl::PrepareOptions(const ConfigOptions& options) {
  env_ = options.env;
  assert(env_);
  cloud_fs_ = dynamic_cast<CloudFileSystem*>(env_->GetFileSystem().get());
  assert(cloud_fs_);
  // Create a random number for the cache directory.
  const std::string uid = trim(env_->GenerateUniqueId());

  // Temporary directory for cache.
  const std::string bucket_dir =
      kCacheDir + pathsep + cloud_fs_->GetSrcBucketName();
  cache_dir_ = bucket_dir + pathsep + uid;

  const auto& base = cloud_fs_->GetBaseFileSystem();
  // Create temporary directories.
  const IOOptions io_opts;
  IODebugContext* dbg = nullptr;
  status_ = base->CreateDirIfMissing(kCacheDir, io_opts, dbg);
  if (status_.ok()) {
    status_ = base->CreateDirIfMissing(bucket_dir, io_opts, dbg);
  }
  if (status_.ok()) {
    status_ = base->CreateDirIfMissing(cache_dir_, io_opts, dbg);
  }
  if (status_.ok()) {
    status_ = StartTailingStream(cloud_fs_->GetSrcBucketName());
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

IOStatus CloudLogControllerImpl::Apply(const Slice& in) {
  uint32_t operation;
  uint64_t offset_in_file;
  uint64_t file_size;
  Slice original_pathname;
  Slice payload;
  IOStatus st;
  bool ret = ExtractLogRecord(in, &operation, &original_pathname,
                              &offset_in_file, &file_size, &payload);
  if (!ret) {
    return IOStatus::IOError("Unable to parse payload from stream");
  }

  // Convert original pathname to a local file path.
  std::string pathname = GetCachePath(original_pathname);

  const FileOptions fo;
  const IOOptions io_opts;
  IODebugContext* dbg = nullptr;

  // Apply operation on cache file.
  if (operation == kAppend) {
    Log(InfoLogLevel::DEBUG_LEVEL, cloud_fs_->GetLogger(),
        "[%s] Tailer: Appending %ld bytes to %s at offset %" PRIu64, Name(),
        payload.size(), pathname.c_str(), offset_in_file);

    auto iter = cache_fds_.find(pathname);

    // If this file is not yet open, open it and store it in cache.
    if (iter == cache_fds_.end()) {
      std::unique_ptr<FSRandomRWFile> result;
      st = cloud_fs_->GetBaseFileSystem()->NewRandomRWFile(pathname, fo,
                                                           &result, dbg);

      if (!st.ok()) {
        // create the file
        std::unique_ptr<FSWritableFile> tmp_writable_file;
        cloud_fs_->GetBaseFileSystem()->NewWritableFile(
            pathname, fo, &tmp_writable_file, dbg);
        tmp_writable_file.reset();
        // Try again.
        st = cloud_fs_->GetBaseFileSystem()->NewRandomRWFile(pathname, fo,
                                                             &result, dbg);
      }

      if (st.ok()) {
        cache_fds_[pathname] = std::move(result);
        Log(InfoLogLevel::DEBUG_LEVEL, cloud_fs_->GetLogger(),
            "[%s] Tailer: Successfully opened file %s and cached", Name(),
            pathname.c_str());
      } else {
        return st;
      }
    }

    FSRandomRWFile* fd = cache_fds_[pathname].get();
    st = fd->Write(offset_in_file, payload, io_opts, dbg);
    if (!st.ok()) {
      Log(InfoLogLevel::DEBUG_LEVEL, cloud_fs_->GetLogger(),
          "[%s] Tailer: Error writing to cached file %s: %s", pathname.c_str(),
          Name(), st.ToString().c_str());
    }
  } else if (operation == kDelete) {
    // Delete file from cache directory.
    auto iter = cache_fds_.find(pathname);
    if (iter != cache_fds_.end()) {
      Log(InfoLogLevel::DEBUG_LEVEL, cloud_fs_->GetLogger(),
          "[%s] Tailer: Delete file %s, but it is still open."
          " Closing it now..",
          Name(), pathname.c_str());
      FSRandomRWFile* fd = iter->second.get();
      fd->Close(io_opts, dbg);
      cache_fds_.erase(iter);
    }

    st = cloud_fs_->GetBaseFileSystem()->DeleteFile(pathname, io_opts, dbg);
    Log(InfoLogLevel::DEBUG_LEVEL, cloud_fs_->GetLogger(),
        "[%s] Tailer: Deleted file: %s %s", Name(), pathname.c_str(),
        st.ToString().c_str());

    if (st.IsNotFound()) {
      st = IOStatus::OK();
    }
  } else if (operation == kClosed) {
    auto iter = cache_fds_.find(pathname);
    if (iter != cache_fds_.end()) {
      FSRandomRWFile* fd = iter->second.get();
      st = fd->Close(io_opts, dbg);
      cache_fds_.erase(iter);
    }
    Log(InfoLogLevel::DEBUG_LEVEL, cloud_fs_->GetLogger(),
        "[%s] Tailer: Closed file %s %s", Name(), pathname.c_str(),
        st.ToString().c_str());
  } else {
    st = IOStatus::IOError("Unknown operation");
    Log(InfoLogLevel::DEBUG_LEVEL, cloud_fs_->GetLogger(),
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

IOStatus CloudLogControllerImpl::StartTailingStream(const std::string& topic) {
  if (tid_) {
    return IOStatus::Busy("Tailer already started");
  }

  auto st = CreateStream(topic);
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
IOStatus CloudLogControllerImpl::Retry(RetryType func) {
  IOStatus stat;
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
      stat = IOStatus::TimedOut();
      break;
    }
  }
  return stat;
}

IOStatus CloudLogControllerImpl::GetFileModificationTime(
    const std::string& fname, uint64_t* time) {
  auto st = status_to_io_status(status());
  if (st.ok()) {
    // map  pathname to cache dir
    std::string pathname = GetCachePath(Slice(fname));
    Log(InfoLogLevel::DEBUG_LEVEL, cloud_fs_->GetLogger(),
        "[kinesis] GetFileModificationTime logfile %s %s", pathname.c_str(),
        "ok");

    auto lambda = [this, pathname = std::move(pathname), time]() {
      return cloud_fs_->GetBaseFileSystem()->GetFileModificationTime(
          pathname, IOOptions(), time, nullptr /*dbg*/);
    };
    st = Retry(lambda);
  }
  return st;
}

IOStatus CloudLogControllerImpl::NewSequentialFile(
    const std::string& fname, const FileOptions& file_opts,
    std::unique_ptr<FSSequentialFile>* result, IODebugContext* dbg) {
  auto st = status_to_io_status(status());
  if (st.ok()) {
    // map  pathname to cache dir
    std::string pathname = GetCachePath(Slice(fname));
    Log(InfoLogLevel::DEBUG_LEVEL, cloud_fs_->GetLogger(),
        "[%s] NewSequentialFile logfile %s %s", Name(), pathname.c_str(), "ok");

    auto lambda = [this, pathname = std::move(pathname), &result, &file_opts,
                   dbg]() {
      return cloud_fs_->GetBaseFileSystem()->NewSequentialFile(
          pathname, file_opts, result, dbg);
    };
    st = Retry(lambda);
  }
  return st;
}

IOStatus CloudLogControllerImpl::NewRandomAccessFile(
    const std::string& fname, const FileOptions& file_opts,
    std::unique_ptr<FSRandomAccessFile>* result, IODebugContext* dbg) {
  auto st = status_to_io_status(status());
  if (st.ok()) {
    // map  pathname to cache dir
    std::string pathname = GetCachePath(Slice(fname));
    Log(InfoLogLevel::DEBUG_LEVEL, cloud_fs_->GetLogger(),
        "[%s] NewRandomAccessFile logfile %s %s", Name(), pathname.c_str(),
        "ok");

    auto lambda = [this, pathname = std::move(pathname), &result, &file_opts,
                   dbg]() {
      return cloud_fs_->GetBaseFileSystem()->NewRandomAccessFile(
          pathname, file_opts, result, dbg);
    };
    st = Retry(lambda);
  }
  return st;
}

IOStatus CloudLogControllerImpl::FileExists(const std::string& fname) {
  auto st = status_to_io_status(status());
  if (st.ok()) {
    // map  pathname to cache dir
    std::string pathname = GetCachePath(Slice(fname));
    Log(InfoLogLevel::DEBUG_LEVEL, cloud_fs_->GetLogger(),
        "[%s] FileExists logfile %s %s", Name(), pathname.c_str(), "ok");

    auto lambda = [this, pathname = std::move(pathname)]() {
      return cloud_fs_->GetBaseFileSystem()->FileExists(pathname, IOOptions(),
                                                        nullptr /*dbg*/);
    };
    st = Retry(lambda);
  }
  return st;
}

IOStatus CloudLogControllerImpl::GetFileSize(const std::string& fname,
                                             uint64_t* size) {
  auto st = status_to_io_status(status());
  if (st.ok()) {
    // map  pathname to cache dir
    std::string pathname = GetCachePath(Slice(fname));
    Log(InfoLogLevel::DEBUG_LEVEL, cloud_fs_->GetLogger(),
        "[%s] GetFileSize logfile %s %s", Name(), pathname.c_str(), "ok");

    auto lambda = [this, pathname, size]() {
      return cloud_fs_->GetBaseFileSystem()->GetFileSize(pathname, IOOptions(),
                                                         size, nullptr /*dbg*/);
    };
    st = Retry(lambda);
  }
  return st;
}

}  // namespace ROCKSDB_NAMESPACE
