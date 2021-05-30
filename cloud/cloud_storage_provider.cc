//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//

#include "rocksdb/cloud/cloud_storage_provider.h"

#include <cinttypes>
#include <mutex>
#include <set>

#include "cloud/cloud_env_impl.h"
#include "cloud/cloud_storage_provider_impl.h"
#include "cloud/filename.h"
#include "file/filename.h"
#include "rocksdb/cloud/cloud_env_options.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "util/coding.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
#ifndef ROCKSDB_LITE
/******************** Readablefile ******************/
CloudStorageReadableFileImpl::CloudStorageReadableFileImpl(
    Logger* info_log, const std::string& bucket, const std::string& fname,
    uint64_t file_size)
    : info_log_(info_log),
      bucket_(bucket),
      fname_(fname),
      offset_(0),
      file_size_(file_size) {
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[%s] CloudReadableFile opening file %s", Name(), fname_.c_str());
}

// sequential access, read data at current offset in file
Status CloudStorageReadableFileImpl::Read(size_t n, Slice* result,
                                          char* scratch) {
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[%s] CloudReadableFile reading %s %ld", Name(), fname_.c_str(), n);
  Status s = Read(offset_, n, result, scratch);

  // If the read successfully returned some data, then update
  // offset_
  if (s.ok()) {
    offset_ += result->size();
  }
  return s;
}

// random access, read data from specified offset in file
Status CloudStorageReadableFileImpl::Read(uint64_t offset, size_t n,
                                          Slice* result, char* scratch) const {
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[%s] CloudReadableFile reading %s at offset %" PRIu64
      " size %" ROCKSDB_PRIszt,
      Name(), fname_.c_str(), offset, n);

  *result = Slice();

  if (offset >= file_size_) {
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
        "[%s] CloudReadableFile reading %s at offset %" PRIu64
        " filesize %" PRIu64 ". Nothing to do",
        Name(), fname_.c_str(), offset, file_size_);
    return Status::OK();
  }

  // trim size if needed
  if (offset + n > file_size_) {
    n = file_size_ - offset;
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
        "[%s] CloudReadableFile reading %s at offset %" PRIu64
        " trimmed size %ld",
        Name(), fname_.c_str(), offset, n);
  }
  uint64_t bytes_read;
  Status st = DoCloudRead(offset, n, scratch, &bytes_read);
  if (st.ok()) {
    *result = Slice(scratch, bytes_read);
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
        "[%s] CloudReadableFile file %s filesize %" PRIu64 " read %" PRIu64
        " bytes",
        Name(), fname_.c_str(), file_size_, bytes_read);
  }
  return st;
}

Status CloudStorageReadableFileImpl::Skip(uint64_t n) {
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[%s] CloudReadableFile file %s skip %" PRIu64, Name(), fname_.c_str(),
      n);
  // Update offset_ so that it does not go beyond filesize
  offset_ += n;
  if (offset_ > file_size_) {
    offset_ = file_size_;
  }
  return Status::OK();
}

/******************** Writablefile ******************/

CloudStorageWritableFileImpl::CloudStorageWritableFileImpl(
    CloudEnv* env, const std::string& local_fname, const std::string& bucket,
    const std::string& cloud_fname, const EnvOptions& options)
    : env_(env),
      fname_(local_fname),
      bucket_(bucket),
      cloud_fname_(cloud_fname) {
  auto fname_no_epoch = RemoveEpoch(fname_);
  // Is this a manifest file?
  is_manifest_ = IsManifestFile(fname_no_epoch);
  assert(IsSstFile(fname_no_epoch) || is_manifest_);

  Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
      "[%s] CloudWritableFile bucket %s opened local file %s "
      "cloud file %s manifest %d",
      Name(), bucket.c_str(), fname_.c_str(), cloud_fname.c_str(),
      is_manifest_);

  auto* file_to_open = &fname_;
  auto local_env = env_->GetBaseEnv();
  Status s;
  if (is_manifest_) {
    s = local_env->FileExists(fname_);
    if (!s.ok() && !s.IsNotFound()) {
      status_ = s;
      return;
    }
    if (s.ok()) {
      // Manifest exists. Instead of overwriting the MANIFEST (which could be
      // bad if we crash mid-write), write to the temporary file and do an
      // atomic rename on Sync() (Sync means we have a valid data in the
      // MANIFEST, so we can crash after it)
      tmp_file_ = fname_ + ".tmp";
      file_to_open = &tmp_file_;
    }
  }

  s = local_env->NewWritableFile(*file_to_open, &local_file_, options);
  if (!s.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, env_->GetLogger(),
        "[%s] CloudWritableFile src %s %s", Name(), fname_.c_str(),
        s.ToString().c_str());
    status_ = s;
  }
}

CloudStorageWritableFileImpl::~CloudStorageWritableFileImpl() {
  if (local_file_ != nullptr) {
    Close();
  }
}

Status CloudStorageWritableFileImpl::Close() {
  if (local_file_ == nullptr) {  // already closed
    return status_;
  }
  Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
      "[%s] CloudWritableFile closing %s", Name(), fname_.c_str());
  assert(status_.ok());

  // close local file
  Status st = local_file_->Close();
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, env_->GetLogger(),
        "[%s] CloudWritableFile closing error on local %s\n", Name(),
        fname_.c_str());
    return st;
  }
  local_file_.reset();

  if (!is_manifest_) {
    status_ = env_->CopyLocalFileToDest(fname_, cloud_fname_);
    if (!status_.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, env_->GetLogger(),
          "[%s] CloudWritableFile closing PutObject failed on local file %s",
          Name(), fname_.c_str());
      return status_;
    }

    // delete local file
    if (!env_->GetCloudEnvOptions().keep_local_sst_files) {
      status_ = env_->GetBaseEnv()->DeleteFile(fname_);
      if (!status_.ok()) {
        Log(InfoLogLevel::ERROR_LEVEL, env_->GetLogger(),
            "[%s] CloudWritableFile closing delete failed on local file %s",
            Name(), fname_.c_str());
        return status_;
      }
    }
    Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
        "[%s] CloudWritableFile closed file %s", Name(), fname_.c_str());
  }
  return Status::OK();
}

// Sync a file to stable storage
Status CloudStorageWritableFileImpl::Sync() {
  if (local_file_ == nullptr) {
    return status_;
  }
  assert(status_.ok());

  // sync local file
  Status stat = local_file_->Sync();

  if (stat.ok() && !tmp_file_.empty()) {
    assert(is_manifest_);
    // We are writing to the temporary file. On a first sync we need to rename
    // the file to the real filename.
    stat = env_->GetBaseEnv()->RenameFile(tmp_file_, fname_);
    // Note: this is not thread safe, but we know that manifest writes happen
    // from the same thread, so we are fine.
    tmp_file_.clear();
  }

  // We copy MANIFEST to cloud on every Sync()
  if (is_manifest_ && stat.ok()) {
    stat = env_->CopyLocalFileToDest(fname_, cloud_fname_);
    if (stat.ok()) {
      Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
          "[%s] CloudWritableFile made manifest %s durable to "
          "bucket %s bucketpath %s.",
          Name(), fname_.c_str(), bucket_.c_str(), cloud_fname_.c_str());
    } else {
      Log(InfoLogLevel::ERROR_LEVEL, env_->GetLogger(),
          "[%s] CloudWritableFile failed to make manifest %s durable to "
          "bucket %s bucketpath %s: %s",
          Name(), fname_.c_str(), bucket_.c_str(), cloud_fname_.c_str(),
          stat.ToString().c_str());
    }
  }
  return stat;
}

CloudStorageProvider::~CloudStorageProvider() {}

Status CloudStorageProvider::Prepare(CloudEnv* env) {
  Status st;
  if (env->HasDestBucket()) {
    // create dest bucket if specified
    if (ExistsBucket(env->GetDestBucketName()).ok()) {
      Log(InfoLogLevel::INFO_LEVEL, env->GetLogger(),
          "[%s] Bucket %s already exists", Name(),
          env->GetDestBucketName().c_str());
    } else if (env->GetCloudEnvOptions().create_bucket_if_missing) {
      Log(InfoLogLevel::INFO_LEVEL, env->GetLogger(),
          "[%s] Going to create bucket %s", Name(),
          env->GetDestBucketName().c_str());
      st = CreateBucket(env->GetDestBucketName());
    } else {
      Log(InfoLogLevel::INFO_LEVEL, env->GetLogger(),
          "[%s] Bucket not found %s", Name(), env->GetDestBucketName().c_str());
      st = Status::NotFound(
          "Bucket not found and create_bucket_if_missing is false");
    }
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, env->GetLogger(),
          "[%s] Unable to create bucket %s %s", Name(),
          env->GetDestBucketName().c_str(), st.ToString().c_str());
      return st;
    }
  }
  return st;
}

CloudStorageProviderImpl::CloudStorageProviderImpl() : rng_(time(nullptr)) {}

CloudStorageProviderImpl::~CloudStorageProviderImpl() {}

Status CloudStorageProviderImpl::Prepare(CloudEnv* env) {
  status_ = Initialize(env);
  if (status_.ok()) {
    status_ = CloudStorageProvider::Prepare(env);
  }
  if (status_.ok()) {
    env_ = env;
  }
  return status_;
}

Status CloudStorageProviderImpl::Initialize(CloudEnv* env) {
  env_ = env;
  return Status::OK();
}

Status CloudStorageProviderImpl::NewCloudReadableFile(
    const std::string& bucket, const std::string& fname,
    std::unique_ptr<CloudStorageReadableFile>* result,
    const EnvOptions& options) {
  CloudObjectInformation info;
  Status st = GetCloudObjectMetadata(bucket, fname, &info);

  if (!st.ok()) {
    return st;
  }
  return DoNewCloudReadableFile(bucket, fname, info.size, info.content_hash,
                                result, options);
}

Status CloudStorageProviderImpl::GetCloudObject(
    const std::string& bucket_name, const std::string& object_path,
    const std::string& local_destination) {
  Env* localenv = env_->GetBaseEnv();
  std::string tmp_destination =
      local_destination + ".tmp-" + std::to_string(rng_.Next());

  uint64_t remote_size;
  Status s =
      DoGetCloudObject(bucket_name, object_path, tmp_destination, &remote_size);
  if (!s.ok()) {
    localenv->DeleteFile(tmp_destination);
    return s;
  }

  // Check if our local file is the same as promised
  uint64_t local_size{0};
  s = localenv->GetFileSize(tmp_destination, &local_size);
  if (!s.ok()) {
    return s;
  }
  if (local_size != remote_size) {
    localenv->DeleteFile(tmp_destination);
    s = Status::IOError("Partial download of a file " + local_destination);
    Log(InfoLogLevel::ERROR_LEVEL, env_->GetLogger(),
        "[%s] GetCloudObject %s/%s local size %" PRIu64
        " != cloud size "
        "%" PRIu64 ". %s",
        Name(), bucket_name.c_str(), object_path.c_str(), local_size,
        remote_size, s.ToString().c_str());
  }

  if (s.ok()) {
    s = localenv->RenameFile(tmp_destination, local_destination);
  }
  Log(InfoLogLevel::INFO_LEVEL, env_->GetLogger(),
      "[%s] GetCloudObject %s/%s size %" PRIu64 ". %s", bucket_name.c_str(),
      Name(), object_path.c_str(), local_size, s.ToString().c_str());
  return s;
}

Status CloudStorageProviderImpl::PutCloudObject(
    const std::string& local_file, const std::string& bucket_name,
    const std::string& object_path) {
  uint64_t fsize = 0;
  // debugging paranoia. Files uploaded to Cloud can never be zero size.
  auto st = env_->GetBaseEnv()->GetFileSize(local_file, &fsize);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, env_->GetLogger(),
        "[%s] PutCloudObject localpath %s error getting size %s", Name(),
        local_file.c_str(), st.ToString().c_str());
    return st;
  }
  if (fsize == 0) {
    Log(InfoLogLevel::ERROR_LEVEL, env_->GetLogger(),
        "[%s] PutCloudObject localpath %s error zero size", Name(),
        local_file.c_str());
    return Status::IOError(local_file + " Zero size.");
  }

  return DoPutCloudObject(local_file, bucket_name, object_path, fsize);
}

#endif  // ROCKSDB_LITE
}  // namespace ROCKSDB_NAMESPACE
