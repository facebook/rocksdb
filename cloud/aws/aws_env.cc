//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#include "cloud/aws/aws_env.h"
#include <unistd.h>
#include <fstream>
#include <iostream>
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"

#ifdef USE_AWS

#include "cloud/aws/aws_file.h"
#include "cloud/aws/aws_retry.h"
#include "cloud/db_cloud_impl.h"

namespace rocksdb {

class AwsS3ClientWrapper::Timer {
 public:
  Timer(CloudRequestCallback* callback, CloudRequestOpType type,
        uint64_t size = 0)
      : callback_(callback), type_(type), size_(size), start_(now()) {}
  ~Timer() {
    if (callback_) {
      (*callback_)(type_, size_, now() - start_, success_);
    }
  }
  void SetSize(uint64_t size) { size_ = size; }
  void SetSuccess(bool success) { success_ = success; }

 private:
  uint64_t now() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::system_clock::now() -
               std::chrono::system_clock::from_time_t(0))
        .count();
  }
  CloudRequestCallback* callback_;
  CloudRequestOpType type_;
  uint64_t size_;
  bool success_{false};
  uint64_t start_;
};

AwsS3ClientWrapper::AwsS3ClientWrapper(
    std::unique_ptr<Aws::S3::S3Client> client,
    std::shared_ptr<CloudRequestCallback> cloud_request_callback)
    : client_(std::move(client)),
      cloud_request_callback_(std::move(cloud_request_callback)) {}

Aws::S3::Model::ListObjectsOutcome AwsS3ClientWrapper::ListObjects(
    const Aws::S3::Model::ListObjectsRequest& request) {
  Timer t(cloud_request_callback_.get(), CloudRequestOpType::kListOp);
  auto outcome = client_->ListObjects(request);
  t.SetSuccess(outcome.IsSuccess());
  return outcome;
}

Aws::S3::Model::CreateBucketOutcome AwsS3ClientWrapper::CreateBucket(
    const Aws::S3::Model::CreateBucketRequest& request) {
  Timer t(cloud_request_callback_.get(), CloudRequestOpType::kCreateOp);
  return client_->CreateBucket(request);
}

Aws::S3::Model::HeadBucketOutcome AwsS3ClientWrapper::HeadBucket(
    const Aws::S3::Model::HeadBucketRequest& request) {
  return client_->HeadBucket(request);
}

Aws::S3::Model::DeleteObjectOutcome AwsS3ClientWrapper::DeleteObject(
    const Aws::S3::Model::DeleteObjectRequest& request) {
  Timer t(cloud_request_callback_.get(), CloudRequestOpType::kDeleteOp);
  auto outcome = client_->DeleteObject(request);
  t.SetSuccess(outcome.IsSuccess());
  return outcome;
}

Aws::S3::Model::CopyObjectOutcome AwsS3ClientWrapper::CopyObject(
    const Aws::S3::Model::CopyObjectRequest& request) {
  Timer t(cloud_request_callback_.get(), CloudRequestOpType::kCopyOp);
  auto outcome = client_->CopyObject(request);
  t.SetSuccess(outcome.IsSuccess());
  return outcome;
}

Aws::S3::Model::GetObjectOutcome AwsS3ClientWrapper::GetObject(
    const Aws::S3::Model::GetObjectRequest& request) {
  Timer t(cloud_request_callback_.get(), CloudRequestOpType::kReadOp);
  auto outcome = client_->GetObject(request);
  if (outcome.IsSuccess()) {
    t.SetSize(outcome.GetResult().GetContentLength());
    t.SetSuccess(true);
  }
  return outcome;
}

Aws::S3::Model::PutObjectOutcome AwsS3ClientWrapper::PutObject(
    const Aws::S3::Model::PutObjectRequest& request, uint64_t size_hint) {
  Timer t(cloud_request_callback_.get(), CloudRequestOpType::kWriteOp,
          size_hint);
  auto outcome = client_->PutObject(request);
  t.SetSuccess(outcome.IsSuccess());
  return outcome;
}

Aws::S3::Model::HeadObjectOutcome AwsS3ClientWrapper::HeadObject(
    const Aws::S3::Model::HeadObjectRequest& request) {
  Timer t(cloud_request_callback_.get(), CloudRequestOpType::kInfoOp);
  auto outcome = client_->HeadObject(request);
  t.SetSuccess(outcome.IsSuccess());
  return outcome;
}

//
// The AWS credentials are specified to the constructor via
// access_key_id and secret_key.
//
AwsEnv::AwsEnv(Env* underlying_env, const std::string& src_bucket_prefix,
               const std::string& src_object_prefix,
               const std::string& src_bucket_region,
               const std::string& dest_bucket_prefix,
               const std::string& dest_object_prefix,
               const std::string& dest_bucket_region,
               const CloudEnvOptions& _cloud_env_options,
               std::shared_ptr<Logger> info_log)
    : CloudEnvImpl(CloudType::kAws, underlying_env),
      info_log_(info_log),
      cloud_env_options(_cloud_env_options),
      src_bucket_prefix_(src_bucket_prefix),
      src_object_prefix_(src_object_prefix),
      src_bucket_region_(src_bucket_region),
      dest_bucket_prefix_(dest_bucket_prefix),
      dest_object_prefix_(dest_object_prefix),
      dest_bucket_region_(dest_bucket_region),
      running_(true),
      has_src_bucket_(false),
      has_dest_bucket_(false),
      dest_equal_src_(false) {
  src_bucket_prefix_ = trim(src_bucket_prefix_);
  src_object_prefix_ = trim(src_object_prefix_);
  src_bucket_region_ = trim(src_bucket_region_);
  dest_bucket_prefix_ = trim(dest_bucket_prefix_);
  dest_object_prefix_ = trim(dest_object_prefix_);
  dest_bucket_region_ = trim(dest_bucket_region_);

  unique_ptr<Aws::Auth::AWSCredentials> creds;
  if (!cloud_env_options.credentials.access_key_id.empty() &&
      !cloud_env_options.credentials.secret_key.empty()) {
    creds.reset(new Aws::Auth::AWSCredentials(
        Aws::String(cloud_env_options.credentials.access_key_id.c_str()),
        Aws::String(cloud_env_options.credentials.secret_key.c_str())));
  }

  Header(info_log_, "      AwsEnv.src_bucket_prefix: %s",
         src_bucket_prefix_.c_str());
  Header(info_log_, "      AwsEnv.src_object_prefix: %s",
         src_object_prefix_.c_str());
  Header(info_log_, "      AwsEnv.src_bucket_region: %s",
         src_bucket_region_.c_str());
  Header(info_log_, "     AwsEnv.dest_bucket_prefix: %s",
         dest_bucket_prefix_.c_str());
  Header(info_log_, "     AwsEnv.dest_object_prefix: %s",
         dest_object_prefix_.c_str());
  Header(info_log_, "     AwsEnv.dest_bucket_region: %s",
         dest_bucket_region_.c_str());
  Header(info_log_, "            AwsEnv.credentials: %s",
         creds ? "[given]" : "[not given]");

  base_env_ = underlying_env;
  Aws::InitAPI(Aws::SDKOptions());
  // create AWS S3 client with appropriate timeouts
  Aws::Client::ClientConfiguration config;
  config.connectTimeoutMs = 30000;
  config.requestTimeoutMs = 600000;

  // Setup how retries need to be done
  config.retryStrategy =
      std::make_shared<AwsRetryStrategy>(cloud_env_options, info_log_);
  if (cloud_env_options.request_timeout_ms != 0) {
    config.requestTimeoutMs = cloud_env_options.request_timeout_ms;
  }

  if (!GetSrcBucketPrefix().empty()) {
    has_src_bucket_ = true;
  }
  if (!GetDestBucketPrefix().empty()) {
    has_dest_bucket_ = true;
  }

  // Do we have two unique buckets?
  dest_equal_src_ = has_src_bucket_ && has_dest_bucket_ &&
                    GetSrcBucketPrefix() == GetDestBucketPrefix() &&
                    GetSrcObjectPrefix() == GetDestObjectPrefix();

  // TODO: support buckets being in different regions
  if (!dest_equal_src_ && has_src_bucket_ && has_dest_bucket_) {
    if (src_bucket_region_ == dest_bucket_region_) {
      // alls good
    } else {
      create_bucket_status_ =
          Status::InvalidArgument("Two different regions not supported");
      Log(InfoLogLevel::ERROR_LEVEL, info_log,
          "[aws] NewAwsEnv Buckets %s, %s in two different regions %s, %s "
          "is not supported",
          src_bucket_prefix_.c_str(), dest_bucket_prefix_.c_str(),
          src_bucket_region_.c_str(), dest_bucket_region_.c_str());
      return;
    }
  }

  // Use specified region if any
  if (src_bucket_region_.empty()) {
    config.region = Aws::String(default_region, strlen(default_region));
  } else {
    config.region =
        Aws::String(src_bucket_region_.c_str(), src_bucket_region_.size());
  }
  Header(info_log_, "AwsEnv connection to endpoint in region: %s",
         config.region.c_str());
  bucket_location_ = Aws::S3::Model::BucketLocationConstraintMapper::
      GetBucketLocationConstraintForName(config.region);

  {
    unique_ptr<Aws::S3::S3Client> s3client(
        creds ? new Aws::S3::S3Client(*creds, config)
              : new Aws::S3::S3Client(config));

    s3client_ = std::make_shared<AwsS3ClientWrapper>(
        std::move(s3client), cloud_env_options.cloud_request_callback);
  }

  // create dest bucket if specified
  if (has_dest_bucket_) {
    if (S3WritableFile::BucketExistsInS3(s3client_, GetDestBucketPrefix(),
                                         bucket_location_)
            .ok()) {
      Log(InfoLogLevel::INFO_LEVEL, info_log,
          "[aws] NewAwsEnv Bucket %s already exists",
          GetDestBucketPrefix().c_str());
    } else if (cloud_env_options.create_bucket_if_missing) {
      Log(InfoLogLevel::INFO_LEVEL, info_log,
          "[aws] NewAwsEnv Going to create bucket %s",
          GetDestBucketPrefix().c_str());
      create_bucket_status_ = S3WritableFile::CreateBucketInS3(
          s3client_, GetDestBucketPrefix(), bucket_location_);
    } else {
      create_bucket_status_ = Status::NotFound(
          "[aws] Bucket not found and create_bucket_if_missing is false");
    }
  }
  if (!create_bucket_status_.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log,
        "[aws] NewAwsEnv Unable to create bucket %s %s",
        GetDestBucketPrefix().c_str(),
        create_bucket_status_.ToString().c_str());
  }

  // create Kinesis client for storing/reading logs
  if (create_bucket_status_.ok() && !cloud_env_options.keep_local_log_files) {
    kinesis_client_ =
        creds ? std::make_shared<Aws::Kinesis::KinesisClient>(*creds, config)
              : std::make_shared<Aws::Kinesis::KinesisClient>(config);
    if (kinesis_client_ == nullptr) {
      create_bucket_status_ =
          Status::IOError("Error in creating Kinesis client");
    }

    // Create Kinesis stream and wait for it to be ready
    if (create_bucket_status_.ok()) {
      create_bucket_status_ = KinesisSystem::CreateStream(
          this, info_log_, kinesis_client_, GetSrcBucketPrefix());
      if (!create_bucket_status_.ok()) {
        Log(InfoLogLevel::ERROR_LEVEL, info_log,
            "[aws] NewAwsEnv Unable to  create stream %s",
            create_bucket_status_.ToString().c_str());
      }
    }
    // create tailer object
    if (create_bucket_status_.ok()) {
      create_bucket_status_ = CreateTailer();
    }
  }
  if (!create_bucket_status_.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log,
        "[aws] NewAwsEnv Unable to create environment %s",
        create_bucket_status_.ToString().c_str());
  }

  file_deletion_thread_ = std::thread([&]() {
    while (true) {
      std::unique_lock<std::mutex> lk(file_deletion_lock_);
      // wait until we're shutting down or there are some files to delete
      file_deletion_cv_.wait(lk, [&]() {
        return running_.load() == false || !files_to_delete_list_.empty();
      });
      if (running_.load() == false) {
        // we're shutting down
        break;
      }
      assert(!files_to_delete_list_.empty());
      assert(files_to_delete_list_.size() == files_to_delete_map_.size());
      bool pred = file_deletion_cv_.wait_until(
          lk, files_to_delete_list_.front().first,
          [&] { return running_.load() == false; });
      if (pred) {
        // i.e. running_ == false
        break;
      }
      // We need to recheck files_to_delete_list_ because it's possible that the
      // file was removed from the list.
      if (!files_to_delete_list_.empty() &&
          files_to_delete_list_.front().first <=
              std::chrono::steady_clock::now()) {
        auto deleting_file = std::move(files_to_delete_list_.front());
        files_to_delete_list_.pop_front();
        files_to_delete_map_.erase(deleting_file.second);
        auto fname =
            MakeTableFileName(GetDestObjectPrefix(), deleting_file.second);
        // we are ready to delete the file!
        auto st = DeletePathInS3(GetDestBucketPrefix(), fname);
        if (!st.ok() && !st.IsNotFound()) {
          Log(InfoLogLevel::ERROR_LEVEL, info_log_,
              "[s3] DeleteFile DeletePathInS3 file %s error %s", fname.c_str(),
              st.ToString().c_str());
        }
      }
    }
  });
}

AwsEnv::~AwsEnv() {
  {
    std::lock_guard<std::mutex> lk(file_deletion_lock_);
    running_ = false;
    file_deletion_cv_.notify_one();
  }
  file_deletion_thread_.join();

  StopPurger();
  if (tid_.joinable()) {
    tid_.join();
  }
}

Status AwsEnv::CreateTailer() {
  if (tailer_) {
    return Status::Busy("Tailer already started");
  }
  // create tailer object
  KinesisSystem* f = new KinesisSystem(this, info_log_);
  tailer_.reset(f);

  // create tailer thread
  if (f->status().ok()) {
    auto lambda = [this]() { tailer_->TailStream(); };
    tid_ = std::thread(lambda);
  }
  return f->status();
}

Status AwsEnv::status() { return create_bucket_status_; }

//
// Check if options are compatible with the S3 storage system
//
Status AwsEnv::CheckOption(const EnvOptions& options) {
  // Cannot mmap files that reside on AWS S3, unless the file is also local
  if (options.use_mmap_reads && !cloud_env_options.keep_local_sst_files) {
    std::string msg = "Mmap only if keep_local_sst_files is set";
    return Status::InvalidArgument(msg);
  }
  return Status::OK();
}

//
// find out whether this is an sst file or a log file.
//
void AwsEnv::GetFileType(const std::string& fname, bool* sstFile, bool* logFile,
                         bool* manifest, bool* identity) {
  *logFile = false;

  *sstFile = IsSstFile(fname);
  if (!*sstFile) {
    *logFile = IsLogFile(fname);
    if (manifest) {
      *manifest = IsManifestFile(fname);
    }
    if (identity) {
      *identity = IsIdentityFile(fname);
    }
  }
}

// Ability to read a file directly from cloud storage
Status AwsEnv::NewSequentialFileCloud(const std::string& bucket_prefix,
                                      const std::string& fname,
                                      unique_ptr<SequentialFile>* result,
                                      const EnvOptions& options) {
  assert(status().ok());
  unique_ptr<S3ReadableFile> file;
  Status st = NewS3ReadableFile(bucket_prefix, fname, &file);
  if (!st.ok()) {
    return st;
  }

  result->reset(dynamic_cast<SequentialFile*>(file.release()));
  return st;
}

// open a file for sequential reading
Status AwsEnv::NewSequentialFile(const std::string& fname,
                                 unique_ptr<SequentialFile>* result,
                                 const EnvOptions& options) {
  assert(status().ok());
  *result = nullptr;

  // Get file type
  bool logfile;
  bool sstfile;
  bool manifest;
  bool identity;
  GetFileType(fname, &sstfile, &logfile, &manifest, &identity);

  auto st = CheckOption(options);
  if (!st.ok()) {
    return st;
  }

  if (sstfile || manifest || identity) {
    // We read first from local storage and then from cloud storage.
    st = base_env_->NewSequentialFile(fname, result, options);

    if (!st.ok()) {
      if (cloud_env_options.keep_local_sst_files || !sstfile) {
        // copy the file to the local storage if keep_local_sst_files is true
        if (has_dest_bucket_) {
          st = GetObject(GetDestBucketPrefix(), destname(fname), fname);
        }
        if (!st.ok() && has_src_bucket_ && !dest_equal_src_) {
          st = GetObject(GetSrcBucketPrefix(), srcname(fname), fname);
        }
        if (st.ok()) {
          // we successfully copied the file, try opening it locally now
          st = base_env_->NewSequentialFile(fname, result, options);
        }
      } else {
        unique_ptr<S3ReadableFile> file;
        if (!st.ok() && has_dest_bucket_) {  // read from destination S3
          st = NewS3ReadableFile(GetDestBucketPrefix(), destname(fname), &file);
        }
        if (!st.ok() && has_src_bucket_) {  // read from src bucket
          st = NewS3ReadableFile(GetSrcBucketPrefix(), srcname(fname), &file);
        }
        if (st.ok()) {
          result->reset(dynamic_cast<SequentialFile*>(file.release()));
        }
      }
    }
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
        "[aws] NewSequentialFile file %s %s", fname.c_str(),
        st.ToString().c_str());
    return st;

  } else if (logfile && !cloud_env_options.keep_local_log_files) {
    // read from Kinesis
    st = tailer_->status();
    if (st.ok()) {
      // map  pathname to cache dir
      std::string pathname =
          KinesisSystem::GetCachePath(tailer_->GetCacheDir(), Slice(fname));
      Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
          "[Kinesis] NewSequentialFile logfile %s %s", pathname.c_str(), "ok");

      auto lambda = [this, pathname, &result, options]() -> Status {
        return base_env_->NewSequentialFile(pathname, result, options);
      };
      return KinesisSystem::Retry(this, lambda);
    }
  }

  // This is neither a sst file or a log file. Read from default env.
  return base_env_->NewSequentialFile(fname, result, options);
}

// open a file for random reading
Status AwsEnv::NewRandomAccessFile(const std::string& fname,
                                   unique_ptr<RandomAccessFile>* result,
                                   const EnvOptions& options) {
  assert(status().ok());
  *result = nullptr;

  // Get file type
  bool logfile;
  bool sstfile;
  bool manifest;
  bool identity;
  GetFileType(fname, &sstfile, &logfile, &manifest, &identity);

  // Validate options
  auto st = CheckOption(options);
  if (!st.ok()) {
    return st;
  }

  if (sstfile || manifest || identity) {
    // Read from local storage and then from cloud storage.
    st = base_env_->NewRandomAccessFile(fname, result, options);

    if (!st.ok() && !base_env_->FileExists(fname).IsNotFound()) {
      // if status is not OK, but file does exist locally, something is wrong
      return st;
    }

    if (cloud_env_options.keep_local_sst_files || !sstfile) {
      if (!st.ok()) {
        // copy the file to the local storage if keep_local_sst_files is true
        if (has_dest_bucket_) {
          st = GetObject(GetDestBucketPrefix(), destname(fname), fname);
        }
        if (!st.ok() && has_src_bucket_ && !dest_equal_src_) {
          st = GetObject(GetSrcBucketPrefix(), srcname(fname), fname);
        }
        if (st.ok()) {
          // we successfully copied the file, try opening it locally now
          st = base_env_->NewRandomAccessFile(fname, result, options);
        }
      }
      // If we are being paranoic, then we validate that our file size is
      // the same as in cloud storage.
      if (st.ok() && cloud_env_options.validate_filesize) {
        uint64_t remote_size = 0;
        uint64_t local_size = 0;
        Status stax = base_env_->GetFileSize(fname, &local_size);
        if (!stax.ok()) {
          return stax;
        }
        stax = Status::NotFound();
        if (has_dest_bucket_) {
          stax = HeadObject(GetDestBucketPrefix(), destname(fname), nullptr,
                            &remote_size, nullptr);
        }
        if (stax.IsNotFound() && has_src_bucket_) {
          stax = HeadObject(GetSrcBucketPrefix(), srcname(fname), nullptr,
                            &remote_size, nullptr);
        }
        if (!stax.ok() || remote_size != local_size) {
          std::string msg = "[aws] HeadObject src " + fname + " local size " +
                            std::to_string(local_size) + " cloud size " +
                            std::to_string(remote_size) + " " + stax.ToString();
          Log(InfoLogLevel::ERROR_LEVEL, info_log_, "%s", msg.c_str());
          return Status::IOError(msg);
        }
      }
    } else if (!st.ok()) {
      // Only execute this code path if keep_local_sst_files == false. If it's
      // true, we will never use S3ReadableFile to read; we copy the file
      // locally and read using base_env.
      unique_ptr<S3ReadableFile> file;
      if (!st.ok() && has_dest_bucket_) {
        st = NewS3ReadableFile(GetDestBucketPrefix(), destname(fname), &file);
      }
      if (!st.ok() && has_src_bucket_) {
        st = NewS3ReadableFile(GetSrcBucketPrefix(), srcname(fname), &file);
      }
      if (st.ok()) {
        result->reset(dynamic_cast<RandomAccessFile*>(file.release()));
      }
    }
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
        "[s3] NewRandomAccessFile file %s %s", fname.c_str(),
        st.ToString().c_str());
    return st;

  } else if (logfile && !cloud_env_options.keep_local_log_files) {
    // read from Kinesis
    st = tailer_->status();
    if (st.ok()) {
      // map  pathname to cache dir
      std::string pathname =
          KinesisSystem::GetCachePath(tailer_->GetCacheDir(), Slice(fname));
      Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
          "[kinesis] NewRandomAccessFile logfile %s %s", pathname.c_str(),
          "ok");

      auto lambda = [this, pathname, &result, options]() -> Status {
        return base_env_->NewRandomAccessFile(pathname, result, options);
      };
      return KinesisSystem::Retry(this, lambda);
    }
  }

  // This is neither a sst file or a log file. Read from default env.
  return base_env_->NewRandomAccessFile(fname, result, options);
}

// create a new file for writing
Status AwsEnv::NewWritableFile(const std::string& fname,
                               unique_ptr<WritableFile>* result,
                               const EnvOptions& options) {
  assert(status().ok());

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[aws] NewWritableFile src '%s'",
      fname.c_str());

  // Get file type
  bool logfile;
  bool sstfile;
  bool manifest;
  bool identity;
  GetFileType(fname, &sstfile, &logfile, &manifest, &identity);
  result->reset();
  Status s;

  if (has_dest_bucket_ && (sstfile || identity || manifest)) {
    std::string cloud_file;
    if (manifest) {
      cloud_file = destname(dirname(fname)) + "/MANIFEST";
    } else {
      cloud_file = destname(fname);
    }

    unique_ptr<S3WritableFile> f(
        new S3WritableFile(this, fname, GetDestBucketPrefix(), cloud_file,
                           options, cloud_env_options));
    s = f->status();
    if (!s.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[s3] NewWritableFile src %s %s", fname.c_str(),
          s.ToString().c_str());
      return s;
    }
    result->reset(dynamic_cast<WritableFile*>(f.release()));

  } else if (logfile && !cloud_env_options.keep_local_log_files) {
    unique_ptr<KinesisWritableFile> f(
        new KinesisWritableFile(this, fname, options));
    if (!f->status().ok()) {
      *result = nullptr;
      s = Status::IOError("[aws] NewWritableFile", fname.c_str());
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[kinesis] NewWritableFile src %s %s", fname.c_str(),
          s.ToString().c_str());
      return s;
    }
    result->reset(dynamic_cast<WritableFile*>(f.release()));
  } else {
    s = base_env_->NewWritableFile(fname, result, options);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[aws] NewWritableFile src %s %s",
      fname.c_str(), s.ToString().c_str());
  return s;
}

class S3Directory : public Directory {
 public:
  explicit S3Directory(AwsEnv* env, const std::string name)
      : env_(env), name_(name) {
    status_ = env_->GetPosixEnv()->NewDirectory(name, &posixDir);
  }

  ~S3Directory() {}

  virtual Status Fsync() {
    if (!status_.ok()) {
      return status_;
    }
    return posixDir->Fsync();
  }

  virtual Status status() { return status_; }

 private:
  AwsEnv* env_;
  std::string name_;
  Status status_;
  unique_ptr<Directory> posixDir;
};

//
//  Returns success only if the directory-bucket exists in the
//  AWS S3 service and the posixEnv local directory exists as well.
//
Status AwsEnv::NewDirectory(const std::string& name,
                            unique_ptr<Directory>* result) {
  assert(status().ok());
  result->reset(nullptr);

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[aws] NewDirectory name '%s'",
      name.c_str());

  // create new object.
  unique_ptr<S3Directory> d(new S3Directory(this, name));

  // Check if the path exists in local dir
  if (!d->status().ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[aws] NewDirectory name %s unable to create local dir", name.c_str());
    return d->status();
  }
  result->reset(d.release());
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[aws] NewDirectory name %s ok",
      name.c_str());
  return Status::OK();
}

//
// Check if the specified filename exists.
//
Status AwsEnv::FileExists(const std::string& fname) {
  assert(status().ok());
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[aws] FileExists path '%s' ",
      fname.c_str());
  Status st;

  // Get file type
  bool logfile;
  bool sstfile;
  bool manifest;
  bool identity;
  GetFileType(fname, &sstfile, &logfile, &manifest, &identity);

  if (sstfile || manifest || identity) {
    // We read first from local storage and then from cloud storage.
    st = base_env_->FileExists(fname);
    if (st.IsNotFound() && has_dest_bucket_) {
      st = ExistsObject(GetDestBucketPrefix(), destname(fname));
    }
    if (!st.ok() && has_src_bucket_) {
      st = ExistsObject(GetSrcBucketPrefix(), srcname(fname));
    }
  } else if (logfile && !cloud_env_options.keep_local_log_files) {
    // read from Kinesis
    st = tailer_->status();
    if (st.ok()) {
      // map  pathname to cache dir
      std::string pathname =
          KinesisSystem::GetCachePath(tailer_->GetCacheDir(), Slice(fname));
      Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
          "[kinesis] FileExists logfile %s %s", pathname.c_str(), "ok");

      auto lambda = [this, pathname]() -> Status {
        return base_env_->FileExists(pathname);
      };
      st = KinesisSystem::Retry(this, lambda);
    }
  } else {
    st = base_env_->FileExists(fname);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[aws] FileExists path '%s' %s",
      fname.c_str(), st.ToString().c_str());
  return st;
}

//
// Appends the names of all children of the specified path from S3
// into the result set.
//
Status AwsEnv::GetChildrenFromS3(const std::string& path,
                                 const std::string& bucket_prefix,
                                 std::vector<std::string>* result) {
  assert(status().ok());
  // The bucket name
  Aws::String bucket = GetBucket(bucket_prefix);

  // S3 paths don't start with '/'
  auto prefix = ltrim_if(path, '/');
  // the starting object marker
  Aws::String marker;
  bool loop = true;

  // get info of bucket+object
  while (loop) {
    Aws::S3::Model::ListObjectsRequest request;
    request.SetBucket(bucket);
    request.SetMaxKeys(50);
    request.SetPrefix(Aws::String(prefix.c_str(), prefix.size()));
    request.SetMarker(marker);

    Aws::S3::Model::ListObjectsOutcome outcome =
        s3client_->ListObjects(request);
    bool isSuccess = outcome.IsSuccess();
    if (!isSuccess) {
      const Aws::Client::AWSError<Aws::S3::S3Errors>& error =
          outcome.GetError();
      std::string errmsg(error.GetMessage().c_str());
      Aws::S3::S3Errors s3err = error.GetErrorType();
      if (s3err == Aws::S3::S3Errors::NO_SUCH_BUCKET ||
          s3err == Aws::S3::S3Errors::NO_SUCH_KEY ||
          s3err == Aws::S3::S3Errors::RESOURCE_NOT_FOUND) {
        Log(InfoLogLevel::ERROR_LEVEL, info_log_,
            "[s3] GetChildren dir %s does not exist", path.c_str(),
            errmsg.c_str());
        return Status::NotFound(path, errmsg.c_str());
      }
      return Status::IOError(path, errmsg.c_str());
    }
    const Aws::S3::Model::ListObjectsResult& res = outcome.GetResult();
    const Aws::Vector<Aws::S3::Model::Object>& objs = res.GetContents();
    for (auto o : objs) {
      const Aws::String& key = o.GetKey();
      // Our path should be a prefix of the fetched value
      std::string keystr(key.c_str(), key.size());
      assert(keystr.find(prefix) == 0);
      if (keystr.find(prefix) != 0) {
        return Status::IOError("Unexpected result from AWS S3: " + keystr);
      }
      auto fname = ltrim_if(keystr.substr(prefix.size()), '/');
      result->push_back(fname);
    }

    // If there are no more entries, then we are done.
    if (!res.GetIsTruncated()) {
      break;
    }
    // The new starting point
    marker = res.GetNextMarker();
    if (marker.empty()) {
      // If response does not include the NextMaker and it is
      // truncated, you can use the value of the last Key in the response
      // as the marker in the subsequent request because all objects
      // are returned in alphabetical order
      marker = objs.back().GetKey();
    }
  }
  return Status::OK();
}

Status AwsEnv::HeadObject(const std::string& bucket_prefix,
                          const std::string& path,
                          Aws::Map<Aws::String, Aws::String>* metadata,
                          uint64_t* size, uint64_t* modtime) {
  Aws::S3::Model::HeadObjectRequest request;
  request.SetBucket(GetBucket(bucket_prefix));
  request.SetKey(Aws::String(path.data(), path.size()));

  auto outcome = s3client_->HeadObject(request);
  bool isSuccess = outcome.IsSuccess();
  if (!isSuccess) {
    const auto& error = outcome.GetError();
    Aws::S3::S3Errors s3err = error.GetErrorType();
    auto errMessage = error.GetMessage();
    if (s3err == Aws::S3::S3Errors::NO_SUCH_BUCKET ||
        s3err == Aws::S3::S3Errors::NO_SUCH_KEY ||
        s3err == Aws::S3::S3Errors::RESOURCE_NOT_FOUND) {
      return Status::NotFound(path, errMessage.c_str());
    }
    return Status::IOError(path, errMessage.c_str());
  }
  auto& res = outcome.GetResult();
  if (metadata != nullptr) {
    *metadata = res.GetMetadata();
  }
  if (size != nullptr) {
    *size = res.GetContentLength();
  }
  if (modtime != nullptr) {
    *modtime = res.GetLastModified().Millis();
  }
  return Status::OK();
}

Status AwsEnv::NewS3ReadableFile(const std::string& bucket_prefix,
                                 const std::string& fname,
                                 unique_ptr<S3ReadableFile>* result) {
  // First, check if the file exists and also find its size. We use size in
  // S3ReadableFile to make sure we always read the valid ranges of the file
  uint64_t size;
  Status st = HeadObject(bucket_prefix, fname, nullptr, &size, nullptr);
  if (!st.ok()) {
    return st;
  }
  result->reset(new S3ReadableFile(this, bucket_prefix, fname, size));
  return Status::OK();
}

//
// Deletes all the objects in our bucket.
//
Status AwsEnv::EmptyBucket(const std::string& bucket_prefix) {
  std::vector<std::string> results;
  Aws::String bucket = GetBucket(bucket_prefix);

  // Get all the objects in the  bucket
  Status st = GetChildrenFromS3("", bucket_prefix, &results);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[s3] EmptyBucket unable to find objects in bucket %s %s",
        bucket.c_str(), st.ToString().c_str());
    return st;
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[s3] EmptyBucket going to delete %d objects in bucket %s",
      results.size(), bucket.c_str());

  // Delete all objects from bucket
  for (auto path : results) {
    st = DeletePathInS3(bucket_prefix, path);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[s3] EmptyBucket Unable to delete %s in bucket %s %s", path.c_str(),
          bucket.c_str(), st.ToString().c_str());
    }
  }
  return st;
}

Status AwsEnv::GetChildren(const std::string& path,
                           std::vector<std::string>* result) {
  assert(status().ok());
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[s3] GetChildren path '%s' ",
      path.c_str());
  result->clear();

  // Fetch the list of children from both buckets in S3
  Status st;
  if (has_src_bucket_) {
    st = GetChildrenFromS3(src_object_prefix_, GetSrcBucketPrefix(), result);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[s3] GetChildren src bucket %s %s error from S3 %s",
          GetSrcBucketPrefix().c_str(), path.c_str(), st.ToString().c_str());
      return st;
    }
  }
  if (has_dest_bucket_ && !dest_equal_src_) {
    st = GetChildrenFromS3(dest_object_prefix_, GetDestBucketPrefix(), result);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[s3] GetChildren dest bucket %s %s error from S3 %s",
          GetDestBucketPrefix().c_str(), path.c_str(), st.ToString().c_str());
      return st;
    }
  }

  // fetch all files that exist in the local posix directory
  std::vector<std::string> local_files;
  st = base_env_->GetChildren(path, &local_files);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[s3] GetChildren %s error on local dir", path.c_str());
    return st;
  }

  for (auto const& value : local_files) {
    result->push_back(value);
  }
  // remove duplicates
  std::sort(result->begin(), result->end());
  result->erase(std::unique(result->begin(), result->end()), result->end());
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[s3] GetChildren %s successfully returned %d files", path.c_str(),
      result->size());
  return Status::OK();
}

void AwsEnv::RemoveFileFromDeletionQueue(uint64_t fileNumber) {
  std::lock_guard<std::mutex> lk(file_deletion_lock_);
  auto pos = files_to_delete_map_.find(fileNumber);
  if (pos != files_to_delete_map_.end()) {
    files_to_delete_list_.erase(pos->second);
    files_to_delete_map_.erase(pos);
  }
}

Status AwsEnv::DeleteFile(const std::string& fname) {
  assert(status().ok());
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[s3] DeleteFile src %s",
      fname.c_str());
  Status st;

  // Get file type
  bool logfile;
  bool sstfile;
  bool manifest;
  bool identity;
  GetFileType(fname, &sstfile, &logfile, &manifest, &identity);

  // Delete from destination bucket and local dir
  if (has_dest_bucket_ && (sstfile || manifest || identity)) {
    if (sstfile) {
      uint64_t fileNumber;
      FileType type;
      WalFileType walType;
      bool ok __attribute__((unused)) =
          ParseFileName(basename(fname), &fileNumber, &type, &walType);
      assert(ok && type == kTableFile);
      // add the remote file deletion to the queue
      std::unique_lock<std::mutex> lk(file_deletion_lock_);
      if (files_to_delete_map_.find(fileNumber) == files_to_delete_map_.end()) {
        files_to_delete_list_.emplace_back(
            std::chrono::steady_clock::now() + file_deletion_delay_,
            fileNumber);
        auto itr = files_to_delete_list_.end();
        --itr;
        files_to_delete_map_.emplace(fileNumber, std::move(itr));
        file_deletion_cv_.notify_one();
      }
    }
    // delete from local
    st = base_env_->DeleteFile(fname);
  } else if (logfile && !cloud_env_options.keep_local_log_files) {
    // read from Kinesis
    st = tailer_->status();
    if (st.ok()) {
      // Log a Delete record to kinesis stream
      unique_ptr<KinesisWritableFile> f(
          new KinesisWritableFile(this, fname, EnvOptions()));
      if (!f->status().ok()) {
        st = Status::IOError("[Kinesis] DeleteFile", fname.c_str());
      } else {
        st = f->LogDelete();
      }
    }
  } else {
    st = base_env_->DeleteFile(fname);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[s3] DeleteFile file %s %s",
      fname.c_str(), st.ToString().c_str());
  return st;
};

//
// Delete the specified path from S3
//
Status AwsEnv::DeletePathInS3(const std::string& bucket_prefix,
                              const std::string& fname) {
  assert(status().ok());
  Status st;
  Aws::String bucket = GetBucket(bucket_prefix);

  // The filename is the same as the object name in the bucket
  Aws::String object = Aws::String(fname.c_str(), fname.size());

  // create request
  Aws::S3::Model::DeleteObjectRequest request;
  request.SetBucket(bucket);
  request.SetKey(object);

  Aws::S3::Model::DeleteObjectOutcome outcome =
      s3client_->DeleteObject(request);
  bool isSuccess = outcome.IsSuccess();
  if (!isSuccess) {
    const Aws::Client::AWSError<Aws::S3::S3Errors>& error = outcome.GetError();
    std::string errmsg(error.GetMessage().c_str());
    Aws::S3::S3Errors s3err = error.GetErrorType();
    if (s3err == Aws::S3::S3Errors::NO_SUCH_BUCKET ||
        s3err == Aws::S3::S3Errors::NO_SUCH_KEY ||
        s3err == Aws::S3::S3Errors::RESOURCE_NOT_FOUND) {
      st = Status::NotFound(fname, errmsg.c_str());
    } else {
      st = Status::IOError(fname, errmsg.c_str());
    }
  }

  Log(InfoLogLevel::INFO_LEVEL, info_log_, "[s3] DeleteFromS3 %s/%s, status %s",
      bucket.c_str(), object.c_str(), st.ToString().c_str());

  return st;
}

// S3 has no concepts of directories, so we just have to forward the request to
// base_env_
Status AwsEnv::CreateDir(const std::string& dirname) {
  assert(status().ok());
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[s3] CreateDir dir '%s'",
      dirname.c_str());
  Status st;

  // create local dir
  st = base_env_->CreateDir(dirname);

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[s3] CreateDir dir %s %s",
      dirname.c_str(), st.ToString().c_str());
  return st;
};

// S3 has no concepts of directories, so we just have to forward the request to
// base_env_
Status AwsEnv::CreateDirIfMissing(const std::string& dirname) {
  assert(status().ok());
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[s3] CreateDirIfMissing dir '%s'",
      dirname.c_str());
  Status st;

  // create directory in base_env_
  st = base_env_->CreateDirIfMissing(dirname);

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[s3] CreateDirIfMissing created dir %s %s", dirname.c_str(),
      st.ToString().c_str());
  return st;
};

// S3 has no concepts of directories, so we just have to forward the request to
// base_env_
Status AwsEnv::DeleteDir(const std::string& dirname) {
  assert(status().ok());
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[s3] DeleteDir src '%s'",
      dirname.c_str());
  Status st = base_env_->DeleteDir(dirname);
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[s3] DeleteDir dir %s %s",
      dirname.c_str(), st.ToString().c_str());
  return st;
};

Status AwsEnv::GetFileSize(const std::string& fname, uint64_t* size) {
  assert(status().ok());
  *size = 0L;
  Status st;

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[aws] GetFileSize src '%s'",
      fname.c_str());

  // Get file type
  bool logfile;
  bool sstfile;
  GetFileType(fname, &sstfile, &logfile);

  if (sstfile) {
    if (base_env_->FileExists(fname).ok()) {
      st = base_env_->GetFileSize(fname, size);
    } else {
      st = Status::NotFound();
      // Get file length from S3
      if (has_dest_bucket_) {
        st = HeadObject(GetDestBucketPrefix(), destname(fname), nullptr, size,
                        nullptr);
      }
      if (st.IsNotFound() && has_src_bucket_) {
        st = HeadObject(GetSrcBucketPrefix(), srcname(fname), nullptr, size,
                        nullptr);
      }
    }
  } else if (logfile && !cloud_env_options.keep_local_log_files) {
    st = tailer_->status();
    if (st.ok()) {
      // map  pathname to cache dir
      std::string pathname =
          KinesisSystem::GetCachePath(tailer_->GetCacheDir(), Slice(fname));
      Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
          "[kinesis] GetFileSize logfile %s %s", pathname.c_str(), "ok");

      auto lambda = [this, pathname, size]() -> Status {
        return base_env_->GetFileSize(pathname, size);
      };
      st = KinesisSystem::Retry(this, lambda);
    }
  } else {
    st = base_env_->GetFileSize(fname, size);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[aws] GetFileSize src '%s' %s %ld",
      fname.c_str(), st.ToString().c_str(), *size);
  return st;
}

Status AwsEnv::GetFileModificationTime(const std::string& fname,
                                       uint64_t* time) {
  assert(status().ok());
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[aws] GetFileModificationTime src '%s'", fname.c_str());
  Status st;

  // Get file type
  bool logfile;
  bool sstfile;
  GetFileType(fname, &sstfile, &logfile);

  if (sstfile) {
    if (base_env_->FileExists(fname).ok()) {
      st = base_env_->GetFileModificationTime(fname, time);
    } else {
      st = Status::NotFound();
      if (has_dest_bucket_) {
        st = HeadObject(GetDestBucketPrefix(), destname(fname), nullptr,
                        nullptr, time);
      }
      if (st.IsNotFound() && has_src_bucket_) {
        st = HeadObject(GetSrcBucketPrefix(), srcname(fname), nullptr, nullptr,
                        time);
      }
    }
  } else if (logfile && !cloud_env_options.keep_local_log_files) {
    st = tailer_->status();
    if (st.ok()) {
      // map  pathname to cache dir
      std::string pathname =
          KinesisSystem::GetCachePath(tailer_->GetCacheDir(), Slice(fname));
      Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
          "[kinesis] GetFileModificationTime logfile %s %s", pathname.c_str(),
          "ok");

      auto lambda = [this, pathname, time]() -> Status {
        return base_env_->GetFileModificationTime(pathname, time);
      };
      st = KinesisSystem::Retry(this, lambda);
    }
  } else {
    st = base_env_->GetFileModificationTime(fname, time);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[aws] GetFileModificationTime src '%s' %s", fname.c_str(),
      st.ToString().c_str());
  return st;
}

// The rename is not atomic. S3 does not support renaming natively.
// Copy file to a new object in S3 and then delete original object.
Status AwsEnv::RenameFile(const std::string& src, const std::string& target) {
  assert(status().ok());
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[aws] RenameFile src '%s' target '%s'", src.c_str(), target.c_str());

  // Get file type of target
  bool logfile;
  bool sstfile;
  bool manifestfile;
  bool idfile;
  GetFileType(target, &sstfile, &logfile, &manifestfile, &idfile);

  // Rename should never be called on sst files.
  if (sstfile) {
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
        "[aws] RenameFile source sstfile %s %s is not supported", src.c_str(),
        target.c_str());
    assert(0);
    return Status::NotSupported(Slice(src), Slice(target));

  } else if (logfile) {
    // Rename should never be called on log files as well
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[aws] RenameFile source logfile %s %s is not supported", src.c_str(),
        target.c_str());
    assert(0);
    return Status::NotSupported(Slice(src), Slice(target));

  } else if (manifestfile) {
    // Rename should never be called on manifest files as well
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[aws] RenameFile source manifest %s %s is not supported", src.c_str(),
        target.c_str());
    assert(0);
    return Status::NotSupported(Slice(src), Slice(target));

  } else if (!idfile || !has_dest_bucket_) {
    return base_env_->RenameFile(src, target);
  }
  // Only ID file should come here
  assert(idfile);
  assert(has_dest_bucket_);
  assert(basename(target) == "IDENTITY");

  // Save Identity to S3
  Status st = SaveIdentitytoS3(src, destname(target));

  // Do the rename on local filesystem too
  if (st.ok()) {
    st = base_env_->RenameFile(src, target);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[s3] RenameFile src %s target %s: %s", src.c_str(), target.c_str(),
      st.ToString().c_str());
  return st;
}

//
// Copy my IDENTITY file to cloud storage. Update dbid registry.
//
Status AwsEnv::SaveIdentitytoS3(const std::string& localfile,
                                const std::string& idfile) {
  assert(basename(idfile) == "IDENTITY");

  // Read id into string
  std::string dbid;
  Status st = ReadFileToString(base_env_, localfile, &dbid);
  dbid = trim(dbid);

  // Upload ID file to  S3
  if (st.ok()) {
    st = PutObject(localfile, GetDestBucketPrefix(), idfile);
  }

  // Save mapping from ID to cloud pathname
  if (st.ok() && !GetDestObjectPrefix().empty()) {
    st = SaveDbid(dbid, GetDestObjectPrefix());
  }
  return st;
}

//
// All db in a bucket are stored in path /.rockset/dbid/<dbid>
// The value of the object is the pathname where the db resides.
//
Status AwsEnv::SaveDbid(const std::string& dbid, const std::string& dirname) {
  assert(status().ok());
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[s3] SaveDbid dbid %s dir '%s'",
      dbid.c_str(), dirname.c_str());

  std::string dbidkey = dbid_registry_ + dbid;
  Aws::String bucket = GetBucket(GetDestBucketPrefix());
  Aws::String key = Aws::String(dbidkey.c_str(), dbidkey.size());

  std::string dirname_tag = "dirname";
  Aws::String dir = Aws::String(dirname_tag.c_str(), dirname_tag.size());

  Aws::Map<Aws::String, Aws::String> metadata;
  metadata[dir] = Aws::String(dirname.c_str(), dirname.size());

  // create request
  Aws::S3::Model::PutObjectRequest put_request;
  put_request.SetBucket(bucket);
  put_request.SetKey(key);
  put_request.SetMetadata(metadata);
  SetEncryptionParameters(put_request);

  Aws::S3::Model::PutObjectOutcome put_outcome =
      s3client_->PutObject(put_request);
  bool isSuccess = put_outcome.IsSuccess();
  if (!isSuccess) {
    const Aws::Client::AWSError<Aws::S3::S3Errors>& error =
        put_outcome.GetError();
    std::string errmsg(error.GetMessage().c_str(), error.GetMessage().size());
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[s3] Bucket %s SaveDbid error in saving dbid %s dirname %s %s",
        bucket.c_str(), dbid.c_str(), dirname.c_str(), errmsg.c_str());
    return Status::IOError(dirname, errmsg.c_str());
  }
  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "[s3] Bucket %s SaveDbid dbid %s dirname %s %s", bucket.c_str(),
      dbid.c_str(), dirname.c_str(), "ok");
  return Status::OK();
};

//
// Given a dbid, retrieves its pathname.
//
Status AwsEnv::GetPathForDbid(const std::string& bucket_prefix,
                              const std::string& dbid, std::string* dirname) {
  std::string dbidkey = dbid_registry_ + dbid;

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[s3] Bucket %s GetPathForDbid dbid %s", bucket_prefix.c_str(),
      dbid.c_str());

  Aws::Map<Aws::String, Aws::String> metadata;
  Status st = HeadObject(bucket_prefix, dbidkey, &metadata);
  if (!st.ok()) {
    if (st.IsNotFound()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[s3] %s GetPathForDbid error non-existent dbid %s %s",
          bucket_prefix.c_str(), dbid.c_str(), st.ToString().c_str());
    } else {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[s3] %s GetPathForDbid error dbid %s %s", bucket_prefix.c_str(),
          dbid.c_str(), st.ToString().c_str());
    }
    return st;
  }

  // Find "dirname" metadata that stores the pathname of the db
  const char* kDirnameTag = "dirname";
  auto it = metadata.find(Aws::String(kDirnameTag));
  if (it != metadata.end()) {
    Aws::String as = it->second;
    dirname->assign(as.c_str(), as.size());
  } else {
    st = Status::NotFound("GetPathForDbid");
  }
  Log(InfoLogLevel::INFO_LEVEL, info_log_, "[s3] %s GetPathForDbid dbid %s %s",
      bucket_prefix.c_str(), dbid.c_str(), st.ToString().c_str());
  return st;
}

//
// Retrieves the list of all registered dbids and their paths
//
Status AwsEnv::GetDbidList(const std::string& bucket_prefix, DbidList* dblist) {
  Aws::String bucket = GetBucket(bucket_prefix);

  // fetch the list all all dbids
  std::vector<std::string> dbid_list;
  Status st = GetChildrenFromS3(dbid_registry_, bucket_prefix, &dbid_list);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[s3] %s GetDbidList error in GetChildrenFromS3 %s", bucket.c_str(),
        st.ToString().c_str());
    return st;
  }
  // for each dbid, fetch the db directory where the db data should reside
  for (auto dbid : dbid_list) {
    std::string dirname;
    st = GetPathForDbid(bucket_prefix, dbid, &dirname);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[s3] %s GetDbidList error in GetPathForDbid(%s) %s", bucket.c_str(),
          dbid.c_str(), st.ToString().c_str());
      return st;
    }
    // insert item into result set
    (*dblist)[dbid] = dirname;
  }
  return st;
}

//
// Deletes the specified dbid from the registry
//
Status AwsEnv::DeleteDbid(const std::string& bucket_prefix,
                          const std::string& dbid) {
  Aws::String bucket = GetBucket(bucket_prefix);

  // fetch the list all all dbids
  std::string dbidkey = dbid_registry_ + dbid;
  Status st = DeletePathInS3(bucket_prefix, dbidkey);
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[s3] %s DeleteDbid DeleteDbid(%s) %s", bucket.c_str(), dbid.c_str(),
      st.ToString().c_str());
  return st;
}

// Returns a list of all objects that start with the specified
// prefix and are stored in the bucket.
Status AwsEnv::ListObjects(const std::string& bucket_name_prefix,
                           const std::string& bucket_object_prefix,
                           BucketObjectMetadata* meta) {
  return GetChildrenFromS3(bucket_object_prefix, bucket_name_prefix,
                           &meta->pathnames);
}

// Deletes the specified object from cloud storage
Status AwsEnv::DeleteObject(const std::string& bucket_name_prefix,
                            const std::string& bucket_object_path) {
  return DeletePathInS3(bucket_name_prefix, bucket_object_path);
}

// Delete the specified object from the specified cloud bucket
Status AwsEnv::ExistsObject(const std::string& bucket_name_prefix,
                            const std::string& bucket_object_path) {
  return HeadObject(bucket_name_prefix, bucket_object_path);
}

// Return size of cloud object
Status AwsEnv::GetObjectSize(const std::string& bucket_name_prefix,
                             const std::string& bucket_object_path,
                             uint64_t* filesize) {
  return HeadObject(bucket_name_prefix, bucket_object_path, nullptr, filesize,
                    nullptr);
}

// Copy the specified cloud object from one location in the cloud
// storage to another location in cloud storage
Status AwsEnv::CopyObject(const std::string& bucket_name_prefix_src,
                          const std::string& bucket_object_path_src,
                          const std::string& bucket_name_prefix_dest,
                          const std::string& bucket_object_path_dest) {
  Status st;
  Aws::String src_bucket = GetBucket(bucket_name_prefix_src);
  Aws::String dest_bucket = GetBucket(bucket_name_prefix_dest);

  // The filename is the same as the object name in the bucket
  Aws::String src_object = Aws::String(bucket_object_path_src.c_str(),
                                       bucket_object_path_src.size());
  Aws::String dest_object = Aws::String(bucket_object_path_dest.c_str(),
                                        bucket_object_path_dest.size());

  Aws::String src_url = src_bucket + src_object;

  // create copy request
  Aws::S3::Model::CopyObjectRequest request;
  request.SetCopySource(src_url);
  request.SetBucket(dest_bucket);
  request.SetKey(dest_object);

  // execute request
  Aws::S3::Model::CopyObjectOutcome outcome = s3client_->CopyObject(request);
  bool isSuccess = outcome.IsSuccess();
  if (!isSuccess) {
    const Aws::Client::AWSError<Aws::S3::S3Errors>& error = outcome.GetError();
    std::string errmsg(error.GetMessage().c_str());
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[aws] S3WritableFile src path %s error in copying to %s %s",
        src_url.c_str(), dest_object.c_str(), errmsg.c_str());
    return Status::IOError(dest_object.c_str(), errmsg.c_str());
  }
  Log(InfoLogLevel::ERROR_LEVEL, info_log_,
      "[aws] S3WritableFile src path %s copied to %s %s", src_url.c_str(),
      dest_object.c_str(), st.ToString().c_str());
  return st;
}

Status AwsEnv::GetObject(const std::string& bucket_name_prefix,
                         const std::string& bucket_object_path,
                         const std::string& local_destination) {
  Env* localenv = GetBaseEnv();
  std::string tmp_destination = local_destination + ".tmp";
  auto s3_bucket = GetBucket(bucket_name_prefix);

  Aws::S3::Model::GetObjectRequest getObjectRequest;
  getObjectRequest.SetBucket(s3_bucket);
  getObjectRequest.SetKey(
      Aws::String(bucket_object_path.data(), bucket_object_path.size()));
  getObjectRequest.SetResponseStreamFactory([tmp_destination]() {
    return Aws::New<Aws::FStream>(Aws::Utils::ARRAY_ALLOCATION_TAG,
                                  tmp_destination, std::ios_base::out);
  });
  auto get_outcome = s3client_->GetObject(getObjectRequest);

  bool isSuccess = get_outcome.IsSuccess();
  if (!isSuccess) {
    localenv->DeleteFile(tmp_destination);
    const auto& error = get_outcome.GetError();
    std::string errmsg(error.GetMessage().c_str(), error.GetMessage().size());
    auto errorType = error.GetErrorType();
    if (errorType == Aws::S3::S3Errors::NO_SUCH_BUCKET ||
        errorType == Aws::S3::S3Errors::NO_SUCH_KEY ||
        errorType == Aws::S3::S3Errors::RESOURCE_NOT_FOUND) {
      return Status::NotFound(errmsg);
    }
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[s3] GetObject bucket %s bucketpath %s error %s.", s3_bucket.c_str(),
        bucket_object_path.c_str(), errmsg.c_str());
    return Status::IOError(errmsg);
  }

  // Paranoia. Files can never be zero size.
  uint64_t file_size;
  auto s = localenv->GetFileSize(tmp_destination, &file_size);
  if (file_size == 0) {
    s = Status::IOError(tmp_destination + "Zero size.");
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[s3] GetObject bucket %s bucketpath %s size %ld. %s",
        s3_bucket.c_str(), bucket_object_path.c_str(), file_size,
        s.ToString().c_str());
  }

  if (s.ok()) {
    s = localenv->RenameFile(tmp_destination, local_destination);
  }
  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "[s3] GetObject bucket %s bucketpath %s size %ld. %s", s3_bucket.c_str(),
      bucket_object_path.c_str(), file_size, s.ToString().c_str());
  return s;
}

Status AwsEnv::PutObject(const std::string& local_file,
                         const std::string& bucket_name_prefix,
                         const std::string& bucket_object_path) {
  uint64_t fsize = 0;
  // debugging paranoia. Files uploaded to S3 can never be zero size.
  auto st = GetBaseEnv()->GetFileSize(local_file, &fsize);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[s3] PutObject localpath %s error getting size %s", local_file.c_str(),
        st.ToString().c_str());
    return st;
  }
  if (fsize == 0) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[s3] PutObject localpath %s error zero size", local_file.c_str());
    return Status::IOError(local_file + " Zero size.");
  }

  auto input_data = Aws::MakeShared<Aws::FStream>(
      bucket_object_path.c_str(), local_file.c_str(),
      std::ios_base::in | std::ios_base::out);

  // Copy entire file into S3.
  // Writes to an S3 object are atomic.
  Aws::S3::Model::PutObjectRequest put_request;
  put_request.SetBucket(GetBucket(bucket_name_prefix));
  put_request.SetKey(
      Aws::String(bucket_object_path.data(), bucket_object_path.size()));
  put_request.SetBody(input_data);
  SetEncryptionParameters(put_request);

  Aws::S3::Model::PutObjectOutcome put_outcome =
      s3client_->PutObject(put_request, fsize);
  bool isSuccess = put_outcome.IsSuccess();
  if (!isSuccess) {
    const Aws::Client::AWSError<Aws::S3::S3Errors>& error =
        put_outcome.GetError();
    std::string errmsg(error.GetMessage().c_str(), error.GetMessage().size());
    st = Status::IOError(local_file, errmsg);
  }

  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "[s3] PutObject %s/%s, size %zu, status %s",
      put_request.GetBucket().c_str(), bucket_object_path.c_str(), fsize,
      st.ToString().c_str());

  return st;
}

void AwsEnv::SetEncryptionParameters(
    Aws::S3::Model::PutObjectRequest& put_request) const {
  if (cloud_env_options.server_side_encryption) {
    if (cloud_env_options.encryption_key_id.empty()) {
      put_request.SetServerSideEncryption(
          Aws::S3::Model::ServerSideEncryption::AES256);
    } else {
      put_request.SetServerSideEncryption(
          Aws::S3::Model::ServerSideEncryption::aws_kms);
      put_request.SetSSEKMSKeyId(cloud_env_options.encryption_key_id.c_str());
    }
  }
}

//
// prepends the configured src object path name
//
std::string AwsEnv::srcname(const std::string& localname) {
  assert(!src_bucket_prefix_.empty());
  return src_object_prefix_ + "/" + basename(localname);
}

//
// prepends the configured dest object path name
//
std::string AwsEnv::destname(const std::string& localname) {
  assert(!dest_bucket_prefix_.empty());
  return dest_object_prefix_ + "/" + basename(localname);
}

Status AwsEnv::LockFile(const std::string& fname, FileLock** lock) {
  // there isn's a very good way to atomically check and create
  // a file via libs3
  *lock = nullptr;
  return Status::OK();
}

Status AwsEnv::UnlockFile(FileLock* lock) { return Status::OK(); }

Status AwsEnv::NewLogger(const std::string& fname, shared_ptr<Logger>* result) {
  return base_env_->NewLogger(fname, result);
}

// The factory method for creating an S3 Env
Status AwsEnv::NewAwsEnv(Env* base_env, const std::string& src_bucket_prefix,
                         const std::string& src_object_prefix,
                         const std::string& src_bucket_region,
                         const std::string& dest_bucket_prefix,
                         const std::string& dest_object_prefix,
                         const std::string& dest_bucket_region,
                         const CloudEnvOptions& cloud_options,
                         std::shared_ptr<Logger> info_log, CloudEnv** cenv) {
  Status status;
  *cenv = nullptr;
  // If underlying env is not defined, then use PosixEnv
  if (!base_env) {
    base_env = Env::Default();
  }
  unique_ptr<AwsEnv> aenv(
      new AwsEnv(base_env, src_bucket_prefix, src_object_prefix,
                 src_bucket_region, dest_bucket_prefix, dest_object_prefix,
                 dest_bucket_region, cloud_options, info_log));
  if (!aenv->status().ok()) {
    status = aenv->status();
  } else {
    *cenv = aenv.release();
  }
  return status;
}

//
// Retrieves the AWS credentials from two environment variables
// called "aws_access_key_id" and "aws_secret_access_key".
//
Status AwsEnv::GetTestCredentials(std::string* aws_access_key_id,
                                  std::string* aws_secret_access_key,
                                  std::string* region) {
  Status st;
  char* id = getenv("AWS_ACCESS_KEY_ID");
  if (id == nullptr) {
    id = getenv("aws_access_key_id");
  }
  char* secret = getenv("AWS_SECRET_ACCESS_KEY");
  if (secret == nullptr) {
    secret = getenv("aws_secret_access_key");
  }

  if (id == nullptr || secret == nullptr) {
    std::string msg =
        "Skipping AWS tests. "
        "AWS credentials should be set "
        "using environment varaibles AWS_ACCESS_KEY_ID and "
        "AWS_SECRET_ACCESS_KEY";
    return Status::IOError(msg);
  }
  aws_access_key_id->assign(id);
  aws_secret_access_key->assign(secret);

  char* reg = getenv("AWS_DEFAULT_REGION");
  if (reg == nullptr) {
    reg = getenv("aws_default_region");
  }

  if (reg != nullptr) {
    region->assign(reg);
  } else {
    region->assign("us-west-2");
  }
  return st;
}

//
// Create a test bucket suffix. This is used for unit tests only.
//
std::string AwsEnv::GetTestBucketSuffix() {
  char* bname = getenv("ROCKSDB_CLOUD_TEST_BUCKET_NAME");
  if (!bname) {
    return std::to_string(geteuid());
  }
  std::string name;
  name.assign(bname);
  return name;
}

//
// Keep retrying the command until it is successful or the timeout has expired
//
Status KinesisSystem::Retry(Env* env, RetryType func) {
  using namespace std::chrono;
  Status stat;
  uint64_t start = env->NowMicros();

  while (true) {
    // If command is successful, return immediately
    stat = func();
    if (stat.ok()) {
      break;
    }
    // sleep for some time
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    // If timeout has expired, return error
    uint64_t now = env->NowMicros();
    if (start + KinesisSystem::retry_period_micros < now) {
      stat = Status::TimedOut();
      break;
    }
  }
  return stat;
}

}  // namespace rocksdb

#else  // USE_AWS

// dummy placeholders used when AWS is not available
namespace rocksdb {
Status AwsEnv::NewSequentialFile(const std::string& fname,
                                 unique_ptr<SequentialFile>* result,
                                 const EnvOptions& options) {
  return Status::NotSupported("Not compiled with aws support");
}

Status NewAwsEnv(Env** s3_env, const std::string& fsname) {
  return Status::NotSupported("Not compiled with aws support");
}
}

#endif
