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
      has_two_unique_buckets_(false) {
  src_bucket_prefix_ = trim(src_bucket_prefix_);
  src_object_prefix_ = trim(src_object_prefix_);
  src_bucket_region_ = trim(src_bucket_region_);
  dest_bucket_prefix_ = trim(dest_bucket_prefix_);
  dest_object_prefix_ = trim(dest_object_prefix_);
  dest_bucket_region_ = trim(dest_bucket_region_);

  base_env_ = underlying_env;
  Aws::InitAPI(Aws::SDKOptions());

  // create AWS creds
  Aws::Auth::AWSCredentials creds(
      Aws::String(cloud_env_options.credentials.access_key_id.c_str()),
      Aws::String(cloud_env_options.credentials.secret_key.c_str()));

  // create AWS S3 client with appropriate timeouts
  Aws::Client::ClientConfiguration config;
  config.connectTimeoutMs = 30000;
  config.requestTimeoutMs = 600000;

  // Setup how retries need to be done
  config.retryStrategy =
      std::make_shared<AwsRetryStrategy>(cloud_env_options, info_log_);

  if (!GetSrcBucketPrefix().empty()) {
    has_src_bucket_ = true;
  }
  if (!GetDestBucketPrefix().empty()) {
    has_dest_bucket_ = true;
  }

  // Do we have two unique buckets?
  if (has_src_bucket_ && has_dest_bucket_ &&
      ((GetSrcBucketPrefix() != GetDestBucketPrefix()) ||
       (GetSrcObjectPrefix() != GetDestObjectPrefix()))) {
    has_two_unique_buckets_ = true;
  }

  // TODO: support buckets being in different regions
  if (has_two_unique_buckets_) {
    if (src_bucket_region_ == dest_bucket_region_) {
      // alls good
    } else {
      create_bucket_status_ = Status::InvalidArgument(
              "Two different regions not supported");
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
    config.region = Aws::String(src_bucket_region_.c_str(),
                                src_bucket_region_.size());
  }
  bucket_location_ = Aws::S3::Model::BucketLocationConstraintMapper::
      GetBucketLocationConstraintForName(config.region);

  s3client_ = std::make_shared<Aws::S3::S3Client>(creds, config);

  // create dest bucket if specified
  if (has_dest_bucket_) {
    create_bucket_status_ = S3WritableFile::CreateBucketInS3(
        s3client_, GetDestBucketPrefix(), bucket_location_);
  }
  if (!create_bucket_status_.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log,
        "[aws] NewAwsEnv Unable to  create bucket %s",
        create_bucket_status_.ToString().c_str());
  }

  // create Kinesis client for storing/reading logs
  if (create_bucket_status_.ok() && !cloud_env_options.keep_local_log_files) {
    kinesis_client_ =
        std::make_shared<Aws::Kinesis::KinesisClient>(creds, config);
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
}

AwsEnv::~AwsEnv() {
  running_ = false;
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
  if (manifest) *manifest = false;

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
  *result = nullptr;

  S3ReadableFile* f = new S3ReadableFile(this, bucket_prefix, fname);
  Status st = f->status();
  if (!st.ok()) {
    delete f;
  } else {
    result->reset(dynamic_cast<SequentialFile*>(f));
  }
  return st;
}

// open a file for sequential reading
Status AwsEnv::NewSequentialFile(const std::string& fname,
                                 unique_ptr<SequentialFile>* result,
                                 const EnvOptions& options) {
  assert(status().ok());
  *result = nullptr;
  Status st;

  // Get file type
  bool logfile;
  bool sstfile;
  bool manifest;
  bool identity;
  GetFileType(fname, &sstfile, &logfile, &manifest, &identity);

  st = CheckOption(options);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[aws] NewSequentialFile file '%s' %s", fname.c_str(),
        st.ToString().c_str());
    return st;
  }

  if (sstfile || manifest || identity) {
    // We read first from local storage and then from cloud storage.
    st = base_env_->NewSequentialFile(fname, result, options);

    if (!st.ok()) {
      S3ReadableFile* f = nullptr;
      if (!st.ok() && has_dest_bucket_) {  // read from destination S3
        f = new S3ReadableFile(this, GetDestBucketPrefix(), destname(fname));
        st = f->status();
      }
      if (!st.ok() && has_src_bucket_) {  // read from src bucket
        delete f;
        f = new S3ReadableFile(this, GetSrcBucketPrefix(), srcname(fname));
        st = f->status();
      }
      if (st.ok()) {
        result->reset(dynamic_cast<SequentialFile*>(f));
      } else {
        delete f;
      }
    }
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
        "[aws] NewSequentialFile file %s %s", fname.c_str(),
        st.ToString().c_str());
    return st;

  } else if (logfile && !cloud_env_options.keep_local_log_files) {
    // read from Kinesis
    assert(tailer_->status().ok());

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

  // This is neither a sst file or a log file. Read from default env.
  return base_env_->NewSequentialFile(fname, result, options);
}

// open a file for random reading
Status AwsEnv::NewRandomAccessFile(const std::string& fname,
                                   unique_ptr<RandomAccessFile>* result,
                                   const EnvOptions& options) {
  assert(status().ok());
  *result = nullptr;
  Status st;

  // Get file type
  bool logfile;
  bool sstfile;
  bool manifest;
  bool identity;
  GetFileType(fname, &sstfile, &logfile, &manifest, &identity);

  // Validate options
  st = CheckOption(options);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[aws] NewRandomAccessFile file '%s' %s", fname.c_str(),
        st.ToString().c_str());
    return st;
  }

  if (sstfile || manifest || identity) {
    // Read from local storage and then from cloud storage.
    st = base_env_->NewRandomAccessFile(fname, result, options);

    if (!st.ok()) {
      S3ReadableFile* f = nullptr;
      if (!st.ok() && has_dest_bucket_) {
        // read from dest S3
        f = new S3ReadableFile(this, GetDestBucketPrefix(), destname(fname));
        st = f->status();
      }
      if (!st.ok() && has_src_bucket_) {
        delete f;
        f = new S3ReadableFile(this, GetSrcBucketPrefix(), srcname(fname));
        st = f->status();
      }
      if (!st.ok()) {
        delete f;
      } else {
        result->reset(dynamic_cast<RandomAccessFile*>(f));
      }
    }
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
        "[s3] NewRandomAccessFile file %s %s", fname.c_str(),
        st.ToString().c_str());
    return st;

  } else if (logfile && !cloud_env_options.keep_local_log_files) {
    // read from Kinesis
    assert(tailer_->status().ok());

    // map  pathname to cache dir
    std::string pathname =
        KinesisSystem::GetCachePath(tailer_->GetCacheDir(), Slice(fname));
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
        "[kinesis] NewRandomAccessFile logfile %s %s", pathname.c_str(), "ok");

    auto lambda = [this, pathname, &result, options]() -> Status {
      return base_env_->NewRandomAccessFile(pathname, result, options);
    };
    return KinesisSystem::Retry(this, lambda);
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

  if (has_dest_bucket_ &&
      (sstfile || identity ||
       (manifest &&
        cloud_env_options.manifest_durable_periodicity_millis > 0))) {
    std::string cloud_file;
    if (manifest) {
      cloud_file = destname(dirname(fname)) + "/MANIFEST";
    } else {
      cloud_file = destname(fname);
    }

    S3WritableFile* f =
        new S3WritableFile(this, fname, GetDestBucketPrefix(), cloud_file,
                           options, cloud_env_options);
    s = f->status();
    if (!s.ok()) {
      delete f;
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[s3] NewWritableFile src %s %s", fname.c_str(),
          s.ToString().c_str());
      return s;
    }
    result->reset(dynamic_cast<WritableFile*>(f));

  } else if (logfile && !cloud_env_options.keep_local_log_files) {
    KinesisWritableFile* f = new KinesisWritableFile(this, fname, options);
    if (f == nullptr || !f->status().ok()) {
      delete f;
      *result = nullptr;
      s = Status::IOError("[aws] NewWritableFile", fname.c_str());
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[kinesis] NewWritableFile src %s %s", fname.c_str(),
          s.ToString().c_str());
      return s;
    }
    result->reset(dynamic_cast<WritableFile*>(f));
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
  S3Directory* d = new S3Directory(this, name);

  // Check if the path exists in local dir
  if (d == nullptr || !d->status().ok()) {
    delete d;
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[aws] NewDirectory name %s unable to create local dir", name.c_str());
    return d->status();
  }
  result->reset(d);
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
      st = PathExistsInS3(destname(fname), GetDestBucketPrefix(), true);
    }
    if (!st.ok() && has_src_bucket_) {
      st = PathExistsInS3(srcname(fname), GetSrcBucketPrefix(), true);
    }

  } else if (logfile && !cloud_env_options.keep_local_log_files) {
    // read from Kinesis
    assert(tailer_->status().ok());

    // map  pathname to cache dir
    std::string pathname =
        KinesisSystem::GetCachePath(tailer_->GetCacheDir(), Slice(fname));
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
        "[kinesis] FileExists logfile %s %s", pathname.c_str(), "ok");

    auto lambda = [this, pathname]() -> Status {
      return base_env_->FileExists(pathname);
    };
    st = KinesisSystem::Retry(this, lambda);
  } else {
    st = base_env_->FileExists(fname);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[aws] FileExists path '%s' %s",
      fname.c_str(), st.ToString().c_str());
  return st;
}

//
// Check if the specified pathname exists as a file or directory
// in AWS-S3
//
Status AwsEnv::PathExistsInS3(const std::string& fname,
                              const std::string& bucket, bool isfile) {
  assert(status().ok());

  // We could have used Aws::S3::Model::ListObjectsRequest to find
  // the file size, but a ListObjectsRequest is not guaranteed to
  // return the most recently created objects. Only a Get is
  // guaranteed to be consistent with Puts. So, we try to read
  // 0 bytes from the object.
  unique_ptr<SequentialFile> fd;
  Slice result;
  S3ReadableFile* f = new S3ReadableFile(this, bucket, fname, isfile);
  fd.reset(f);
  Status ret = f->Read(0, &result, nullptr);
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[s3] PathExistsInS3 path '%s' %s",
      fname.c_str(), ret.ToString().c_str());
  return ret;
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

  // the starting object marker
  Aws::String prefix = Aws::String(path.c_str(), path.size());
  Aws::String marker;
  bool loop = true;

  // get info of bucket+object
  while (loop) {
    Aws::S3::Model::ListObjectsRequest request;
    request.SetBucket(bucket);
    request.SetMaxKeys(50);
    request.SetPrefix(prefix);
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
      assert(keystr.find(path) == 0);
      if (keystr.find(path) != 0) {
        loop = false;
        break;
      }
      result->push_back(keystr);
    }

    // If there are no more entries, then we are done.
    if (!res.GetIsTruncated()) {
      break;
    }
    // The new starting point
    marker = res.GetNextMarker();
  }
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
  assert(!IsSstFile(path));
  result->clear();

  // Fetch the list of children from both buckets in S3
  Status st;
  if (has_src_bucket_) {
    st = GetChildrenFromS3(srcname(path), GetSrcBucketPrefix(), result);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[s3] GetChildren src bucket %s %s error from S3 %s",
          GetSrcBucketPrefix().c_str(), path.c_str(), st.ToString().c_str());
      return st;
    }
  }
  if (has_dest_bucket_ && two_unique_buckets()) {
    st = GetChildrenFromS3(srcname(path), GetDestBucketPrefix(), result);
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
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[s3] GetChildren %s successfully returned %d files", path.c_str(),
      result->size());
  return Status::OK();
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
    st = DeletePathInS3(GetDestBucketPrefix(), destname(fname));
    if (!st.ok() && !st.IsNotFound()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[s3] DeleteFile DeletePathInS3 file %s error %s", fname.c_str(),
          st.ToString().c_str());
      return st;
    }
    // delete from local
    st = base_env_->DeleteFile(fname);
  } else if (logfile && !cloud_env_options.keep_local_log_files) {
    // read from Kinesis
    assert(tailer_->status().ok());

    // Log a Delete record to kinesis stream
    KinesisWritableFile* f = new KinesisWritableFile(this, fname, EnvOptions());
    if (f == nullptr || !f->status().ok()) {
      st = Status::IOError("[Kinesis] DeleteFile", fname.c_str());
      delete f;
    } else {
      st = f->LogDelete();
      delete f;
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
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[s3] S3WritableFile bucket %s error in deleting non-existent %s %s",
          bucket.c_str(), fname.c_str(), errmsg.c_str());
      return Status::NotFound(fname, errmsg.c_str());
    }
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[s3] S3WritableFile bucket %s error in deleting %s %s",
        bucket.c_str(), fname.c_str(), errmsg.c_str());
    return Status::IOError(fname, errmsg.c_str());
  }
  return Status::OK();
}

//
// Create a new directory.
// Create a new entry in the destination bucket and create a
// directory in the local posix env.
//
Status AwsEnv::CreateDir(const std::string& dirname) {
  assert(status().ok());
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[s3] CreateDir dir '%s'",
      dirname.c_str());
  Status st;

  if (has_dest_bucket_) {
    // Get bucket name
    Aws::String bucket = GetBucket(GetDestBucketPrefix());
    std::string dname = destname(dirname);
    Aws::String object = Aws::String(dname.c_str(), dname.size());

    // create an empty object
    Aws::S3::Model::PutObjectRequest put_request;
    put_request.SetBucket(bucket);
    put_request.SetKey(object);
    Aws::S3::Model::PutObjectOutcome put_outcome =
        s3client_->PutObject(put_request);
    bool isSuccess = put_outcome.IsSuccess();
    if (!isSuccess) {
      const Aws::Client::AWSError<Aws::S3::S3Errors>& error =
          put_outcome.GetError();
      std::string errmsg(error.GetMessage().c_str(), error.GetMessage().size());
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[s3] CreateDir bucket %s error in creating dir %s %s\n",
          bucket.c_str(), dirname.c_str(), errmsg.c_str());
      return Status::IOError(dirname, errmsg.c_str());
    }
  }
  // create local dir as well.
  st = base_env_->CreateDir(dirname);

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[s3] CreateDir dir %s %s",
      dirname.c_str(), st.ToString().c_str());
  return st;
};

//
// Directories are created as a bucket in the AWS S3 service
// as well as a local directory via posix env.
//
Status AwsEnv::CreateDirIfMissing(const std::string& dirname) {
  assert(status().ok());
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[s3] CreateDirIfMissing dir '%s'",
      dirname.c_str());
  Status st;

  if (has_dest_bucket_) {
    // Get bucket name
    Aws::String bucket = GetBucket(GetDestBucketPrefix());
    std::string dname = destname(dirname);
    Aws::String object = Aws::String(dname.c_str(), dname.size());

    // create request
    Aws::S3::Model::PutObjectRequest put_request;
    put_request.SetBucket(bucket);
    put_request.SetKey(object);
    Aws::S3::Model::PutObjectOutcome put_outcome =
        s3client_->PutObject(put_request);
    bool isSuccess = put_outcome.IsSuccess();
    if (!isSuccess) {
      const Aws::Client::AWSError<Aws::S3::S3Errors>& error =
          put_outcome.GetError();
      std::string errmsg(error.GetMessage().c_str(), error.GetMessage().size());
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[s3] CreateDirIfMissing error in creating bucket %s %s",
          bucket.c_str(), errmsg.c_str());
      return Status::IOError(dirname, errmsg.c_str());
    }
  }
  // create the same directory in the posix filesystem as well
  st = base_env_->CreateDirIfMissing(dirname);

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[s3] CreateDirIfMissing created dir %s %s", dirname.c_str(),
      st.ToString().c_str());
  return st;
};

Status AwsEnv::DeleteDir(const std::string& dirname) {
  assert(status().ok());
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[s3] DeleteDir src '%s'",
      dirname.c_str());
  assert(!IsSstFile(dirname));
  Status st;

  if (has_dest_bucket_) {
    // Verify that the S3 directory has no children
    std::vector<std::string> results;
    st = GetChildrenFromS3(destname(dirname), GetDestBucketPrefix(), &results);
    if (st.ok() && results.size() != 0) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[s3] DeleteDir error in deleting nonempty dir %s with %d entries",
          dirname.c_str(), results.size());
      for (auto name : results) {
        Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[s3] DeleteDir entry %s",
            name.c_str());
      }
      return Status::IOError("[s3] DeleteDir error in deleting nonempty dir",
                             dirname);
    }
    // Delete directory from S3
    st = DeletePathInS3(GetDestBucketPrefix(), destname(dirname));
  }

  // delete the same directory in the posix filesystem as well
  if (st.ok()) {
    st = base_env_->DeleteDir(dirname);
  }
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
    // Get file length from S3
    st = Status::NotFound();
    if (has_dest_bucket_) {
      st = GetFileInfoInS3(GetDestBucketPrefix(), destname(fname), size,
                           nullptr);
    }
    if (st.IsNotFound() && has_src_bucket_) {
      st = GetFileInfoInS3(GetSrcBucketPrefix(), srcname(fname), size, nullptr);
    }
    if (st.IsNotFound()) {
      st = base_env_->GetFileSize(fname, size);
    }

  } else if (logfile && !cloud_env_options.keep_local_log_files) {
    assert(tailer_->status().ok());

    // map  pathname to cache dir
    std::string pathname =
        KinesisSystem::GetCachePath(tailer_->GetCacheDir(), Slice(fname));
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
        "[kinesis] GetFileSize logfile %s %s", pathname.c_str(), "ok");

    auto lambda = [this, pathname, size]() -> Status {
      return base_env_->GetFileSize(pathname, size);
    };
    st = KinesisSystem::Retry(this, lambda);
  } else {
    st = base_env_->GetFileSize(fname, size);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[aws] GetFileSize src '%s' %s %ld",
      fname.c_str(), st.ToString().c_str(), *size);
  return st;
}

//
// Check if the specified pathname exists as a file or directory
// in AWS-S3
//
Status AwsEnv::GetFileInfoInS3(const std::string& bucket_prefix,
                               const std::string& fname, uint64_t* size,
                               uint64_t* modtime) {
  if (size) {
    *size = 0L;
  }
  if (modtime) {
    *modtime = 0L;
  }

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[s3] GetFileInfoInS3 src '%s'",
      fname.c_str());

  // We could have used Aws::S3::Model::ListObjectsRequest to find
  // the file size, but a ListObjectsRequest is not guaranteed to
  // return the most recently created objects. Only a Get is
  // guaranteed to be consistent with Puts. So, we try to read
  // 0 bytes from the object.
  unique_ptr<SequentialFile> fd;
  Slice result;
  S3ReadableFile* f = new S3ReadableFile(this, bucket_prefix, fname);
  fd.reset(f);
  Status ret = f->Read(0, &result, nullptr);
  if (!ret.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_, "[s3] GetFileInfoInS3 dir %s %s",
        fname.c_str(), ret.ToString().c_str());
    return ret;
  }
  if (size) {
    *size = f->GetSize();
  }
  if (modtime) {
    *modtime = f->GetLastModTime();
  }
  return ret;
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
    // Get file length from S3
    st = Status::NotFound();
    if (has_dest_bucket_) {
      st = GetFileInfoInS3(GetDestBucketPrefix(), destname(fname), nullptr,
                           time);
    }
    if (st.IsNotFound() && has_src_bucket_) {
      st = GetFileInfoInS3(GetSrcBucketPrefix(), srcname(fname), nullptr, time);
    }
    if (st.IsNotFound()) {
      st = base_env_->FileExists(fname);
    }

  } else if (logfile && !cloud_env_options.keep_local_log_files) {
    assert(tailer_->status().ok());
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
  Aws::String bucket = GetBucket(GetDestBucketPrefix());

  // Read id into string
  std::string dbid;
  Status st = DBCloudImpl::ReadFileIntoString(base_env_, localfile, &dbid);
  dbid = trim(dbid);

  // Upload ID file to  S3
  if (st.ok()) {
    Aws::String target(idfile.c_str(), idfile.size());
    st = S3WritableFile::CopyToS3(this, localfile, bucket, target);
  }

  // Save mapping from ID to dirname
  if (st.ok()) {
    st = SaveDbid(dbid, dirname(idfile));
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
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
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
  Aws::String bucket = GetBucket(bucket_prefix);
  Aws::String key = Aws::String(dbidkey.c_str(), dbidkey.size());

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[s3] Bucket %s GetPathForDbid dbid %s", bucket.c_str(), dbid.c_str());

  // set up S3 request to read the head
  Aws::S3::Model::HeadObjectRequest request;
  request.SetBucket(bucket);
  request.SetKey(key);

  Aws::S3::Model::HeadObjectOutcome outcome = s3client_->HeadObject(request);
  bool isSuccess = outcome.IsSuccess();
  if (!isSuccess) {
    const Aws::Client::AWSError<Aws::S3::S3Errors>& error = outcome.GetError();
    std::string errmsg(error.GetMessage().c_str(), error.GetMessage().size());
    Aws::S3::S3Errors s3err = error.GetErrorType();

    if (s3err == Aws::S3::S3Errors::NO_SUCH_BUCKET ||
        s3err == Aws::S3::S3Errors::NO_SUCH_KEY ||
        s3err == Aws::S3::S3Errors::RESOURCE_NOT_FOUND ||
        s3err == Aws::S3::S3Errors::UNKNOWN) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[s3] %s GetPathForDbid error non-existent dbid %s %s",
          bucket.c_str(), dbid.c_str(), errmsg.c_str());
      return Status::NotFound(dbid, errmsg.c_str());
    }
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[s3] %s GetPathForDbid error dbid %s %s", bucket.c_str(), dbid.c_str(),
        errmsg.c_str());
    return Status::IOError(dbid, errmsg.c_str());
  }
  const Aws::S3::Model::HeadObjectResult& res = outcome.GetResult();
  const Aws::Map<Aws::String, Aws::String> metadata = res.GetMetadata();

  // Find "dirname" metadata that stores the pathname of the db
  std::string dirname_tag = "dirname";
  Aws::String dir = Aws::String(dirname_tag.c_str(), dirname_tag.size());
  auto it = metadata.find(dir);
  Status st;
  if (it != metadata.end()) {
    Aws::String as = it->second;
    dirname->assign(as.c_str(), as.size());
  } else {
    st = Status::NotFound("GetPathForDbid");
  }
  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "[s3] %s GetPathForDbid dbid %s %s", bucket.c_str(), dbid.c_str(),
      st.ToString().c_str());
  return st;
};

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
  AwsEnv* aenv = new AwsEnv(base_env, src_bucket_prefix, src_object_prefix,
                            src_bucket_region,
                            dest_bucket_prefix, dest_object_prefix, dest_bucket_region,
                            cloud_options, info_log);
  if (aenv == nullptr) {
    status = Status::IOError("No More memory");
  } else if (!aenv->status().ok()) {
    status = aenv->status();
    delete aenv;
  } else {
    *cenv = aenv;
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
