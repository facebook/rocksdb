//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#include "cloud/aws/aws_env.h"
#include <unistd.h>
#include <chrono>
#include <fstream>
#include <iostream>
#include <memory>
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"

#ifdef USE_AWS

#include <aws/core/utils/threading/Executor.h>

#include "cloud/aws/aws_file.h"
#include "cloud/aws/aws_kafka.h"
#include "cloud/aws/aws_kinesis.h"
#include "cloud/aws/aws_log.h"
#include "cloud/aws/aws_retry.h"
#include "cloud/db_cloud_impl.h"

namespace rocksdb {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"

namespace detail {

using ScheduledJob =
    std::pair<std::chrono::steady_clock::time_point, std::function<void(void)>>;
struct Comp {
  bool operator()(const ScheduledJob& a, const ScheduledJob& b) const {
    return a.first < b.first;
  }
};
struct JobHandle {
  std::multiset<ScheduledJob, Comp>::iterator itr;
  JobHandle(std::multiset<ScheduledJob, Comp>::iterator i)
      : itr(std::move(i)) {}
};

class JobExecutor {
 public:
  shared_ptr<JobHandle> ScheduleJob(std::chrono::steady_clock::time_point time,
                                    std::function<void(void)> callback);
  void CancelJob(JobHandle* handle);

  JobExecutor();
  ~JobExecutor();

 private:
  void DoWork();

  std::mutex mutex_;
  // Notified when the earliest job to be scheduled has changed.
  std::condition_variable jobs_changed_cv_;
  std::multiset<ScheduledJob, Comp> scheduled_jobs_;
  bool shutting_down_{false};

  std::thread thread_;
};

JobExecutor::JobExecutor() {
  thread_ = std::thread([this]() { DoWork(); });
}

JobExecutor::~JobExecutor() {
  {
    std::lock_guard<std::mutex> lk(mutex_);
    shutting_down_ = true;
    jobs_changed_cv_.notify_all();
  }
  if (thread_.joinable()) {
    thread_.join();
  }
}

shared_ptr<JobHandle> JobExecutor::ScheduleJob(
    std::chrono::steady_clock::time_point time,
    std::function<void(void)> callback) {
  std::lock_guard<std::mutex> lk(mutex_);
  auto itr = scheduled_jobs_.emplace(time, std::move(callback));
  if (itr == scheduled_jobs_.begin()) {
    jobs_changed_cv_.notify_all();
  }
  return std::make_shared<JobHandle>(itr);
}

void JobExecutor::CancelJob(JobHandle* handle) {
  std::lock_guard<std::mutex> lk(mutex_);
  if (scheduled_jobs_.begin() == handle->itr) {
    jobs_changed_cv_.notify_all();
  }
  scheduled_jobs_.erase(handle->itr);
}

void JobExecutor::DoWork() {
  while (true) {
    std::unique_lock<std::mutex> lk(mutex_);
    if (shutting_down_) {
        break;
    }
    if (scheduled_jobs_.empty()) {
        jobs_changed_cv_.wait(lk);
        continue;
    }
    auto earliest_job = scheduled_jobs_.begin();
    auto earliest_job_time = earliest_job->first;
    if (earliest_job_time >= std::chrono::steady_clock::now()) {
        jobs_changed_cv_.wait_until(lk, earliest_job_time);
        continue;
    }
    // invoke the function
    lk.unlock();
    earliest_job->second();
    lk.lock();
    scheduled_jobs_.erase(earliest_job);
  }
}

}  // namespace detail

detail::JobExecutor* GetJobExecutor() {
  static detail::JobExecutor executor;
  return &executor;
}

Aws::Utils::Threading::Executor* GetAwsTransferManagerExecutor() {
  static Aws::Utils::Threading::PooledThreadExecutor executor(8);
  return &executor;
}


template <typename T>
void SetEncryptionParameters(const CloudEnvOptions& cloud_env_options,
                             T& put_request) {
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

class CloudRequestCallbackGuard {
 public:
  CloudRequestCallbackGuard(CloudRequestCallback* callback,
                            CloudRequestOpType type, uint64_t size = 0)
      : callback_(callback), type_(type), size_(size), start_(now()) {}

  ~CloudRequestCallbackGuard() {
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
    std::shared_ptr<Aws::S3::S3Client> client,
    std::shared_ptr<CloudRequestCallback> cloud_request_callback)
    : client_(std::move(client)),
      cloud_request_callback_(std::move(cloud_request_callback)) {}

Aws::S3::Model::ListObjectsOutcome AwsS3ClientWrapper::ListObjects(
    const Aws::S3::Model::ListObjectsRequest& request) {
  CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                              CloudRequestOpType::kListOp);
  auto outcome = client_->ListObjects(request);
  t.SetSuccess(outcome.IsSuccess());
  return outcome;
}

Aws::S3::Model::CreateBucketOutcome AwsS3ClientWrapper::CreateBucket(
    const Aws::S3::Model::CreateBucketRequest& request) {
  CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                              CloudRequestOpType::kCreateOp);
  return client_->CreateBucket(request);
}

Aws::S3::Model::HeadBucketOutcome AwsS3ClientWrapper::HeadBucket(
    const Aws::S3::Model::HeadBucketRequest& request) {
  CloudRequestCallbackGuard t(cloud_request_callback_.get(),
			      CloudRequestOpType::kInfoOp);
  return client_->HeadBucket(request);
}

Aws::S3::Model::DeleteObjectOutcome AwsS3ClientWrapper::DeleteObject(
    const Aws::S3::Model::DeleteObjectRequest& request) {
  CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                              CloudRequestOpType::kDeleteOp);
  auto outcome = client_->DeleteObject(request);
  t.SetSuccess(outcome.IsSuccess());
  return outcome;
}

Aws::S3::Model::CopyObjectOutcome AwsS3ClientWrapper::CopyObject(
    const Aws::S3::Model::CopyObjectRequest& request) {
  CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                              CloudRequestOpType::kCopyOp);
  auto outcome = client_->CopyObject(request);
  t.SetSuccess(outcome.IsSuccess());
  return outcome;
}

Aws::S3::Model::GetObjectOutcome AwsS3ClientWrapper::GetObject(
    const Aws::S3::Model::GetObjectRequest& request) {
  CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                              CloudRequestOpType::kReadOp);
  auto outcome = client_->GetObject(request);
  if (outcome.IsSuccess()) {
    t.SetSize(outcome.GetResult().GetContentLength());
    t.SetSuccess(true);
  }
  return outcome;
}

Aws::S3::Model::PutObjectOutcome AwsS3ClientWrapper::PutObject(
    const Aws::S3::Model::PutObjectRequest& request, uint64_t size_hint) {
  CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                              CloudRequestOpType::kWriteOp, size_hint);
  auto outcome = client_->PutObject(request);
  t.SetSuccess(outcome.IsSuccess());
  return outcome;
}

Aws::S3::Model::HeadObjectOutcome AwsS3ClientWrapper::HeadObject(
    const Aws::S3::Model::HeadObjectRequest& request) {
  CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                              CloudRequestOpType::kInfoOp);
  auto outcome = client_->HeadObject(request);
  t.SetSuccess(outcome.IsSuccess());
  return outcome;
}

//
// The AWS credentials are specified to the constructor via
// access_key_id and secret_key.
//
AwsEnv::AwsEnv(Env* underlying_env, const CloudEnvOptions& _cloud_env_options,
               const std::shared_ptr<Logger>& info_log)
    : CloudEnvImpl(_cloud_env_options, underlying_env),
      info_log_(info_log),
      running_(true) {
  Aws::InitAPI(Aws::SDKOptions());
  if (cloud_env_options.src_bucket.GetRegion().empty() ||
      cloud_env_options.dest_bucket.GetRegion().empty()) {
    std::string region;
    if (! CloudEnvOptions::GetNameFromEnvironment("AWS_DEFAULT_REGION", "aws_default_region", &region)) {
      region = default_region;
    }
    if (cloud_env_options.src_bucket.GetRegion().empty()) {
      cloud_env_options.src_bucket.SetRegion(region);
    }
    if (cloud_env_options.dest_bucket.GetRegion().empty()) {
      cloud_env_options.dest_bucket.SetRegion(region);
    }
  }

  unique_ptr<Aws::Auth::AWSCredentials> creds;
  if (!cloud_env_options.credentials.access_key_id.empty() &&
      !cloud_env_options.credentials.secret_key.empty()) {
    creds.reset(new Aws::Auth::AWSCredentials(
        ToAwsString(cloud_env_options.credentials.access_key_id),
        ToAwsString(cloud_env_options.credentials.secret_key)));
  }

  Header(info_log_, "      AwsEnv.src_bucket_name: %s",
         cloud_env_options.src_bucket.GetBucketName().c_str());
  Header(info_log_, "      AwsEnv.src_object_path: %s",
         cloud_env_options.src_bucket.GetObjectPath().c_str());
  Header(info_log_, "      AwsEnv.src_bucket_region: %s",
         cloud_env_options.src_bucket.GetRegion().c_str());
  Header(info_log_, "     AwsEnv.dest_bucket_name: %s",
         cloud_env_options.dest_bucket.GetBucketName().c_str());
  Header(info_log_, "     AwsEnv.dest_object_path: %s",
         cloud_env_options.dest_bucket.GetObjectPath().c_str());
  Header(info_log_, "     AwsEnv.dest_bucket_region: %s",
         cloud_env_options.dest_bucket.GetRegion().c_str());
  Header(info_log_, "            AwsEnv.credentials: %s",
         creds ? "[given]" : "[not given]");

  base_env_ = underlying_env;
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

  // TODO: support buckets being in different regions
  if (!SrcMatchesDest() && HasSrcBucket() && HasDestBucket()) {
    if (cloud_env_options.src_bucket.GetRegion() == cloud_env_options.dest_bucket.GetRegion()) {
      // alls good
    } else {
      create_bucket_status_ =
          Status::InvalidArgument("Two different regions not supported");
      Log(InfoLogLevel::ERROR_LEVEL, info_log,
          "[aws] NewAwsEnv Buckets %s, %s in two different regions %s, %s "
          "is not supported",
	  cloud_env_options.src_bucket.GetBucketName().c_str(),
	  cloud_env_options.dest_bucket.GetBucketName().c_str(),
          cloud_env_options.src_bucket.GetRegion().c_str(),
          cloud_env_options.dest_bucket.GetRegion().c_str());
      return;
    }
  }

  // Use specified region if any
  config.region = ToAwsString(cloud_env_options.src_bucket.GetRegion());
  Header(info_log_, "AwsEnv connection to endpoint in region: %s",
         config.region.c_str());
  bucket_location_ = Aws::S3::Model::BucketLocationConstraintMapper::
      GetBucketLocationConstraintForName(config.region);

  {
    auto s3client = creds ? std::make_shared<Aws::S3::S3Client>(*creds, config)
                          : std::make_shared<Aws::S3::S3Client>(config);

      s3client_ = std::make_shared<AwsS3ClientWrapper>(
        std::move(s3client), cloud_env_options.cloud_request_callback);
  }

  {
    Aws::Transfer::TransferManagerConfiguration transferManagerConfig(
        GetAwsTransferManagerExecutor());
    transferManagerConfig.s3Client = s3client_->GetClient();
    SetEncryptionParameters(cloud_env_options,
                            transferManagerConfig.putObjectTemplate);
    SetEncryptionParameters(
        cloud_env_options, transferManagerConfig.createMultipartUploadTemplate);
    awsTransferManager_ =
        Aws::Transfer::TransferManager::Create(transferManagerConfig);
  }

  // create dest bucket if specified
  if (HasDestBucket()) {
    if (S3WritableFile::BucketExistsInS3(s3client_, GetDestBucketName(),
                                         bucket_location_)
            .ok()) {
      Log(InfoLogLevel::INFO_LEVEL, info_log,
          "[aws] NewAwsEnv Bucket %s already exists",
          GetDestBucketName().c_str());
    } else if (cloud_env_options.create_bucket_if_missing) {
      Log(InfoLogLevel::INFO_LEVEL, info_log,
          "[aws] NewAwsEnv Going to create bucket %s",
          GetDestBucketName().c_str());
      create_bucket_status_ = S3WritableFile::CreateBucketInS3(
          s3client_, GetDestBucketName(), bucket_location_);
    } else {
      create_bucket_status_ = Status::NotFound(
          "[aws] Bucket not found and create_bucket_if_missing is false");
    }
  }
  if (!create_bucket_status_.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log,
        "[aws] NewAwsEnv Unable to create bucket %s %s",
        GetDestBucketName().c_str(),
        create_bucket_status_.ToString().c_str());
  }

  // create cloud log client for storing/reading logs
  if (create_bucket_status_.ok() && !cloud_env_options.keep_local_log_files) {
    if (cloud_env_options.log_type == kLogKinesis) {
      std::unique_ptr<Aws::Kinesis::KinesisClient> kinesis_client;
      kinesis_client.reset(creds
                               ? new Aws::Kinesis::KinesisClient(*creds, config)
                               : new Aws::Kinesis::KinesisClient(config));

      if (!kinesis_client) {
        create_bucket_status_ =
            Status::IOError("Error in creating Kinesis client");
      }

      if (create_bucket_status_.ok()) {
        cloud_log_controller_.reset(
            new KinesisController(this, info_log_, std::move(kinesis_client)));

        if (!cloud_log_controller_) {
          create_bucket_status_ =
              Status::IOError("Error in creating Kinesis controller");
        }
      }
    } else if (cloud_env_options.log_type == kLogKafka) {
#ifdef USE_KAFKA
      KafkaController* kafka_controller = nullptr;

      create_bucket_status_ = KafkaController::create(
          this, info_log_, cloud_env_options, &kafka_controller);

      cloud_log_controller_.reset(kafka_controller);
#else
      create_bucket_status_ = Status::NotSupported(
          "In order to use Kafka, make sure you're compiling with "
          "USE_KAFKA=1");

      Log(InfoLogLevel::ERROR_LEVEL, info_log,
          "[aws] NewAwsEnv Unknown log type %d. %s",
          cloud_env_options.log_type,
          create_bucket_status_.ToString().c_str());
#endif /* USE_KAFKA */
    } else {
      create_bucket_status_ =
          Status::NotSupported("We currently only support Kinesis and Kafka");

      Log(InfoLogLevel::ERROR_LEVEL, info_log,
          "[aws] NewAwsEnv Unknown log type %d. %s", cloud_env_options.log_type,
          create_bucket_status_.ToString().c_str());
    }

    // Create Kinesis stream and wait for it to be ready
    if (create_bucket_status_.ok()) {
      create_bucket_status_ =
          cloud_log_controller_->CreateStream(GetSrcBucketName());
      if (!create_bucket_status_.ok()) {
        Log(InfoLogLevel::ERROR_LEVEL, info_log,
            "[aws] NewAwsEnv Unable to create stream %s",
            create_bucket_status_.ToString().c_str());
      }
    }

    if (create_bucket_status_.ok()) {
      create_bucket_status_ = StartTailingStream();
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

  {
    std::lock_guard<std::mutex> lk(files_to_delete_mutex_);
    using std::swap;
    for (auto& e : files_to_delete_) {
      GetJobExecutor()->CancelJob(e.second.get());
    }
    files_to_delete_.clear();
  }

  StopPurger();
  if (tid_ && tid_->joinable()) {
    tid_->join();
  }
}

Status AwsEnv::StartTailingStream() {
  if (tid_) {
    return Status::Busy("Tailer already started");
  }

  // create tailer thread
  auto lambda = [this]() { cloud_log_controller_->TailStream(); };
  tid_.reset(new std::thread(lambda));

  return Status::OK();
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

// Ability to read a file directly from cloud storage
Status AwsEnv::NewSequentialFileCloud(const std::string& bucket,
                                      const std::string& fname,
                                      unique_ptr<SequentialFile>* result,
                                      const EnvOptions& options) {
  assert(status().ok());
  unique_ptr<S3ReadableFile> file;
  Status st = NewS3ReadableFile(bucket, fname, &file);
  if (!st.ok()) {
    return st;
  }

  result->reset(dynamic_cast<SequentialFile*>(file.release()));
  return st;
}

// open a file for sequential reading
Status AwsEnv::NewSequentialFile(const std::string& logical_fname,
                                 unique_ptr<SequentialFile>* result,
                                 const EnvOptions& options) {
  assert(status().ok());
  result->reset();

  auto fname = RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       manifest = (file_type == RocksDBFileType::kManifestFile),
       identity = (file_type == RocksDBFileType::kIdentityFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

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
        if (HasDestBucket()) {
          st = GetObject(GetDestBucketName(), destname(fname), fname);
        }
        if (!st.ok() && HasSrcBucket() && !SrcMatchesDest()) {
          st = GetObject(GetSrcBucketName(), srcname(fname), fname);
        }
        if (st.ok()) {
          // we successfully copied the file, try opening it locally now
          st = base_env_->NewSequentialFile(fname, result, options);
        }
      } else {
        unique_ptr<S3ReadableFile> file;
        if (!st.ok() && HasDestBucket()) {  // read from destination S3
          st = NewS3ReadableFile(GetDestBucketName(), destname(fname), &file);
        }
        if (!st.ok() && HasSrcBucket()) {  // read from src bucket
          st = NewS3ReadableFile(GetSrcBucketName(), srcname(fname), &file);
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
    st = cloud_log_controller_->status();
    if (st.ok()) {
      // map  pathname to cache dir
      std::string pathname = CloudLogController::GetCachePath(
          cloud_log_controller_->GetCacheDir(), Slice(fname));
      Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
          "[Kinesis] NewSequentialFile logfile %s %s", pathname.c_str(), "ok");

      auto lambda = [this, pathname, &result, options]() -> Status {
        return base_env_->NewSequentialFile(pathname, result, options);
      };
      return CloudLogController::Retry(this, lambda);
    }
  }

  // This is neither a sst file or a log file. Read from default env.
  return base_env_->NewSequentialFile(fname, result, options);
}

// open a file for random reading
Status AwsEnv::NewRandomAccessFile(const std::string& logical_fname,
                                   unique_ptr<RandomAccessFile>* result,
                                   const EnvOptions& options) {
  assert(status().ok());
  result->reset();

  auto fname = RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       manifest = (file_type == RocksDBFileType::kManifestFile),
       identity = (file_type == RocksDBFileType::kIdentityFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

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
        if (HasDestBucket()) {
          st = GetObject(GetDestBucketName(), destname(fname), fname);
        }
        if (!st.ok() && HasSrcBucket() && !SrcMatchesDest()) {
          st = GetObject(GetSrcBucketName(), srcname(fname), fname);
        }
        if (st.ok()) {
          // we successfully copied the file, try opening it locally now
          st = base_env_->NewRandomAccessFile(fname, result, options);
        }
      }
      // If we are being paranoic, then we validate that our file size is
      // the same as in cloud storage.
      if (st.ok() && sstfile && cloud_env_options.validate_filesize) {
        uint64_t remote_size = 0;
        uint64_t local_size = 0;
        Status stax = base_env_->GetFileSize(fname, &local_size);
        if (!stax.ok()) {
          return stax;
        }
        stax = Status::NotFound();
        if (HasDestBucket()) {
          stax = HeadObject(GetDestBucketName(), destname(fname), nullptr,
                            &remote_size, nullptr);
        }
        if (stax.IsNotFound() && HasSrcBucket()) {
          stax = HeadObject(GetSrcBucketName(), srcname(fname), nullptr,
                            &remote_size, nullptr);
        }
        if (stax.IsNotFound() && !HasDestBucket()) {
          // It is legal for file to not be present in S3 if destination bucket
          // is not set.
        } else if (!stax.ok() || remote_size != local_size) {
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
      if (!st.ok() && HasDestBucket()) {
        st = NewS3ReadableFile(GetDestBucketName(), destname(fname), &file);
      }
      if (!st.ok() && HasSrcBucket()) {
        st = NewS3ReadableFile(GetSrcBucketName(), srcname(fname), &file);
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
    st = cloud_log_controller_->status();
    if (st.ok()) {
      // map  pathname to cache dir
      std::string pathname = CloudLogController::GetCachePath(
          cloud_log_controller_->GetCacheDir(), Slice(fname));
      Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
          "[kinesis] NewRandomAccessFile logfile %s %s", pathname.c_str(),
          "ok");

      auto lambda = [this, pathname, &result, options]() -> Status {
        return base_env_->NewRandomAccessFile(pathname, result, options);
      };
      return CloudLogController::Retry(this, lambda);
    }
  }

  // This is neither a sst file or a log file. Read from default env.
  return base_env_->NewRandomAccessFile(fname, result, options);
}

// create a new file for writing
Status AwsEnv::NewWritableFile(const std::string& logical_fname,
                               unique_ptr<WritableFile>* result,
                               const EnvOptions& options) {
  assert(status().ok());
  result->reset();

  auto fname = RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       manifest = (file_type == RocksDBFileType::kManifestFile),
       identity = (file_type == RocksDBFileType::kIdentityFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  Status s;

  if (HasDestBucket() && (sstfile || identity || manifest)) {
    unique_ptr<S3WritableFile> f(
        new S3WritableFile(this, fname, GetDestBucketName(), destname(fname),
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
    std::unique_ptr<CloudLogWritableFile> f(
        cloud_log_controller_->CreateWritableFile(fname, options));
    if (!f || !f->status().ok()) {
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
Status AwsEnv::FileExists(const std::string& logical_fname) {
  assert(status().ok());
  Status st;

  auto fname = RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       manifest = (file_type == RocksDBFileType::kManifestFile),
       identity = (file_type == RocksDBFileType::kIdentityFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  if (sstfile || manifest || identity) {
    // We read first from local storage and then from cloud storage.
    st = base_env_->FileExists(fname);
    if (st.IsNotFound() && HasDestBucket()) {
      st = ExistsObject(GetDestBucketName(), destname(fname));
    }
    if (!st.ok() && HasSrcBucket()) {
      st = ExistsObject(GetSrcBucketName(), srcname(fname));
    }
  } else if (logfile && !cloud_env_options.keep_local_log_files) {
    // read from Kinesis
    st = cloud_log_controller_->status();
    if (st.ok()) {
      // map  pathname to cache dir
      std::string pathname = CloudLogController::GetCachePath(
          cloud_log_controller_->GetCacheDir(), Slice(fname));
      Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
          "[kinesis] FileExists logfile %s %s", pathname.c_str(), "ok");

      auto lambda = [this, pathname]() -> Status {
        return base_env_->FileExists(pathname);
      };
      st = CloudLogController::Retry(this, lambda);
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
                                 const std::string& bucket,
                                 std::vector<std::string>* result) {
  assert(status().ok());

  // S3 paths don't start with '/'
  auto prefix = ltrim_if(path, '/');
  // S3 paths better end with '/', otherwise we might also get a list of files
  // in a directory for which our path is a prefix
  prefix = ensure_ends_with_pathsep(std::move(prefix));
  // the starting object marker
  Aws::String marker;
  bool loop = true;

  // get info of bucket+object
  while (loop) {
    Aws::S3::Model::ListObjectsRequest request;
    request.SetBucket(ToAwsString(bucket));
    request.SetMaxKeys(50);
    request.SetPrefix(ToAwsString(prefix));
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
      auto fname = keystr.substr(prefix.size());
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

Status AwsEnv::HeadObject(const std::string& bucket,
                          const std::string& path,
                          Aws::Map<Aws::String, Aws::String>* metadata,
                          uint64_t* size, uint64_t* modtime) {
  Aws::S3::Model::HeadObjectRequest request;
  request.SetBucket(ToAwsString(bucket));
  request.SetKey(ToAwsString(path));

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

Status AwsEnv::NewS3ReadableFile(const std::string& bucket,
                                 const std::string& fname,
                                 unique_ptr<S3ReadableFile>* result) {
  // First, check if the file exists and also find its size. We use size in
  // S3ReadableFile to make sure we always read the valid ranges of the file
  uint64_t size;
  Status st = HeadObject(bucket, fname, nullptr, &size, nullptr);
  if (!st.ok()) {
    return st;
  }
  result->reset(new S3ReadableFile(this, bucket, fname, size));
  return Status::OK();
}

//
// Deletes all the objects with the specified path prefix in our bucket
//
Status AwsEnv::EmptyBucket(const std::string& bucket,
                           const std::string& s3_object_prefix) {
  std::vector<std::string> results;

  // Get all the objects in the  bucket
  Status st = GetChildrenFromS3(s3_object_prefix, bucket, &results);
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
    st = DeletePathInS3(bucket, path);
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
  if (HasSrcBucket()) {
    st = GetChildrenFromS3(GetSrcObjectPath(), GetSrcBucketName(), result);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[s3] GetChildren src bucket %s %s error from S3 %s",
          GetSrcBucketName().c_str(), path.c_str(), st.ToString().c_str());
      return st;
    }
  }
  if (HasDestBucket() && !SrcMatchesDest()) {
    st = GetChildrenFromS3(GetDestObjectPath(), GetDestBucketName(), result);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[s3] GetChildren dest bucket %s %s error from S3 %s",
          GetDestBucketName().c_str(), path.c_str(), st.ToString().c_str());
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

  // Remove all results that are not supposed to be visible.
  result->erase(
      std::remove_if(result->begin(), result->end(),
                     [&](const std::string& f) {
                       auto noepoch = RemoveEpoch(f);
                       if (!IsSstFile(noepoch) && !IsManifestFile(noepoch)) {
                         return false;
                       }
                       return RemapFilename(noepoch) != f;
                     }),
      result->end());
  // Remove the epoch, remap into RocksDB's domain
  for (size_t i = 0; i < result->size(); ++i) {
    auto noepoch = RemoveEpoch(result->at(i));
    if (IsSstFile(noepoch) || IsManifestFile(noepoch)) {
      // remap sst and manifest files
      result->at(i) = noepoch;
    }
  }
  // remove duplicates
  std::sort(result->begin(), result->end());
  result->erase(std::unique(result->begin(), result->end()), result->end());

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[s3] GetChildren %s successfully returned %d files", path.c_str(),
      result->size());
  return Status::OK();
}

void AwsEnv::RemoveFileFromDeletionQueue(const std::string& filename) {
  std::lock_guard<std::mutex> lk(files_to_delete_mutex_);
  auto itr = files_to_delete_.find(filename);
  if (itr != files_to_delete_.end()) {
    GetJobExecutor()->CancelJob(itr->second.get());
    files_to_delete_.erase(itr);
  }
}

Status AwsEnv::DeleteFile(const std::string& logical_fname) {
  assert(status().ok());

  auto fname = RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       manifest = (file_type == RocksDBFileType::kManifestFile),
       identity = (file_type == RocksDBFileType::kIdentityFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  if (manifest) {
    // We don't delete manifest files. The reason for this is that even though
    // RocksDB creates manifest with different names (like MANIFEST-00001,
    // MANIFEST-00008) we actually map all of them to the same filename
    // MANIFEST-[epoch].
    // When RocksDB wants to roll the MANIFEST (let's say from 1 to 8) it does
    // the following:
    // 1. Create a new MANIFEST-8
    // 2. Write everything into MANIFEST-8
    // 3. Sync MANIFEST-8
    // 4. Store "MANIFEST-8" in CURRENT file
    // 5. Delete MANIFEST-1
    //
    // What RocksDB cloud does behind the scenes (the numbers match the list
    // above):
    // 1. Create manifest file MANIFEST-[epoch].tmp
    // 2. Forward RocksDB writes to the file created in the first step
    // 3. Atomic rename from MANIFEST-[epoch].tmp to MANIFEST-[epoch]. The old
    // file with the same file name is overwritten.
    // 4. Nothing. Whatever the contents of CURRENT file, we don't care, we
    // always remap MANIFEST files to the correct with the latest epoch.
    // 5. Also nothing. There is no file to delete, because we have overwritten
    // it in the third step.
    return Status::OK();
  }

  Status st;
  // Delete from destination bucket and local dir
  if (sstfile || manifest || identity) {
    if (HasDestBucket()) {
      // add the remote file deletion to the queue
      st = DeleteCloudFileFromDest(basename(fname));
    }
    // delete from local, too. Ignore the result, though. The file might not be
    // there locally.
    base_env_->DeleteFile(fname);
  } else if (logfile && !cloud_env_options.keep_local_log_files) {
    // read from Kinesis
    st = cloud_log_controller_->status();
    if (st.ok()) {
      // Log a Delete record to kinesis stream
      std::unique_ptr<CloudLogWritableFile> f(
          cloud_log_controller_->CreateWritableFile(fname, EnvOptions()));
      if (!f || !f->status().ok()) {
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
}

Status AwsEnv::DeleteCloudFileFromDest(const std::string& fname) {
  assert(HasDestBucket());
  auto base = basename(fname);
  // add the job to delete the file in 1 hour
  auto doDeleteFile = [this, base]() {
    {
      std::lock_guard<std::mutex> lk(files_to_delete_mutex_);
      auto itr = files_to_delete_.find(base);
      if (itr == files_to_delete_.end()) {
        // File was removed from files_to_delete_, do not delete!
        return;
      }
      files_to_delete_.erase(itr);
    }
    auto path = GetDestObjectPath() + "/" + base;
    // we are ready to delete the file!
    auto st = DeletePathInS3(GetDestBucketName(), path);
    if (!st.ok() && !st.IsNotFound()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[s3] DeleteFile DeletePathInS3 file %s error %s", path.c_str(),
          st.ToString().c_str());
    }
  };
  {
    std::lock_guard<std::mutex> lk(files_to_delete_mutex_);
    if (files_to_delete_.find(base) != files_to_delete_.end()) {
      // already in the queue
      return Status::OK();
    }
  }
  {
    std::lock_guard<std::mutex> lk(files_to_delete_mutex_);
    auto handle = GetJobExecutor()->ScheduleJob(
        std::chrono::steady_clock::now() + file_deletion_delay_,
        std::move(doDeleteFile));
    files_to_delete_.emplace(base, std::move(handle));
  }
  return Status::OK();
}

//
// Delete the specified path from S3
//
Status AwsEnv::DeletePathInS3(const std::string& bucket,
                              const std::string& fname) {
  assert(status().ok());
  Status st;

  // The filename is the same as the object name in the bucket
  Aws::String object = ToAwsString(fname);

  // create request
  Aws::S3::Model::DeleteObjectRequest request;
  request.SetBucket(ToAwsString(bucket));
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

Status AwsEnv::GetFileSize(const std::string& logical_fname, uint64_t* size) {
  assert(status().ok());
  *size = 0L;

  auto fname = RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  Status st;
  if (sstfile) {
    if (base_env_->FileExists(fname).ok()) {
      st = base_env_->GetFileSize(fname, size);
    } else {
      st = Status::NotFound();
      // Get file length from S3
      if (HasDestBucket()) {
        st = HeadObject(GetDestBucketName(), destname(fname), nullptr, size,
                        nullptr);
      }
      if (st.IsNotFound() && HasSrcBucket()) {
        st = HeadObject(GetSrcBucketName(), srcname(fname), nullptr, size,
                        nullptr);
      }
    }
  } else if (logfile && !cloud_env_options.keep_local_log_files) {
    st = cloud_log_controller_->status();
    if (st.ok()) {
      // map  pathname to cache dir
      std::string pathname = CloudLogController::GetCachePath(
          cloud_log_controller_->GetCacheDir(), Slice(fname));
      Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
          "[kinesis] GetFileSize logfile %s %s", pathname.c_str(), "ok");

      auto lambda = [this, pathname, size]() -> Status {
        return base_env_->GetFileSize(pathname, size);
      };
      st = CloudLogController::Retry(this, lambda);
    }
  } else {
    st = base_env_->GetFileSize(fname, size);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[aws] GetFileSize src '%s' %s %ld",
      fname.c_str(), st.ToString().c_str(), *size);
  return st;
}

Status AwsEnv::GetFileModificationTime(const std::string& logical_fname,
                                       uint64_t* time) {
  assert(status().ok());
  *time = 0;

  auto fname = RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  Status st;
  if (sstfile) {
    if (base_env_->FileExists(fname).ok()) {
      st = base_env_->GetFileModificationTime(fname, time);
    } else {
      st = Status::NotFound();
      if (HasDestBucket()) {
        st = HeadObject(GetDestBucketName(), destname(fname), nullptr,
                        nullptr, time);
      }
      if (st.IsNotFound() && HasSrcBucket()) {
        st = HeadObject(GetSrcBucketName(), srcname(fname), nullptr, nullptr,
                        time);
      }
    }
  } else if (logfile && !cloud_env_options.keep_local_log_files) {
    st = cloud_log_controller_->status();
    if (st.ok()) {
      // map  pathname to cache dir
      std::string pathname = CloudLogController::GetCachePath(
          cloud_log_controller_->GetCacheDir(), Slice(fname));
      Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
          "[kinesis] GetFileModificationTime logfile %s %s", pathname.c_str(),
          "ok");

      auto lambda = [this, pathname, time]() -> Status {
        return base_env_->GetFileModificationTime(pathname, time);
      };
      st = CloudLogController::Retry(this, lambda);
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
Status AwsEnv::RenameFile(const std::string& logical_src,
                          const std::string& logical_target) {
  assert(status().ok());

  auto src = RemapFilename(logical_src);
  auto target = RemapFilename(logical_target);
  // Get file type of target
  auto file_type = GetFileType(target);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       manifest = (file_type == RocksDBFileType::kManifestFile),
       identity = (file_type == RocksDBFileType::kIdentityFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

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
  } else if (manifest) {
    // Rename should never be called on manifest files as well
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[aws] RenameFile source manifest %s %s is not supported", src.c_str(),
        target.c_str());
    assert(0);
    return Status::NotSupported(Slice(src), Slice(target));

  } else if (!identity || !HasDestBucket()) {
    return base_env_->RenameFile(src, target);
  }
  // Only ID file should come here
  assert(identity);
  assert(HasDestBucket());
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

Status AwsEnv::LinkFile(const std::string& src, const std::string& target) {
  // We only know how to link file if both src and dest buckets are empty
  if (HasDestBucket() || HasSrcBucket()) {
    return Status::NotSupported();
  }
  auto src_remapped = RemapFilename(src);
  auto target_remapped = RemapFilename(target);
  return base_env_->LinkFile(src_remapped, target_remapped);
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
    st = PutObject(localfile, GetDestBucketName(), idfile);
  }

  // Save mapping from ID to cloud pathname
  if (st.ok() && !GetDestObjectPath().empty()) {
    st = SaveDbid(GetDestBucketName(), dbid, GetDestObjectPath());
  }
  return st;
}

//
// All db in a bucket are stored in path /.rockset/dbid/<dbid>
// The value of the object is the pathname where the db resides.
//
Status AwsEnv::SaveDbid(const std::string& bucket_name, const std::string& dbid,
                        const std::string& dirname) {
  assert(status().ok());
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[s3] SaveDbid dbid %s dir '%s'",
      dbid.c_str(), dirname.c_str());

  std::string dbidkey = dbid_registry_ + dbid;
  Aws::String s3_bucket = ToAwsString(bucket_name);
  Aws::String key = ToAwsString(dbidkey);

  Aws::Map<Aws::String, Aws::String> metadata;
  metadata[ToAwsString("dirname")] = ToAwsString(dirname);

  // create request
  Aws::S3::Model::PutObjectRequest put_request;
  put_request.SetBucket(s3_bucket);
  put_request.SetKey(key);
  put_request.SetMetadata(metadata);
  SetEncryptionParameters(cloud_env_options, put_request);

  Aws::S3::Model::PutObjectOutcome put_outcome =
      s3client_->PutObject(put_request);
  bool isSuccess = put_outcome.IsSuccess();
  if (!isSuccess) {
    const Aws::Client::AWSError<Aws::S3::S3Errors>& error =
        put_outcome.GetError();
    std::string errmsg(error.GetMessage().c_str(), error.GetMessage().size());
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[s3] Bucket %s SaveDbid error in saving dbid %s dirname %s %s",
        bucket_name.c_str(), dbid.c_str(), dirname.c_str(), errmsg.c_str());
    return Status::IOError(dirname, errmsg.c_str());
  }
  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "[s3] Bucket %s SaveDbid dbid %s dirname %s %s", bucket_name.c_str(),
      dbid.c_str(), dirname.c_str(), "ok");
  return Status::OK();
};

//
// Given a dbid, retrieves its pathname.
//
Status AwsEnv::GetPathForDbid(const std::string& bucket,
                              const std::string& dbid, std::string* dirname) {
  std::string dbidkey = dbid_registry_ + dbid;

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[s3] Bucket %s GetPathForDbid dbid %s", bucket.c_str(),
      dbid.c_str());

  Aws::Map<Aws::String, Aws::String> metadata;
  Status st = HeadObject(bucket, dbidkey, &metadata);
  if (!st.ok()) {
    if (st.IsNotFound()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[s3] %s GetPathForDbid error non-existent dbid %s %s",
          bucket.c_str(), dbid.c_str(), st.ToString().c_str());
    } else {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[s3] %s GetPathForDbid error dbid %s %s", bucket.c_str(),
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
      bucket.c_str(), dbid.c_str(), st.ToString().c_str());
  return st;
}

//
// Retrieves the list of all registered dbids and their paths
//
Status AwsEnv::GetDbidList(const std::string& bucket, DbidList* dblist) {

  // fetch the list all all dbids
  std::vector<std::string> dbid_list;
  Status st = GetChildrenFromS3(dbid_registry_, bucket, &dbid_list);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[s3] %s GetDbidList error in GetChildrenFromS3 %s", bucket.c_str(),
        st.ToString().c_str());
    return st;
  }
  // for each dbid, fetch the db directory where the db data should reside
  for (auto dbid : dbid_list) {
    std::string dirname;
    st = GetPathForDbid(bucket, dbid, &dirname);
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
Status AwsEnv::DeleteDbid(const std::string& bucket,
                          const std::string& dbid) {

  // fetch the list all all dbids
  std::string dbidkey = dbid_registry_ + dbid;
  Status st = DeletePathInS3(bucket, dbidkey);
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[s3] %s DeleteDbid DeleteDbid(%s) %s", bucket.c_str(), dbid.c_str(),
      st.ToString().c_str());
  return st;
}

// Returns a list of all objects that start with the specified
// prefix and are stored in the bucket.
Status AwsEnv::ListObjects(const std::string& bucket_name,
                           const std::string& object_path,
                           BucketObjectMetadata* meta) {
  return GetChildrenFromS3(object_path, bucket_name,
                           &meta->pathnames);
}

// Deletes the specified object from cloud storage
Status AwsEnv::DeleteObject(const std::string& bucket_name,
                            const std::string& object_path) {
  return DeletePathInS3(bucket_name, object_path);
}

// Delete the specified object from the specified cloud bucket
Status AwsEnv::ExistsObject(const std::string& bucket_name,
                            const std::string& object_path) {
  return HeadObject(bucket_name, object_path);
}

// Return size of cloud object
Status AwsEnv::GetObjectSize(const std::string& bucket_name,
                             const std::string& object_path,
                             uint64_t* filesize) {
  return HeadObject(bucket_name, object_path, nullptr, filesize,
                    nullptr);
}

// Copy the specified cloud object from one location in the cloud
// storage to another location in cloud storage
Status AwsEnv::CopyObject(const std::string& bucket_name_src,
                          const std::string& object_path_src,
                          const std::string& bucket_name_dest,
                          const std::string& object_path_dest) {
  Status st;
  Aws::String src_bucket = ToAwsString(bucket_name_src);
  Aws::String dest_bucket = ToAwsString(bucket_name_dest);

  // The filename is the same as the object name in the bucket
  Aws::String src_object = ToAwsString(object_path_src);
  Aws::String dest_object = ToAwsString(object_path_dest);

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

AwsEnv::GetObjectResult AwsEnv::DoGetObject(const Aws::String& bucket,
                                            const Aws::String& key,
                                            const std::string& destination) {
  Aws::S3::Model::GetObjectRequest getObjectRequest;
  getObjectRequest.SetBucket(bucket);
  getObjectRequest.SetKey(key);

  getObjectRequest.SetResponseStreamFactory([destination]() {
    return Aws::New<Aws::FStream>(Aws::Utils::ARRAY_ALLOCATION_TAG, destination,
                                  std::ios_base::out);
  });
  auto getOutcome = s3client_->GetObject(getObjectRequest);
  GetObjectResult result;
  result.success = getOutcome.IsSuccess();
  if (!result.success) {
    result.error = getOutcome.GetError();
  } else {
    result.objectSize = getOutcome.GetResult().GetContentLength();
  }
  return result;
}

AwsEnv::GetObjectResult AwsEnv::DoGetObjectWithTransferManager(
    const Aws::String& bucket, const Aws::String& key,
    const std::string& destination) {
  CloudRequestCallbackGuard guard(
      cloud_env_options.cloud_request_callback.get(),
      CloudRequestOpType::kReadOp);

  auto transferHandle =
      awsTransferManager_->DownloadFile(bucket, key, ToAwsString(destination));

  transferHandle->WaitUntilFinished();

  GetObjectResult result;
  result.success =
      transferHandle->GetStatus() == Aws::Transfer::TransferStatus::COMPLETED;
  if (!result.success) {
    result.error = transferHandle->GetLastError();
  } else {
    result.objectSize = transferHandle->GetBytesTotalSize();
  }

  guard.SetSize(result.objectSize);
  guard.SetSuccess(result.success);
  return result;
}

Status AwsEnv::GetObject(const std::string& bucket_name,
                         const std::string& object_path,
                         const std::string& local_destination) {
  Env* localenv = GetBaseEnv();
  std::string tmp_destination = local_destination + ".tmp";

  GetObjectResult result;
  if (cloud_env_options.use_aws_transfer_manager) {
    result = DoGetObjectWithTransferManager(ToAwsString(bucket_name), ToAwsString(object_path), tmp_destination);
  } else {
    result = DoGetObject(ToAwsString(bucket_name), ToAwsString(object_path), tmp_destination);
  }

  if (!result.success) {
    localenv->DeleteFile(tmp_destination);
    const auto& error = result.error;
    std::string errmsg(error.GetMessage().c_str(), error.GetMessage().size());
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[s3] GetObject %s/%s error %s.", bucket_name.c_str(),
        object_path.c_str(), errmsg.c_str());
    auto errorType = error.GetErrorType();
    if (errorType == Aws::S3::S3Errors::NO_SUCH_BUCKET ||
        errorType == Aws::S3::S3Errors::NO_SUCH_KEY ||
        errorType == Aws::S3::S3Errors::RESOURCE_NOT_FOUND) {
      return Status::NotFound(std::move(errmsg));
    }
    return Status::IOError(std::move(errmsg));
  }

  // Check if our local file is the same as S3 promised
  uint64_t file_size{0};
  auto s = localenv->GetFileSize(tmp_destination, &file_size);
  if (!s.ok()) {
      return s;
  }
  if (file_size != result.objectSize) {
    localenv->DeleteFile(tmp_destination);
    s = Status::IOError("Partial download of a file " + local_destination);
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[s3] GetObject %s/%s local size %ld != cloud size "
        "%ld. %s",
        bucket_name.c_str(), object_path.c_str(), file_size,
        result.objectSize, s.ToString().c_str());
  }

  if (s.ok()) {
    s = localenv->RenameFile(tmp_destination, local_destination);
  }
  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "[s3] GetObject %s/%s size %ld. %s", bucket_name.c_str(),
      object_path.c_str(), file_size, s.ToString().c_str());
  return s;
}

AwsEnv::PutObjectResult AwsEnv::DoPutObject(const std::string& filename,
                                            const Aws::String& bucket,
                                            const Aws::String& key,
                                            uint64_t sizeHint) {
  auto inputData = Aws::MakeShared<Aws::FStream>(
      key.c_str(), filename.c_str(), std::ios_base::in | std::ios_base::out);

  Aws::S3::Model::PutObjectRequest putRequest;
  putRequest.SetBucket(bucket);
  putRequest.SetKey(key);
  putRequest.SetBody(inputData);
  SetEncryptionParameters(cloud_env_options,  putRequest);

  auto putOutcome = s3client_->PutObject(putRequest, sizeHint);
  PutObjectResult result;
  result.success = putOutcome.IsSuccess();
  if (!result.success) {
    result.error = putOutcome.GetError();
  }
  return result;
}

AwsEnv::PutObjectResult AwsEnv::DoPutObjectWithTransferManager(
    const std::string& filename, const Aws::String& bucket,
    const Aws::String& key, uint64_t sizeHint) {
  CloudRequestCallbackGuard guard(
      cloud_env_options.cloud_request_callback.get(),
      CloudRequestOpType::kWriteOp, sizeHint);

  auto transferHandle = awsTransferManager_->UploadFile(
      ToAwsString(filename), bucket, key, Aws::DEFAULT_CONTENT_TYPE,
      Aws::Map<Aws::String, Aws::String>());

  transferHandle->WaitUntilFinished();

  PutObjectResult result;
  result.success =
      transferHandle->GetStatus() == Aws::Transfer::TransferStatus::COMPLETED;
  if (!result.success) {
    result.error = transferHandle->GetLastError();
  }

  guard.SetSuccess(result.success);
  return result;
}

Status AwsEnv::PutObject(const std::string& local_file,
                         const std::string& bucket_name,
                         const std::string& object_path) {
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

  auto s3_bucket = ToAwsString(bucket_name);
  PutObjectResult result;
  if (cloud_env_options.use_aws_transfer_manager) {
    result = DoPutObjectWithTransferManager(
        local_file, s3_bucket, ToAwsString(object_path), fsize);
  } else {
    result = DoPutObject(local_file, s3_bucket, ToAwsString(object_path),
                         fsize);
  }
  if (!result.success) {
    auto error = result.error;
    std::string errmsg(error.GetMessage().c_str(), error.GetMessage().size());
    st = Status::IOError(local_file, errmsg);
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[s3] PutObject %s/%s, size %zu, ERROR %s", s3_bucket.c_str(),
        object_path.c_str(), fsize, errmsg.c_str());
  } else {
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[s3] PutObject %s/%s, size %zu, OK", s3_bucket.c_str(),
        object_path.c_str(), fsize);
  }

  return st;
}

//
// prepends the configured src object path name
//
std::string AwsEnv::srcname(const std::string& localname) {
  assert(cloud_env_options.src_bucket.IsValid());
  return cloud_env_options.src_bucket.GetObjectPath() + "/" + basename(localname);
}

//
// prepends the configured dest object path name
//
std::string AwsEnv::destname(const std::string& localname) {
  assert(cloud_env_options.dest_bucket.IsValid());
  return cloud_env_options.dest_bucket.GetObjectPath() + "/" + basename(localname);
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
Status AwsEnv::NewAwsEnv(Env* base_env,
                         const CloudEnvOptions& cloud_options,
                         const std::shared_ptr<Logger> & info_log, CloudEnv** cenv) {
  Status status;
  *cenv = nullptr;
  // If underlying env is not defined, then use PosixEnv
  if (!base_env) {
    base_env = Env::Default();
  }
  unique_ptr<AwsEnv> aenv(new AwsEnv(base_env, cloud_options, info_log));
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

std::string AwsEnv::GetWALCacheDir() {
  return cloud_log_controller_->GetCacheDir();
}

//
// Keep retrying the command until it is successful or the timeout has expired
//
Status CloudLogController::Retry(Env* env, RetryType func) {
  Status stat;
  std::chrono::microseconds start(env->NowMicros());

  while (true) {
    // If command is successful, return immediately
    stat = func();
    if (stat.ok()) {
      break;
    }
    // sleep for some time
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // If timeout has expired, return error
    std::chrono::microseconds now(env->NowMicros());
    if (start + CloudLogController::kRetryPeriod < now) {
      stat = Status::TimedOut();
      break;
    }
  }
  return stat;
}

#pragma GCC diagnostic pop
}  // namespace rocksdb

#endif
