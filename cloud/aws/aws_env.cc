//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#include "cloud/aws/aws_env.h"

#include <unistd.h>

#include <chrono>
#include <cinttypes>
#include <fstream>
#include <iostream>
#include <memory>
#include <set>

#include "cloud/cloud_log_controller_impl.h"
#include "cloud/cloud_storage_provider_impl.h"
#include "cloud/filename.h"
#include "port/port_posix.h"
#include "rocksdb/cloud/cloud_log_controller.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"

#ifdef USE_AWS
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#endif

#include "cloud/aws/aws_file.h"
#include "cloud/db_cloud_impl.h"

namespace rocksdb {

static const std::unordered_map<std::string, AwsAccessType> AwsAccessTypeMap = {
    {"undefined", AwsAccessType::kUndefined},
    {"simple", AwsAccessType::kSimple},
    {"instance", AwsAccessType::kInstance},
    {"EC2", AwsAccessType::kInstance},
    {"environment", AwsAccessType::kEnvironment},
    {"config", AwsAccessType::kConfig},
    {"anonymous", AwsAccessType::kAnonymous},
};

template <typename T>
bool ParseEnum(const std::unordered_map<std::string, T>& type_map,
               const std::string& type, T* value) {
  auto iter = type_map.find(type);
  if (iter != type_map.end()) {
    *value = iter->second;
    return true;
  }
  return false;
}

AwsAccessType AwsCloudAccessCredentials::GetAccessType() const {
  if (type != AwsAccessType::kUndefined) {
    return type;
  } else if (!config_file.empty()) {
    return AwsAccessType::kConfig;
  } else if (!access_key_id.empty() || !secret_key.empty()) {
    return AwsAccessType::kSimple;
  }
  return AwsAccessType::kUndefined;
}

Status AwsCloudAccessCredentials::TEST_Initialize() {
  std::string type_str;
  if (CloudEnvOptions::GetNameFromEnvironment(
          "ROCKSDB_AWS_ACCESS_TYPE", "rocksdb_aws_access_type", &type_str)) {
    ParseEnum<AwsAccessType>(AwsAccessTypeMap, type_str, &type);
  }
  return HasValid();
}

Status AwsCloudAccessCredentials::CheckCredentials(
    const AwsAccessType& aws_type) const {
#ifndef USE_AWS
  (void)aws_type;
  return Status::NotSupported("AWS not supported");
#else
  if (aws_type == AwsAccessType::kSimple) {
    if ((access_key_id.empty() && getenv("AWS_ACCESS_KEY_ID") == nullptr) ||
        (secret_key.empty() && getenv("AWS_SECRET_ACCESS_KEY") == nullptr)) {
      return Status::InvalidArgument(
          "AWS Credentials require both access ID and secret keys");
    }
  } else if (aws_type == AwsAccessType::kTaskRole) {
    return Status::InvalidArgument(
        "AWS access type: Task Role access is not supported.");
  }
  return Status::OK();
#endif
}

void AwsCloudAccessCredentials::InitializeSimple(
    const std::string& aws_access_key_id, const std::string& aws_secret_key) {
  type = AwsAccessType::kSimple;
  access_key_id = aws_access_key_id;
  secret_key = aws_secret_key;
}

void AwsCloudAccessCredentials::InitializeConfig(
    const std::string& aws_config_file) {
  type = AwsAccessType::kConfig;
  config_file = aws_config_file;
}

Status AwsCloudAccessCredentials::HasValid() const {
  AwsAccessType aws_type = GetAccessType();
  Status status = CheckCredentials(aws_type);
  return status;
}

Status AwsCloudAccessCredentials::GetCredentialsProvider(
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider>* result) const {
  result->reset();

  AwsAccessType aws_type = GetAccessType();
  Status status = CheckCredentials(aws_type);
  if (status.ok()) {
    switch (aws_type) {
#ifdef USE_AWS
      case AwsAccessType::kSimple: {
        const char* access_key =
            (access_key_id.empty() ? getenv("AWS_ACCESS_KEY_ID")
                                   : access_key_id.c_str());
        const char* secret =
            (secret_key.empty() ? getenv("AWS_SECRET_ACCESS_KEY")
                                : secret_key.c_str());
        result->reset(
            new Aws::Auth::SimpleAWSCredentialsProvider(access_key, secret));
        break;
      }
      case AwsAccessType::kConfig:
        if (!config_file.empty()) {
          result->reset(new Aws::Auth::ProfileConfigFileAWSCredentialsProvider(
              config_file.c_str()));
        } else {
          result->reset(
              new Aws::Auth::ProfileConfigFileAWSCredentialsProvider());
        }
        break;
      case AwsAccessType::kInstance:
        result->reset(new Aws::Auth::InstanceProfileCredentialsProvider());
        break;
      case AwsAccessType::kAnonymous:
        result->reset(new Aws::Auth::AnonymousAWSCredentialsProvider());
        break;
      case AwsAccessType::kEnvironment:
        result->reset(new Aws::Auth::EnvironmentAWSCredentialsProvider());
        break;
      case AwsAccessType::kUndefined:
        // Use AWS SDK's default credential chain
        result->reset();
        break;
#endif
      default:
        status = Status::NotSupported("AWS credentials type not supported");
        break;  // not supported
    }
  }
  return status;
}

#ifdef USE_AWS
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
  std::shared_ptr<JobHandle> ScheduleJob(
      std::chrono::steady_clock::time_point time,
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

std::shared_ptr<JobHandle> JobExecutor::ScheduleJob(
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

//
// The AWS credentials are specified to the constructor via
// access_key_id and secret_key.
//
AwsEnv::AwsEnv(Env* underlying_env, const CloudEnvOptions& _cloud_env_options,
               const std::shared_ptr<Logger>& info_log)
    : CloudEnvImpl(_cloud_env_options, underlying_env, info_log),
      rng_(time(nullptr)) {
  Aws::InitAPI(Aws::SDKOptions());
  if (cloud_env_options.src_bucket.GetRegion().empty() ||
      cloud_env_options.dest_bucket.GetRegion().empty()) {
    std::string region;
    if (!CloudEnvOptions::GetNameFromEnvironment(
            "AWS_DEFAULT_REGION", "aws_default_region", &region)) {
      region = default_region;
    }
    if (cloud_env_options.src_bucket.GetRegion().empty()) {
      cloud_env_options.src_bucket.SetRegion(region);
    }
    if (cloud_env_options.dest_bucket.GetRegion().empty()) {
      cloud_env_options.dest_bucket.SetRegion(region);
    }
  }
  base_env_ = underlying_env;
}

AwsEnv::~AwsEnv() {
  {
    std::lock_guard<std::mutex> lk(files_to_delete_mutex_);
    using std::swap;
    for (auto& e : files_to_delete_) {
      GetJobExecutor()->CancelJob(e.second.get());
    }
    files_to_delete_.clear();
  }

  StopPurger();
}

void AwsEnv::Shutdown() { Aws::ShutdownAPI(Aws::SDKOptions()); }

void AwsEnv::RemoveFileFromDeletionQueue(const std::string& filename) {
  std::lock_guard<std::mutex> lk(files_to_delete_mutex_);
  auto itr = files_to_delete_.find(filename);
  if (itr != files_to_delete_.end()) {
    GetJobExecutor()->CancelJob(itr->second.get());
    files_to_delete_.erase(itr);
  }
}

Status AwsEnv::DeleteFile(const std::string& logical_fname) {
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
    st = cloud_env_options.cloud_log_controller->status();
    if (st.ok()) {
      // Log a Delete record to kinesis stream
      std::unique_ptr<CloudLogWritableFile> f(
          cloud_env_options.cloud_log_controller->CreateWritableFile(
              fname, EnvOptions()));
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

Status AwsEnv::CopyLocalFileToDest(const std::string& local_name,
                                   const std::string& dest_name) {
  RemoveFileFromDeletionQueue(basename(local_name));
  return cloud_env_options.storage_provider->PutCloudObject(
      local_name, GetDestBucketName(), dest_name);
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
    auto st = cloud_env_options.storage_provider->DeleteCloudObject(
        GetDestBucketName(), path);
    if (!st.ok() && !st.IsNotFound()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[aws] DeleteFile DeletePathInS3 file %s error %s", path.c_str(),
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
// All db in a bucket are stored in path /.rockset/dbid/<dbid>
// The value of the object is the pathname where the db resides.
//
Status AwsEnv::SaveDbid(const std::string& bucket_name, const std::string& dbid,
                        const std::string& dirname) {
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[s3] SaveDbid dbid %s dir '%s'",
      dbid.c_str(), dirname.c_str());

  std::string dbidkey = dbid_registry_ + dbid;
  std::unordered_map<std::string, std::string> metadata;
  metadata["dirname"] = dirname;

  Status st = cloud_env_options.storage_provider->PutCloudObjectMetadata(
      bucket_name, dbidkey, metadata);

  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[aws] Bucket %s SaveDbid error in saving dbid %s dirname %s %s",
        bucket_name.c_str(), dbid.c_str(), dirname.c_str(),
        st.ToString().c_str());
  } else {
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[aws] Bucket %s SaveDbid dbid %s dirname %s %s", bucket_name.c_str(),
        dbid.c_str(), dirname.c_str(), "ok");
  }
  return st;
};

//
// Given a dbid, retrieves its pathname.
//
Status AwsEnv::GetPathForDbid(const std::string& bucket,
                              const std::string& dbid, std::string* dirname) {
  std::string dbidkey = dbid_registry_ + dbid;

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[s3] Bucket %s GetPathForDbid dbid %s", bucket.c_str(), dbid.c_str());

  CloudObjectInformation info;
  Status st = cloud_env_options.storage_provider->GetCloudObjectMetadata(
      bucket, dbidkey, &info);
  if (!st.ok()) {
    if (st.IsNotFound()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[aws] %s GetPathForDbid error non-existent dbid %s %s",
          bucket.c_str(), dbid.c_str(), st.ToString().c_str());
    } else {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[aws] %s GetPathForDbid error dbid %s %s", bucket.c_str(),
          dbid.c_str(), st.ToString().c_str());
    }
    return st;
  }

  // Find "dirname" metadata that stores the pathname of the db
  const char* kDirnameTag = "dirname";
  auto it = info.metadata.find(kDirnameTag);
  if (it != info.metadata.end()) {
    *dirname = it->second;
  } else {
    st = Status::NotFound("GetPathForDbid");
  }
  Log(InfoLogLevel::INFO_LEVEL, info_log_, "[aws] %s GetPathForDbid dbid %s %s",
      bucket.c_str(), dbid.c_str(), st.ToString().c_str());
  return st;
}

//
// Retrieves the list of all registered dbids and their paths
//
Status AwsEnv::GetDbidList(const std::string& bucket, DbidList* dblist) {
  // fetch the list all all dbids
  std::vector<std::string> dbid_list;
  Status st = cloud_env_options.storage_provider->ListCloudObjects(
      bucket, dbid_registry_, &dbid_list);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[aws] %s GetDbidList error in GetChildrenFromS3 %s", bucket.c_str(),
        st.ToString().c_str());
    return st;
  }
  // for each dbid, fetch the db directory where the db data should reside
  for (auto dbid : dbid_list) {
    std::string dirname;
    st = GetPathForDbid(bucket, dbid, &dirname);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[aws] %s GetDbidList error in GetPathForDbid(%s) %s", bucket.c_str(),
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
Status AwsEnv::DeleteDbid(const std::string& bucket, const std::string& dbid) {
  // fetch the list all all dbids
  std::string dbidkey = dbid_registry_ + dbid;
  Status st =
      cloud_env_options.storage_provider->DeleteCloudObject(bucket, dbidkey);
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[aws] %s DeleteDbid DeleteDbid(%s) %s", bucket.c_str(), dbid.c_str(),
      st.ToString().c_str());
  return st;
}

Status AwsEnv::LockFile(const std::string& /*fname*/, FileLock** lock) {
  // there isn's a very good way to atomically check and create
  // a file via libs3
  *lock = nullptr;
  return Status::OK();
}

Status AwsEnv::UnlockFile(FileLock* /*lock*/) { return Status::OK(); }

// The factory method for creating an S3 Env
Status AwsEnv::NewAwsEnv(Env* base_env, const CloudEnvOptions& cloud_options,
                         const std::shared_ptr<Logger>& info_log,
                         CloudEnv** cenv) {
  Status status;
  *cenv = nullptr;
  // If underlying env is not defined, then use PosixEnv
  if (!base_env) {
    base_env = Env::Default();
  }
  // These lines of code are likely temporary until the new configuration stuff
  // comes into play.
  CloudEnvOptions options = cloud_options;  // Make a copy
  status =
      CloudStorageProviderImpl::CreateS3Provider(&options.storage_provider);
  if (status.ok() && !cloud_options.keep_local_log_files) {
    if (cloud_options.log_type == kLogKinesis) {
      status = CloudLogControllerImpl::CreateKinesisController(
          &options.cloud_log_controller);
    } else if (cloud_options.log_type == kLogKafka) {
      status = CloudLogControllerImpl::CreateKafkaController(
          &options.cloud_log_controller);
    } else {
      status =
          Status::NotSupported("We currently only support Kinesis and Kafka");
      Log(InfoLogLevel::ERROR_LEVEL, info_log,
          "[aws] NewAwsEnv Unknown log type %d. %s", cloud_options.log_type,
          status.ToString().c_str());
    }
  }
  if (!status.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log,
        "[aws] NewAwsEnv Unable to create environment %s",
        status.ToString().c_str());
    return status;
  }
  std::unique_ptr<AwsEnv> aenv(new AwsEnv(base_env, options, info_log));
  status = aenv->Prepare();
  if (status.ok()) {
    *cenv = aenv.release();
  }
  return status;
}

std::string AwsEnv::GetWALCacheDir() {
  return cloud_env_options.cloud_log_controller->GetCacheDir();
}

#endif  // USE_AWS
}  // namespace rocksdb
