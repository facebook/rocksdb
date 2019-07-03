// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include <unistd.h>

#include "cloud/aws/aws_env.h"
#include "cloud/cloud_env_impl.h"
#include "cloud/cloud_env_wrapper.h"
#include "cloud/db_cloud_impl.h"
#include "cloud/filename.h"
#include "port/likely.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "util/file_reader_writer.h"
#include "util/filename.h"

namespace rocksdb {

bool CloudEnvOptions::GetNameFromEnvironment(const char *name, const char *alt, std::string * result) {
  
  char *value = getenv(name);               // See if name is set in the environment
  if (value == nullptr && alt != nullptr) { // Not set.  Do we have an alt name?
    value = getenv(alt);                    // See if alt is in the environment
  }
  if (value != nullptr) {                   // Did we find the either name/alt in the env?
    result->assign(value);                  // Yes, update result
    return true;                            // And return success
  } else {
    return false;                           // No, return not found
  }
}
void CloudEnvOptions::TEST_Initialize(const std::string & bucket,
				    const std::string & object,
				    const std::string & region) {
  src_bucket.TEST_Initialize(bucket, object, region);
  dest_bucket = src_bucket;
}

BucketOptions::BucketOptions() {
    prefix_ = "rockset.";
}
  
void BucketOptions::SetBucketName(const std::string & bucket,
				  const std::string & prefix) {
  if (!prefix.empty()) {
    prefix_ = prefix;
  }
  
  bucket_ = bucket;
  if (bucket_.empty()) {
    name_.clear();
  } else {
    name_ = prefix_ + bucket_;
  }
}
  

// Initializes the bucket properties

void BucketOptions::TEST_Initialize(const std::string & bucket,
				    const std::string & object,
				    const std::string & region) {
  std::string prefix;
  // If the bucket name is not set, then the bucket name is not set,
  // Set it to either the value of the environment variable or geteuid
  if (! CloudEnvOptions::GetNameFromEnvironment("ROCKSDB_CLOUD_TEST_BUCKET_NAME",
						"ROCKSDB_CLOUD_BUCKET_NAME", &bucket_)) {
    bucket_ = bucket + std::to_string(geteuid());
  }
  if (CloudEnvOptions::GetNameFromEnvironment("ROCKSDB_CLOUD_TEST_BUCKET_PREFIX",
					      "ROCKSDB_CLOUD_BUCKET_PREFIX", &prefix)) {
    prefix_ = prefix;
  }
  name_ = prefix_ + bucket_;
  if (! CloudEnvOptions::GetNameFromEnvironment("ROCKSDB_CLOUD_TEST_OBECT_PATH",
						"ROCKSDB_CLOUD_OBJECT_PATH", &object_)) {
    object_ = object;
  }  
  if (! CloudEnvOptions::GetNameFromEnvironment("ROCKSDB_CLOUD_TEST_REGION",
						"ROCKSDB_CLOUD_REGION", &region_)) {
    region_ = region;
  }
}
  
CloudEnv::~CloudEnv() {}

CloudEnvWrapper::~CloudEnvWrapper() {}

CloudEnvImpl::CloudEnvImpl(const CloudEnvOptions& opts, Env* base_env)
  : CloudEnv(opts),
      base_env_(base_env), purger_is_running_(true) {}

CloudEnvImpl::~CloudEnvImpl() { StopPurger(); }

void CloudEnvImpl::StopPurger() {
  {
    std::lock_guard<std::mutex> lk(purger_lock_);
    purger_is_running_ = false;
    purger_cv_.notify_one();
  }

  // wait for the purger to stop
  if (purge_thread_.joinable()) {
    purge_thread_.join();
  }
}

Status CloudEnvImpl::LoadLocalCloudManifest(const std::string& dbname) {
  if (cloud_manifest_) {
    cloud_manifest_.reset();
  }
  unique_ptr<SequentialFile> file;
  auto cloudManifestFile = CloudManifestFile(dbname);
  auto s = GetBaseEnv()->NewSequentialFile(cloudManifestFile, &file, EnvOptions());
  if (!s.ok()) {
    return s;
  }
  return CloudManifest::LoadFromLog(
      unique_ptr<SequentialFileReader>(
          new SequentialFileReader(std::move(file), cloudManifestFile)),
      &cloud_manifest_);
}

std::string CloudEnvImpl::RemapFilename(const std::string& logical_path) const {
  if (UNLIKELY(GetCloudType() == CloudType::kCloudNone) ||
      UNLIKELY(test_disable_cloud_manifest_)) {
    return logical_path;
  }
  auto file_name = basename(logical_path);
  uint64_t fileNumber;
  FileType type;
  WalFileType walType;
  if (file_name == "MANIFEST") {
    type = kDescriptorFile;
  } else {
    bool ok = ParseFileName(file_name, &fileNumber, &type, &walType);
    if (!ok) {
      return logical_path;
    }
  }
  Slice epoch;
  switch (type) {
    case kTableFile:
      // We should not be accessing sst files before CLOUDMANIFEST is loaded
      assert(cloud_manifest_);
      epoch = cloud_manifest_->GetEpoch(fileNumber);
      break;
    case kDescriptorFile:
      // We should not be accessing MANIFEST files before CLOUDMANIFEST is
      // loaded
      assert(cloud_manifest_);
      // Even though logical file might say MANIFEST-000001, we cut the number
      // suffix and store MANIFEST-[epoch] in the cloud and locally.
      file_name = "MANIFEST";
      epoch = cloud_manifest_->GetCurrentEpoch();
      break;
    default:
      return logical_path;
  };
  auto dir = dirname(logical_path);
  return dir + (dir.empty() ? "" : "/") + file_name +
         (epoch.empty() ? "" : ("-" + epoch.ToString()));
}

Status CloudEnvImpl::DeleteInvisibleFiles(const std::string& dbname) {
  Status s;
  if (!GetDestBucketName().empty()) {
    BucketObjectMetadata metadata;
    s = ListObjects(GetDestBucketName(), GetDestObjectPath(), &metadata);
    if (!s.ok()) {
      return s;
    }

    for (auto& fname : metadata.pathnames) {
      auto noepoch = RemoveEpoch(fname);
      if (IsSstFile(noepoch) || IsManifestFile(noepoch)) {
        if (RemapFilename(noepoch) != fname) {
          // Ignore returned status on purpose.
          Log(InfoLogLevel::INFO_LEVEL, info_log_,
              "DeleteInvisibleFiles deleting %s from destination bucket",
              fname.c_str());
          DeleteCloudFileFromDest(fname);
        }
      }
    }
  }
  std::vector<std::string> children;
  s = GetBaseEnv()->GetChildren(dbname, &children);
  if (!s.ok()) {
    return s;
  }
  for (auto& fname : children) {
    auto noepoch = RemoveEpoch(fname);
    if (IsSstFile(noepoch) || IsManifestFile(noepoch)) {
      if (RemapFilename(RemoveEpoch(fname)) != fname) {
        // Ignore returned status on purpose.
        Log(InfoLogLevel::INFO_LEVEL, info_log_,
            "DeleteInvisibleFiles deleting file %s from local dir",
            fname.c_str());
        GetBaseEnv()->DeleteFile(dbname + "/" + fname);
      }
    }
  }
  return s;
}

void CloudEnvImpl::TEST_InitEmptyCloudManifest() {
  CloudManifest::CreateForEmptyDatabase("", &cloud_manifest_);
}

Status CloudEnv::NewAwsEnv(Env* base_env,
			   const std::string& src_cloud_bucket,
                           const std::string& src_cloud_object,
                           const std::string& src_cloud_region,
                           const std::string& dest_cloud_bucket,
                           const std::string& dest_cloud_object,
                           const std::string& dest_cloud_region,
                           const CloudEnvOptions& cloud_options,
                           const std::shared_ptr<Logger> & logger, CloudEnv** cenv) {
  CloudEnvOptions options = cloud_options;
  if (!src_cloud_bucket.empty()) options.src_bucket.SetBucketName(src_cloud_bucket);
  if (!src_cloud_object.empty()) options.src_bucket.SetObjectPath(src_cloud_object);
  if (!src_cloud_region.empty()) options.src_bucket.SetRegion(src_cloud_region);
  if (!dest_cloud_bucket.empty()) options.dest_bucket.SetBucketName(dest_cloud_bucket);
  if (!dest_cloud_object.empty()) options.dest_bucket.SetObjectPath(dest_cloud_object);
  if (!dest_cloud_region.empty()) options.dest_bucket.SetRegion(dest_cloud_region);
  return NewAwsEnv(base_env, options, logger, cenv);
}
  
Status CloudEnv::NewAwsEnv(Env* base_env, 
                           const CloudEnvOptions& options,
                           const std::shared_ptr<Logger> & logger, CloudEnv** cenv) {
#ifndef USE_AWS
  return Status::NotSupported("RocksDB Cloud not compiled with AWS support");
#else
  // Dump out cloud env options
  options.Dump(logger.get());

  // If the src bucket is not specified, then this is a pass-through cloud env.
  if (! options.dest_bucket.IsValid() &&
      ! options.src_bucket.IsValid()) {
    *cenv = new CloudEnvWrapper(options, base_env);
    return Status::OK();
  }

  Status st = AwsEnv::NewAwsEnv(base_env, options, logger, cenv);
  if (st.ok()) {
    // store a copy of the logger
    CloudEnvImpl* cloud = static_cast<CloudEnvImpl*>(*cenv);
    cloud->info_log_ = logger;

    // start the purge thread only if there is a destination bucket
    if (options.dest_bucket.IsValid() && options.run_purger) {
      cloud->purge_thread_ = std::thread([cloud] { cloud->Purger(); });
    }
  }
  return st;
#endif
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
