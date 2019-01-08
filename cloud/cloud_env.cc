// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

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

CloudEnv::~CloudEnv() {}

CloudEnvWrapper::~CloudEnvWrapper() {}

CloudEnvImpl::CloudEnvImpl(
      CloudType cloud_type, LogType log_type, Env* base_env)
    : cloud_type_(cloud_type), log_type_(log_type),
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
  if (!GetDestBucketPrefix().empty()) {
    BucketObjectMetadata metadata;
    s = ListObjects(GetDestBucketPrefix(), GetDestObjectPrefix(), &metadata);
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

Status CloudEnv::NewAwsEnv(Env* base_env, const std::string& src_cloud_storage,
                           const std::string& src_cloud_object_prefix,
                           const std::string& src_cloud_region,
                           const std::string& dest_cloud_storage,
                           const std::string& dest_cloud_object_prefix,
                           const std::string& dest_cloud_region,
                           const CloudEnvOptions& options,
                           std::shared_ptr<Logger> logger, CloudEnv** cenv) {
#ifndef USE_AWS
  return Status::NotSupported("RocksDB Cloud not compiled with AWS support");
#else
  // Dump out cloud env options
  options.Dump(logger.get());

  // If the src bucket is not specified, then this is a pass-through cloud env.
  if (src_cloud_storage.empty() && dest_cloud_storage.empty()) {
    *cenv = new CloudEnvWrapper(base_env);
    return Status::OK();
  }

  Status st = AwsEnv::NewAwsEnv(base_env, src_cloud_storage,
                                src_cloud_object_prefix, src_cloud_region,
                                dest_cloud_storage, dest_cloud_object_prefix,
                                dest_cloud_region, options, logger, cenv);
  if (st.ok()) {
    // store a copy of the logger
    CloudEnvImpl* cloud = static_cast<CloudEnvImpl*>(*cenv);
    cloud->info_log_ = logger;

    // start the purge thread only if there is a destination bucket
    if (!dest_cloud_storage.empty() && options.run_purger) {
      cloud->purge_thread_ = std::thread([cloud] { cloud->Purger(); });
    }
  }
  return st;
#endif
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
