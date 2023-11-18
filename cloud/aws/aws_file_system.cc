//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#ifndef ROCKSDB_LITE
#include "cloud/aws/aws_file_system.h"

#include <chrono>
#include <cinttypes>
#include <fstream>
#include <iostream>
#include <memory>
#include <set>

#include "cloud/cloud_log_controller_impl.h"
#include "cloud/cloud_scheduler.h"
#include "rocksdb/cloud/cloud_storage_provider_impl.h"
#include "cloud/filename.h"
#include "port/port.h"
#include "rocksdb/cloud/cloud_log_controller.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "util/string_util.h"

#ifdef USE_AWS
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#endif

#include "cloud/aws/aws_file.h"
#include "cloud/db_cloud_impl.h"

namespace ROCKSDB_NAMESPACE {

static const std::unordered_map<std::string, AwsAccessType> AwsAccessTypeMap = {
    {"undefined", AwsAccessType::kUndefined},
    {"simple", AwsAccessType::kSimple},
    {"instance", AwsAccessType::kInstance},
    {"EC2", AwsAccessType::kInstance},
    {"environment", AwsAccessType::kEnvironment},
    {"config", AwsAccessType::kConfig},
    {"anonymous", AwsAccessType::kAnonymous},
};

AwsCloudAccessCredentials::AwsCloudAccessCredentials() {
  std::string type_str;
  if (CloudFileSystemOptions::GetNameFromEnvironment(
          "ROCKSDB_AWS_ACCESS_TYPE", "rocksdb_aws_access_type", &type_str)) {
    ParseEnum<AwsAccessType>(AwsAccessTypeMap, type_str, &type);
  } else if (getenv("AWS_ACCESS_KEY_ID") != nullptr &&
             getenv("AWS_SECRET_ACCESS_KEY") != nullptr) {
    type = AwsAccessType::kEnvironment;
  }
}

AwsAccessType AwsCloudAccessCredentials::GetAccessType() const {
  if (type != AwsAccessType::kUndefined) {
    return type;
  } else if (!config_file.empty()) {
    return AwsAccessType::kConfig;
  } else if (!access_key_id.empty() || !secret_key.empty()) {
    return AwsAccessType::kSimple;
  } else if (getenv("AWS_ACCESS_KEY_ID") != nullptr &&
             getenv("AWS_SECRET_ACCESS_KEY") != nullptr) {
    return AwsAccessType::kEnvironment;
  } else {
    return AwsAccessType::kUndefined;
  }
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

  if (provider) {
    *result = provider;
    return Status::OK();
  }

  AwsAccessType aws_type = GetAccessType();
  Status status = CheckCredentials(aws_type);
  if (status.ok()) {
#ifdef USE_AWS
    switch (aws_type) {
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
      default:
        status = Status::NotSupported("AWS credentials type not supported");
        break;  // not supported
    }
#else
    status = Status::NotSupported("AWS credentials type not supported");
#endif
  }
  return status;
}

#ifdef USE_AWS

//
// The AWS credentials are specified to the constructor via
// access_key_id and secret_key.
//
AwsFileSystem::AwsFileSystem(const std::shared_ptr<FileSystem>& underlying_fs,
                             const CloudFileSystemOptions& _cloud_fs_options,
                             const std::shared_ptr<Logger>& info_log)
    : CloudFileSystemImpl(_cloud_fs_options, underlying_fs, info_log) {
}

// If you do not specify a region, then S3 buckets are created in the
// standard-region which might not satisfy read-your-own-writes. So,
// explicitly make the default region be us-west-2.
Status AwsFileSystem::PrepareOptions(const ConfigOptions& options) {
  if (cloud_fs_options.src_bucket.GetRegion().empty() ||
      cloud_fs_options.dest_bucket.GetRegion().empty()) {
    std::string region;
    if (!CloudFileSystemOptions::GetNameFromEnvironment(
            "AWS_DEFAULT_REGION", "aws_default_region", &region)) {
      region = default_region;
    }
    if (cloud_fs_options.src_bucket.GetRegion().empty()) {
      cloud_fs_options.src_bucket.SetRegion(region);
    }
    if (cloud_fs_options.dest_bucket.GetRegion().empty()) {
      cloud_fs_options.dest_bucket.SetRegion(region);
    }
  }
  if (cloud_fs_options.storage_provider == nullptr) {
    // If the user has not specified a storage provider, then use the default
    // provider for this CloudType
    Status s = CloudStorageProvider::CreateFromString(
        options, CloudStorageProviderImpl::kS3(),
        &cloud_fs_options.storage_provider);
    if (!s.ok()) {
      return s;
    }
  }
  return CloudFileSystemImpl::PrepareOptions(options);
}

// The factory method for creating an S3 Env
Status AwsFileSystem::NewAwsFileSystem(
    const std::shared_ptr<FileSystem>& base_fs,
    const CloudFileSystemOptions& cloud_options,
    const std::shared_ptr<Logger>& info_log, CloudFileSystem** cfs) {
  Status status;
  *cfs = nullptr;
  // If underlying FileSystem is not defined, then use PosixFileSystem
  auto fs = base_fs;
  if (!fs) {
    fs = FileSystem::Default();
  }
  std::unique_ptr<AwsFileSystem> afs(
      new AwsFileSystem(fs, cloud_options, info_log));

  auto env = afs->NewCompositeEnvFromThis(Env::Default());
  ConfigOptions config_options;
  config_options.env = env.get();
  status = afs->PrepareOptions(config_options);
  if (status.ok()) {
    *cfs = afs.release();
  }
  return status;
}

Status AwsFileSystem::NewAwsFileSystem(const std::shared_ptr<FileSystem>& fs,
                                       std::unique_ptr<CloudFileSystem>* cfs) {
  cfs->reset(new AwsFileSystem(fs, CloudFileSystemOptions()));
  return Status::OK();
}

#endif  // USE_AWS

int CloudFileSystemImpl::RegisterAwsObjects(ObjectLibrary& library,
                                            const std::string& /*arg*/) {
  int count = 0;

#ifdef USE_AWS
  library.AddFactory<FileSystem>(
      CloudFileSystemImpl::kAws(),
      [](const std::string& /*uri*/, std::unique_ptr<FileSystem>* guard,
         std::string* errmsg) {
        std::unique_ptr<CloudFileSystem> cguard;
        Status s =
            AwsFileSystem::NewAwsFileSystem(FileSystem::Default(), &cguard);
        if (s.ok()) {
          guard->reset(cguard.release());
          return guard->get();
        } else {
          *errmsg = s.ToString();
          return static_cast<FileSystem*>(nullptr);
        }
      });
  count++;
#endif  // USE_AWS

  library.AddFactory<CloudLogController>(
      CloudLogControllerImpl::kKinesis(),
      [](const std::string& /*uri*/, std::unique_ptr<CloudLogController>* guard,
         std::string* errmsg) {
        Status s = CloudLogControllerImpl::CreateKinesisController(guard);
        if (!s.ok()) {
          *errmsg = s.ToString();
        }
        return guard->get();
      });
  count++;
  library.AddFactory<CloudStorageProvider>(  // s3
      CloudStorageProviderImpl::kS3(),
      [](const std::string& /*uri*/,
         std::unique_ptr<CloudStorageProvider>* guard, std::string* errmsg) {
        Status s = CloudStorageProviderImpl::CreateS3Provider(guard);
        if (!s.ok()) {
          *errmsg = s.ToString();
        }
        return guard->get();
      });
  count++;
  return count;
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
