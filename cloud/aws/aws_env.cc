//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#ifndef ROCKSDB_LITE
#include "cloud/aws/aws_env.h"

#include <chrono>
#include <cinttypes>
#include <fstream>
#include <iostream>
#include <memory>
#include <set>

#include "cloud/cloud_log_controller_impl.h"
#include "cloud/cloud_scheduler.h"
#include "cloud/cloud_storage_provider_impl.h"
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
  if (CloudEnvOptions::GetNameFromEnvironment(
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
AwsEnv::AwsEnv(Env* underlying_env, const CloudEnvOptions& _cloud_env_options,
               const std::shared_ptr<Logger>& info_log)
    : CloudEnvImpl(_cloud_env_options, underlying_env, info_log) {
  Aws::InitAPI(Aws::SDKOptions());
}

// If you do not specify a region, then S3 buckets are created in the
// standard-region which might not satisfy read-your-own-writes. So,
// explicitly make the default region be us-west-2.
Status AwsEnv::PrepareOptions(const ConfigOptions& options) {
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
  if (cloud_env_options.storage_provider == nullptr) {
    // If the user has not specified a storage provider, then use the default
    // provider for this CloudType
    Status s = CloudStorageProvider::CreateFromString(
        options, CloudStorageProviderImpl::kS3(),
        &cloud_env_options.storage_provider);
    if (!s.ok()) {
      return s;
    }
  }
  return CloudEnvImpl::PrepareOptions(options);
}

void AwsEnv::Shutdown() { Aws::ShutdownAPI(Aws::SDKOptions()); }


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
  std::unique_ptr<AwsEnv> aenv(new AwsEnv(base_env, cloud_options, info_log));
  ConfigOptions config_options;
  config_options.env = aenv.get();
  status = aenv->PrepareOptions(config_options);
  if (status.ok()) {
    *cenv = aenv.release();
  }
  return status;
}
#endif  // USE_AWS

Status AwsEnv::NewAwsEnv(Env* env, std::unique_ptr<CloudEnv>* cenv) {
#ifdef USE_AWS
  cenv->reset(new AwsEnv(env, CloudEnvOptions()));
  return Status::OK();
#else
  (void) env;
  cenv->reset();
  return Status::NotSupported("AWS not supported");
#endif // USE_AWS
}

int CloudEnvImpl::RegisterAwsObjects(ObjectLibrary& library,
                                     const std::string& /*arg*/) {
  int count = 0;
  library.Register<Env>(CloudEnvImpl::kAws(),
                        [](const std::string& /*uri*/,
                           std::unique_ptr<Env>* guard, std::string* errmsg) {
                          std::unique_ptr<CloudEnv> cguard;
                          Status s = AwsEnv::NewAwsEnv(Env::Default(), &cguard);
                          if (s.ok()) {
                            guard->reset(cguard.release());
                            return guard->get();
                          } else {
                            *errmsg = s.ToString();
                            return static_cast<Env*>(nullptr);
                          }
                        });
  count++;
  library.Register<CloudLogController>(
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
  library.Register<CloudStorageProvider>(  // s3
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
#endif // ROCKSDB_LITE
