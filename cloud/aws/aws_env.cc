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
  // These lines of code are likely temporary until the new configuration stuff
  // comes into play.
  CloudEnvOptions options = cloud_options;  // Make a copy

  // If the user has not specified a storage provider, then use the default
  // provider for this CloudType
  if (!options.storage_provider) {
    status =
      CloudStorageProviderImpl::CreateS3Provider(&options.storage_provider);
    Log(InfoLogLevel::ERROR_LEVEL, info_log,
        "[aws] NewAwsEnv Created default S3 storage provider for cloud type %d. %s",
	cloud_options.cloud_type, status.ToString().c_str());
  }
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
#endif  // USE_AWS
}  // namespace ROCKSDB_NAMESPACE
#endif // ROCKSDB_LITE
