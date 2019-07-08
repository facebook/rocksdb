// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include <cinttypes>
#include "cloud/aws/aws_env.h"
#include "cloud/cloud_env_impl.h"
#include "cloud/cloud_env_wrapper.h"
#include "cloud/db_cloud_impl.h"
#include "rocksdb/env.h"

namespace rocksdb {

static const std::unordered_map<std::string, AwsAccessType> AwsAccessTypeMap =
        { { "undefined",   AwsAccessType::kUndefined },
          { "simple",      AwsAccessType::kSimple },
          { "instance",    AwsAccessType::kInstance },
          { "EC2",         AwsAccessType::kInstance },
          { "environment", AwsAccessType::kEnvironment },
          { "config",      AwsAccessType::kConfig },
          { "anonymous",   AwsAccessType::kAnonymous },
        };

template <typename T>  bool ParseEnum(const std::unordered_map<std::string, T>& type_map,
                                      const std::string& type, T* value) {
  auto iter = type_map.find(type);
  if (iter != type_map.end()) {
    *value = iter->second;
    return true;
  }
  return false;
}

AwsCloudAccessCredentials::AwsCloudAccessCredentials() {
  type = AwsAccessType::kUndefined;
}

bool AwsCloudAccessCredentials::Set() {
  // don't set twice
  if (type == AwsAccessType::kUndefined) {
    return true;
  }

  SetRegion();
  if (!SetAccessType()) {
    return false;
  }
  if (!SetProvider()) {
    return false;
  }
  return true;
}

bool AwsCloudAccessCredentials::Set(const std::string& aws_region) {
  SetRegion(aws_region);

  // don't set twice
  if (type == AwsAccessType::kUndefined) {
    return true;
  }
  if (!SetAccessType()) {
    return false;
  }
  if (!SetProvider()) {
    return false;
  }
  return true;
}

bool AwsCloudAccessCredentials::SetSimple(const std::string& aws_access_key_id, const std::string& aws_secret_key,
        const std::string& aws_region) {
  type = AwsAccessType::kSimple;
  access_key_id = aws_access_key_id;
  secret_key = aws_secret_key;
  if (aws_region.empty()) {
    region = "us-west-2";
  }
  else {
    region = aws_region;
  }
  if (access_key_id.empty() || secret_key.empty()) {
    status = Status::InvalidArgument("Simple AWS Credentials require both access ID and secret keys");
    return false;
  }
  else {
    provider.reset(new Aws::Auth::SimpleAWSCredentialsProvider(access_key_id.c_str(), secret_key.c_str()));
  }
  return true;
}


Status AwsCloudAccessCredentials::AreValid() const {
  if (!provider && status.ok()) {
    return Status::InvalidArgument("AWS Credentials are not set");
  }
  return status;
}

bool AwsCloudAccessCredentials::SetAccessType() {
  AwsAccessType accessType = AwsAccessType::kUndefined;

  std::string type_str;
  if (CloudEnvOptions::GetNameFromEnvironment("AWS_ACCESS_TYPE", "aws_access_type", &type_str)) {
    ParseEnum<AwsAccessType>(AwsAccessTypeMap, type_str, &accessType);
  }

  if (accessType == AwsAccessType::kUndefined ||
      accessType == AwsAccessType::kConfig) {
    if (CloudEnvOptions::GetNameFromEnvironment("AWS_CREDENTIALS_FILE", "aws_credentials_file", &config_file)) {
      accessType = AwsAccessType::kConfig;
    }
  }
  if (accessType == AwsAccessType::kUndefined ||
      accessType == AwsAccessType::kSimple) {
    if (CloudEnvOptions::GetNameFromEnvironment("AWS_ACCESS_KEY_ID", "aws_access_key_id", &access_key_id) &&
        CloudEnvOptions::GetNameFromEnvironment("AWS_SECRET_ACCESS_KEY", "aws_secret_access_key", &secret_key)) {
      accessType = AwsAccessType::kSimple;
    }
  }

  if (accessType == AwsAccessType::kUndefined) {
    return false;
  }

  type = accessType;
  return true;
}

AwsAccessType AwsCloudAccessCredentials::GetAccessType() const {
  return type;
}

bool AwsCloudAccessCredentials::SetProvider() {
  provider.reset();
  status = Status::OK();
#ifndef USE_AWS
  status = Status::NotSupported("AWS not supported");
  return false;
#else

  AwsAccessType accessType = GetAccessType();
  switch (accessType) {
  case AwsAccessType::kSimple:
    if (access_key_id.empty() || secret_key.empty()) {
      status = Status::InvalidArgument("Simple AWS Credentials require both access ID and secret keys");
    }
    else {
      provider.reset(new Aws::Auth::SimpleAWSCredentialsProvider(access_key_id.c_str(), secret_key.c_str()));
    }
    break;

  case AwsAccessType::kConfig:
    if (!config_file.empty()) {
      provider.reset(new Aws::Auth::ProfileConfigFileAWSCredentialsProvider(config_file.c_str()));
    } else {
      provider.reset(new Aws::Auth::ProfileConfigFileAWSCredentialsProvider());
    }
    break;
  case AwsAccessType::kInstance:
    provider.reset(new Aws::Auth::InstanceProfileCredentialsProvider());
    break;
  case AwsAccessType::kAnonymous:
    provider.reset(new Aws::Auth::AnonymousAWSCredentialsProvider());
    break;
  case AwsAccessType::kEnvironment:
    provider.reset(new Aws::Auth::EnvironmentAWSCredentialsProvider());
    break;
  default: // Fall through
    status = Status::InvalidArgument("Invalid AWS Credentials configuration");
    return false;
  }
  return true;
#endif
}

Status AwsCloudAccessCredentials::GetProvider(std::shared_ptr<Aws::Auth::AWSCredentialsProvider> * result) const {
  *result = provider;
  return status;
}

void AwsCloudAccessCredentials::SetRegion(const std::string& aws_region) {
  if (aws_region.empty()) {
    region = "us-west-2";
  } else {
    region = aws_region;
  }
}

void AwsCloudAccessCredentials::SetRegion() {
  if (!CloudEnvOptions::GetNameFromEnvironment("AWS_DEFAULT_REGION", "aws_default_region", &region)) {
    region = "us-west-2";
  }
}

std::string AwsCloudAccessCredentials::GetRegion() const {
  return region;
}

void CloudEnvOptions::Dump(Logger* log) const {
  Header(log, "                         COptions.cloud_type: %u", cloud_type);
  Header(log, "                           COptions.log_type: %u", log_type);
  Header(log, "               COptions.keep_local_sst_files: %d",
         keep_local_sst_files);
  Header(log, "               COptions.keep_local_log_files: %d",
         keep_local_log_files);
  Header(log, "             COptions.server_side_encryption: %d",
         server_side_encryption);
  Header(log, "                  COptions.encryption_key_id: %s",
         encryption_key_id.c_str());
  Header(log, "           COptions.create_bucket_if_missing: %s",
         create_bucket_if_missing ? "true" : "false");
  Header(log, "                         COptions.run_purger: %s",
         run_purger ? "true" : "false");
  Header(log, "           COptions.ephemeral_resync_on_open: %s",
         ephemeral_resync_on_open ? "true" : "false");
  Header(log, "             COptions.skip_dbid_verification: %s",
         skip_dbid_verification ? "true" : "false");
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
