// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "cloud/aws/aws_env.h"
#include "cloud/db_cloud_impl.h"

namespace rocksdb {

CloudEnv::CloudEnv(CloudType type, Env* base_env) :
  cloud_type_(type),
  is_clone_(false),
  base_env_(base_env) {
}

CloudEnv::~CloudEnv() {
}

Status CloudEnv::NewAwsEnv(Env* base_env,
		           const std::string& cloud_storage,
	                   const CloudEnvOptions& options,
			   std::shared_ptr<Logger> logger,
			   CloudEnv** cenv) {

  // Sanitize options do not allow uploading files to cloud store
  CloudEnvOptions env_options = options;
  env_options.keep_local_sst_files = true;
  env_options.keep_local_log_files = true;
  env_options.manifest_durable_periodicity_millis = 0; 

  return AwsEnv::NewAwsEnv(base_env, cloud_storage,
			   env_options, logger, cenv);
}



}  // namespace rocksdb
#endif  // ROCKSDB_LITE
