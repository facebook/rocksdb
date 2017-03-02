// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "cloud/aws/aws_env.h"
#include "cloud/db_cloud_impl.h"
#include "cloud/filename.h"
#include "cloud/cloud_env_impl.h"
#include "cloud/cloud_env_wrapper.h"

namespace rocksdb {

CloudEnvWrapper::~CloudEnvWrapper() {
}

CloudEnvImpl::CloudEnvImpl(CloudType type, Env* base_env) :
  cloud_type_(type),
  base_env_(base_env),
  purger_is_running_(true) {
}

CloudEnvImpl::~CloudEnvImpl() {
  // tell the purger to stop
  purger_is_running_ = false;

  // wait for the purger to stop
  if (purge_thread_.joinable()) {
    purge_thread_.join();
  }
}

Status CloudEnv::NewAwsEnv(Env* base_env,
		           const std::string& src_cloud_storage,
                           const std::string& src_cloud_object_prefix,
                           const std::string& dest_cloud_storage,
                           const std::string& dest_cloud_object_prefix,
	                   const CloudEnvOptions& options,
			   std::shared_ptr<Logger> logger,
			   CloudEnv** cenv) {

  // If the src bucket is not specified, then this is a pass-through cloud env.
  if (src_cloud_storage.empty() && dest_cloud_storage.empty()) {
    *cenv = new CloudEnvWrapper(base_env);
    return Status::OK();
  }

  Status st = AwsEnv::NewAwsEnv(base_env,
		                src_cloud_storage, src_cloud_object_prefix,
				dest_cloud_storage, dest_cloud_object_prefix,
			        options, logger, cenv);
  if (st.ok()) {
    // store a copy of the logger
    CloudEnvImpl* cloud = static_cast<CloudEnvImpl *>(*cenv);
    cloud->info_log_ = logger;

    // start the purge thread
    cloud->purge_thread_ = std::thread([cloud] { cloud->Purger(); });
  }
  return st;
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
