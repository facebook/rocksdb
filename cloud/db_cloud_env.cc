// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "cloud/aws/aws_env.h"
#include "cloud/aws/aws_file.h"
#include "cloud/db_cloud_impl.h"

namespace rocksdb {

CloudEnv::CloudEnv(CloudType type, Env* base_env) :
  cloud_type_(type),
  is_cloud_direct_(false),
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

  return AwsEnv::NewAwsEnv(base_env, cloud_storage,
			   options, logger, cenv);
}

//
// Marks this env as a clone env
//
Status CloudEnv::SetClone(const std::string& src_dbid) {
  is_clone_ = true;
  src_dbid_ = src_dbid;

  // Map the src dbid to a pathname of the src db
  // src db dir
  return GetPathForDbid(src_dbid, &src_dbdir_);
}
//
// Maps a pathname from the clone to the corresponding file in src db
//
std::string CloudEnv::MapClonePathToSrcPath(const std::string& fname) {
#ifdef USE_AWS
  assert(is_clone_);
  return src_dbdir_ + "/" + basename(fname);  
#else
  return "MapClonePathToSrcPath not available";
#endif
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
