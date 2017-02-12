// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "cloud/aws/aws_env.h"
#include "cloud/db_cloud_impl.h"
#include "db/auto_roll_logger.h"

namespace rocksdb {

namespace {

//
// Create appropriate files in the clone dir
//
Status SanitizeCloneDirectory(const Options& options,
		              const std::string & dbname,
			      bool readonly) {
  CloudEnv* cenv = static_cast<CloudEnv *>(options.env);
  if (cenv->GetCloudType() != CloudType::kAws) {
    return Status::InvalidArgument("SanitizeCloneDirectory: Invalid Type");
  }
  // TODO
  return Status::OK();
}

} // namespace unnamed

DBCloudImpl::DBCloudImpl(DB* db) :
	DBCloud(db),
	cenv_(nullptr) {
}

DBCloudImpl::~DBCloudImpl() {
}

Status DBCloud::Open(
    const Options& options,
    const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles,
    DBCloud** dbptr,
    bool read_only) {

  DB* db = nullptr;
  *dbptr = nullptr;
  Status st;

  // applications need to specify a cloud environment for CloudEnv to work
  if (!options.env) {
    return Status::InvalidArgument("Specify a default environment");
  }

  // Store pointer to env, just for sanity check
  CloudEnv* cenv = static_cast<CloudEnv *>(options.env);
  assert(cenv->GetCloudType() == CloudType::kAws);

  if (read_only) {
    st = DB::OpenForReadOnly(options, dbname, column_families,
                             handles, &db);
  } else {
    st = DB::Open(options, dbname, column_families, handles, &db);
  }
  if (st.ok()) {
    DBCloudImpl* d = new DBCloudImpl(db);
    d->cenv_ = cenv; 
    *dbptr = d;
  }
  return st;
}

Status DBCloud::OpenClone(
    const Options& options, const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles,
    DBCloud** dbptr,
    bool read_only) {

  Status st = SanitizeCloneDirectory(options, dbname, read_only);
  if (!st.ok()) {
    return st;
  }

  st = DBCloud::Open(options, dbname, column_families,
		     handles, dbptr, read_only);

  if (st.ok()) {
    // Mark env instance as serving a clone.
    CloudEnv* cenv = static_cast<CloudEnv *>(options.env);
    cenv->SetClone();
  }
  return st;
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
