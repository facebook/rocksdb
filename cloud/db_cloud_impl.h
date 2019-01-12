// Copyright (c) 2017 Rockset

#pragma once

#ifndef ROCKSDB_LITE
#include <deque>
#include <string>
#include <vector>

#include "rocksdb/cloud/db_cloud.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"

namespace rocksdb {

//
// All writes to this DB can be configured to be persisted
// in cloud storage.
//
class DBCloudImpl : public DBCloud {
  friend DBCloud;

 public:
  virtual ~DBCloudImpl();
  Status Savepoint();

 protected:
  // The CloudEnv used by this open instance.
  CloudEnv* cenv_;

 private:
  // Maximum manifest file size
  static const uint64_t max_manifest_file_size = 4 * 1024L * 1024L;

  explicit DBCloudImpl(DB* db);

  // Does the dir need to be re-initialized?
  static Status NeedsReinitialization(CloudEnv* cenv, const Options& options,
                                      const std::string& clone_dir,
                                      bool* do_reinit);

  static Status GetCloudDbid(CloudEnvImpl* cenv, const Options& options,
                             const std::string& local_dir,
                             std::string* src_dbid, std::string* dest_dbid);

  static Status ResyncDir(CloudEnvImpl* cenv, const Options& options,
                          const std::string& local_dir);

  static Status SanitizeDirectory(const Options& options,
                                  const std::string& clone_name,
                                  bool read_only);

  static Status CreateNewIdentityFile(CloudEnv* cenv, const Options& options,
                                      const std::string& dbid,
                                      const std::string& local_name);

  static Status FetchCloudManifest(CloudEnv* cenv,
                                   const Options& options,
                                   const std::string& local_dbname,
                                   bool force);

  static Status MaybeMigrateManifestFile(Env* env,
                                         const Options& options,
                                         const std::string& local_dbname);

  static Status RollNewEpoch(CloudEnvImpl* cenv,
                             const std::string& local_dbname);
};
}
#endif  // ROCKSDB_LITE
