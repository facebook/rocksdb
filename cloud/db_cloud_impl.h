// Copyright (c) 2017 Rockset

#pragma once

#ifndef ROCKSDB_LITE
#include <deque>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/cloud/db_cloud.h"

namespace rocksdb {

//
// All writes to this DB can be configured to be persisted
// in cloud storage.
//
class DBCloudImpl : public DBCloud {
 friend DBCloud;
 public:
  explicit DBCloudImpl(DB* db);

  virtual ~DBCloudImpl();

  // Get the contents of the specified file into a string
  static Status ReadFileIntoString(Env* env,
		                   const std::string& pathname,
				   std::string* id);
 protected:
  // The CloudEnv used by this open instance.
  CloudEnv* cenv_;

 private:

  // Does the dir need to be re-initialized?
  static Status NeedsReinitialization(CloudEnv* cenv,
                                      const Options& options,
                                      const std::string& clone_dir,
                                      bool* do_reinit);

  static Status SanitizeDirectory(const Options& options,
		                  const std::string& clone_name,
		                  bool readonly);

  // copies a file from the cloud
  static Status CopyFile(CloudEnv* src_env,
		         Env* dest_env,
			 const std::string& bucket_prefix,
			 const std::string& srcname,
			 const std::string& destname,
			 bool do_sync = true);
};

}
#endif  // ROCKSDB_LITE
