// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "cloud/purge.h"
#include "cloud/aws/aws_env.h"

namespace rocksdb {

//
// Keep running till running is true
//
void CloudEnvImpl::Purger() {
  uint64_t last_run = 0;
  // Run purge once every 10 minutes

  while (purger_is_running_) {
    uint64_t now = base_env_->NowMicros();
    if (last_run + 600000 < now) {
      last_run = now;
      PurgeObsoleteDbid();
      PurgeObsoleteFiles();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}


Status CloudEnvImpl::PurgeObsoleteFiles() {

  // Step1: fetch a list of all pathnames in S3
  // Step2: from all MANIFEST files in Step 1, compile a list of all live files
  // Step3: delete all files that are (Step1 - Step 2)

  return Status::OK();
}

Status CloudEnvImpl::PurgeObsoleteDbid() {
  // fetch list of all registered dbids
  DbidList dbid_list;
  Status st = GetDbidList(&dbid_list);

  // loop though all dbids. If the pathname does not exist in the bucket, then
  // delete this dbid from the registry.
  if (st.ok()) {
    for (auto iter = dbid_list.begin(); iter != dbid_list.end(); ++iter) {

      std::unique_ptr<SequentialFile> result;
      std::string path = iter->second + "/MANIFEST";
      st = NewSequentialFileCloud(path, &result, EnvOptions());
      if (st.ok() || !st.IsNotFound()) {
        // data for this dbid possibly exists, leave it alone.
	continue;
      }
      // this dbid can be cleaned up

      st = DeleteDbid(iter->first);
      Log(InfoLogLevel::WARN_LEVEL, info_log_,
          "[pg] dbid %s non-existent dbpath %s %s",  
          iter->first.c_str(), iter->second.c_str(),
          st.ToString().c_str());
    }
  }
  return st;
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
