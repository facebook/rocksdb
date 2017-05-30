// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include <set>
#include "cloud/purge.h"
#include "cloud/manifest.h"
#include "cloud/aws/aws_env.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace rocksdb {

//
// Keep running till running is true
//
void CloudEnvImpl::Purger() {
  Status st;
  uint64_t last_run = 0;
  // Run purge once every period.
  uint64_t period = GetCloudEnvOptions().purger_periodicity_millis;

  std::vector<std::string> to_be_deleted_paths;
  std::vector<std::string> to_be_deleted_dbids;

  while (purger_is_running_) {
    uint64_t now = base_env_->NowMicros();
    if (last_run + period < now) {
      // delete the objects that were detected to be obsolete in the last
      // run. This ensures that obsolete files are not immediately deleted
      // because we need to give the clone-to-local-dir code ample time to
      // copy them to local dir at clone-creation time.

      // delete obsolete dbids
      for (const auto& p : to_be_deleted_dbids) {
        // TODO more unit tests before we delete data
        // st = DeleteDbid(GetDestBucketPrefix(), p);
        Log(InfoLogLevel::WARN_LEVEL, info_log_,
            "[pg] dbid %s non-existent dbpath %s deleted. %s",
            GetDestBucketPrefix().c_str(), p.c_str(),  st.ToString().c_str());
      }

      // delete obsolete paths
      for (const auto& p : to_be_deleted_paths) {
        // TODO more unit tests before we delete data
        // st = DeleteObject(GetDestBucketPrefix(), p);
        Log(InfoLogLevel::WARN_LEVEL, info_log_,
            "[pg] bucket prefix %s obsolete dbpath %s deleted. %s",
            GetDestBucketPrefix().c_str(), p.c_str(),  st.ToString().c_str());
      }

      last_run = now;
      to_be_deleted_paths.clear();
      to_be_deleted_dbids.clear();
      FindObsoleteFiles(GetDestBucketPrefix(), &to_be_deleted_paths);
      FindObsoleteDbid(GetDestBucketPrefix(), &to_be_deleted_dbids);
    }
    std::this_thread::sleep_for(std::chrono::seconds(10));
  }
}

Status CloudEnvImpl::FindObsoleteFiles(const std::string& bucket_name_prefix,
                                       std::vector<std::string>* pathnames) {
  std::set<std::string> live_files;

  // fetch list of all registered dbids
  DbidList dbid_list;
  Status st = GetDbidList(bucket_name_prefix, &dbid_list);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[pg] GetDbidList on bucket prefix %s. %s",
        bucket_name_prefix.c_str(), st.ToString().c_str());
    return st;
  }

  std::unique_ptr<CloudManifest> extractor(new CloudManifest(info_log_,
                                             this, bucket_name_prefix));

  // Step2: from all MANIFEST files in Step 1, compile a list of all live files
  // Loop though all dbids.
  for (auto iter = dbid_list.begin(); iter != dbid_list.end(); ++iter) {
    std::unique_ptr<SequentialFile> result;
    std::string mpath = iter->second + "/MANIFEST";
    std::set<uint64_t> file_nums;
    st = extractor->GetLiveFiles(mpath, &file_nums);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[pg] dbid %s extracted files from path %s %s",
          iter->first.c_str(), iter->second.c_str(), st.ToString().c_str());
    } else {
      // insert all the live files into our temp array
      for (auto it = file_nums.begin(); it != file_nums.end(); ++it) {
        live_files.insert(mpath + "/" + std::to_string(*it) + ".sst");
      }
    }
  }

  // Get all files from all dbpaths in this bucket
  BucketObjectMetadata all_files;

  // Scan all the db directories in this bucket. Retrieve the list
  // of files in all these db directories.
  for (auto iter = dbid_list.begin(); iter != dbid_list.end(); ++iter) {
    std::unique_ptr<SequentialFile> result;
    std::string mpath = iter->second;

    st = ListObjects(bucket_name_prefix, mpath, &all_files);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[pg] Unable to list objects in bucketprefix %s path_prefix %s. %s",
          bucket_name_prefix.c_str(), mpath.c_str(), st.ToString().c_str());
    }
  }

  // If a file does not belong to live_files, then it can be deleted
  for (const auto& candidate: all_files.pathnames) {
    if (live_files.find(candidate) == live_files.end()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[pg] File to delete in bucket prefix %s path %s",
          bucket_name_prefix.c_str(), candidate.c_str());
      pathnames->push_back(candidate);
    }
  }
  return Status::OK();
}

Status CloudEnvImpl::FindObsoleteDbid(const std::string& bucket_name_prefix,
                                      std::vector<std::string>* to_delete_list) {
  // fetch list of all registered dbids
  DbidList dbid_list;
  Status st = GetDbidList(bucket_name_prefix, &dbid_list);

  // loop though all dbids. If the pathname does not exist in the bucket, then
  // this dbid is a candidate for deletion.
  if (st.ok()) {
    for (auto iter = dbid_list.begin(); iter != dbid_list.end(); ++iter) {
      std::unique_ptr<SequentialFile> result;
      std::string path = iter->second + "/MANIFEST";
      st = NewSequentialFileCloud(GetDestBucketPrefix(), path, &result,
                                  EnvOptions());
      // this dbid can be cleaned up
      if (st.IsNotFound()) {
        to_delete_list->push_back(iter->first);

        Log(InfoLogLevel::WARN_LEVEL, info_log_,
            "[pg] dbid %s non-existent dbpath %s scheduled for deletion",
            iter->first.c_str(), iter->second.c_str());
      }
    }
  }
  return st;
}
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
