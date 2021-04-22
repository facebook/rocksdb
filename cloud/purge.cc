// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include "cloud/purge.h"

#include <chrono>
#include <set>

#include "cloud/db_cloud_impl.h"
#include "cloud/filename.h"
#include "cloud/manifest_reader.h"
#include "file/filename.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// A map from a dbid to the list of all its parent dbids.
typedef std::map<std::string, std::vector<std::string>> DbidParents;

//
// Keep running till running is true
//
void CloudEnvImpl::Purger() {
  Status st;
  // Run purge once every period.
  auto period =
      std::chrono::milliseconds(GetCloudEnvOptions().purger_periodicity_millis);

  std::vector<std::string> to_be_deleted_paths;
  std::vector<std::string> to_be_deleted_dbids;

  while (true) {
    std::unique_lock<std::mutex> lk(purger_lock_);
    purger_cv_.wait_for(lk, period, [&]() { return purger_is_running_; });
    if (!purger_is_running_) {
      break;
    }
    // delete the objects that were detected to be obsolete in the last
    // run. This ensures that obsolete files are not immediately deleted
    // because we need to give the clone-to-local-dir code ample time to
    // copy them to local dir at clone-creation time.

    // delete obsolete dbids
    for (const auto& p : to_be_deleted_dbids) {
      // TODO more unit tests before we delete data
      // st = DeleteDbid(GetDestBucketName(), p);
      Log(InfoLogLevel::WARN_LEVEL, info_log_,
          "[pg] dbid %s non-existent dbpath %s deleted. %s",
          GetDestBucketName().c_str(), p.c_str(), st.ToString().c_str());
    }

    // delete obsolete paths
    for (const auto& p : to_be_deleted_paths) {
      // TODO more unit tests before we delete data
      // st = DeleteCloudObject(GetDestBucketName(), p);
      Log(InfoLogLevel::WARN_LEVEL, info_log_,
          "[pg] bucket prefix %s obsolete dbpath %s deleted. %s",
          GetDestBucketName().c_str(), p.c_str(), st.ToString().c_str());
    }

    to_be_deleted_paths.clear();
    to_be_deleted_dbids.clear();
    FindObsoleteFiles(GetDestBucketName(), &to_be_deleted_paths);
    FindObsoleteDbid(GetDestBucketName(), &to_be_deleted_dbids);
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
        "[pg] GetDbidList on bucket prefix %s. %s", bucket_name_prefix.c_str(),
        st.ToString().c_str());
    return st;
  }

  // For each of the dbids names, extract its list of parent-dbs
  DbidParents parents;
  st = extractParents(bucket_name_prefix, dbid_list, &parents);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[pg] extractParents on bucket prefix %s. %s",
        bucket_name_prefix.c_str(), st.ToString().c_str());
    return st;
  }

  std::unique_ptr<ManifestReader> extractor(
      new ManifestReader(info_log_, this, bucket_name_prefix));

  // Step2: from all MANIFEST files in Step 1, compile a list of all live files
  for (auto iter = dbid_list.begin(); iter != dbid_list.end(); ++iter) {
    std::unique_ptr<SequentialFile> result;
    std::set<uint64_t> file_nums;
    st = extractor->GetLiveFiles(iter->second, &file_nums);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[pg] dbid %s extracted files from path %s %s", iter->first.c_str(),
          iter->second.c_str(), st.ToString().c_str());
    } else {
      // This file can reside either in this leaf db's path or reside in any of
      // the parent db's paths. Compute all possible paths and insert them into
      // live_files
      for (auto it = file_nums.begin(); it != file_nums.end(); ++it) {
        // list of all parent dbids
        const std::vector<std::string>& parent_dbids = parents[iter->first];

        for (const auto& db : parent_dbids) {
          // parent db's paths
          const std::string& parent_path = dbid_list[db];
          live_files.insert(MakeTableFileName(parent_path, *it));
        }
      }
    }
  }

  // Get all files from all dbpaths in this bucket
  std::vector<std::string> all_files;

  // Scan all the db directories in this bucket. Retrieve the list
  // of files in all these db directories.
  for (auto iter = dbid_list.begin(); iter != dbid_list.end(); ++iter) {
    std::unique_ptr<SequentialFile> result;
    std::string mpath = iter->second;

    std::vector<std::string> objects;
    st = GetStorageProvider()->ListCloudObjects(bucket_name_prefix, mpath,
                                                &objects);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[pg] Unable to list objects in bucketprefix %s path_prefix %s. %s",
          bucket_name_prefix.c_str(), mpath.c_str(), st.ToString().c_str());
    }
    for (auto& o : objects) {
      all_files.push_back(mpath + "/" + o);
    }
  }

  // If a file does not belong to live_files, then it can be deleted
  for (const auto& candidate : all_files) {
    if (live_files.find(candidate) == live_files.end() &&
        ends_with(candidate, ".sst")) {
      Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
          "[pg] bucket prefix %s path %s marked for deletion",
          bucket_name_prefix.c_str(), candidate.c_str());
      pathnames->push_back(candidate);
    }
  }
  return Status::OK();
}

Status CloudEnvImpl::FindObsoleteDbid(
    const std::string& bucket_name_prefix,
    std::vector<std::string>* to_delete_list) {
  // fetch list of all registered dbids
  DbidList dbid_list;
  Status st = GetDbidList(bucket_name_prefix, &dbid_list);

  // loop though all dbids. If the pathname does not exist in the bucket, then
  // this dbid is a candidate for deletion.
  if (st.ok()) {
    for (auto iter = dbid_list.begin(); iter != dbid_list.end(); ++iter) {
      std::unique_ptr<SequentialFile> result;
      std::string path = CloudManifestFile(iter->second);
      st = GetStorageProvider()->ExistsCloudObject(GetDestBucketName(), path);
      // this dbid can be cleaned up
      if (st.IsNotFound()) {
        to_delete_list->push_back(iter->first);

        Log(InfoLogLevel::WARN_LEVEL, info_log_,
            "[pg] dbid %s non-existent dbpath %s scheduled for deletion",
            iter->first.c_str(), iter->second.c_str());
        // We don't want to fail the final call
        st = Status::OK();
      }
    }
  }
  return st;
}

//
// For each of the dbids in the list, extract the entire list of
// parent dbids.
Status CloudEnvImpl::extractParents(const std::string& bucket_name_prefix,
                                    const DbidList& dbid_list,
                                    DbidParents* parents) {
  const std::string delimiter(DBID_SEPARATOR);
  // use current time as seed for random generator
  std::srand(static_cast<unsigned int>(std::time(0)));
  const std::string random = std::to_string(std::rand());
  const std::string scratch(SCRATCH_LOCAL_DIR);
  Status st;
  for (auto iter = dbid_list.begin(); iter != dbid_list.end(); ++iter) {
    // download IDENTITY
    std::string cloudfile = iter->second + "/IDENTITY";
    std::string localfile = scratch + "/.rockset_IDENTITY." + random;
    st = GetStorageProvider()->GetCloudObject(bucket_name_prefix, cloudfile,
                                              localfile);
    if (!st.ok() && !st.IsNotFound()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[pg] Unable to download IDENTITY file from "
          "bucket %s. %s. Aborting...",
          bucket_name_prefix.c_str(), st.ToString().c_str());
      return st;
    } else if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[pg] Unable to download IDENTITY file from "
          "bucket %s. %s. Skipping...",
          bucket_name_prefix.c_str(), st.ToString().c_str());
      continue;
    }

    // Read the dbid from the ID file
    std::string all_dbid;
    st = ReadFileToString(base_env_, localfile, &all_dbid);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_, "[pg] Unable to read %s %s",
          localfile.c_str(), st.ToString().c_str());
      return st;
    }
    st = base_env_->DeleteFile(localfile);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_, "[pg] Unable to delete %s %s",
          localfile.c_str(), st.ToString().c_str());
      return st;
    }

    // all_dbids is of the form 1x45-555rockset678a-6577rockset7789-9aef
    all_dbid = rtrim_if(trim(all_dbid), '\n');

    // We want to return parents[1x45-555] = [678a-6577, 7789-9aef]
    std::vector<std::string> parent_dbids;
    size_t start = 0, end = 0;
    while (end != std::string::npos) {
      end = all_dbid.find(delimiter, start);

      // If at end, use length=maxLength.  Else use length=end-start.
      parent_dbids.push_back(all_dbid.substr(
          start, (end == std::string::npos) ? std::string::npos : end - start));

      // If at end, use start=maxSize.  Else use start=end+delimiter.
      start = ((end > (std::string::npos - delimiter.size()))
                   ? std::string::npos
                   : end + delimiter.size());
    }
    if (parent_dbids.size() > 0) {
      std::string leaf_dbid = parent_dbids[parent_dbids.size() - 1];
      // Verify that the leaf dbid matches the one that we retrived from
      // CloudEnv
      if (leaf_dbid != iter->first) {
        Log(InfoLogLevel::ERROR_LEVEL, info_log_,
            "[pg] The IDENTITY file for dbid '%s' contains leaf dbid as '%s'",
            iter->first.c_str(), leaf_dbid.c_str());
        return st;
      }
      (*parents)[leaf_dbid] = parent_dbids;
    }
  }
  return st;
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
