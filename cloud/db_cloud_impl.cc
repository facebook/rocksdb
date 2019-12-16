// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "cloud/db_cloud_impl.h"

#include <inttypes.h>

#include "cloud/aws/aws_env.h"
#include "cloud/filename.h"
#include "cloud/manifest_reader.h"
#include "file/file_util.h"
#include "logging/auto_roll_logger.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/persistent_cache.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "util/xxhash.h"

namespace rocksdb {

DBCloudImpl::DBCloudImpl(DB* db) : DBCloud(db), cenv_(nullptr) {}

DBCloudImpl::~DBCloudImpl() {}

Status DBCloud::Open(const Options& options, const std::string& dbname,
                     const std::string& persistent_cache_path,
                     const uint64_t persistent_cache_size_gb, DBCloud** dbptr,
                     bool read_only) {
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;
  DBCloud* dbcloud = nullptr;
  Status s =
      DBCloud::Open(options, dbname, column_families, persistent_cache_path,
                    persistent_cache_size_gb, &handles, &dbcloud, read_only);
  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    delete handles[0];
    *dbptr = dbcloud;
  }
  return s;
}




Status DBCloud::Open(const Options& opt, const std::string& local_dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     const std::string& persistent_cache_path,
                     const uint64_t persistent_cache_size_gb,
                     std::vector<ColumnFamilyHandle*>* handles, DBCloud** dbptr,
                     bool read_only) {
  Status st;
  Options options = opt;

  // Created logger if it is not already pre-created by user.
  if (!options.info_log) {
    CreateLoggerFromOptions(local_dbname, options, &options.info_log);
  }

  CloudEnvImpl* cenv = static_cast<CloudEnvImpl*>(options.env);
  if (!cenv->info_log_) {
    cenv->info_log_ = options.info_log;
  }
  Env* local_env = cenv->GetBaseEnv();
  if (!read_only) {
    local_env->CreateDirIfMissing(
        local_dbname);  // MJR: TODO: Move into sanitize
  }

  st = cenv->SanitizeDirectory(options, local_dbname, read_only);

  if (st.ok()) {
    st = cenv->LoadCloudManifest(local_dbname, read_only);
  }
  if (!st.ok()) {
    return st;
  }
  // If a persistent cache path is specified, then we set it in the options.
  if (!persistent_cache_path.empty() && persistent_cache_size_gb) {
    // Get existing options. If the persistent cache is already set, then do
    // not make any change. Otherwise, configure it.
    void* bopt = options.table_factory->GetOptions();
    if (bopt != nullptr) {
      BlockBasedTableOptions* tableopt =
          static_cast<BlockBasedTableOptions*>(bopt);
      if (!tableopt->persistent_cache) {
        std::shared_ptr<PersistentCache> pcache;
        st =
            NewPersistentCache(options.env, persistent_cache_path,
                               persistent_cache_size_gb * 1024L * 1024L * 1024L,
                               options.info_log, false, &pcache);
        if (st.ok()) {
          tableopt->persistent_cache = pcache;
          Log(InfoLogLevel::INFO_LEVEL, options.info_log,
              "Created persistent cache %s with size %" PRIu64 "GB",
              persistent_cache_path.c_str(), persistent_cache_size_gb);
        } else {
          Log(InfoLogLevel::INFO_LEVEL, options.info_log,
              "Unable to create persistent cache %s. %s",
              persistent_cache_path.c_str(), st.ToString().c_str());
          return st;
        }
      }
    }
  }
  // We do not want a very large MANIFEST file because the MANIFEST file is
  // uploaded to S3 for every update, so always enable rolling of Manifest file
  options.max_manifest_file_size = DBCloudImpl::max_manifest_file_size;

  DB* db = nullptr;
  std::string dbid;
  if (read_only) {
    st = DB::OpenForReadOnly(options, local_dbname, column_families, handles,
                             &db);
  } else {
    st = DB::Open(options, local_dbname, column_families, handles, &db);
  }

  // now that the database is opened, all file sizes have been verified and we
  // no longer need to verify file sizes for each file that we open. Note that
  // this might have a data race with background compaction, but it's not a big
  // deal, since it's a boolean and it does not impact correctness in any way.
  if (cenv->GetCloudEnvOptions().validate_filesize) {
    *const_cast<bool*>(&cenv->GetCloudEnvOptions().validate_filesize) = false;
  }

  if (st.ok()) {
    DBCloudImpl* cloud = new DBCloudImpl(db);
    *dbptr = cloud;
    db->GetDbIdentity(dbid);
  }
  Log(InfoLogLevel::INFO_LEVEL, options.info_log,
      "Opened cloud db with local dir %s dbid %s. %s", local_dbname.c_str(),
      dbid.c_str(), st.ToString().c_str());
  return st;
}

Status DBCloudImpl::Savepoint() {
  std::string dbid;
  Options default_options = GetOptions();
  Status st = GetDbIdentity(dbid);
  if (!st.ok()) {
    Log(InfoLogLevel::INFO_LEVEL, default_options.info_log,
        "Savepoint could not get dbid %s", st.ToString().c_str());
    return st;
  }
  CloudEnvImpl* cenv = static_cast<CloudEnvImpl*>(GetEnv());

  // If there is no destination bucket, then nothing to do
  if (!cenv->HasDestBucket()) {
    Log(InfoLogLevel::INFO_LEVEL, default_options.info_log,
        "Savepoint on cloud dbid %s has no destination bucket, nothing to do.",
        dbid.c_str());
    return st;
  }

  Log(InfoLogLevel::INFO_LEVEL, default_options.info_log,
      "Savepoint on cloud dbid  %s", dbid.c_str());

  // find all sst files in the db
  std::vector<LiveFileMetaData> live_files;
  GetLiveFilesMetaData(&live_files);

  // If an sst file does not exist in the destination path, then remember it
  std::vector<std::string> to_copy;
  for (auto onefile : live_files) {
    auto remapped_fname = cenv->RemapFilename(onefile.name);
    std::string destpath = cenv->GetDestObjectPath() + "/" + remapped_fname;
    if (!cenv->ExistsObject(cenv->GetDestBucketName(), destpath).ok()) {
      to_copy.push_back(remapped_fname);
    }
  }

  // copy all files in parallel
  std::atomic<size_t> next_file_meta_idx(0);
  int max_threads = default_options.max_file_opening_threads;

  std::function<void()> load_handlers_func = [&]() {
    while (true) {
      size_t idx = next_file_meta_idx.fetch_add(1);
      if (idx >= to_copy.size()) {
        break;
      }
      auto& onefile = to_copy[idx];
      Status s = cenv->CopyObject(
          cenv->GetSrcBucketName(), cenv->GetSrcObjectPath() + "/" + onefile,
          cenv->GetDestBucketName(), cenv->GetDestObjectPath() + "/" + onefile);
      if (!s.ok()) {
        Log(InfoLogLevel::INFO_LEVEL, default_options.info_log,
            "Savepoint on cloud dbid  %s error in copying srcbucket %s srcpath "
            "%s dest bucket %s dest path %s. %s",
            dbid.c_str(), cenv->GetSrcBucketName().c_str(),
            cenv->GetSrcObjectPath().c_str(), cenv->GetDestBucketName().c_str(),
            cenv->GetDestObjectPath().c_str(), s.ToString().c_str());
        if (st.ok()) {
          st = s;  // save at least one error
        }
        break;
      }
    }
  };

  if (max_threads <= 1) {
    load_handlers_func();
  } else {
    std::vector<port::Thread> threads;
    for (int i = 0; i < max_threads; i++) {
      threads.emplace_back(load_handlers_func);
    }
    for (auto& t : threads) {
      t.join();
    }
  }
  return st;
}

Status DBCloudImpl::CheckpointToCloud(const BucketOptions& destination,
                                      const CheckpointToCloudOptions& options) {
  DisableFileDeletions();
  auto st = DoCheckpointToCloud(destination, options);
  EnableFileDeletions(false);
  return st;
}

Status DBCloudImpl::DoCheckpointToCloud(
    const BucketOptions& destination, const CheckpointToCloudOptions& options) {
  std::vector<std::string> live_files;
  uint64_t manifest_file_size{0};
  auto cenv = static_cast<CloudEnvImpl*>(GetEnv());
  auto base_env = cenv->GetBaseEnv();

  auto st =
      GetLiveFiles(live_files, &manifest_file_size, options.flush_memtable);
  if (!st.ok()) {
    return st;
  }

  std::vector<std::pair<std::string, std::string>> files_to_copy;
  for (auto& f : live_files) {
    uint64_t number = 0;
    FileType type;
    auto ok = ParseFileName(f, &number, &type);
    if (!ok) {
      return Status::InvalidArgument("Unknown file " + f);
    }
    if (type != kTableFile) {
      // ignore
      continue;
    }
    auto remapped_fname = cenv->RemapFilename(f);
    files_to_copy.emplace_back(remapped_fname, remapped_fname);
  }

  // IDENTITY file
  std::string dbid;
  st = ReadFileToString(cenv, IdentityFileName(GetName()), &dbid);
  if (!st.ok()) {
    return st;
  }
  dbid = rtrim_if(trim(dbid), '\n');
  files_to_copy.emplace_back(IdentityFileName(""), IdentityFileName(""));

  // MANIFEST file
  auto current_epoch = cenv->GetCloudManifest()->GetCurrentEpoch().ToString();
  auto manifest_fname = ManifestFileWithEpoch("", current_epoch);
  auto tmp_manifest_fname = manifest_fname + ".tmp";
  st =
      CopyFile(base_env, GetName() + "/" + manifest_fname,
               GetName() + "/" + tmp_manifest_fname, manifest_file_size, false);
  if (!st.ok()) {
    return st;
  }
  files_to_copy.emplace_back(tmp_manifest_fname, std::move(manifest_fname));

  // CLOUDMANIFEST file
  files_to_copy.emplace_back(CloudManifestFile(""), CloudManifestFile(""));

  std::atomic<size_t> next_file_to_copy{0};
  int thread_count = std::max(1, options.thread_count);
  std::vector<Status> thread_statuses;
  thread_statuses.resize(thread_count);

  auto do_copy = [&](size_t threadId) {
    while (true) {
      size_t idx = next_file_to_copy.fetch_add(1);
      if (idx >= files_to_copy.size()) {
        break;
      }

      auto& f = files_to_copy[idx];
      auto copy_st =
          cenv->PutObject(GetName() + "/" + f.first, destination.GetBucketName(),
                          destination.GetObjectPath() + "/" + f.second);
      if (!copy_st.ok()) {
        thread_statuses[threadId] = std::move(copy_st);
        break;
      }
    }
  };

  if (thread_count == 1) {
    do_copy(0);
  } else {
    std::vector<std::thread> threads;
    for (int i = 0; i < thread_count; ++i) {
      threads.emplace_back([&, i]() { do_copy(i); });
    }
    for (auto& t : threads) {
      t.join();
    }
  }

  for (auto& s : thread_statuses) {
    if (!s.ok()) {
      st = s;
      break;
    }
  }

  if (!st.ok()) {
      return st;
  }

  // Ignore errors
  base_env->DeleteFile(tmp_manifest_fname);

  st = cenv->SaveDbid(destination.GetBucketName(), dbid,
                      destination.GetObjectPath());
  return st;
}
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
