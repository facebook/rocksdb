// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "cloud/db_cloud_impl.h"

#include <inttypes.h>

#include "cloud/aws/aws_env.h"
#include "cloud/cloud_env_wrapper.h"
#include "cloud/filename.h"
#include "cloud/manifest_reader.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/persistent_cache.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "util/auto_roll_logger.h"
#include "util/file_reader_writer.h"
#include "util/file_util.h"
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

namespace {
Status writeCloudManifest(Env* local_env, CloudManifest* manifest,
                          std::string fname) {
  // Write to tmp file and atomically rename later. This helps if we crash
  // mid-write :)
  auto tmp_fname = fname + ".tmp";
  std::unique_ptr<WritableFile> file;
  Status s = local_env->NewWritableFile(tmp_fname, &file, EnvOptions());
  if (s.ok()) {
    s = manifest->WriteToLog(unique_ptr<WritableFileWriter>(
        new WritableFileWriter(std::move(file), tmp_fname, EnvOptions())));
  }
  if (s.ok()) {
    s = local_env->RenameFile(tmp_fname, fname);
  }
  return s;
}

// we map a longer string given by env->GenerateUniqueId() into 16-byte string
std::string generateNewEpochId(Env* env) {
  auto uniqueId = env->GenerateUniqueId();
  size_t split = uniqueId.size() / 2;
  auto low = uniqueId.substr(0, split);
  auto hi = uniqueId.substr(split);
  uint64_t hash =
      XXH32(low.data(), static_cast<int>(low.size()), 0) +
      (static_cast<uint64_t>(XXH32(hi.data(), static_cast<int>(hi.size()), 0))
       << 32);
  char buf[17];
  snprintf(buf, sizeof buf, "%0" PRIx64, hash);
  return buf;
}
};  // namespace

Status DBCloud::PreloadCloudManifest(CloudEnv* env, const Options& options,
                                     const std::string& local_dbname) {
  CloudEnvImpl* cenv = static_cast<CloudEnvImpl*>(env);
  Status st;
  Env* local_env = cenv->GetBaseEnv();
  local_env->CreateDirIfMissing(local_dbname);
  if (cenv->GetCloudType() != CloudType::kCloudNone) {
    st =
        DBCloudImpl::MaybeMigrateManifestFile(local_env, options, local_dbname);
    if (st.ok()) {
      // Init cloud manifest
      st = DBCloudImpl::FetchCloudManifest(cenv, options, local_dbname, false);
    }
    if (st.ok()) {
      // Inits CloudEnvImpl::cloud_manifest_, which will enable us to read files
      // from the cloud
      st = cenv->LoadLocalCloudManifest(local_dbname);
    }
  }
  return st;
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
  Env* local_env = cenv->GetBaseEnv();
  if (!read_only) {
    local_env->CreateDirIfMissing(local_dbname);
  }

  st = DBCloudImpl::SanitizeDirectory(options, local_dbname, read_only);
  if (!st.ok()) {
    return st;
  }

  if (cenv->GetCloudType() != CloudType::kCloudNone) {
    st =
        DBCloudImpl::MaybeMigrateManifestFile(local_env, options, local_dbname);
    if (st.ok()) {
      // Init cloud manifest
      st = DBCloudImpl::FetchCloudManifest(cenv, options, local_dbname, false);
    }
    if (st.ok()) {
      // Inits CloudEnvImpl::cloud_manifest_, which will enable us to read files
      // from the cloud
      st = cenv->LoadLocalCloudManifest(local_dbname);
    }
    if (st.ok()) {
      // Rolls the new epoch in CLOUDMANIFEST
      st = DBCloudImpl::RollNewEpoch(cenv, local_dbname);
    }
    if (!st.ok()) {
      return st;
    }

    // Do the cleanup, but don't fail if the cleanup fails.
    if (!read_only) {
      st = cenv->DeleteInvisibleFiles(local_dbname);
      if (!st.ok()) {
        Log(InfoLogLevel::INFO_LEVEL, options.info_log,
            "Failed to delete invisible files: %s", st.ToString().c_str());
        // Ignore the fail
        st = Status::OK();
      }
    }
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
              "Created persistent cache %s with size %ld GB",
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
  if (cenv->GetDestObjectPath().empty() || cenv->GetDestBucketName().empty()) {
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

Status DBCloudImpl::CreateNewIdentityFile(CloudEnv* cenv,
                                          const Options& options,
                                          const std::string& dbid,
                                          const std::string& local_name) {
  const EnvOptions soptions;
  auto tmp_identity_path = local_name + "/IDENTITY.tmp";
  Env* env = cenv->GetBaseEnv();
  Status st;
  {
    unique_ptr<WritableFile> destfile;
    st = env->NewWritableFile(tmp_identity_path, &destfile, soptions);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, options.info_log,
          "[db_cloud_impl] Unable to create local IDENTITY file to %s %s",
          tmp_identity_path.c_str(), st.ToString().c_str());
      return st;
    }
    st = destfile->Append(Slice(dbid));
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, options.info_log,
          "[db_cloud_impl] Unable to write new dbid to local IDENTITY file "
          "%s %s",
          tmp_identity_path.c_str(), st.ToString().c_str());
      return st;
    }
  }
  Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
      "[db_cloud_impl] Written new dbid %s to %s %s", dbid.c_str(),
      tmp_identity_path.c_str(), st.ToString().c_str());

  // Rename ID file on local filesystem and upload it to dest bucket too
  st = cenv->RenameFile(tmp_identity_path, local_name + "/IDENTITY");
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, options.info_log,
        "[db_cloud_impl] Unable to rename newly created IDENTITY.tmp "
        " to IDENTITY. %S",
        st.ToString().c_str());
    return st;
  }
  return st;
}

//
// Shall we re-initialize the local dir?
//
Status DBCloudImpl::NeedsReinitialization(CloudEnv* cenv,
                                          const Options& options,
                                          const std::string& local_dir,
                                          bool* do_reinit) {
  Log(InfoLogLevel::INFO_LEVEL, options.info_log,
      "[db_cloud_impl] NeedsReinitialization: "
      "checking local dir %s src bucket %s src path %s "
      "dest bucket %s dest path %s",
      local_dir.c_str(), cenv->GetSrcBucketName().c_str(),
      cenv->GetSrcObjectPath().c_str(), cenv->GetDestBucketName().c_str(),
      cenv->GetDestObjectPath().c_str());

  CloudEnvImpl* cimpl = static_cast<CloudEnvImpl*>(cenv);

  // If no buckets are specified, then we cannot reinit anyways
  if (cenv->GetSrcBucketName().empty() && cenv->GetDestBucketName().empty()) {
    Log(InfoLogLevel::INFO_LEVEL, options.info_log,
        "[db_cloud_impl] NeedsReinitialization: "
        "Both src and dest buckets are empty");
    *do_reinit = false;
    return Status::OK();
  }

  // assume that directory does needs reinitialization
  *do_reinit = true;

  // get local env
  Env* env = cenv->GetBaseEnv();

  // Check if local directory exists
  auto st = env->FileExists(local_dir);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, options.info_log,
        "[db_cloud_impl] NeedsReinitialization: "
        "failed to access local dir %s: %s",
        local_dir.c_str(), st.ToString().c_str());
    // If the directory is not found, we should create it. In case of an other
    // IO error, we need to fail
    return st.IsNotFound() ? Status::OK() : st;
  }

  // Check if CURRENT file exists
  st = env->FileExists(CurrentFileName(local_dir));
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, options.info_log,
        "[db_cloud_impl] NeedsReinitialization: "
        "failed to find CURRENT file %s: %s",
        CurrentFileName(local_dir).c_str(), st.ToString().c_str());
    return st.IsNotFound() ? Status::OK() : st;
  }

  if (cenv->GetCloudEnvOptions().skip_dbid_verification) {
    *do_reinit = false;
    return Status::OK();
  }

  // Read DBID file from local dir
  std::string local_dbid;
  st = ReadFileToString(env, IdentityFileName(local_dir), &local_dbid);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, options.info_log,
        "[db_cloud_impl] NeedsReinitialization: "
        "local dir %s unable to read local dbid: %s",
        local_dir.c_str(), st.ToString().c_str());
    return st.IsNotFound() ? Status::OK() : st;
  }
  local_dbid = rtrim_if(trim(local_dbid), '\n');
  auto& src_bucket = cenv->GetSrcBucketName();
  auto& dest_bucket = cenv->GetDestBucketName();

  // We found a dbid in the local dir. Verify that it matches
  // what we found on the cloud.
  std::string src_object_path;

  // If a src bucket is specified, then get src dbid
  if (!src_bucket.empty()) {
    st = cenv->GetPathForDbid(src_bucket, local_dbid, &src_object_path);
    if (!st.ok() && !st.IsNotFound()) {
      // Unable to fetch data from S3. Fail Open request.
      Log(InfoLogLevel::ERROR_LEVEL, options.info_log,
          "[db_cloud_impl] NeedsReinitialization: "
          "Local dbid is %s but unable to find src dbid",
          local_dbid.c_str());
      return st;
    }
    Log(InfoLogLevel::INFO_LEVEL, options.info_log,
        "[db_cloud_impl] NeedsReinitialization: "
        "Local dbid is %s and src object path in registry is '%s'",
        local_dbid.c_str(), src_object_path.c_str());

    if (st.ok()) {
      src_object_path = rtrim_if(trim(src_object_path), '/');
    }
    Log(InfoLogLevel::INFO_LEVEL, options.info_log,
        "[db_cloud_impl] NeedsReinitialization: "
        "Local dbid %s configured src path %s src dbid registry",
        local_dbid.c_str(), src_object_path.c_str());
  }
  std::string dest_object_path;

  // If a dest bucket is specified, then get dest dbid
  if (!dest_bucket.empty()) {
    st = cenv->GetPathForDbid(dest_bucket, local_dbid, &dest_object_path);
    if (!st.ok() && !st.IsNotFound()) {
      // Unable to fetch data from S3. Fail Open request.
      Log(InfoLogLevel::ERROR_LEVEL, options.info_log,
          "[db_cloud_impl] NeedsReinitialization: "
          "Local dbid is %s but unable to find dest dbid",
          local_dbid.c_str());
      return st;
    }
    Log(InfoLogLevel::INFO_LEVEL, options.info_log,
        "[db_cloud_impl] NeedsReinitialization: "
        "Local dbid is %s and dest object path in registry is '%s'",
        local_dbid.c_str(), dest_object_path.c_str());

    if (st.ok()) {
      dest_object_path = rtrim_if(trim(dest_object_path), '/');
      std::string dest_specified_path = cenv->GetDestObjectPath();
      dest_specified_path = rtrim_if(trim(dest_specified_path), '/');

      // If the registered dest path does not match the one specified in
      // our env, then fail the OpenDB request.
      if (dest_object_path != dest_specified_path) {
        Log(InfoLogLevel::ERROR_LEVEL, options.info_log,
            "[db_cloud_impl] NeedsReinitialization: "
            "Local dbid %s dest path specified in env is %s "
            " but dest path in registry is %s",
            local_dbid.c_str(), cenv->GetDestObjectPath().c_str(),
            dest_object_path.c_str());
        return Status::InvalidArgument(
            "[db_cloud_impl] NeedsReinitialization: bad dest path");
      }
    }
    Log(InfoLogLevel::INFO_LEVEL, options.info_log,
        "[db_cloud_impl] NeedsReinitialization: "
        "Local dbid %s configured path %s matches the dest dbid registry",
        local_dbid.c_str(), dest_object_path.c_str());
  }

  // Ephemeral clones do not write their dbid to the cloud registry.
  // Then extract them from the env-configured paths
  std::string src_dbid;
  std::string dest_dbid;
  st = GetCloudDbid(cimpl, options, local_dir, &src_dbid, &dest_dbid);
  if (!st.ok()) {
    Log(InfoLogLevel::INFO_LEVEL, options.info_log,
        "[db_cloud_impl] NeedsReinitialization: "
        "Unable to extract dbid from cloud paths %s",
        st.ToString().c_str());
    return st;
  }

  // If we found a src_dbid, then it should be a prefix of local_dbid
  if (!src_dbid.empty()) {
    size_t pos = local_dbid.find(src_dbid);
    if (pos == std::string::npos) {
      Log(InfoLogLevel::ERROR_LEVEL, options.info_log,
          "[db_cloud_impl] NeedsReinitialization: "
          "dbid %s in src bucket %s is not a prefix of local dbid %s",
          src_dbid.c_str(), src_bucket.c_str(), local_dbid.c_str());
      return Status::OK();
    }
    Log(InfoLogLevel::INFO_LEVEL, options.info_log,
        "[db_cloud_impl] NeedsReinitialization: "
        "dbid %s in src bucket %s is a prefix of local dbid %s",
        src_dbid.c_str(), src_bucket.c_str(), local_dbid.c_str());

    // If the local dbid is an exact match with the src dbid, then ensure
    // that we cannot run in a 'clone' mode.
    if (local_dbid == src_dbid) {
      Log(InfoLogLevel::INFO_LEVEL, options.info_log,
          "[db_cloud_impl] NeedsReinitialization: "
          "dbid %s in src bucket %s is same as local dbid",
          src_dbid.c_str(), src_bucket.c_str());

      if (!dest_bucket.empty() && src_bucket != dest_bucket) {
        Log(InfoLogLevel::ERROR_LEVEL, options.info_log,
            "[db_cloud_impl] NeedsReinitialization: "
            "local dbid %s in same as src dbid but clone mode specified",
            local_dbid.c_str());
        return Status::OK();
      }
    }
  }

  // If we found a dest_dbid, then it should be a prefix of local_dbid
  if (!dest_dbid.empty()) {
    size_t pos = local_dbid.find(dest_dbid);
    if (pos == std::string::npos) {
      Log(InfoLogLevel::ERROR_LEVEL, options.info_log,
          "[db_cloud_impl] NeedsReinitialization: "
          "dbid %s in dest bucket %s is not a prefix of local dbid %s",
          dest_dbid.c_str(), dest_bucket.c_str(), local_dbid.c_str());
      return Status::OK();
    }
    Log(InfoLogLevel::INFO_LEVEL, options.info_log,
        "[db_cloud_impl] NeedsReinitialization: "
        "dbid %s in dest bucket %s is a prefix of local dbid %s",
        dest_dbid.c_str(), dest_bucket.c_str(), local_dbid.c_str());

    // If the local dbid is an exact match with the destination dbid, then
    // ensure that we are run not in a 'clone' mode.
    if (local_dbid == dest_dbid) {
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] NeedsReinitialization: "
          "dbid %s in dest bucket %s is same as local dbid",
          dest_dbid.c_str(), dest_bucket.c_str());

      if (!src_bucket.empty() && src_bucket != dest_bucket) {
        Log(InfoLogLevel::ERROR_LEVEL, options.info_log,
            "[db_cloud_impl] NeedsReinitialization: "
            "local dbid %s in same as dest dbid but clone mode specified",
            local_dbid.c_str());
        return Status::OK();
      }
    }
  }

  // We found a local dbid but we did not find this dbid in bucket registry.
  if (src_object_path.empty() && dest_object_path.empty()) {
    Log(InfoLogLevel::ERROR_LEVEL, options.info_log,
        "[db_cloud_impl] NeedsReinitialization: "
        "local dbid %s does not have a mapping in cloud registry "
        "src bucket %s or dest bucket %s",
        local_dbid.c_str(), src_bucket.c_str(), dest_bucket.c_str());

    // This is an ephemeral clone. Resync all files from cloud.
    // If the  resycn failed, then return success to indicate that
    // the local directory needs to be completely removed and recreated.
    st = ResyncDir(cimpl, options, local_dir);
    if (!st.ok()) {
      return Status::OK();
    }
  }
  // ID's in the local dir are valid.

  // The DBID of the local dir is compatible with the src and dest buckets.
  // We do not need any re-initialization of local dir.
  *do_reinit = false;
  return Status::OK();
}

//
// Check and fix all files in local dir and cloud dir.
// This should be called only for ephemeral clones.
// Returns Status::OK if we want to keep all local data,
// otherwise all local data will be erased.
//
Status DBCloudImpl::ResyncDir(CloudEnvImpl* cenv, const Options& options,
                              const std::string& local_dir) {
  auto& src_bucket = cenv->GetSrcBucketName();
  auto& dest_bucket = cenv->GetDestBucketName();
  if (!dest_bucket.empty()) {
    Log(InfoLogLevel::ERROR_LEVEL, options.info_log,
        "[db_cloud_impl] ResyncDir: "
        "not an ephemeral clone local dir %s "
        "src bucket %s dest bucket %s",
        local_dir.c_str(), src_bucket.c_str(), dest_bucket.c_str());
    return Status::InvalidArgument();
  }
  // If ephemeral_resync_on_open is false, then we want to keep all local
  // data intact.
  if (!cenv->GetCloudEnvOptions().ephemeral_resync_on_open) {
    return Status::OK();
  }

  // Copy the src cloud manifest to local dir. This essentially means that
  // the local db is resycned with the src cloud bucket. All new files will be
  // pulled in as needed when we open the db.
  return FetchCloudManifest(cenv, options, local_dir, true);
}

//
// Extract the src dbid and the dest dbid from the cloud paths
//
Status DBCloudImpl::GetCloudDbid(CloudEnvImpl* cenv, const Options& options,
                                 const std::string& local_dir,
                                 std::string* src_dbid,
                                 std::string* dest_dbid) {
  // get local env
  Env* env = cenv->GetBaseEnv();

  // use a tmp file in local dir
  std::string tmpfile = IdentityFileName(local_dir) + ".cloud.tmp";

  // Delete any old data remaining there
  env->DeleteFile(tmpfile);

  // Read dbid from src bucket if it exists
  if (!cenv->GetSrcBucketName().empty()) {
    Status st =
        cenv->GetObject(cenv->GetSrcBucketName(),
                        cenv->GetSrcObjectPath() + "/IDENTITY", tmpfile);
    if (!st.ok() && !st.IsNotFound()) {
      return st;
    }
    if (st.ok()) {
      std::string sid;
      st = ReadFileToString(env, tmpfile, &sid);
      if (st.ok()) {
        src_dbid->assign(rtrim_if(trim(sid), '\n'));
        env->DeleteFile(tmpfile);
      } else {
        Log(InfoLogLevel::ERROR_LEVEL, options.info_log,
            "[db_cloud_impl] GetCloudDbid: "
            "local dir %s unable to read src dbid: %s",
            local_dir.c_str(), st.ToString().c_str());
        return st;
      }
    }
  }

  // Read dbid from dest bucket if it exists
  if (!cenv->GetDestBucketName().empty()) {
    Status st =
        cenv->GetObject(cenv->GetDestBucketName(),
                        cenv->GetDestObjectPath() + "/IDENTITY", tmpfile);
    if (!st.ok() && !st.IsNotFound()) {
      return st;
    }
    if (st.ok()) {
      std::string sid;
      st = ReadFileToString(env, tmpfile, &sid);
      if (st.ok()) {
        dest_dbid->assign(rtrim_if(trim(sid), '\n'));
        env->DeleteFile(tmpfile);
      } else {
        Log(InfoLogLevel::ERROR_LEVEL, options.info_log,
            "[db_cloud_impl] GetCloudDbid: "
            "local dir %s unable to read dest dbid: %s",
            local_dir.c_str(), st.ToString().c_str());
        return st;
      }
    }
  }
  return Status::OK();
}

//
// Create appropriate files in the clone dir
//
Status DBCloudImpl::SanitizeDirectory(const Options& options,
                                      const std::string& local_name,
                                      bool read_only) {
  EnvOptions soptions;

  CloudEnvImpl* cenv = static_cast<CloudEnvImpl*>(options.env);
  if (cenv->GetCloudType() == CloudType::kCloudNone) {
    // We don't need to SanitizeDirectory()
    Log(InfoLogLevel::INFO_LEVEL, options.info_log,
        "[db_cloud_impl] SanitizeDirectory skipping dir %s for non-cloud env",
        local_name.c_str());
    return Status::OK();
  }
  if (cenv->GetCloudType() != CloudType::kCloudAws) {
    Log(InfoLogLevel::ERROR_LEVEL, options.info_log,
        "[db_cloud_impl] SanitizeDirectory dir %s found non aws env",
        local_name.c_str());
    return Status::NotSupported("We only support AWS for now.");
  }
  // acquire the local env
  Env* env = cenv->GetBaseEnv();

  // Shall we reinitialize the clone dir?
  bool do_reinit = true;
  Status st =
      DBCloudImpl::NeedsReinitialization(cenv, options, local_name, &do_reinit);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, options.info_log,
        "[db_cloud_impl] SanitizeDirectory error inspecting dir %s %s",
        local_name.c_str(), st.ToString().c_str());
    return st;
  }

  // If there is no destination bucket, then we need to suck in all sst files
  // from source bucket at db startup time. We do this by setting max_open_files
  // = -1
  if (cenv->GetDestBucketName().empty()) {
    if (options.max_open_files != -1) {
      Log(InfoLogLevel::ERROR_LEVEL, options.info_log,
          "[db_cloud_impl] SanitizeDirectory error.  "
          " No destination bucket specified. Set options.max_open_files = -1 "
          " to copy in all sst files from src bucket %s into local dir %s",
          cenv->GetSrcObjectPath().c_str(), local_name.c_str());
      return Status::InvalidArgument(
          "No destination bucket. "
          "Set options.max_open_files = -1");
    }
    if (!cenv->GetCloudEnvOptions().keep_local_sst_files && !read_only) {
      Log(InfoLogLevel::ERROR_LEVEL, options.info_log,
          "[db_cloud_impl] SanitizeDirectory error.  "
          " No destination bucket specified. Set options.keep_local_sst_files "
          "= true to copy in all sst files from src bucket %s into local dir "
          "%s",
          cenv->GetSrcObjectPath().c_str(), local_name.c_str());
      return Status::InvalidArgument(
          "No destination bucket. "
          "Set options.keep_local_sst_files = true");
    }
  }

  if (!do_reinit) {
    Log(InfoLogLevel::INFO_LEVEL, options.info_log,
        "[db_cloud_impl] SanitizeDirectory local directory %s is good",
        local_name.c_str());
    return Status::OK();
  }
  Log(InfoLogLevel::ERROR_LEVEL, options.info_log,
      "[db_cloud_impl] SanitizeDirectory local directory %s cleanup needed",
      local_name.c_str());

  // Delete all local files
  std::vector<Env::FileAttributes> result;
  st = env->GetChildrenFileAttributes(local_name, &result);
  if (!st.ok() && !st.IsNotFound()) {
    return st;
  }
  for (auto file : result) {
    if (file.name == "." || file.name == "..") {
      continue;
    }
    if (file.name.find("LOG") == 0) {  // keep LOG files
      continue;
    }
    std::string pathname = local_name + "/" + file.name;
    st = env->DeleteFile(pathname);
    if (!st.ok()) {
      return st;
    }
    Log(InfoLogLevel::INFO_LEVEL, options.info_log,
        "[db_cloud_impl] SanitizeDirectory cleaned-up: '%s'", pathname.c_str());
  }

  if (!st.ok()) {
    Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
        "[db_cloud_impl] SanitizeDirectory error opening dir %s %s",
        local_name.c_str(), st.ToString().c_str());
    return st;
  }

  bool dest_equal_src = cenv->GetSrcBucketName() == cenv->GetDestBucketName() &&
                        cenv->GetSrcObjectPath() == cenv->GetDestObjectPath();

  Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
      "[db_cloud_impl] SanitizeDirectory dest_equal_src = %d", dest_equal_src);

  bool got_identity_from_dest = false, got_identity_from_src = false;

  // Download IDENTITY, first try destination, then source
  if (!cenv->GetDestBucketName().empty()) {
    // download IDENTITY from dest
    st = cenv->GetObject(cenv->GetDestBucketName(),
                         IdentityFileName(cenv->GetDestObjectPath()),
                         IdentityFileName(local_name));
    if (!st.ok() && !st.IsNotFound()) {
      // If there was an error and it's not IsNotFound() we need to bail
      return st;
    }
    got_identity_from_dest = st.ok();
  }
  if (!cenv->GetSrcBucketName().empty() && !dest_equal_src &&
      !got_identity_from_dest) {
    // download IDENTITY from src
    st = cenv->GetObject(cenv->GetSrcBucketName(),
                         IdentityFileName(cenv->GetSrcObjectPath()),
                         IdentityFileName(local_name));
    if (!st.ok() && !st.IsNotFound()) {
      // If there was an error and it's not IsNotFound() we need to bail
      return st;
    }
    got_identity_from_src = st.ok();
  }

  if (!got_identity_from_src && !got_identity_from_dest) {
    // There isn't a valid db in either the src or dest bucket.
    // Return with a success code so that a new DB can be created.
    Log(InfoLogLevel::ERROR_LEVEL, options.info_log,
        "[db_cloud_impl] No valid dbs in src bucket %s src path %s "
        "or dest bucket %s dest path %s",
        cenv->GetSrcBucketName().c_str(), cenv->GetSrcObjectPath().c_str(),
        cenv->GetDestBucketName().c_str(), cenv->GetDestObjectPath().c_str());
    return Status::OK();
  }

  if (got_identity_from_src && !dest_equal_src) {
    // If we got dbid from src but not from dest.
    // Then we are just opening this database as a clone (for the first time).
    // Either as a true clone or as an ephemeral clone.
    // Create a new dbid for this clone.
    std::string src_dbid;
    st = ReadFileToString(env, IdentityFileName(local_name), &src_dbid);
    if (!st.ok()) {
      return st;
    }
    src_dbid = rtrim_if(trim(src_dbid), '\n');

    std::string new_dbid = src_dbid +
                           std::string(CloudEnvImpl::DBID_SEPARATOR) +
                           env->GenerateUniqueId();

    st = CreateNewIdentityFile(cenv, options, new_dbid, local_name);
    if (!st.ok()) {
      return st;
    }
  }

  // create dummy CURRENT file to point to the dummy manifest (cloud env will
  // remap the filename appropriately, this is just to fool the underyling
  // RocksDB)
  {
    unique_ptr<WritableFile> destfile;
    st = env->NewWritableFile(CurrentFileName(local_name), &destfile, soptions);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, options.info_log,
          "[db_cloud_impl] Unable to create local CURRENT file to %s %s",
          local_name.c_str(), st.ToString().c_str());
      return st;
    }
    Log(InfoLogLevel::INFO_LEVEL, options.info_log,
        "[db_cloud_impl] SanitizeDirectory creating dummy CURRENT file");
    std::string manifestfile =
        "MANIFEST-000001\n";  // CURRENT file needs a newline
    st = destfile->Append(Slice(manifestfile));
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, options.info_log,
          "[db_cloud_impl] Unable to write local CURRENT file to %s %s",
          local_name.c_str(), st.ToString().c_str());
      return st;
    }
  }
  return Status::OK();
}

Status DBCloudImpl::FetchCloudManifest(CloudEnv* cenv, const Options& options,
                                       const std::string& local_dbname,
                                       bool force) {
  bool dest = !cenv->GetDestBucketName().empty();
  bool src = !cenv->GetSrcBucketName().empty();
  bool dest_equal_src = cenv->GetSrcBucketName() == cenv->GetDestBucketName() &&
                        cenv->GetSrcObjectPath() == cenv->GetDestObjectPath();
  std::string cloudmanifest = CloudManifestFile(local_dbname);
  if (!dest && !force && cenv->GetBaseEnv()->FileExists(cloudmanifest).ok()) {
    // nothing to do here, we have our cloud manifest
    Log(InfoLogLevel::INFO_LEVEL, options.info_log,
        "[db_cloud_impl] FetchCloudManifest: Nothing to do %s exists",
        cloudmanifest.c_str());
    return Status::OK();
  }
  // first try to get cloudmanifest from dest
  if (dest) {
    Status st = cenv->GetObject(cenv->GetDestBucketName(),
                                CloudManifestFile(cenv->GetDestObjectPath()),
                                cloudmanifest);
    if (!st.ok() && !st.IsNotFound()) {
      // something went wrong, bail out
      Log(InfoLogLevel::INFO_LEVEL, options.info_log,
          "[db_cloud_impl] FetchCloudManifest: Failed to fetch "
          " cloud manifest %s from dest %s",
          cloudmanifest.c_str(), cenv->GetDestBucketName().c_str());
      return st;
    }
    if (st.ok()) {
      // found it!
      Log(InfoLogLevel::INFO_LEVEL, options.info_log,
          "[db_cloud_impl] FetchCloudManifest: Fetched"
          " cloud manifest %s from dest %s",
          cloudmanifest.c_str(), cenv->GetDestBucketName().c_str());
      return st;
    }
  }
  // we couldn't get cloud manifest from dest, need to try from src?
  if (src && !dest_equal_src) {
    Status st = cenv->GetObject(cenv->GetSrcBucketName(),
                                CloudManifestFile(cenv->GetSrcObjectPath()),
                                cloudmanifest);
    if (!st.ok() && !st.IsNotFound()) {
      // something went wrong, bail out
      Log(InfoLogLevel::INFO_LEVEL, options.info_log,
          "[db_cloud_impl] FetchCloudManifest: Failed to fetch "
          " cloud manifest %s from src %s",
          cloudmanifest.c_str(), cenv->GetSrcBucketName().c_str());
      return st;
    }
    if (st.ok()) {
      // found it!
      Log(InfoLogLevel::INFO_LEVEL, options.info_log,
          "[db_cloud_impl] FetchCloudManifest: Fetched"
          " cloud manifest %s from src %s",
          cloudmanifest.c_str(), cenv->GetSrcBucketName().c_str());
      return st;
    }
  }
  Log(InfoLogLevel::INFO_LEVEL, options.info_log,
      "[db_cloud_impl] FetchCloudManifest: Creating new"
      " cloud manifest for %s",
      local_dbname.c_str());

  // No cloud manifest, create an empty one
  unique_ptr<CloudManifest> manifest;
  CloudManifest::CreateForEmptyDatabase("", &manifest);
  return writeCloudManifest(cenv->GetBaseEnv(), manifest.get(), cloudmanifest);
}

Status DBCloudImpl::MaybeMigrateManifestFile(Env* local_env,
                                             const Options& options,
                                             const std::string& local_dbname) {
  std::string manifest_filename;
  auto st = local_env->FileExists(CurrentFileName(local_dbname));
  if (st.IsNotFound()) {
    // No need to migrate
    Log(InfoLogLevel::INFO_LEVEL, options.info_log,
        "[db_cloud_impl] MaybeMigrateManifestFile: No need to migrate %s",
        CurrentFileName(local_dbname).c_str());
    return Status::OK();
  }
  if (!st.ok()) {
    return st;
  }
  st = ReadFileToString(local_env, CurrentFileName(local_dbname),
                        &manifest_filename);
  if (!st.ok()) {
    return st;
  }
  // Note: This rename is important for migration. If we are just starting on
  // an old database, our local MANIFEST filename will be something like
  // MANIFEST-00001 instead of MANIFEST. If we don't do the rename we'll
  // download MANIFEST file from the cloud, which might not be what we want do
  // to (especially for databases which don't have a destination bucket
  // specified). This piece of code can be removed post-migration.
  manifest_filename = local_dbname + "/" + rtrim_if(manifest_filename, '\n');
  if (local_env->FileExists(manifest_filename).IsNotFound()) {
    // manifest doesn't exist, shrug
    Log(InfoLogLevel::INFO_LEVEL, options.info_log,
        "[db_cloud_impl] MaybeMigrateManifestFile: Manifest %s does not exist",
        manifest_filename.c_str());
    return Status::OK();
  }
  return local_env->RenameFile(manifest_filename, local_dbname + "/MANIFEST");
}

Status DBCloudImpl::RollNewEpoch(CloudEnvImpl* cenv,
                                 const std::string& local_dbname) {
  auto oldEpoch = cenv->GetCloudManifest()->GetCurrentEpoch().ToString();
  // Find next file number. We use dummy MANIFEST filename, which should get
  // remapped into the correct MANIFEST filename through CloudManifest.
  // After this call we should also have a local file named
  // MANIFEST-<current_epoch> (unless st.IsNotFound()).
  uint64_t maxFileNumber;
  auto st = ManifestReader::GetMaxFileNumberFromManifest(
      cenv, local_dbname + "/MANIFEST-000001", &maxFileNumber);
  if (st.IsNotFound()) {
    // This is a new database!
    maxFileNumber = 0;
    st = Status::OK();
  } else if (!st.ok()) {
    // uh oh
    return st;
  }
  // roll new epoch
  auto newEpoch = generateNewEpochId(cenv);
  cenv->GetCloudManifest()->AddEpoch(maxFileNumber, newEpoch);
  cenv->GetCloudManifest()->Finalize();
  if (maxFileNumber > 0) {
    // Meaning, this is not a new database and we should have
    // ManifestFileWithEpoch(local_dbname, oldEpoch) locally.
    // In that case, we have to move our old manifest to the new filename.
    // However, we don't move here, we copy. If we moved and crashed immediately
    // after (before writing CLOUDMANIFEST), we'd corrupt our database. The old
    // MANIFEST file will be cleaned up in DeleteInvisibleFiles().
    st = CopyFile(cenv->GetBaseEnv(),
                  ManifestFileWithEpoch(local_dbname, oldEpoch),
                  ManifestFileWithEpoch(local_dbname, newEpoch), 0, true);
    if (!st.ok()) {
      return st;
    }
  }
  st = writeCloudManifest(cenv->GetBaseEnv(), cenv->GetCloudManifest(),
                          CloudManifestFile(local_dbname));
  if (!st.ok()) {
    return st;
  }
  // TODO(igor): Compact cloud manifest by looking at live files in the database
  // and removing epochs that don't contain any live files.

  if (!cenv->GetDestBucketName().empty()) {
    // upload new manifest, only if we have it (i.e. this is not a new
    // database, indicated by maxFileNumber)
    if (maxFileNumber > 0) {
      st = cenv->PutObject(
          ManifestFileWithEpoch(local_dbname, newEpoch),
          cenv->GetDestBucketName(),
          ManifestFileWithEpoch(cenv->GetDestObjectPath(), newEpoch));
      if (!st.ok()) {
        return st;
      }
    }
    // upload new cloud manifest
    st = cenv->PutObject(CloudManifestFile(local_dbname),
                         cenv->GetDestBucketName(),
                         CloudManifestFile(cenv->GetDestObjectPath()));
    if (!st.ok()) {
      return st;
    }
  }
  return Status::OK();
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
