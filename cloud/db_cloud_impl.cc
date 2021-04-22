// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include "cloud/db_cloud_impl.h"

#include <cinttypes>

#include "cloud/filename.h"
#include "cloud/manifest_reader.h"
#include "env/composite_env_wrapper.h"
#include "file/file_util.h"
#include "file/sst_file_manager_impl.h"
#include "logging/auto_roll_logger.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/persistent_cache.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "util/xxhash.h"
#include "utilities/persistent_cache/block_cache_tier.h"

namespace ROCKSDB_NAMESPACE {

namespace {
/**
 * This ConstantSstFileManager uses the same size for every sst files added.
 */
class ConstantSizeSstFileManager : public SstFileManagerImpl {
 public:
  ConstantSizeSstFileManager(int64_t constant_file_size, Env* env,
                             std::shared_ptr<Logger> logger,
                             int64_t rate_bytes_per_sec,
                             double max_trash_db_ratio,
                             uint64_t bytes_max_delete_chunk)
      : SstFileManagerImpl(env, std::make_shared<LegacyFileSystemWrapper>(env),
                           std::move(logger), rate_bytes_per_sec,
                           max_trash_db_ratio, bytes_max_delete_chunk),
        constant_file_size_(constant_file_size) {
    assert(constant_file_size_ >= 0);
  }

  Status OnAddFile(const std::string& file_path, bool compaction) override {
    return SstFileManagerImpl::OnAddFile(
        file_path, uint64_t(constant_file_size_), compaction);
  }

 private:
  const int64_t constant_file_size_;
};
}  // namespace

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
  // Use a constant sized SST File Manager if necesary.
  // NOTE: if user already passes in an SST File Manager, we will respect user's
  // SST File Manager instead.
  auto constant_sst_file_size =
      cenv->GetCloudEnvOptions().constant_sst_file_size_in_sst_file_manager;
  if (constant_sst_file_size >= 0 && options.sst_file_manager == nullptr) {
    // rate_bytes_per_sec, max_trash_db_ratio, bytes_max_delete_chunk are
    // default values in NewSstFileManager.
    // If users don't use Options.sst_file_manager, then these values are used
    // currently when creating an SST File Manager.
    options.sst_file_manager = std::make_shared<ConstantSizeSstFileManager>(
        constant_sst_file_size, options.env, options.info_log,
        0 /* rate_bytes_per_sec */, 0.25 /* max_trash_db_ratio */,
        64 * 1024 * 1024 /* bytes_max_delete_chunk */);
  }

  Env* local_env = cenv->GetBaseEnv();
  if (!read_only) {
    local_env->CreateDirIfMissing(
        local_dbname);  // MJR: TODO: Move into sanitize
  }

  // If cloud manifest is already loaded, this means the directory has been
  // sanitized (possibly by the call to ListColumnFamilies())
  if (cenv->GetCloudManifest() == nullptr) {
    st = cenv->SanitizeDirectory(options, local_dbname, read_only);

    if (st.ok()) {
      st = cenv->LoadCloudManifest(local_dbname, read_only);
    }
    if (!st.ok()) {
      return st;
    }
  }
  // If a persistent cache path is specified, then we set it in the options.
  if (!persistent_cache_path.empty() && persistent_cache_size_gb) {
    // Get existing options. If the persistent cache is already set, then do
    // not make any change. Otherwise, configure it.
    auto* tableopt =
        options.table_factory->GetOptions<BlockBasedTableOptions>();
    if (tableopt != nullptr && !tableopt->persistent_cache) {
      PersistentCacheConfig config(
          local_env, persistent_cache_path,
          persistent_cache_size_gb * 1024L * 1024L * 1024L, options.info_log);
      auto pcache = std::make_shared<BlockCacheTier>(config);
      st = pcache->Open();
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

  auto provider = cenv->GetStorageProvider();
  // If an sst file does not exist in the destination path, then remember it
  std::vector<std::string> to_copy;
  for (auto onefile : live_files) {
    auto remapped_fname = cenv->RemapFilename(onefile.name);
    std::string destpath = cenv->GetDestObjectPath() + "/" + remapped_fname;
    if (!provider->ExistsCloudObject(cenv->GetDestBucketName(), destpath)
             .ok()) {
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
      Status s = provider->CopyCloudObject(
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
  LegacyFileSystemWrapper fs(base_env);
  st =
      CopyFile(&fs, GetName() + "/" + manifest_fname,
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
    auto provider = cenv->GetStorageProvider();
    while (true) {
      size_t idx = next_file_to_copy.fetch_add(1);
      if (idx >= files_to_copy.size()) {
        break;
      }

      auto& f = files_to_copy[idx];
      auto copy_st = provider->PutCloudObject(
          GetName() + "/" + f.first, destination.GetBucketName(),
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

Status DBCloudImpl::ExecuteRemoteCompactionRequest(
    const PluggableCompactionParam& inputParams,
    PluggableCompactionResult* result, bool doSanitize) {
  auto cenv = static_cast<CloudEnvImpl*>(GetEnv());

  // run the compaction request on the underlying local database
  Status status = GetBaseDB()->ExecuteRemoteCompactionRequest(
      inputParams, result, doSanitize);
  if (!status.ok()) {
    return status;
  }

  // convert the local pathnames to the cloud pathnames
  for (unsigned int i = 0; i < result->output_files.size(); i++) {
    OutputFile* outfile = &result->output_files[i];
    outfile->pathname = cenv->RemapFilename(outfile->pathname);
  }
  return Status::OK();
}

Status DBCloud::ListColumnFamilies(const DBOptions& db_options,
                                   const std::string& name,
                                   std::vector<std::string>* column_families) {
  CloudEnvImpl* cenv = static_cast<CloudEnvImpl*>(db_options.env);

  Env* local_env = cenv->GetBaseEnv();
  local_env->CreateDirIfMissing(name);

  Status st;
  st = cenv->SanitizeDirectory(db_options, name, false);
  if (st.ok()) {
    st = cenv->LoadCloudManifest(name, false);
  }
  if (st.ok()) {
    st = DB::ListColumnFamilies(db_options, name, column_families);
  }

  return st;
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
