// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include "rocksdb/db.h"
#include "cloud/aws/aws_env.h"
#include "cloud/db_cloud_impl.h"
#include "cloud/filename.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/persistent_cache.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "util/auto_roll_logger.h"

namespace rocksdb {

DBCloudImpl::DBCloudImpl(DB* db) : DBCloud(db), cenv_(nullptr) {}

DBCloudImpl::~DBCloudImpl() {
  // Issue a blocking flush so that the latest manifest
  // is made durable in the cloud.
  Flush(FlushOptions());
}

Status DBCloud::Open(const Options& options, const std::string& dbname,
                     DB** dbptr) {
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;
  DBCloud* dbcloud = nullptr;
  Status s = DBCloud::Open(options, dbname, column_families, "", 0, &handles,
                           &dbcloud);
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
    st = CreateLoggerFromOptions(local_dbname, options, &options.info_log);
  }

  st = DBCloudImpl::SanitizeDirectory(options, local_dbname, read_only);
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
              "Created persistent cache %s", persistent_cache_path.c_str());
        } else {
          Log(InfoLogLevel::INFO_LEVEL, options.info_log,
              "Unable to create persistent cache %s. %s",
              persistent_cache_path.c_str(), st.ToString().c_str());
          return st;
        }
      }
    }
  }

  DB* db;
  if (read_only) {
    st = DB::OpenForReadOnly(options, local_dbname, column_families, handles,
                             &db);
  } else {
    st = DB::Open(options, local_dbname, column_families, handles, &db);
  }
  if (st.ok()) {
    *dbptr = new DBCloudImpl(db);
  }
  return st;
}

//
// Read the contents of the file (upto 64 K) into a memory buffer
//
Status DBCloudImpl::ReadFileIntoString(Env* env, const std::string& filename,
                                       std::string* id) {
  const EnvOptions soptions;
  unique_ptr<SequentialFile> file;
  Status s;
  {
    s = env->NewSequentialFile(filename, &file, soptions);
    if (!s.ok()) {
      return s;
    }
  }
  char buffer[64 * 1024];

  uint64_t file_size;
  s = env->GetFileSize(filename, &file_size);
  if (!s.ok()) {
    return s;
  }
  if (file_size > sizeof(buffer)) {
    return Status::IOError(
        "DBCloudImpl::ReadFileIntoString"
        " Insufficient buffer size");
  }
  Slice slice;
  s = file->Read(static_cast<size_t>(file_size), &slice, buffer);
  if (!s.ok()) {
    return s;
  }
  id->assign(slice.ToString());
  return s;
}

//
// Shall we re-initialize the local dir?
//
Status DBCloudImpl::NeedsReinitialization(CloudEnv* cenv,
                                          const Options& options,
                                          const std::string& local_dir,
                                          bool* do_reinit) {
  Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
      "[db_cloud_impl] NeedsReinitialization: "
      "checking local dir %s src bucket %s src path %s "
      "dest bucket %s dest path %s",
      local_dir.c_str(), cenv->GetSrcBucketPrefix().c_str(),
      cenv->GetSrcObjectPrefix().c_str(), cenv->GetDestBucketPrefix().c_str(),
      cenv->GetDestObjectPrefix().c_str());

  // If no buckets are specified, then we cannot reinit anyways
  if (cenv->GetSrcBucketPrefix().empty() &&
      cenv->GetSrcBucketPrefix().empty()) {
    *do_reinit = false;
    return Status::OK();
  }

  // assume that directory does needs reinitialization
  *do_reinit = true;

  // get local env
  Env* env = cenv->GetBaseEnv();

  // Does the local directory exist
  unique_ptr<Directory> dir;
  Status st = env->NewDirectory(local_dir, &dir);

  // If directory does not exist, then re-initialize
  if (!st.ok()) {
    return Status::OK();
  }

  // Check that the DB ID file exists in localdir
  std::string idfilename = local_dir + "/IDENTITY";
  st = env->FileExists(idfilename);
  if (!st.ok()) {
    Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
        "[db_cloud_impl] NeedsReinitialization: "
        "local dir %s does not exist",
        local_dir.c_str());
    return Status::OK();
  }
  // Read DBID file from local dir
  std::string local_dbid;
  st = ReadFileIntoString(env, idfilename, &local_dbid);
  if (!st.ok()) {
    Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
        "[db_cloud_impl] NeedsReinitialization: "
        "local dir %s unable to read local dbid",
        local_dir.c_str());
    return Status::OK();
  }
  std::string src_bucket = cenv->GetSrcBucketPrefix();
  std::string dest_bucket = cenv->GetDestBucketPrefix();

  // We found a dbid in the local dir. Verify that it matches
  // what we found on the cloud.
  std::string src_dbid;
  std::string src_object_path;

  // If a src bucket is specified, then get src dbid
  if (!src_bucket.empty()) {
    st = cenv->GetPathForDbid(src_bucket, local_dbid, &src_object_path);
    if (!st.ok() && !st.IsNotFound()) {
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] NeedsReinitialization: "
          "Local dbid is %s but unable to find src dbid",
          local_dbid.c_str());
      return Status::OK();
    }
    Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
        "[db_cloud_impl] NeedsReinitialization: "
        "Local dbid is %s and src object path in registry is '%s'",
        local_dbid.c_str(), src_object_path.c_str());

    if (st.ok()) {
      src_object_path = rtrim_if(trim(src_object_path), '/');
      std::string src_specified_path = cenv->GetSrcObjectPrefix();
      src_specified_path = rtrim_if(trim(src_specified_path), '/');

      // If the registered source path does not match the one specified in
      // our env, then fail the OpenDB  request.
      if (src_object_path != src_specified_path) {
        Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
            "[db_cloud_impl] NeedsReinitialization: "
            "Local dbid %s src path specified in env is %s "
            " but src path in registry is %s",
            local_dbid.c_str(), cenv->GetSrcObjectPrefix().c_str(),
            src_object_path.c_str());
        return Status::InvalidArgument(
            "[db_cloud_impl] NeedsReinitialization: bad src path");
      }
    }
    Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
        "[db_cloud_impl] NeedsReinitialization: "
        "Local dbid %d configured path %s matches the src dbid registry",
        local_dbid.c_str(), src_object_path.c_str());
  }
  std::string dest_dbid;
  std::string dest_object_path;

  // If a dest bucket is specified, then get dest dbid
  if (!dest_bucket.empty()) {
    st = cenv->GetPathForDbid(dest_bucket, local_dbid, &dest_object_path);
    if (!st.ok() && !st.IsNotFound()) {
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] NeedsReinitialization: "
          "Local dbid is %s but unable to find dest dbid",
          local_dbid.c_str());
      return Status::OK();
    }
    Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
        "[db_cloud_impl] NeedsReinitialization: "
        "Local dbid is %s and dest object path in registry is '%s'",
        local_dbid.c_str(), dest_object_path.c_str());

    if (st.ok()) {
      dest_object_path = rtrim_if(trim(dest_object_path), '/');
      std::string dest_specified_path = cenv->GetDestObjectPrefix();
      dest_specified_path = rtrim_if(trim(dest_specified_path), '/');

      // If the registered dest path does not match the one specified in
      // our env, then fail the OpenDB request.
      if (dest_object_path != dest_specified_path) {
        Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
            "[db_cloud_impl] NeedsReinitialization: "
            "Local dbid %s dest path specified in env is %s "
            " but dest path in registry is %s",
            local_dbid.c_str(), cenv->GetDestObjectPrefix().c_str(),
            dest_object_path.c_str());
        return Status::InvalidArgument(
            "[db_cloud_impl] NeedsReinitialization: bad dest path");
      }
    }
    Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
        "[db_cloud_impl] NeedsReinitialization: "
        "Local dbid %d configured path %s matches the dest dbid registry",
        local_dbid.c_str(), dest_object_path.c_str());
  }
  // If we found a src_dbid, then it should be a prefix of local_dbid
  if (!src_dbid.empty()) {
    size_t pos = local_dbid.find(src_dbid);
    if (pos == std::string::npos) {
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] NeedsReinitialization: "
          "dbid %s in src bucket %s is not a prefix of local dbid %s",
          src_dbid.c_str(), src_bucket.c_str(), local_dbid.c_str());
      return Status::OK();
    }
    Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
        "[db_cloud_impl] NeedsReinitialization: "
        "dbid %s in src bucket %s is a prefix of local dbid %s",
        src_dbid.c_str(), src_bucket.c_str(), local_dbid.c_str());

    // If the local dbid is an exact match with the src dbid, then ensure
    // that we cannot run in a 'clone' mode.
    if (local_dbid == src_dbid) {
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] NeedsReinitialization: "
          "dbid %s in src bucket %s is same as local dbid",
          src_dbid.c_str(), src_bucket.c_str());

      if (!dest_bucket.empty() && src_bucket != dest_bucket) {
        Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
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
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] NeedsReinitialization: "
          "dbid %s in dest bucket %s is not a prefix of local dbid %s",
          dest_dbid.c_str(), dest_bucket.c_str(), local_dbid.c_str());
      return Status::OK();
    }
    Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
        "[db_cloud_impl] NeedsReinitialization: "
        "dbid %s in dest bucket %s is a prefix of local dbid %s",
        dest_dbid.c_str(), dest_bucket.c_str(), local_dbid.c_str());

    // If the local dbid is an exact match with the destination dbid, then
    // ensure
    // that we are run not in a 'clone' mode.
    if (local_dbid == dest_dbid) {
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] NeedsReinitialization: "
          "dbid %s in dest bucket %s is same as local dbid",
          dest_dbid.c_str(), dest_bucket.c_str());

      if (!src_bucket.empty() && src_bucket != dest_bucket) {
        Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
            "[db_cloud_impl] NeedsReinitialization: "
            "local dbid %s in same as dest dbid but clone mode specified",
            local_dbid.c_str());
        return Status::OK();
      }
    }
  }
  // We found a local dbid but we did not find this dbid mapping in the bucket.
  if (src_object_path.empty() && dest_object_path.empty()) {
    Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
        "[db_cloud_impl] NeedsReinitialization: "
        "local dbid %s does not have a mapping in src bucket "
        "%s or dest bucket %s",
        local_dbid.c_str(), src_bucket.c_str(), dest_bucket.c_str());
    return Status::OK();
  }
  // ID's in the local dir are valid.

  // Check to see that we have a non-zero CURRENT file
  std::string manifest_name;
  st = ReadFileIntoString(env, local_dir + "/CURRENT", &manifest_name);
  if (!st.ok()) {
    Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
        "[db_cloud_impl] NeedsReinitialization: "
        "Unable to read CURRENT file in local dir %s. %s",
        local_dir.c_str(), st.ToString().c_str());
    return Status::OK();
  }
  manifest_name = rtrim_if(trim(manifest_name), '/');

  // Check to see that we have a non-zero MANIFEST xxx
  uint64_t size = 0;
  std::string mname = local_dir + "/" + manifest_name;
  st = env->GetFileSize(mname, &size);
  if (!st.ok() || size == 0) {
    Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
        "[db_cloud_impl] NeedsReinitialization: "
        "Unable to read MANIFEST file '%s' in local dir %s. %s",
        mname.c_str(), local_dir.c_str(), st.ToString().c_str());
    return Status::OK();
  }
  Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
      "[db_cloud_impl] NeedsReinitialization: "
      "Valid manifest file %s in local dir %s",
      manifest_name.c_str(), local_dir.c_str());

  // The DBID of the local dir is compatible with the src and dest buckets.
  // We do not need any re-initialization of local dir.
  *do_reinit = false;
  return Status::OK();
}

//
// Create appropriate files in the clone dir
//
Status DBCloudImpl::SanitizeDirectory(const Options& options,
                                      const std::string& local_name,
                                      bool readonly) {
  EnvOptions soptions;

  CloudEnvImpl* cenv = static_cast<CloudEnvImpl*>(options.env);
  if (cenv->GetCloudType() == CloudType::kNone) {
    // We don't need to SanitizeDirectory()
    return Status::OK();
  }
  if (cenv->GetCloudType() != CloudType::kAws) {
    return Status::NotSupported("We only support AWS for now.");
  }
  // acquire the local env
  Env* env = cenv->GetBaseEnv();

  // Shall we reinitialize the clone dir?
  bool do_reinit = true;
  Status st =
      DBCloudImpl::NeedsReinitialization(cenv, options, local_name, &do_reinit);
  if (!st.ok()) {
    Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
        "[db_cloud_impl] SanitizeDirectory error inspecting dir %s %s",
        local_name.c_str(), st.ToString().c_str());
    return st;
  }

  if (!do_reinit) {
    Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
        "[db_cloud_impl] SanitizeDirectory local directory %s is good",
        local_name.c_str());
    return Status::OK();
  }
  Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
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
    Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
        "[db_cloud_impl] SanitizeDirectory cleaned-up: '%s'", pathname.c_str());
  }

  // If directory does not exist, create it
  if (st.IsNotFound()) {
    if (readonly) {
      return st;
    }
    st = env->CreateDirIfMissing(local_name);
  }
  if (!st.ok()) {
    Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
        "[db_cloud_impl] SanitizeDirectory error opening dir %s %s",
        local_name.c_str(), st.ToString().c_str());
    return st;
  }

  // Download files from dest bucket
  if (!cenv->GetDestBucketPrefix().empty()) {
    // download MANIFEST
    std::string cloudfile = cenv->GetDestObjectPrefix() + "/MANIFEST";
    std::string localfile = local_name + "/MANIFEST.dest";
    st = DBCloudImpl::CopyFile(cenv, env, cenv->GetDestBucketPrefix(),
                               cloudfile, localfile);
    if (!st.ok()) {
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] Unable to download MANIFEST file from "
          "dest bucket %s. %s",
          cenv->GetDestBucketPrefix().c_str(), st.ToString().c_str());
    } else {
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] Downloaded MANIFEST file from "
          "dest bucket %s. %s",
          cenv->GetDestBucketPrefix().c_str(), st.ToString().c_str());
    }

    // download IDENTITY
    cloudfile = cenv->GetDestObjectPrefix() + "/IDENTITY";
    localfile = local_name + "/IDENTITY.dest";
    st = DBCloudImpl::CopyFile(cenv, env, cenv->GetDestBucketPrefix(),
                               cloudfile, localfile);
    if (!st.ok()) {
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] Unable to download IDENTITY file from "
          "dest bucket %s. %s",
          cenv->GetDestBucketPrefix().c_str(), st.ToString().c_str());
    } else {
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] Downloaded IDENTITY file from "
          "dest bucket %s. %s",
          cenv->GetDestBucketPrefix().c_str(), st.ToString().c_str());
    }
  }

  // Download files from src bucket
  if (!cenv->GetSrcBucketPrefix().empty()) {
    // download MANIFEST
    std::string cloudfile = cenv->GetSrcObjectPrefix() + "/MANIFEST";
    std::string localfile = local_name + "/MANIFEST.src";
    st = DBCloudImpl::CopyFile(cenv, env, cenv->GetSrcBucketPrefix(), cloudfile,
                               localfile);
    if (!st.ok()) {
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] Unable to download MANIFEST file from "
          "bucket %s. %s",
          cenv->GetSrcBucketPrefix().c_str(), st.ToString().c_str());
    } else {
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] Download MANIFEST file from "
          "src bucket %s. %s",
          cenv->GetSrcBucketPrefix().c_str(), st.ToString().c_str());
    }

    // download IDENTITY
    cloudfile = cenv->GetSrcObjectPrefix() + "/IDENTITY";
    localfile = local_name + "/IDENTITY.src";
    st = DBCloudImpl::CopyFile(cenv, env, cenv->GetSrcBucketPrefix(), cloudfile,
                               localfile);
    if (!st.ok()) {
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] Unable to download IDENTITY file from "
          "bucket %s. %s",
          cenv->GetSrcBucketPrefix().c_str(), st.ToString().c_str());
    } else {
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] Download IDENTITY file from "
          "src bucket %s. %s",
          cenv->GetSrcBucketPrefix().c_str(), st.ToString().c_str());
    }
  }
  // If an ID file exists in the dest, use it.

  if (env->FileExists(local_name + "/IDENTITY.dest").ok() &&
      env->FileExists(local_name + "/MANIFEST.dest").ok()) {
    Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
        "[db_cloud_impl] Downloaded IDENTITY and MANIFEST "
        "from dest bucket are potential candidates");

    st = env->RenameFile(local_name + "/IDENTITY.dest",
                         local_name + "/IDENTITY");
    if (!st.ok()) {
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] Unable to rename IDENTITY.dest %s",
          st.ToString().c_str());
      return st;
    }
    st = env->RenameFile(local_name + "/MANIFEST.dest",
                         local_name + "/MANIFEST-000001");
    if (!st.ok()) {
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] Unable to rename MANIFEST.dest %s",
          st.ToString().c_str());
      return st;
    }
    st = env->DeleteFile(local_name + "/IDENTITY.src");
    if (!st.ok()) {
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] Unable to delete IDENTITY.src %s",
          st.ToString().c_str());
    }
    st = env->DeleteFile(local_name + "/MANIFEST.src");
    if (!st.ok()) {
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] Unable to delete MANIFEST.src %s",
          st.ToString().c_str());
    }
  } else if (env->FileExists(local_name + "/IDENTITY.src").ok() &&
             env->FileExists(local_name + "/MANIFEST.src").ok()) {
    Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
        "[db_cloud_impl] Downloaded IDENTITY and MANIFEST "
        "from src bucket are potential candidates");

    // There isn't a ID file in the dest bucket but there exists
    // a ID file exists in the src bucket. Read src dbid.

    std::string src_dbid;
    st = ReadFileToString(env, local_name + "/IDENTITY.src", &src_dbid);
    if (!st.ok()) {
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] Unable to read IDENTITY.src %s",
          st.ToString().c_str());
      return st;
    }
    src_dbid = rtrim_if(trim(src_dbid), '\n');

    // If the dest bucketpath is the same as the src or no destination
    // bucket is specified, then it is not a clone. So continue to use
    // the src_dbid
    std::string new_dbid;
    if ((cenv->GetSrcBucketPrefix() == cenv->GetDestBucketPrefix() &&
         (cenv->GetSrcObjectPrefix() == cenv->GetDestObjectPrefix())) ||
        cenv->GetDestBucketPrefix().empty()) {
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] Reopening an existing cloud-db with dbid %s",
          src_dbid.c_str());

      new_dbid = src_dbid;
      st = env->RenameFile(local_name + "/IDENTITY.src",
                           local_name + "/IDENTITY");
      if (!st.ok()) {
        Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
            "[db_cloud_impl] Unable to rename IDENTITY.src %s",
            st.ToString().c_str());
        return st;
      }

    } else {
      // concoct a new dbid for this clone.
      new_dbid = src_dbid + "rockset" + env->GenerateUniqueId();

      // write to a newly created ID file
      {
        unique_ptr<WritableFile> destfile;
        st = env->NewWritableFile(local_name + "/IDENTITY.tmp", &destfile,
                                  soptions);
        if (!st.ok()) {
          Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
              "[db_cloud_impl] Unable to create local IDENTITY file to %s %s",
              local_name.c_str(), st.ToString().c_str());
          return st;
        }
        st = destfile->Append(Slice(new_dbid));
        if (!st.ok()) {
          Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
              "[db_cloud_impl] Unable to write new dbid to local IDENTITY file "
              "%s %s",
              local_name.c_str(), st.ToString().c_str());
          return st;
        }
      }
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] Written new clone dbid %s to %s%s %s",
          new_dbid.c_str(), local_name.c_str(), "/IDENTITY.tmp",
          st.ToString().c_str());

      // Rename ID file on local filesystem and upload it to dest bucket too
      st = cenv->RenameFile(local_name + "/IDENTITY.tmp",
                            local_name + "/IDENTITY");
      if (!st.ok()) {
        Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
            "[db_cloud_impl] Unable to rename newly created IDENTITY.tmp "
            " to IDENTITY. %S",
            st.ToString().c_str());
        return st;
      }

      // delete unused ID file
      st = env->DeleteFile(local_name + "/IDENTITY.src");
      if (!st.ok()) {
        Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
            "[db_cloud_impl] Unable to delete unneeded IDENTITY.src %s",
            st.ToString().c_str());
      }
    }
    // Rename src manifest file
    st = env->RenameFile(local_name + "/MANIFEST.src",
                         local_name + "/MANIFEST-000001");
    if (!st.ok()) {
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] Unable to rename IDENTITY.src %s",
          st.ToString().c_str());
      return st;
    }
  } else {
    // There isn't a valid db in either the src or dest bucket.
    // Return with a success code so that a new DB can be created.
    Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
        "[db_cloud_impl] No valid dbs in src bucket %s src path %s "
        "or dest bucket %s dest path %s",
        cenv->GetSrcBucketPrefix().c_str(), cenv->GetSrcObjectPrefix().c_str(),
        cenv->GetDestBucketPrefix().c_str(),
        cenv->GetDestObjectPrefix().c_str());
    return Status::OK();
  }

  // create CURRENT file to point to the manifest
  {
    unique_ptr<WritableFile> destfile;
    st =
        env->NewWritableFile(local_name + "/" + "CURRENT", &destfile, soptions);
    if (!st.ok()) {
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] Unable to create local CURRENT file to %s %s",
          local_name.c_str(), st.ToString().c_str());
      return st;
    }
    std::string manifestfile =
        "MANIFEST-000001\n";  // CURRENT file needs a newline
    st = destfile->Append(Slice(manifestfile));
    if (!st.ok()) {
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] Unable to write local CURRENT file to %s %s",
          local_name.c_str(), st.ToString().c_str());
      return st;
    }
  }
  return Status::OK();
}

//
// Copy file from cloud to local
//
Status DBCloudImpl::CopyFile(CloudEnv* src_env, Env* dest_env,
                             const std::string& bucket_prefix,
                             const std::string& srcname,
                             const std::string& destname, bool do_sync) {
  const EnvOptions soptions;
  unique_ptr<SequentialFile> srcfile;
  Status s = src_env->NewSequentialFileCloud(bucket_prefix, srcname, &srcfile,
                                             soptions);
  if (!s.ok()) {
    return s;
  }

  unique_ptr<WritableFile> destfile;
  s = dest_env->NewWritableFile(destname, &destfile, soptions);

  // copy 64K at a time
  char buffer[64 * 1024];
  while (s.ok()) {
    Slice slice;
    s = srcfile->Read(sizeof(buffer), &slice, buffer);
    if (s.ok()) {
      if (slice.size() == 0) {
        break;  // we are done.
      }
      s = destfile->Append(slice);
    }
  }
  if (s.ok() && do_sync) {
    s = destfile->Sync();
  }
  return s;
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
