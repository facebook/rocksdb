// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include <cinttypes>

#include "cloud/cloud_env_impl.h"
#include "cloud/cloud_env_wrapper.h"
#include "cloud/cloud_log_controller.h"
#include "cloud/filename.h"
#include "cloud/manifest_reader.h"
#include "file/filename.h"
#include "file/file_util.h"
#include "port/likely.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "util/file_reader_writer.h"
#include "util/xxhash.h"

namespace rocksdb {

  CloudEnvImpl::CloudEnvImpl(const CloudEnvOptions& opts, Env* base, const std::shared_ptr<Logger>& l)
    : CloudEnv(opts, base, l), purger_is_running_(true) {}

CloudEnvImpl::~CloudEnvImpl() {
  if (cloud_log_controller_) {
    cloud_log_controller_->StopTailingStream();
  }
  StopPurger();
}

void CloudEnvImpl::StopPurger() {
  {
    std::lock_guard<std::mutex> lk(purger_lock_);
    purger_is_running_ = false;
    purger_cv_.notify_one();
  }

  // wait for the purger to stop
  if (purge_thread_.joinable()) {
    purge_thread_.join();
  }
}

Status CloudEnvImpl::LoadLocalCloudManifest(const std::string& dbname) {
  if (cloud_manifest_) {
    cloud_manifest_.reset();
  }
  std::unique_ptr<SequentialFile> file;
  auto cloudManifestFile = CloudManifestFile(dbname);
  auto s =
      GetBaseEnv()->NewSequentialFile(cloudManifestFile, &file, EnvOptions());
  if (!s.ok()) {
    return s;
  }
  return CloudManifest::LoadFromLog(
    std::unique_ptr<SequentialFileReader>(
          new SequentialFileReader(std::move(file), cloudManifestFile)),
      &cloud_manifest_);
}

std::string CloudEnvImpl::RemapFilename(const std::string& logical_path) const {
  if (UNLIKELY(GetCloudType() == CloudType::kCloudNone) ||
      UNLIKELY(test_disable_cloud_manifest_)) {
    return logical_path;
  }
  auto file_name = basename(logical_path);
  uint64_t fileNumber;
  FileType type;
  WalFileType walType;
  if (file_name == "MANIFEST") {
    type = kDescriptorFile;
  } else {
    bool ok = ParseFileName(file_name, &fileNumber, &type, &walType);
    if (!ok) {
      return logical_path;
    }
  }
  Slice epoch;
  switch (type) {
    case kTableFile:
      // We should not be accessing sst files before CLOUDMANIFEST is loaded
      assert(cloud_manifest_);
      epoch = cloud_manifest_->GetEpoch(fileNumber);
      break;
    case kDescriptorFile:
      // We should not be accessing MANIFEST files before CLOUDMANIFEST is
      // loaded
      assert(cloud_manifest_);
      // Even though logical file might say MANIFEST-000001, we cut the number
      // suffix and store MANIFEST-[epoch] in the cloud and locally.
      file_name = "MANIFEST";
      epoch = cloud_manifest_->GetCurrentEpoch();
      break;
    default:
      return logical_path;
  };
  auto dir = dirname(logical_path);
  return dir + (dir.empty() ? "" : "/") + file_name +
         (epoch.empty() ? "" : ("-" + epoch.ToString()));
}

Status CloudEnvImpl::DeleteInvisibleFiles(const std::string& dbname) {
  Status s;
  if (HasDestBucket()) {
    BucketObjectMetadata metadata;
    s = ListObjects(GetDestBucketName(), GetDestObjectPath(), &metadata);
    if (!s.ok()) {
      return s;
    }

    for (auto& fname : metadata.pathnames) {
      auto noepoch = RemoveEpoch(fname);
      if (IsSstFile(noepoch) || IsManifestFile(noepoch)) {
        if (RemapFilename(noepoch) != fname) {
          // Ignore returned status on purpose.
          Log(InfoLogLevel::INFO_LEVEL, info_log_,
              "DeleteInvisibleFiles deleting %s from destination bucket",
              fname.c_str());
          DeleteCloudFileFromDest(fname);
        }
      }
    }
  }
  std::vector<std::string> children;
  s = GetBaseEnv()->GetChildren(dbname, &children);
  if (!s.ok()) {
    return s;
  }
  for (auto& fname : children) {
    auto noepoch = RemoveEpoch(fname);
    if (IsSstFile(noepoch) || IsManifestFile(noepoch)) {
      if (RemapFilename(RemoveEpoch(fname)) != fname) {
        // Ignore returned status on purpose.
        Log(InfoLogLevel::INFO_LEVEL, info_log_,
            "DeleteInvisibleFiles deleting file %s from local dir",
            fname.c_str());
        GetBaseEnv()->DeleteFile(dbname + "/" + fname);
      }
    }
  }
  return s;
}

void CloudEnvImpl::TEST_InitEmptyCloudManifest() {
  CloudManifest::CreateForEmptyDatabase("", &cloud_manifest_);
}

Status CloudEnvImpl::CreateNewIdentityFile(const std::string& dbid,
                                           const std::string& local_name) {
  const EnvOptions soptions;
  auto tmp_identity_path = local_name + "/IDENTITY.tmp";
  Env* env = GetBaseEnv();
  Status st;
  {
    std::unique_ptr<WritableFile> destfile;
    st = env->NewWritableFile(tmp_identity_path, &destfile, soptions);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[cloud_env_impl] Unable to create local IDENTITY file to %s %s",
          tmp_identity_path.c_str(), st.ToString().c_str());
      return st;
    }
    st = destfile->Append(Slice(dbid));
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[cloud_env_impl] Unable to write new dbid to local IDENTITY file "
          "%s %s",
          tmp_identity_path.c_str(), st.ToString().c_str());
      return st;
    }
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[cloud_env_impl] Written new dbid %s to %s %s", dbid.c_str(),
      tmp_identity_path.c_str(), st.ToString().c_str());

  // Rename ID file on local filesystem and upload it to dest bucket too
  st = RenameFile(tmp_identity_path, local_name + "/IDENTITY");
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[cloud_env_impl] Unable to rename newly created IDENTITY.tmp "
        " to IDENTITY. %s",
        st.ToString().c_str());
    return st;
  }
  return st;
}

Status CloudEnvImpl::writeCloudManifest(CloudManifest* manifest,
                                        const std::string& fname) {
  Env* local_env = GetBaseEnv();
  // Write to tmp file and atomically rename later. This helps if we crash
  // mid-write :)
  auto tmp_fname = fname + ".tmp";
  std::unique_ptr<WritableFile> file;
  Status s = local_env->NewWritableFile(tmp_fname, &file, EnvOptions());
  if (s.ok()) {
    s = manifest->WriteToLog(std::unique_ptr<WritableFileWriter>(
        new WritableFileWriter(std::move(file), tmp_fname, EnvOptions())));
  }
  if (s.ok()) {
    s = local_env->RenameFile(tmp_fname, fname);
  }
  return s;
}

// we map a longer string given by env->GenerateUniqueId() into 16-byte string
std::string CloudEnvImpl::generateNewEpochId() {
  auto uniqueId = GenerateUniqueId();
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

//
// Shall we re-initialize the local dir?
//
Status CloudEnvImpl::NeedsReinitialization(const std::string& local_dir,
                                           bool* do_reinit) {
  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "[cloud_env_impl] NeedsReinitialization: "
      "checking local dir %s src bucket %s src path %s "
      "dest bucket %s dest path %s",
      local_dir.c_str(), GetSrcBucketName().c_str(), GetSrcObjectPath().c_str(),
      GetDestBucketName().c_str(), GetDestObjectPath().c_str());

  // If no buckets are specified, then we cannot reinit anyways
  if (!HasSrcBucket() && !HasDestBucket()) {
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_env_impl] NeedsReinitialization: "
        "Both src and dest buckets are empty");
    *do_reinit = false;
    return Status::OK();
  }

  // assume that directory does needs reinitialization
  *do_reinit = true;

  // get local env
  Env* env = GetBaseEnv();

  // Check if local directory exists
  auto st = env->FileExists(local_dir);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[cloud_env_impl] NeedsReinitialization: "
        "failed to access local dir %s: %s",
        local_dir.c_str(), st.ToString().c_str());
    // If the directory is not found, we should create it. In case of an other
    // IO error, we need to fail
    return st.IsNotFound() ? Status::OK() : st;
  }

  // Check if CURRENT file exists
  st = env->FileExists(CurrentFileName(local_dir));
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[cloud_env_impl] NeedsReinitialization: "
        "failed to find CURRENT file %s: %s",
        CurrentFileName(local_dir).c_str(), st.ToString().c_str());
    return st.IsNotFound() ? Status::OK() : st;
  }

  if (cloud_env_options.skip_dbid_verification) {
    *do_reinit = false;
    return Status::OK();
  }

  // Read DBID file from local dir
  std::string local_dbid;
  st = ReadFileToString(env, IdentityFileName(local_dir), &local_dbid);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[cloud_env_impl] NeedsReinitialization: "
        "local dir %s unable to read local dbid: %s",
        local_dir.c_str(), st.ToString().c_str());
    return st.IsNotFound() ? Status::OK() : st;
  }
  local_dbid = rtrim_if(trim(local_dbid), '\n');

  // We found a dbid in the local dir. Verify that it matches
  // what we found on the cloud.
  std::string src_object_path;
  auto& src_bucket = GetSrcBucketName();
  auto& dest_bucket = GetDestBucketName();

  // If a src bucket is specified, then get src dbid
  if (HasSrcBucket()) {
    st = GetPathForDbid(src_bucket, local_dbid, &src_object_path);
    if (!st.ok() && !st.IsNotFound()) {
      // Unable to fetch data from S3. Fail Open request.
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[cloud_env_impl] NeedsReinitialization: "
          "Local dbid is %s but unable to find src dbid",
          local_dbid.c_str());
      return st;
    }
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_env_impl] NeedsReinitialization: "
        "Local dbid is %s and src object path in registry is '%s'",
        local_dbid.c_str(), src_object_path.c_str());

    if (st.ok()) {
      src_object_path = rtrim_if(trim(src_object_path), '/');
    }
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_env_impl] NeedsReinitialization: "
        "Local dbid %s configured src path %s src dbid registry",
        local_dbid.c_str(), src_object_path.c_str());
  }
  std::string dest_object_path;

  // If a dest bucket is specified, then get dest dbid
  if (HasDestBucket()) {
    st = GetPathForDbid(dest_bucket, local_dbid, &dest_object_path);
    if (!st.ok() && !st.IsNotFound()) {
      // Unable to fetch data from S3. Fail Open request.
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[cloud_env_impl] NeedsReinitialization: "
          "Local dbid is %s but unable to find dest dbid",
          local_dbid.c_str());
      return st;
    }
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_env_impl] NeedsReinitialization: "
        "Local dbid is %s and dest object path in registry is '%s'",
        local_dbid.c_str(), dest_object_path.c_str());

    if (st.ok()) {
      dest_object_path = rtrim_if(trim(dest_object_path), '/');
      std::string dest_specified_path = GetDestObjectPath();
      dest_specified_path = rtrim_if(trim(dest_specified_path), '/');

      // If the registered dest path does not match the one specified in
      // our env, then fail the OpenDB request.
      if (dest_object_path != dest_specified_path) {
        Log(InfoLogLevel::ERROR_LEVEL, info_log_,
            "[cloud_env_impl] NeedsReinitialization: "
            "Local dbid %s dest path specified in env is %s "
            " but dest path in registry is %s",
            local_dbid.c_str(), GetDestObjectPath().c_str(),
            dest_object_path.c_str());
        return Status::InvalidArgument(
            "[cloud_env_impl] NeedsReinitialization: bad dest path");
      }
    }
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_env_impl] NeedsReinitialization: "
        "Local dbid %s configured path %s matches the dest dbid registry",
        local_dbid.c_str(), dest_object_path.c_str());
  }

  // Ephemeral clones do not write their dbid to the cloud registry.
  // Then extract them from the env-configured paths
  std::string src_dbid;
  std::string dest_dbid;
  st = GetCloudDbid(local_dir, &src_dbid, &dest_dbid);
  if (!st.ok()) {
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_env_impl] NeedsReinitialization: "
        "Unable to extract dbid from cloud paths %s",
        st.ToString().c_str());
    return st;
  }

  // If we found a src_dbid, then it should be a prefix of local_dbid
  if (!src_dbid.empty()) {
    size_t pos = local_dbid.find(src_dbid);
    if (pos == std::string::npos) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[cloud_env_impl] NeedsReinitialization: "
          "dbid %s in src bucket %s is not a prefix of local dbid %s",
          src_dbid.c_str(), src_bucket.c_str(), local_dbid.c_str());
      return Status::OK();
    }
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_env_impl] NeedsReinitialization: "
        "dbid %s in src bucket %s is a prefix of local dbid %s",
        src_dbid.c_str(), src_bucket.c_str(), local_dbid.c_str());

    // If the local dbid is an exact match with the src dbid, then ensure
    // that we cannot run in a 'clone' mode.
    if (local_dbid == src_dbid) {
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_env_impl] NeedsReinitialization: "
          "dbid %s in src bucket %s is same as local dbid",
          src_dbid.c_str(), src_bucket.c_str());

      if (HasDestBucket() && !SrcMatchesDest()) {
        Log(InfoLogLevel::ERROR_LEVEL, info_log_,
            "[cloud_env_impl] NeedsReinitialization: "
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
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[cloud_env_impl] NeedsReinitialization: "
          "dbid %s in dest bucket %s is not a prefix of local dbid %s",
          dest_dbid.c_str(), dest_bucket.c_str(), local_dbid.c_str());
      return Status::OK();
    }
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_env_impl] NeedsReinitialization: "
        "dbid %s in dest bucket %s is a prefix of local dbid %s",
        dest_dbid.c_str(), dest_bucket.c_str(), local_dbid.c_str());

    // If the local dbid is an exact match with the destination dbid, then
    // ensure that we are run not in a 'clone' mode.
    if (local_dbid == dest_dbid) {
      Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
          "[cloud_env_impl] NeedsReinitialization: "
          "dbid %s in dest bucket %s is same as local dbid",
          dest_dbid.c_str(), dest_bucket.c_str());

      if (HasSrcBucket() && !SrcMatchesDest()) {
        Log(InfoLogLevel::ERROR_LEVEL, info_log_,
            "[cloud_env_impl] NeedsReinitialization: "
            "local dbid %s in same as dest dbid but clone mode specified",
            local_dbid.c_str());
        return Status::OK();
      }
    }
  }

  // We found a local dbid but we did not find this dbid in bucket registry.
  if (src_object_path.empty() && dest_object_path.empty()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[cloud_env_impl] NeedsReinitialization: "
        "local dbid %s does not have a mapping in cloud registry "
        "src bucket %s or dest bucket %s",
        local_dbid.c_str(), src_bucket.c_str(), dest_bucket.c_str());

    // This is an ephemeral clone. Resync all files from cloud.
    // If the  resycn failed, then return success to indicate that
    // the local directory needs to be completely removed and recreated.
    st = ResyncDir(local_dir);
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
Status CloudEnvImpl::ResyncDir(const std::string& local_dir) {
  if (HasDestBucket()) {
    auto& src_bucket = GetSrcBucketName();
    auto& dest_bucket = GetDestBucketName();
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[cloud_env_impl] ResyncDir: "
        "not an ephemeral clone local dir %s "
        "src bucket %s dest bucket %s",
        local_dir.c_str(), src_bucket.c_str(), dest_bucket.c_str());
    return Status::InvalidArgument();
  }
  // If ephemeral_resync_on_open is false, then we want to keep all local
  // data intact.
  if (!cloud_env_options.ephemeral_resync_on_open) {
    return Status::OK();
  }

  // Copy the src cloud manifest to local dir. This essentially means that
  // the local db is resycned with the src cloud bucket. All new files will be
  // pulled in as needed when we open the db.
  return FetchCloudManifest(local_dir, true);
}

//
// Extract the src dbid and the dest dbid from the cloud paths
//
Status CloudEnvImpl::GetCloudDbid(const std::string& local_dir,
                                  std::string* src_dbid,
                                  std::string* dest_dbid) {
  // get local env
  Env* env = GetBaseEnv();

  // use a tmp file in local dir
  std::string tmpfile = IdentityFileName(local_dir) + ".cloud.tmp";

  // Delete any old data remaining there
  env->DeleteFile(tmpfile);

  // Read dbid from src bucket if it exists
  if (HasSrcBucket()) {
    Status st = GetObject(GetSrcBucketName(), GetSrcObjectPath() + "/IDENTITY",
                          tmpfile);
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
        Log(InfoLogLevel::ERROR_LEVEL, info_log_,
            "[cloud_env_impl] GetCloudDbid: "
            "local dir %s unable to read src dbid: %s",
            local_dir.c_str(), st.ToString().c_str());
        return st;
      }
    }
  }

  // Read dbid from dest bucket if it exists
  if (HasDestBucket()) {
    Status st = GetObject(GetDestBucketName(),
                          GetDestObjectPath() + "/IDENTITY", tmpfile);
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
        Log(InfoLogLevel::ERROR_LEVEL, info_log_,
            "[cloud_env_impl] GetCloudDbid: "
            "local dir %s unable to read dest dbid: %s",
            local_dir.c_str(), st.ToString().c_str());
        return st;
      }
    }
  }
  return Status::OK();
}

Status CloudEnvImpl::MaybeMigrateManifestFile(const std::string& local_dbname) {
  std::string manifest_filename;
  Env* local_env = GetBaseEnv();
  auto st = local_env->FileExists(CurrentFileName(local_dbname));
  if (st.IsNotFound()) {
    // No need to migrate
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_env_impl] MaybeMigrateManifestFile: No need to migrate %s",
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
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_env_impl] MaybeMigrateManifestFile: Manifest %s does not exist",
        manifest_filename.c_str());
    return Status::OK();
  }
  return local_env->RenameFile(manifest_filename, local_dbname + "/MANIFEST");
}

Status CloudEnvImpl::PreloadCloudManifest(const std::string& local_dbname) {
  Status st;
  Env* local_env = GetBaseEnv();
  local_env->CreateDirIfMissing(local_dbname);
  if (GetCloudType() != CloudType::kCloudNone) {
    st = MaybeMigrateManifestFile(local_dbname);
    if (st.ok()) {
      // Init cloud manifest
      st = FetchCloudManifest(local_dbname, false);
    }
    if (st.ok()) {
      // Inits CloudEnvImpl::cloud_manifest_, which will enable us to read files
      // from the cloud
      st = LoadLocalCloudManifest(local_dbname);
    }
  }
  return st;
}

Status CloudEnvImpl::LoadCloudManifest(const std::string& local_dbname,
                                       bool read_only) {
  Status st;
  if (GetCloudType() != CloudType::kCloudNone) {
    st = MaybeMigrateManifestFile(local_dbname);
    if (st.ok()) {
      // Init cloud manifest
      st = FetchCloudManifest(local_dbname, false);
    }
    if (st.ok()) {
      // Inits CloudEnvImpl::cloud_manifest_, which will enable us to read files
      // from the cloud
      st = LoadLocalCloudManifest(local_dbname);
    }
    if (st.ok()) {
      // Rolls the new epoch in CLOUDMANIFEST
      st = RollNewEpoch(local_dbname);
    }
    if (!st.ok()) {
      return st;
    }

    // Do the cleanup, but don't fail if the cleanup fails.
    if (!read_only) {
      st = DeleteInvisibleFiles(local_dbname);
      if (!st.ok()) {
        Log(InfoLogLevel::INFO_LEVEL, info_log_,
            "Failed to delete invisible files: %s", st.ToString().c_str());
        // Ignore the fail
        st = Status::OK();
      }
    }
  }
  return st;
}

//
// Create appropriate files in the clone dir
//
Status CloudEnvImpl::SanitizeDirectory(const DBOptions& options,
                                       const std::string& local_name,
                                       bool read_only) {
  EnvOptions soptions;

  // acquire the local env
  Env* env = GetBaseEnv();
  if (!read_only) {
    env->CreateDirIfMissing(local_name);
  }

  if (GetCloudType() == CloudType::kCloudNone) {
    // We don't need to SanitizeDirectory()
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_env_impl] SanitizeDirectory skipping dir %s for non-cloud env",
        local_name.c_str());
    return Status::OK();
  }
  if (GetCloudType() != CloudType::kCloudAws) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[cloud_env_impl] SanitizeDirectory dir %s found non aws env",
        local_name.c_str());
    return Status::NotSupported("We only support AWS for now.");
  }

  // Shall we reinitialize the clone dir?
  bool do_reinit = true;
  Status st = NeedsReinitialization(local_name, &do_reinit);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[cloud_env_impl] SanitizeDirectory error inspecting dir %s %s",
        local_name.c_str(), st.ToString().c_str());
    return st;
  }

  // If there is no destination bucket, then we need to suck in all sst files
  // from source bucket at db startup time. We do this by setting max_open_files
  // = -1
  if (!HasDestBucket()) {
    if (options.max_open_files != -1) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[cloud_env_impl] SanitizeDirectory error.  "
          " No destination bucket specified. Set options.max_open_files = -1 "
          " to copy in all sst files from src bucket %s into local dir %s",
          GetSrcObjectPath().c_str(), local_name.c_str());
      return Status::InvalidArgument(
          "No destination bucket. "
          "Set options.max_open_files = -1");
    }
    if (!cloud_env_options.keep_local_sst_files && !read_only) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[cloud_env_impl] SanitizeDirectory error.  "
          " No destination bucket specified. Set options.keep_local_sst_files "
          "= true to copy in all sst files from src bucket %s into local dir "
          "%s",
          GetSrcObjectPath().c_str(), local_name.c_str());
      return Status::InvalidArgument(
          "No destination bucket. "
          "Set options.keep_local_sst_files = true");
    }
  }

  if (!do_reinit) {
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_env_impl] SanitizeDirectory local directory %s is good",
        local_name.c_str());
    return Status::OK();
  }
  Log(InfoLogLevel::ERROR_LEVEL, info_log_,
      "[cloud_env_impl] SanitizeDirectory local directory %s cleanup needed",
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
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_env_impl] SanitizeDirectory cleaned-up: '%s'",
        pathname.c_str());
  }

  if (!st.ok()) {
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
        "[cloud_env_impl] SanitizeDirectory error opening dir %s %s",
        local_name.c_str(), st.ToString().c_str());
    return st;
  }

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[cloud_env_impl] SanitizeDirectory dest_equal_src = %d",
      SrcMatchesDest());

  bool got_identity_from_dest = false, got_identity_from_src = false;

  // Download IDENTITY, first try destination, then source
  if (HasDestBucket()) {
    // download IDENTITY from dest
    st = GetObject(GetDestBucketName(), IdentityFileName(GetDestObjectPath()),
                   IdentityFileName(local_name));
    if (!st.ok() && !st.IsNotFound()) {
      // If there was an error and it's not IsNotFound() we need to bail
      return st;
    }
    got_identity_from_dest = st.ok();
  }
  if (!got_identity_from_dest && HasSrcBucket() && !SrcMatchesDest()) {
    // download IDENTITY from src
    st = GetObject(GetSrcBucketName(), IdentityFileName(GetSrcObjectPath()),
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
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[cloud_env_impl] No valid dbs in src bucket %s src path %s "
        "or dest bucket %s dest path %s",
        GetSrcBucketName().c_str(), GetSrcObjectPath().c_str(),
        GetDestBucketName().c_str(), GetDestObjectPath().c_str());
    return Status::OK();
  }

  if (got_identity_from_src && !SrcMatchesDest()) {
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

    std::string new_dbid =
        src_dbid + std::string(DBID_SEPARATOR) + env->GenerateUniqueId();

    st = CreateNewIdentityFile(new_dbid, local_name);
    if (!st.ok()) {
      return st;
    }
  }

  // create dummy CURRENT file to point to the dummy manifest (cloud env will
  // remap the filename appropriately, this is just to fool the underyling
  // RocksDB)
  {
    std::unique_ptr<WritableFile> destfile;
    st = env->NewWritableFile(CurrentFileName(local_name), &destfile, soptions);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[cloud_env_impl] Unable to create local CURRENT file to %s %s",
          local_name.c_str(), st.ToString().c_str());
      return st;
    }
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_env_impl] SanitizeDirectory creating dummy CURRENT file");
    std::string manifestfile =
        "MANIFEST-000001\n";  // CURRENT file needs a newline
    st = destfile->Append(Slice(manifestfile));
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[cloud_env_impl] Unable to write local CURRENT file to %s %s",
          local_name.c_str(), st.ToString().c_str());
      return st;
    }
  }
  return st;
}

Status CloudEnvImpl::FetchCloudManifest(const std::string& local_dbname,
                                        bool force) {
  std::string cloudmanifest = CloudManifestFile(local_dbname);
  if (!SrcMatchesDest() && !force &&
      GetBaseEnv()->FileExists(cloudmanifest).ok()) {
    // nothing to do here, we have our cloud manifest
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_env_impl] FetchCloudManifest: Nothing to do %s exists",
        cloudmanifest.c_str());
    return Status::OK();
  }
  // first try to get cloudmanifest from dest
  if (HasDestBucket()) {
    Status st =
        GetObject(GetDestBucketName(), CloudManifestFile(GetDestObjectPath()),
                  cloudmanifest);
    if (!st.ok() && !st.IsNotFound()) {
      // something went wrong, bail out
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_env_impl] FetchCloudManifest: Failed to fetch "
          " cloud manifest %s from dest %s",
          cloudmanifest.c_str(), GetDestBucketName().c_str());
      return st;
    }
    if (st.ok()) {
      // found it!
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_env_impl] FetchCloudManifest: Fetched"
          " cloud manifest %s from dest %s",
          cloudmanifest.c_str(), GetDestBucketName().c_str());
      return st;
    }
  }
  // we couldn't get cloud manifest from dest, need to try from src?
  if (HasSrcBucket() && !SrcMatchesDest()) {
    Status st = GetObject(GetSrcBucketName(),
                          CloudManifestFile(GetSrcObjectPath()), cloudmanifest);
    if (!st.ok() && !st.IsNotFound()) {
      // something went wrong, bail out
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_env_impl] FetchCloudManifest: Failed to fetch "
          " cloud manifest %s from src %s",
          cloudmanifest.c_str(), GetSrcBucketName().c_str());
      return st;
    }
    if (st.ok()) {
      // found it!
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_env_impl] FetchCloudManifest: Fetched"
          " cloud manifest %s from src %s",
          cloudmanifest.c_str(), GetSrcBucketName().c_str());
      return st;
    }
  }
  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "[cloud_env_impl] FetchCloudManifest: Creating new"
      " cloud manifest for %s",
      local_dbname.c_str());

  // No cloud manifest, create an empty one
  std::unique_ptr<CloudManifest> manifest;
  CloudManifest::CreateForEmptyDatabase("", &manifest);
  return writeCloudManifest(manifest.get(), cloudmanifest);
}

Status CloudEnvImpl::RollNewEpoch(const std::string& local_dbname) {
  auto oldEpoch = GetCloudManifest()->GetCurrentEpoch().ToString();
  // Find next file number. We use dummy MANIFEST filename, which should get
  // remapped into the correct MANIFEST filename through CloudManifest.
  // After this call we should also have a local file named
  // MANIFEST-<current_epoch> (unless st.IsNotFound()).
  uint64_t maxFileNumber;
  auto st = ManifestReader::GetMaxFileNumberFromManifest(
      this, local_dbname + "/MANIFEST-000001", &maxFileNumber);
  if (st.IsNotFound()) {
    // This is a new database!
    maxFileNumber = 0;
    st = Status::OK();
  } else if (!st.ok()) {
    // uh oh
    return st;
  }
  // roll new epoch
  auto newEpoch = generateNewEpochId();
  GetCloudManifest()->AddEpoch(maxFileNumber, newEpoch);
  GetCloudManifest()->Finalize();
  if (maxFileNumber > 0) {
    // Meaning, this is not a new database and we should have
    // ManifestFileWithEpoch(local_dbname, oldEpoch) locally.
    // In that case, we have to move our old manifest to the new filename.
    // However, we don't move here, we copy. If we moved and crashed immediately
    // after (before writing CLOUDMANIFEST), we'd corrupt our database. The old
    // MANIFEST file will be cleaned up in DeleteInvisibleFiles().
    st = CopyFile(GetBaseEnv(), ManifestFileWithEpoch(local_dbname, oldEpoch),
                  ManifestFileWithEpoch(local_dbname, newEpoch), 0, true);
    if (!st.ok()) {
      return st;
    }
  }
  st = writeCloudManifest(GetCloudManifest(), CloudManifestFile(local_dbname));
  if (!st.ok()) {
    return st;
  }
  // TODO(igor): Compact cloud manifest by looking at live files in the database
  // and removing epochs that don't contain any live files.

  if (HasDestBucket()) {
    // upload new manifest, only if we have it (i.e. this is not a new
    // database, indicated by maxFileNumber)
    if (maxFileNumber > 0) {
      st = PutObject(ManifestFileWithEpoch(local_dbname, newEpoch),
                     GetDestBucketName(),
                     ManifestFileWithEpoch(GetDestObjectPath(), newEpoch));
      if (!st.ok()) {
        return st;
      }
    }
    // upload new cloud manifest
    st = PutObject(CloudManifestFile(local_dbname), GetDestBucketName(),
                   CloudManifestFile(GetDestObjectPath()));
    if (!st.ok()) {
      return st;
    }
  }
  return Status::OK();
}
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
