// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "cloud/aws/aws_env.h"
#include "cloud/db_cloud_impl.h"
#include "db/auto_roll_logger.h"

namespace rocksdb {


DBCloudImpl::DBCloudImpl(DB* db) :
	DBCloud(db),
	cenv_(nullptr) {
}

DBCloudImpl::~DBCloudImpl() {
}

Status DBCloud::Open(
    const Options& options,
    const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles,
    DBCloud** dbptr,
    bool read_only) {

  DB* db = nullptr;
  *dbptr = nullptr;
  Status st;

  // applications need to specify a cloud environment for CloudEnv to work
  if (!options.env) {
    return Status::InvalidArgument("Specify a default environment");
  }

  // Store pointer to env, just for sanity check
  CloudEnvImpl* cenv = static_cast<CloudEnvImpl *>(options.env);
  assert(cenv->GetCloudType() == CloudType::kAws);

  if (read_only) {
    st = DB::OpenForReadOnly(options, dbname, column_families,
                             handles, &db);
  } else {
    st = DB::Open(options, dbname, column_families, handles, &db);
  }
  if (st.ok()) {
    DBCloudImpl* d = new DBCloudImpl(db);
    d->cenv_ = cenv; 
    *dbptr = d;
  }
  return st;
}

Status DBCloud::OpenClone(
    const Options& options,
    const std::string& src_dbid, 
    const std::string& clone_dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles,
    DBCloud** dbptr,
    bool read_only) {

  // Mark env instance as serving a clone.
  CloudEnv* cenv = static_cast<CloudEnv *>(options.env);

  // Make the cloud env always fetch data from the cloud
  cenv->SetCloudDirect();

  Status st = DBCloudImpl::SanitizeCloneDirectory(options, src_dbid,
		             clone_dbname, read_only);
  if (!st.ok()) {
    return st;
  }

  // Mark the env to act as a clone env from now on.
  cenv->ClearCloudDirect();
  cenv->SetClone(src_dbid);

  st = DBCloud::Open(options, clone_dbname, column_families,
		     handles, dbptr, read_only);
  return st;
}

//
// Read the contents of the file (upto 64 K) into a memory buffer
//
Status DBCloudImpl::ReadFileIntoString(Env* env,
		const std::string& filename,
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
  char buffer[64*1024];

  uint64_t file_size;
  s = env->GetFileSize(filename, &file_size);
  if (!s.ok()) {
    return s;
  }
  if (file_size > sizeof(buffer)) {
    return Status::IOError("DBCloudImpl::ReadFileIntoString"
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
// Shall we re-initialize the clone dir?
//
Status DBCloudImpl::NeedsReinitialization(CloudEnv* cenv,
		             const Options& options,
		             const std::string& src_dbid,
		             const std::string& clone_dir,
			     bool* do_reinit) {
  // assume that directory does needs reinitialization
  *do_reinit = true;

  // get local env
  Env* env = cenv->GetBaseEnv();

  // Does the local directory exist
  unique_ptr<Directory> dir;
  Status st = env->NewDirectory(clone_dir, &dir);

  // If directory does not exist, then re-initialize
  if (!st.ok()) {
    return Status::OK();
  }

  // Check that the DB ID fie exists in clonedir
  std::string idfilename = clone_dir + "/" + "/IDENTITY";
  st = env->FileExists(idfilename);
  if (!st.ok()) {
    return Status::OK();
  }
  // Read DBID file from clone dir
  std::string local_dbid;
  st = ReadFileIntoString(env, idfilename, &local_dbid);
  if (!st.ok()) {
    return Status::OK();
  }

  // The local DBID = "SRC-DBID" + "rockset" + UUID
  std::string prefix = src_dbid + "rockset";
  size_t pos = local_dbid.find(prefix);
  if (pos == std::string::npos || pos != 0) {
    std::string err = "[db_cloud_impl] NeedsReinitialization: "
	              "Local dbid is " + local_dbid +
	              " but src dbid is " + src_dbid;
    Log(InfoLogLevel::DEBUG_LEVEL, options.info_log, err.c_str());
    return Status::OK();
  }

  //
  // The DBID of the clone matches that of the source_bucket.
  // We do not need any re-initialization of local dir.
  //
  *do_reinit = false;
  return Status::OK();
}


//
// Create appropriate files in the clone dir
//
Status DBCloudImpl::SanitizeCloneDirectory(const Options& options,
		              const std::string& src_dbid,
		              const std::string& clone_name,
			      bool readonly) {
  EnvOptions soptions;

  CloudEnvImpl* cenv = static_cast<CloudEnvImpl *>(options.env);
  if (cenv->GetCloudType() != CloudType::kAws) {
    return Status::InvalidArgument("SanitizeCloneDirectory: Invalid Type");
  }
  // acquire the local env
  Env* env = cenv->GetBaseEnv();

  // Shall we reinitialize the clone dir?
  bool do_reinit = true;
  Status st = DBCloudImpl::NeedsReinitialization(cenv,
		             options, src_dbid, clone_name,
			     &do_reinit);
  if (!st.ok()) {
    Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
	"[db_cloud_impl] SanitizeCloneDirectory error inspecting dir %s %s",
	clone_name.c_str(), st.ToString().c_str());
    return st;
  }

  if (!do_reinit) {
    Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
	"[db_cloud_impl] SanitizeCloneDirectory local clone %s is good",
	clone_name.c_str());
    return Status::OK();
  }

  // Delete all local files
  std::vector<Env::FileAttributes> result;
  st = env->GetChildrenFileAttributes(clone_name, &result);
  if (!st.ok() && !st.IsNotFound()) {
    return st;
  }
  for (auto file : result) {
    if (file.name == "." || file.name == "..") {
      continue;
    }
    std::string pathname = clone_name + "/" + file.name;
    st = env->DeleteFile(pathname);
    if (!st.ok()) {
      return st;
    }
  }

  // If directory does not exist, create it
  if (!st.ok() && st.IsNotFound()) {
    if (readonly) {
      return st;
    }
    st = env->CreateDirIfMissing(clone_name);
  }
  if (!st.ok()) {
    Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
	"[db_cloud_impl] SanitizeCloneDirectory error opening dir %s %s",
	clone_name.c_str(), st.ToString().c_str());
    return st;
  }

  // find the directory of the src_db
  std::string srcdb_dir;
  st = cenv->GetPathForDbid(src_dbid, &srcdb_dir);
  if (!st.ok()) {
    Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
	"[db_cloud_impl] Clone %s Unable to find src dbdir for dbid %s %s",
	clone_name.c_str(), src_dbid.c_str(), st.ToString().c_str());
    return st;
  }

  // download MANIFEST
  std::string manifestfile = "MANIFEST-000000";
  std::string src_manifestfile = srcdb_dir + "/MANIFEST";
  st = DBCloudImpl::CopyFile(cenv, env,
		             src_manifestfile,
			     clone_name + "/" + manifestfile);
  if (!st.ok()) {
    Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
	"[db_cloud_impl] Unable to download MANIFEST file from %s to %s %s",
	src_manifestfile.c_str(), clone_name.c_str(), st.ToString().c_str());
    return st;
  } else {
    Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
	"[db_cloud_impl] Download cloud MANIFEST file from % to %s %s",
	src_manifestfile.c_str(), clone_name.c_str(), st.ToString().c_str());
  }

  // write new dbid to IDENTITY file
  {
    // create new dbid
    std::string new_dbid = src_dbid + "rockset" + env->GenerateUniqueId();

    unique_ptr<WritableFile> destfile;
    st = env->NewWritableFile(clone_name + "/" + "IDENTITY",
		              &destfile, soptions);
    if (!st.ok()) {
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] Unable to create local IDENTITY file to %s %s",
          clone_name.c_str(), st.ToString().c_str());
      return st;
    }
    st = destfile->Append(Slice(new_dbid));
    if (!st.ok()) {
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] Unable to write local IDENTITY file to %s %s",
          clone_name.c_str(), st.ToString().c_str());
      return st;
    }
    Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
        "[db_cloud_impl] Write dbid %s to local IDENTITY file %s %s",
        new_dbid.c_str(), clone_name.c_str(), st.ToString().c_str());
  }
  // create CURRENT file to point to the manifest
  {
    unique_ptr<WritableFile> destfile;
    st = env->NewWritableFile(clone_name + "/" +"CURRENT",
		              &destfile, soptions);
    if (!st.ok()) {
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] Unable to create local CURRENT file to %s %s",
          clone_name.c_str(), st.ToString().c_str());
      return st;
    }
    manifestfile += "\n";   // CURRENT file needs a newline
    st = destfile->Append(Slice(manifestfile));
    if (!st.ok()) {
      Log(InfoLogLevel::DEBUG_LEVEL, options.info_log,
          "[db_cloud_impl] Unable to write local IDENTITY file to %s %s",
          clone_name.c_str(), st.ToString().c_str());
      return st;
    }
  }
  return Status::OK();
}

//
// Copy file
//
Status DBCloudImpl::CopyFile(
    Env* src_env,
    Env* dest_env,
    const std::string& srcname,
    const std::string& destname,
    uint64_t size,
    bool do_sync) {

  const EnvOptions soptions;
  unique_ptr<SequentialFile> srcfile;
  Status s = src_env->NewSequentialFile(srcname, &srcfile, soptions);
  if (!s.ok()) {
    return s;
  }

  // If size, is not specified, copy the entire object.
  if (size == 0) {
    s = src_env->GetFileSize(srcname, &size);
    if (!s.ok()) {
      return s;
    }
  }
  unique_ptr<WritableFile> destfile;
  s = dest_env->NewWritableFile(destname, &destfile, soptions);

  // copy 64K at a time
  char buffer[64 * 1024];
  Slice slice;
  while (size > 0 && s.ok()) {
    size_t bytes_to_read = std::min(sizeof(buffer), static_cast<size_t>(size));
    s = srcfile->Read(bytes_to_read, &slice, buffer);
    if (s.ok()) {
      if (slice.size() == 0) {
        return Status::Corruption("file too small");
      }
      s = destfile->Append(slice);
    }
    if (!s.ok()) {
      return s;
    }
    size -= slice.size();
  }
  if (s.ok() && do_sync) {
    s = destfile->Sync();
  }
  return s;
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
