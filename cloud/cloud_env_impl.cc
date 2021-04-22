// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include "cloud/cloud_env_impl.h"

#include <cinttypes>

#include "cloud/cloud_env_wrapper.h"
#include "cloud/cloud_scheduler.h"
#include "cloud/filename.h"
#include "cloud/manifest_reader.h"
#include "env/composite_env_wrapper.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "file/writable_file_writer.h"
#include "port/likely.h"
#include "rocksdb/cloud/cloud_log_controller.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "util/xxhash.h"

namespace ROCKSDB_NAMESPACE {

CloudEnvImpl::CloudEnvImpl(const CloudEnvOptions& opts, Env* base,
                           const std::shared_ptr<Logger>& l)
    : CloudEnv(opts, base, l), purger_is_running_(true) {
  scheduler_ = CloudScheduler::Get();
}

CloudEnvImpl::~CloudEnvImpl() {
  if (cloud_env_options.cloud_log_controller) {
    cloud_env_options.cloud_log_controller->StopTailingStream();
  }
  {
    std::lock_guard<std::mutex> lk(files_to_delete_mutex_);
    using std::swap;
    for (auto& e : files_to_delete_) {
      scheduler_->CancelJob(e.second);
    }
    files_to_delete_.clear();
  }
  StopPurger();
}

Status CloudEnvImpl::ExistsCloudObject(const std::string& fname) {
  Status st = Status::NotFound();
  if (HasDestBucket()) {
    st = GetStorageProvider()->ExistsCloudObject(GetDestBucketName(),
                                                 destname(fname));
  }
  if (st.IsNotFound() && HasSrcBucket() && !SrcMatchesDest()) {
    st = GetStorageProvider()->ExistsCloudObject(GetSrcBucketName(),
                                                 srcname(fname));
  }
  return st;
}

Status CloudEnvImpl::GetCloudObject(const std::string& fname) {
  Status st = Status::NotFound();
  if (HasDestBucket()) {
    st = GetStorageProvider()->GetCloudObject(GetDestBucketName(),
                                              destname(fname), fname);
  }
  if (st.IsNotFound() && HasSrcBucket() && !SrcMatchesDest()) {
    st = GetStorageProvider()->GetCloudObject(GetSrcBucketName(),
                                              srcname(fname), fname);
  }
  return st;
}

Status CloudEnvImpl::GetCloudObjectSize(const std::string& fname,
                                        uint64_t* remote_size) {
  Status st = Status::NotFound();
  if (HasDestBucket()) {
    st = GetStorageProvider()->GetCloudObjectSize(GetDestBucketName(),
                                                  destname(fname), remote_size);
  }
  if (st.IsNotFound() && HasSrcBucket() && !SrcMatchesDest()) {
    st = GetStorageProvider()->GetCloudObjectSize(GetSrcBucketName(),
                                                  srcname(fname), remote_size);
  }
  return st;
}

Status CloudEnvImpl::GetCloudObjectModificationTime(const std::string& fname,
                                                    uint64_t* time) {
  Status st = Status::NotFound();
  if (HasDestBucket()) {
    st = GetStorageProvider()->GetCloudObjectModificationTime(
        GetDestBucketName(), destname(fname), time);
  }
  if (st.IsNotFound() && HasSrcBucket() && !SrcMatchesDest()) {
    st = GetStorageProvider()->GetCloudObjectModificationTime(
        GetSrcBucketName(), srcname(fname), time);
  }
  return st;
}

Status CloudEnvImpl::ListCloudObjects(const std::string& path,
                                      std::vector<std::string>* result) {
  Status st;
  // Fetch the list of children from both cloud buckets
  if (HasSrcBucket()) {
    st = GetStorageProvider()->ListCloudObjects(GetSrcBucketName(),
                                                GetSrcObjectPath(), result);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[%s] GetChildren src bucket %s %s error from %s %s", Name(),
          GetSrcBucketName().c_str(), path.c_str(),
          GetStorageProvider()->Name(), st.ToString().c_str());
      return st;
    }
  }
  if (HasDestBucket() && !SrcMatchesDest()) {
    st = GetStorageProvider()->ListCloudObjects(GetDestBucketName(),
                                                GetDestObjectPath(), result);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[%s] GetChildren dest bucket %s %s error from %s %s", Name(),
          GetDestBucketName().c_str(), path.c_str(),
          GetStorageProvider()->Name(), st.ToString().c_str());
    }
  }
  return st;
}

Status CloudEnvImpl::NewCloudReadableFile(
    const std::string& fname, std::unique_ptr<CloudStorageReadableFile>* result,
    const EnvOptions& options) {
  Status st = Status::NotFound();
  if (HasDestBucket()) {  // read from destination
    st = GetStorageProvider()->NewCloudReadableFile(
        GetDestBucketName(), destname(fname), result, options);
    if (st.ok()) {
      return st;
    }
  }
  if (HasSrcBucket() && !SrcMatchesDest()) {  // read from src bucket
    st = GetStorageProvider()->NewCloudReadableFile(
        GetSrcBucketName(), srcname(fname), result, options);
  }
  return st;
}

// open a file for sequential reading
Status CloudEnvImpl::NewSequentialFile(const std::string& logical_fname,
                                       std::unique_ptr<SequentialFile>* result,
                                       const EnvOptions& options) {
  result->reset();

  auto fname = RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       manifest = (file_type == RocksDBFileType::kManifestFile),
       identity = (file_type == RocksDBFileType::kIdentityFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  auto st = CheckOption(options);
  if (!st.ok()) {
    return st;
  }

  if (sstfile || manifest || identity) {
    // We read first from local storage and then from cloud storage.
    st = base_env_->NewSequentialFile(fname, result, options);

    if (!st.ok()) {
      if (cloud_env_options.keep_local_sst_files || !sstfile) {
        // copy the file to the local storage if keep_local_sst_files is true
        st = GetCloudObject(fname);
        if (st.ok()) {
          // we successfully copied the file, try opening it locally now
          st = base_env_->NewSequentialFile(fname, result, options);
        }
      } else {
        std::unique_ptr<CloudStorageReadableFile> file;
        st = NewCloudReadableFile(fname, &file, options);
        if (st.ok()) {
          result->reset(file.release());
        }
      }
    }
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
        "[%s] NewSequentialFile file %s %s", Name(), fname.c_str(),
        st.ToString().c_str());
    return st;

  } else if (logfile && !cloud_env_options.keep_local_log_files) {
    return cloud_env_options.cloud_log_controller->NewSequentialFile(
        fname, result, options);
  }

  // This is neither a sst file or a log file. Read from default env.
  return base_env_->NewSequentialFile(fname, result, options);
}

// Ability to read a file directly from cloud storage
Status CloudEnvImpl::NewSequentialFileCloud(
    const std::string& bucket, const std::string& fname,
    std::unique_ptr<SequentialFile>* result, const EnvOptions& options) {
  std::unique_ptr<CloudStorageReadableFile> file;
  Status st =
      GetStorageProvider()->NewCloudReadableFile(bucket, fname, &file, options);
  if (!st.ok()) {
    return st;
  }

  result->reset(file.release());
  return st;
}

// open a file for random reading
Status CloudEnvImpl::NewRandomAccessFile(
    const std::string& logical_fname, std::unique_ptr<RandomAccessFile>* result,
    const EnvOptions& options) {
  result->reset();

  auto fname = RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       manifest = (file_type == RocksDBFileType::kManifestFile),
       identity = (file_type == RocksDBFileType::kIdentityFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  // Validate options
  auto st = CheckOption(options);
  if (!st.ok()) {
    return st;
  }

  if (sstfile || manifest || identity) {
    // Read from local storage and then from cloud storage.
    st = base_env_->NewRandomAccessFile(fname, result, options);

    if (!st.ok() && !base_env_->FileExists(fname).IsNotFound()) {
      // if status is not OK, but file does exist locally, something is wrong
      return st;
    }

    if (cloud_env_options.keep_local_sst_files || !sstfile) {
      if (!st.ok()) {
        // copy the file to the local storage if keep_local_sst_files is true
        st = GetCloudObject(fname);
        if (st.ok()) {
          // we successfully copied the file, try opening it locally now
          st = base_env_->NewRandomAccessFile(fname, result, options);
        }
      }
      // If we are being paranoic, then we validate that our file size is
      // the same as in cloud storage.
      if (st.ok() && sstfile && cloud_env_options.validate_filesize) {
        uint64_t remote_size = 0;
        uint64_t local_size = 0;
        Status stax = base_env_->GetFileSize(fname, &local_size);
        if (!stax.ok()) {
          return stax;
        }
        stax = Status::NotFound();
        if (HasDestBucket()) {
          stax = GetCloudObjectSize(fname, &remote_size);
        }
        if (stax.IsNotFound() && !HasDestBucket()) {
          // It is legal for file to not be present in storage provider if
          // destination bucket is not set.
        } else if (!stax.ok() || remote_size != local_size) {
          std::string msg = std::string("[") + Name() + "] HeadObject src " +
                            fname + " local size " +
                            std::to_string(local_size) + " cloud size " +
                            std::to_string(remote_size) + " " + stax.ToString();
          Log(InfoLogLevel::ERROR_LEVEL, info_log_, "%s", msg.c_str());
          return Status::IOError(msg);
        }
      }
    } else if (!st.ok()) {
      // Only execute this code path if keep_local_sst_files == false. If it's
      // true, we will never use CloudReadableFile to read; we copy the file
      // locally and read using base_env.
      std::unique_ptr<CloudStorageReadableFile> file;
      st = NewCloudReadableFile(fname, &file, options);
      if (st.ok()) {
        result->reset(file.release());
      }
    }
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
        "[%s] NewRandomAccessFile file %s %s", Name(), fname.c_str(),
        st.ToString().c_str());
    return st;

  } else if (logfile && !cloud_env_options.keep_local_log_files) {
    // read from LogController
    st = cloud_env_options.cloud_log_controller->NewRandomAccessFile(
        fname, result, options);
    return st;
  }

  // This is neither a sst file or a log file. Read from default env.
  return base_env_->NewRandomAccessFile(fname, result, options);
}

// create a new file for writing
Status CloudEnvImpl::NewWritableFile(const std::string& logical_fname,
                                     std::unique_ptr<WritableFile>* result,
                                     const EnvOptions& options) {
  result->reset();

  auto fname = RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       manifest = (file_type == RocksDBFileType::kManifestFile),
       identity = (file_type == RocksDBFileType::kIdentityFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  Status s;

  if (HasDestBucket() && (sstfile || identity || manifest)) {
    std::unique_ptr<CloudStorageWritableFile> f;
    GetStorageProvider()->NewCloudWritableFile(fname, GetDestBucketName(),
                                               destname(fname), &f, options);
    s = f->status();
    if (!s.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[%s] NewWritableFile src %s %s", Name(), fname.c_str(),
          s.ToString().c_str());
      return s;
    }
    result->reset(f.release());
  } else if (logfile && !cloud_env_options.keep_local_log_files) {
    std::unique_ptr<CloudLogWritableFile> f(
        cloud_env_options.cloud_log_controller->CreateWritableFile(fname,
                                                                   options));
    if (!f || !f->status().ok()) {
      std::string msg = std::string("[") + Name() + "] NewWritableFile";
      s = Status::IOError(msg, fname.c_str());
      Log(InfoLogLevel::ERROR_LEVEL, info_log_, "%s src %s %s", msg.c_str(),
          fname.c_str(), s.ToString().c_str());
      return s;
    }
    result->reset(f.release());
  } else {
    s = base_env_->NewWritableFile(fname, result, options);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] NewWritableFile src %s %s",
      Name(), fname.c_str(), s.ToString().c_str());
  return s;
}

Status CloudEnvImpl::ReopenWritableFile(const std::string& fname,
                                        std::unique_ptr<WritableFile>* result,
                                        const EnvOptions& options) {
  // This is not accurately correct because there is no wasy way to open
  // an provider file in append mode. We still need to support this because
  // rocksdb's ExternalSstFileIngestionJob invokes this api to reopen
  // a pre-created file to flush/sync it.
  return base_env_->ReopenWritableFile(fname, result, options);
}

//
// Check if the specified filename exists.
//
Status CloudEnvImpl::FileExists(const std::string& logical_fname) {
  Status st;

  auto fname = RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       manifest = (file_type == RocksDBFileType::kManifestFile),
       identity = (file_type == RocksDBFileType::kIdentityFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  if (sstfile || manifest || identity) {
    // We read first from local storage and then from cloud storage.
    st = base_env_->FileExists(fname);
    if (st.IsNotFound()) {
      st = ExistsCloudObject(fname);
    }
  } else if (logfile && !cloud_env_options.keep_local_log_files) {
    // read from controller
    st = cloud_env_options.cloud_log_controller->FileExists(fname);
  } else {
    st = base_env_->FileExists(fname);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] FileExists path '%s' %s",
      Name(), fname.c_str(), st.ToString().c_str());
  return st;
}

Status CloudEnvImpl::GetChildren(const std::string& path,
                                 std::vector<std::string>* result) {
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] GetChildren path '%s' ",
      Name(), path.c_str());
  result->clear();

  Status st;
  if (!cloud_env_options.skip_cloud_files_in_getchildren) {
    // Fetch the list of children from the cloud
    st = ListCloudObjects(path, result);
    if (!st.ok()) {
      return st;
    }
  }

  // fetch all files that exist in the local posix directory
  std::vector<std::string> local_files;
  st = base_env_->GetChildren(path, &local_files);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[%s] GetChildren %s error on local dir", Name(), path.c_str());
    return st;
  }

  for (auto const& value : local_files) {
    result->push_back(value);
  }

  // Remove all results that are not supposed to be visible.
  result->erase(
      std::remove_if(result->begin(), result->end(),
                     [&](const std::string& f) {
                       auto noepoch = RemoveEpoch(f);
                       if (!IsSstFile(noepoch) && !IsManifestFile(noepoch)) {
                         return false;
                       }
                       return RemapFilename(noepoch) != f;
                     }),
      result->end());
  // Remove the epoch, remap into RocksDB's domain
  for (size_t i = 0; i < result->size(); ++i) {
    auto noepoch = RemoveEpoch(result->at(i));
    if (IsSstFile(noepoch) || IsManifestFile(noepoch)) {
      // remap sst and manifest files
      result->at(i) = noepoch;
    }
  }
  // remove duplicates
  std::sort(result->begin(), result->end());
  result->erase(std::unique(result->begin(), result->end()), result->end());

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[%s] GetChildren %s successfully returned %" ROCKSDB_PRIszt " files",
      Name(), path.c_str(), result->size());
  return Status::OK();
}

Status CloudEnvImpl::GetFileSize(const std::string& logical_fname,
                                 uint64_t* size) {
  *size = 0L;

  auto fname = RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  Status st;
  if (sstfile) {
    if (base_env_->FileExists(fname).ok()) {
      st = base_env_->GetFileSize(fname, size);
    } else {
      st = GetCloudObjectSize(fname, size);
    }
  } else if (logfile && !cloud_env_options.keep_local_log_files) {
    st = cloud_env_options.cloud_log_controller->GetFileSize(fname, size);
  } else {
    st = base_env_->GetFileSize(fname, size);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[%s] GetFileSize src '%s' %s %" PRIu64, Name(), fname.c_str(),
      st.ToString().c_str(), *size);
  return st;
}

Status CloudEnvImpl::GetFileModificationTime(const std::string& logical_fname,
                                             uint64_t* time) {
  *time = 0;

  auto fname = RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  Status st;
  if (sstfile) {
    if (base_env_->FileExists(fname).ok()) {
      st = base_env_->GetFileModificationTime(fname, time);
    } else {
      st = GetCloudObjectModificationTime(fname, time);
    }
  } else if (logfile && !cloud_env_options.keep_local_log_files) {
    st = cloud_env_options.cloud_log_controller->GetFileModificationTime(fname,
                                                                         time);
  } else {
    st = base_env_->GetFileModificationTime(fname, time);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[%s] GetFileModificationTime src '%s' %s", Name(), fname.c_str(),
      st.ToString().c_str());
  return st;
}

// The rename may not be atomic. Some cloud vendords do not support renaming
// natively. Copy file to a new object and then delete original object.
Status CloudEnvImpl::RenameFile(const std::string& logical_src,
                                const std::string& logical_target) {
  auto src = RemapFilename(logical_src);
  auto target = RemapFilename(logical_target);
  // Get file type of target
  auto file_type = GetFileType(target);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       manifest = (file_type == RocksDBFileType::kManifestFile),
       identity = (file_type == RocksDBFileType::kIdentityFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  // Rename should never be called on sst files.
  if (sstfile) {
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
        "[%s] RenameFile source sstfile %s %s is not supported", Name(),
        src.c_str(), target.c_str());
    assert(0);
    return Status::NotSupported(Slice(src), Slice(target));
  } else if (logfile) {
    // Rename should never be called on log files as well
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[%s] RenameFile source logfile %s %s is not supported", Name(),
        src.c_str(), target.c_str());
    assert(0);
    return Status::NotSupported(Slice(src), Slice(target));
  } else if (manifest) {
    // Rename should never be called on manifest files as well
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[%s] RenameFile source manifest %s %s is not supported", Name(),
        src.c_str(), target.c_str());
    assert(0);
    return Status::NotSupported(Slice(src), Slice(target));

  } else if (!identity || !HasDestBucket()) {
    return base_env_->RenameFile(src, target);
  }
  // Only ID file should come here
  assert(identity);
  assert(HasDestBucket());
  assert(basename(target) == "IDENTITY");

  // Save Identity to Cloud
  Status st = SaveIdentityToCloud(src, destname(target));

  // Do the rename on local filesystem too
  if (st.ok()) {
    st = base_env_->RenameFile(src, target);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[%s] RenameFile src %s target %s: %s", Name(), src.c_str(),
      target.c_str(), st.ToString().c_str());
  return st;
}

Status CloudEnvImpl::LinkFile(const std::string& src,
                              const std::string& target) {
  // We only know how to link file if both src and dest buckets are empty
  if (HasDestBucket() || HasSrcBucket()) {
    return Status::NotSupported();
  }
  auto src_remapped = RemapFilename(src);
  auto target_remapped = RemapFilename(target);
  return base_env_->LinkFile(src_remapped, target_remapped);
}

class CloudDirectory : public Directory {
 public:
  explicit CloudDirectory(CloudEnv* env, const std::string name)
      : env_(env), name_(name) {
    status_ = env_->GetBaseEnv()->NewDirectory(name, &posixDir);
  }

  ~CloudDirectory() {}

  virtual Status Fsync() {
    if (!status_.ok()) {
      return status_;
    }
    return posixDir->Fsync();
  }

  virtual Status status() { return status_; }

 private:
  CloudEnv* env_;
  std::string name_;
  Status status_;
  std::unique_ptr<Directory> posixDir;
};

//  Returns success only if the directory-bucket exists in the
//  StorageProvider and the posixEnv local directory exists as well.
Status CloudEnvImpl::NewDirectory(const std::string& name,
                                  std::unique_ptr<Directory>* result) {
  result->reset(nullptr);

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] NewDirectory name '%s'",
      Name(), name.c_str());

  // create new object.
  std::unique_ptr<CloudDirectory> d(new CloudDirectory(this, name));

  // Check if the path exists in local dir
  if (!d->status().ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[%s] NewDirectory name %s unable to create local dir", Name(),
        name.c_str());
    return d->status();
  }
  result->reset(d.release());
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] NewDirectory name %s ok",
      Name(), name.c_str());
  return Status::OK();
}

// Cloud storage providers have no concepts of directories,
// so we just have to forward the request to the base_env_
Status CloudEnvImpl::CreateDir(const std::string& dirname) {
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] CreateDir dir '%s'", Name(),
      dirname.c_str());
  Status st;

  // create local dir
  st = base_env_->CreateDir(dirname);

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] CreateDir dir %s %s", Name(),
      dirname.c_str(), st.ToString().c_str());
  return st;
};

// Cloud storage providers have no concepts of directories,
// so we just have to forward the request to the base_env_
Status CloudEnvImpl::CreateDirIfMissing(const std::string& dirname) {
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] CreateDirIfMissing dir '%s'",
      Name(), dirname.c_str());
  Status st;

  // create directory in base_env_
  st = base_env_->CreateDirIfMissing(dirname);

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[%s] CreateDirIfMissing created dir %s %s", Name(), dirname.c_str(),
      st.ToString().c_str());
  return st;
};

// Cloud storage providers have no concepts of directories,
// so we just have to forward the request to the base_env_
Status CloudEnvImpl::DeleteDir(const std::string& dirname) {
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] DeleteDir src '%s'", Name(),
      dirname.c_str());
  Status st = base_env_->DeleteDir(dirname);
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] DeleteDir dir %s %s", Name(),
      dirname.c_str(), st.ToString().c_str());
  return st;
};

Status CloudEnvImpl::DeleteFile(const std::string& logical_fname) {
  auto fname = RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       manifest = (file_type == RocksDBFileType::kManifestFile),
       identity = (file_type == RocksDBFileType::kIdentityFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  if (manifest) {
    // We don't delete manifest files. The reason for this is that even though
    // RocksDB creates manifest with different names (like MANIFEST-00001,
    // MANIFEST-00008) we actually map all of them to the same filename
    // MANIFEST-[epoch].
    // When RocksDB wants to roll the MANIFEST (let's say from 1 to 8) it does
    // the following:
    // 1. Create a new MANIFEST-8
    // 2. Write everything into MANIFEST-8
    // 3. Sync MANIFEST-8
    // 4. Store "MANIFEST-8" in CURRENT file
    // 5. Delete MANIFEST-1
    //
    // What RocksDB cloud does behind the scenes (the numbers match the list
    // above):
    // 1. Create manifest file MANIFEST-[epoch].tmp
    // 2. Forward RocksDB writes to the file created in the first step
    // 3. Atomic rename from MANIFEST-[epoch].tmp to MANIFEST-[epoch]. The old
    // file with the same file name is overwritten.
    // 4. Nothing. Whatever the contents of CURRENT file, we don't care, we
    // always remap MANIFEST files to the correct with the latest epoch.
    // 5. Also nothing. There is no file to delete, because we have overwritten
    // it in the third step.
    return Status::OK();
  }

  Status st;
  // Delete from destination bucket and local dir
  if (sstfile || manifest || identity) {
    if (HasDestBucket()) {
      // add the remote file deletion to the queue
      st = DeleteCloudFileFromDest(basename(fname));
    }
    // delete from local, too. Ignore the result, though. The file might not be
    // there locally.
    base_env_->DeleteFile(fname);
  } else if (logfile && !cloud_env_options.keep_local_log_files) {
    // read from Log Controller
    st = cloud_env_options.cloud_log_controller->status();
    if (st.ok()) {
      // Log a Delete record to controller stream
      std::unique_ptr<CloudLogWritableFile> f(
          cloud_env_options.cloud_log_controller->CreateWritableFile(
              fname, EnvOptions()));
      if (!f || !f->status().ok()) {
        std::string msg =
            "[" + std::string(cloud_env_options.cloud_log_controller->Name()) +
            "] DeleteFile";
        st = Status::IOError(msg, fname.c_str());
      } else {
        st = f->LogDelete();
      }
    }
  } else {
    st = base_env_->DeleteFile(fname);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] DeleteFile file %s %s",
      Name(), fname.c_str(), st.ToString().c_str());
  return st;
}

void CloudEnvImpl::RemoveFileFromDeletionQueue(const std::string& filename) {
  std::lock_guard<std::mutex> lk(files_to_delete_mutex_);
  auto itr = files_to_delete_.find(filename);
  if (itr != files_to_delete_.end()) {
    scheduler_->CancelJob(itr->second);
    files_to_delete_.erase(itr);
  }
}

Status CloudEnvImpl::CopyLocalFileToDest(const std::string& local_name,
                                         const std::string& dest_name) {
  RemoveFileFromDeletionQueue(basename(local_name));
  return GetStorageProvider()->PutCloudObject(local_name, GetDestBucketName(),
                                              dest_name);
}

Status CloudEnvImpl::DeleteCloudFileFromDest(const std::string& fname) {
  assert(HasDestBucket());
  auto base = basename(fname);
  // add the job to delete the file in 1 hour
  auto doDeleteFile = [this, base](void*) {
    {
      std::lock_guard<std::mutex> lk(files_to_delete_mutex_);
      auto itr = files_to_delete_.find(base);
      if (itr == files_to_delete_.end()) {
        // File was removed from files_to_delete_, do not delete!
        return;
      }
      files_to_delete_.erase(itr);
    }
    auto path = GetDestObjectPath() + "/" + base;
    // we are ready to delete the file!
    auto st =
        GetStorageProvider()->DeleteCloudObject(GetDestBucketName(), path);
    if (!st.ok() && !st.IsNotFound()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[%s] DeleteFile file %s error %s", Name(), path.c_str(),
          st.ToString().c_str());
    }
  };
  {
    std::lock_guard<std::mutex> lk(files_to_delete_mutex_);
    if (files_to_delete_.find(base) != files_to_delete_.end()) {
      // already in the queue
      return Status::OK();
    }
  }
  {
    std::lock_guard<std::mutex> lk(files_to_delete_mutex_);
    auto handle = scheduler_->ScheduleJob(file_deletion_delay_,
                                          std::move(doDeleteFile), nullptr);
    files_to_delete_.emplace(base, std::move(handle));
  }
  return Status::OK();
}

// Copy my IDENTITY file to cloud storage. Update dbid registry.
Status CloudEnvImpl::SaveIdentityToCloud(const std::string& localfile,
                                         const std::string& idfile) {
  assert(basename(idfile) == "IDENTITY");

  // Read id into string
  std::string dbid;
  Status st = ReadFileToString(base_env_, localfile, &dbid);
  dbid = trim(dbid);

  // Upload ID file to provider
  if (st.ok()) {
    st = GetStorageProvider()->PutCloudObject(localfile, GetDestBucketName(),
                                              idfile);
  }

  // Save mapping from ID to cloud pathname
  if (st.ok() && !GetDestObjectPath().empty()) {
    st = SaveDbid(GetDestBucketName(), dbid, GetDestObjectPath());
  }
  return st;
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
  return CloudEnvImpl::LoadLocalCloudManifest(dbname, GetBaseEnv(),
                                              &cloud_manifest_);
}

Status CloudEnvImpl::LoadLocalCloudManifest(
    const std::string& dbname, Env* base_env,
    std::unique_ptr<CloudManifest>* cloud_manifest) {
  std::unique_ptr<SequentialFile> file;
  auto cloud_manifest_file_name = CloudManifestFile(dbname);
  auto s = base_env->NewSequentialFile(cloud_manifest_file_name, &file,
                                       EnvOptions());
  if (!s.ok()) {
    return s;
  }
  return CloudManifest::LoadFromLog(
      std::unique_ptr<SequentialFileReader>(new SequentialFileReader(
          NewLegacySequentialFileWrapper(file), cloud_manifest_file_name)),
      cloud_manifest);
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
    std::vector<std::string> pathnames;
    s = GetStorageProvider()->ListCloudObjects(GetDestBucketName(),
                                               GetDestObjectPath(), &pathnames);
    if (!s.ok()) {
      return s;
    }

    for (auto& fname : pathnames) {
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
        new WritableFileWriter(NewLegacyWritableFileWrapper(std::move(file)),
                               tmp_fname, EnvOptions())));
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

// Check if options are compatible with the cloud storage system
Status CloudEnvImpl::CheckOption(const EnvOptions& options) {
  // Cannot mmap files that reside on cloud storage, unless the file is also
  // local
  if (options.use_mmap_reads && !cloud_env_options.keep_local_sst_files) {
    std::string msg = "Mmap only if keep_local_sst_files is set";
    return Status::InvalidArgument(msg);
  }
  return Status::OK();
}

//
// prepends the configured src object path name
//
std::string CloudEnvImpl::srcname(const std::string& localname) {
  assert(cloud_env_options.src_bucket.IsValid());
  return cloud_env_options.src_bucket.GetObjectPath() + "/" +
         basename(localname);
}

//
// prepends the configured dest object path name
//
std::string CloudEnvImpl::destname(const std::string& localname) {
  assert(cloud_env_options.dest_bucket.IsValid());
  return cloud_env_options.dest_bucket.GetObjectPath() + "/" +
         basename(localname);
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
      // Unable to fetch data from provider Fail Open request.
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
      // Unable to fetch data from provider Fail Open request.
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
  // This is an ephemeral clone.
  if (src_object_path.empty() && dest_object_path.empty()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[cloud_env_impl] NeedsReinitialization: "
        "local dbid %s does not have a mapping in cloud registry "
        "src bucket %s or dest bucket %s",
        local_dbid.c_str(), src_bucket.c_str(), dest_bucket.c_str());

    // The local CLOUDMANIFEST on ephemeral clone is by definition out-of-sync
    // with the CLOUDMANIFEST in the cloud. That means we need to make sure the
    // local MANIFEST is compatible with the local CLOUDMANIFEST. Otherwise
    // there is no way we can recover since all MANIFEST files on the cloud are
    // only compatible with CLOUDMANIFEST on the cloud.
    //
    // If the local MANIFEST is not compatible with local CLOUDMANIFEST, we will
    // need to reinitialize the entire directory.
    std::unique_ptr<CloudManifest> cloud_manifest;
    Env* base_env = GetBaseEnv();
    Status load_status =
        LoadLocalCloudManifest(local_dir, base_env, &cloud_manifest);
    if (load_status.ok()) {
      std::string current_epoch = cloud_manifest->GetCurrentEpoch().ToString();
      Status local_manifest_exists =
          base_env->FileExists(ManifestFileWithEpoch(local_dir, current_epoch));
      if (!local_manifest_exists.ok()) {
        Log(InfoLogLevel::WARN_LEVEL, info_log_,
            "[cloud_env_impl] NeedsReinitialization: CLOUDMANIFEST exists "
            "locally, but no local MANIFEST is compatible");
        return Status::OK();
      }
    }

    // Resync all files from cloud.
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
    Status st = GetStorageProvider()->GetCloudObject(
        GetSrcBucketName(), GetSrcObjectPath() + "/IDENTITY", tmpfile);
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
    Status st = GetStorageProvider()->GetCloudObject(
        GetDestBucketName(), GetDestObjectPath() + "/IDENTITY", tmpfile);
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

  // Shall we reinitialize the clone dir?
  bool do_reinit = true;
  Status st = NeedsReinitialization(local_name, &do_reinit);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[cloud_env_impl] SanitizeDirectory error inspecting dir %s %s",
        local_name.c_str(), st.ToString().c_str());
    return st;
  }

  // If there is no destination bucket, then we prefer to suck in all sst files
  // from source bucket at db startup time. We do this by setting max_open_files
  // = -1
  if (!HasDestBucket()) {
    if (options.max_open_files != -1) {
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_env_impl] SanitizeDirectory info.  "
          " No destination bucket specified and options.max_open_files != -1 "
          " so sst files from src bucket %s are not copied into local dir %s "
          "at startup",
          GetSrcObjectPath().c_str(), local_name.c_str());
    }
    if (!cloud_env_options.keep_local_sst_files && !read_only) {
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_env_impl] SanitizeDirectory info.  "
          " No destination bucket specified and options.keep_local_sst_files "
          "is false. Existing sst files from src bucket %s will not be "
          " downloaded into local dir but newly created sst files will "
          " remain in local dir %s",
          GetSrcObjectPath().c_str(), local_name.c_str());
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
    st = GetStorageProvider()->GetCloudObject(
        GetDestBucketName(), IdentityFileName(GetDestObjectPath()),
        IdentityFileName(local_name));
    if (!st.ok() && !st.IsNotFound()) {
      // If there was an error and it's not IsNotFound() we need to bail
      return st;
    }
    got_identity_from_dest = st.ok();
  }
  if (!got_identity_from_dest && HasSrcBucket() && !SrcMatchesDest()) {
    // download IDENTITY from src
    st = GetStorageProvider()->GetCloudObject(
        GetSrcBucketName(), IdentityFileName(GetSrcObjectPath()),
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
    Status st = GetStorageProvider()->GetCloudObject(
        GetDestBucketName(), CloudManifestFile(GetDestObjectPath()),
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
    Status st = GetStorageProvider()->GetCloudObject(
        GetSrcBucketName(), CloudManifestFile(GetSrcObjectPath()),
        cloudmanifest);
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
    LegacyFileSystemWrapper fs(GetBaseEnv());
    st = CopyFile(&fs, ManifestFileWithEpoch(local_dbname, oldEpoch),
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
      st = GetStorageProvider()->PutCloudObject(
          ManifestFileWithEpoch(local_dbname, newEpoch), GetDestBucketName(),
          ManifestFileWithEpoch(GetDestObjectPath(), newEpoch));
      if (!st.ok()) {
        return st;
      }
    }
    // upload new cloud manifest
    st = GetStorageProvider()->PutCloudObject(
        CloudManifestFile(local_dbname), GetDestBucketName(),
        CloudManifestFile(GetDestObjectPath()));
    if (!st.ok()) {
      return st;
    }
  }
  return Status::OK();
}

// All db in a bucket are stored in path /.rockset/dbid/<dbid>
// The value of the object is the pathname where the db resides.
Status CloudEnvImpl::SaveDbid(const std::string& bucket_name,
                              const std::string& dbid,
                              const std::string& dirname) {
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] SaveDbid dbid %s dir '%s'",
      Name(), dbid.c_str(), dirname.c_str());

  std::string dbidkey = GetDbIdKey(dbid);
  std::unordered_map<std::string, std::string> metadata;
  metadata["dirname"] = dirname;

  Status st = GetStorageProvider()->PutCloudObjectMetadata(bucket_name, dbidkey,
                                                           metadata);

  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[%s] Bucket %s SaveDbid error in saving dbid %s dirname %s %s", Name(),
        bucket_name.c_str(), dbid.c_str(), dirname.c_str(),
        st.ToString().c_str());
  } else {
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[%s] Bucket %s SaveDbid dbid %s dirname %s %s", bucket_name.c_str(),
        Name(), dbid.c_str(), dirname.c_str(), "ok");
  }
  return st;
};

//
// Given a dbid, retrieves its pathname.
//
Status CloudEnvImpl::GetPathForDbid(const std::string& bucket,
                                    const std::string& dbid,
                                    std::string* dirname) {
  std::string dbidkey = GetDbIdKey(dbid);

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[%s] Bucket %s GetPathForDbid dbid %s", Name(), bucket.c_str(),
      dbid.c_str());

  CloudObjectInformation info;
  Status st =
      GetStorageProvider()->GetCloudObjectMetadata(bucket, dbidkey, &info);
  if (!st.ok()) {
    if (st.IsNotFound()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[%s] %s GetPathForDbid error non-existent dbid %s %s", Name(),
          bucket.c_str(), dbid.c_str(), st.ToString().c_str());
    } else {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[%s] %s GetPathForDbid error dbid %s %s", bucket.c_str(), Name(),
          dbid.c_str(), st.ToString().c_str());
    }
    return st;
  }

  // Find "dirname" metadata that stores the pathname of the db
  const char* kDirnameTag = "dirname";
  auto it = info.metadata.find(kDirnameTag);
  if (it != info.metadata.end()) {
    *dirname = it->second;
  } else {
    st = Status::NotFound("GetPathForDbid");
  }
  Log(InfoLogLevel::INFO_LEVEL, info_log_, "[%s] %s GetPathForDbid dbid %s %s",
      Name(), bucket.c_str(), dbid.c_str(), st.ToString().c_str());
  return st;
}

//
// Retrieves the list of all registered dbids and their paths
//
Status CloudEnvImpl::GetDbidList(const std::string& bucket, DbidList* dblist) {
  // fetch the list all all dbids
  std::vector<std::string> dbid_list;
  Status st = GetStorageProvider()->ListCloudObjects(bucket, kDbIdRegistry(),
                                                     &dbid_list);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[%s] %s GetDbidList error in GetChildrenFromS3 %s", Name(),
        bucket.c_str(), st.ToString().c_str());
    return st;
  }
  // for each dbid, fetch the db directory where the db data should reside
  for (auto dbid : dbid_list) {
    std::string dirname;
    st = GetPathForDbid(bucket, dbid, &dirname);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[%s] %s GetDbidList error in GetPathForDbid(%s) %s", Name(),
          bucket.c_str(), dbid.c_str(), st.ToString().c_str());
      return st;
    }
    // insert item into result set
    (*dblist)[dbid] = dirname;
  }
  return st;
}

//
// Deletes the specified dbid from the registry
//
Status CloudEnvImpl::DeleteDbid(const std::string& bucket,
                                const std::string& dbid) {
  // fetch the list all all dbids
  std::string dbidkey = GetDbIdKey(dbid);
  Status st = GetStorageProvider()->DeleteCloudObject(bucket, dbidkey);
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[%s] %s DeleteDbid DeleteDbid(%s) %s", Name(), bucket.c_str(),
      dbid.c_str(), st.ToString().c_str());
  return st;
}

Status CloudEnvImpl::LockFile(const std::string& /*fname*/, FileLock** lock) {
  // there isn's a very good way to atomically check and create cloud file
  *lock = nullptr;
  return Status::OK();
}

Status CloudEnvImpl::UnlockFile(FileLock* /*lock*/) { return Status::OK(); }

std::string CloudEnvImpl::GetWALCacheDir() {
  return cloud_env_options.cloud_log_controller->GetCacheDir();
}

Status CloudEnvImpl::Prepare() {
  Header(info_log_, "     %s.src_bucket_name: %s", Name(),
         cloud_env_options.src_bucket.GetBucketName().c_str());
  Header(info_log_, "     %s.src_object_path: %s", Name(),
         cloud_env_options.src_bucket.GetObjectPath().c_str());
  Header(info_log_, "     %s.src_bucket_region: %s", Name(),
         cloud_env_options.src_bucket.GetRegion().c_str());
  Header(info_log_, "     %s.dest_bucket_name: %s", Name(),
         cloud_env_options.dest_bucket.GetBucketName().c_str());
  Header(info_log_, "     %s.dest_object_path: %s", Name(),
         cloud_env_options.dest_bucket.GetObjectPath().c_str());
  Header(info_log_, "     %s.dest_bucket_region: %s", Name(),
         cloud_env_options.dest_bucket.GetRegion().c_str());

  Status s;
  if (cloud_env_options.src_bucket.GetBucketName().empty() !=
      cloud_env_options.src_bucket.GetObjectPath().empty()) {
    s = Status::InvalidArgument("Must specify both src bucket name and path");
  } else if (cloud_env_options.dest_bucket.GetBucketName().empty() !=
             cloud_env_options.dest_bucket.GetObjectPath().empty()) {
    s = Status::InvalidArgument("Must specify both dest bucket name and path");
  } else {
    if (!GetStorageProvider()) {
      s = Status::InvalidArgument(
          "Cloud environment requires a storage provider");
    } else {
      Header(info_log_, "     %s.storage_provider: %s", Name(),
             GetStorageProvider()->Name());
      s = GetStorageProvider()->Prepare(this);
    }
    if (s.ok()) {
      if (cloud_env_options.cloud_log_controller) {
        Header(info_log_, "     %s.log controller: %s", Name(),
               cloud_env_options.cloud_log_controller->Name());
        s = cloud_env_options.cloud_log_controller->Prepare(this);
      } else if (!cloud_env_options.keep_local_log_files) {
        s = Status::InvalidArgument(
            "Log controller required for remote log files");
      }
    }
  }
  return s;
}
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
