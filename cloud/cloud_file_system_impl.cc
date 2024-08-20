// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include "rocksdb/cloud/cloud_file_system_impl.h"

#include <cinttypes>

#include "cloud/cloud_log_controller_impl.h"
#include "cloud/cloud_manifest.h"
#include "cloud/cloud_scheduler.h"
#include "cloud/filename.h"
#include "cloud/manifest_reader.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "file/writable_file_writer.h"
#include "port/port_posix.h"
#include "rocksdb/cloud/cloud_file_deletion_scheduler.h"
#include "rocksdb/cloud/cloud_log_controller.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/options_type.h"
#include "test_util/sync_point.h"
#include "util/xxhash.h"

namespace ROCKSDB_NAMESPACE {

CloudFileSystemImpl::CloudFileSystemImpl(
    const CloudFileSystemOptions& opts, const std::shared_ptr<FileSystem>& base,
    const std::shared_ptr<Logger>& logger)
    : info_log_(logger),
      cloud_fs_options(opts),
      base_fs_(base),
      purger_is_running_(true) {
  RegisterOptions(&cloud_fs_options,
                  &CloudFileSystemOptions::cloud_fs_option_type_info);
  if (opts.cloud_file_deletion_delay) {
    cloud_file_deletion_scheduler_ = CloudFileDeletionScheduler::Create(
        CloudScheduler::Get(), *opts.cloud_file_deletion_delay);
  }
}

CloudFileSystemImpl::~CloudFileSystemImpl() {
  if (cloud_fs_options.cloud_log_controller) {
    cloud_fs_options.cloud_log_controller->StopTailingStream();
  }
  StopPurger();
  cloud_fs_options.cloud_log_controller.reset();
  cloud_fs_options.storage_provider.reset();
}

IOStatus CloudFileSystemImpl::ExistsCloudObject(const std::string& fname) {
  auto st = IOStatus::NotFound();
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

IOStatus CloudFileSystemImpl::GetCloudObject(const std::string& fname) {
  auto st = IOStatus::NotFound();
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

IOStatus CloudFileSystemImpl::GetCloudObjectSize(const std::string& fname,
                                                 uint64_t* remote_size) {
  auto st = IOStatus::NotFound();
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

IOStatus CloudFileSystemImpl::GetCloudObjectModificationTime(
    const std::string& fname, uint64_t* time) {
  auto st = IOStatus::NotFound();
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

IOStatus CloudFileSystemImpl::ListCloudObjects(
    const std::string& path, std::vector<std::string>* result) {
  IOStatus st;
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

IOStatus CloudFileSystemImpl::NewCloudReadableFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<CloudStorageReadableFile>* result, IODebugContext* dbg) {
  auto st = IOStatus::NotFound();
  if (HasDestBucket()) {  // read from destination
    st = GetStorageProvider()->NewCloudReadableFile(
        GetDestBucketName(), destname(fname), options, result, dbg);
    if (st.ok()) {
      return st;
    }
  }
  if (HasSrcBucket() && !SrcMatchesDest()) {  // read from src bucket
    st = GetStorageProvider()->NewCloudReadableFile(
        GetSrcBucketName(), srcname(fname), options, result, dbg);
  }
  return st;
}

// open a file for sequential reading
IOStatus CloudFileSystemImpl::NewSequentialFile(
    const std::string& logical_fname, const FileOptions& file_opts,
    std::unique_ptr<FSSequentialFile>* result, IODebugContext* dbg) {
  result->reset();

  auto fname = RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       manifest = (file_type == RocksDBFileType::kManifestFile),
       identity = (file_type == RocksDBFileType::kIdentityFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  auto st = status_to_io_status(CheckOption(file_opts));
  if (!st.ok()) {
    return st;
  }

  if (sstfile || manifest || identity) {
    if (cloud_fs_options.keep_local_sst_files || !sstfile) {
      // We read first from local storage and then from cloud storage.
      st = base_fs_->NewSequentialFile(fname, file_opts, result, dbg);
      if (!st.ok()) {
        // copy the file to the local storage if keep_local_sst_files is true
        st = GetCloudObject(fname);
        if (st.ok()) {
          // we successfully copied the file, try opening it locally now
          st = base_fs_->NewSequentialFile(fname, file_opts, result, dbg);
        }
      }
    } else {
      std::unique_ptr<CloudStorageReadableFile> file;
      st = NewCloudReadableFile(fname, file_opts, &file, dbg);
      if (st.ok()) {
        result->reset(file.release());
      }
    }
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
        "[%s] NewSequentialFile file %s %s", Name(), fname.c_str(),
        st.ToString().c_str());
    return st;

  } else if (logfile && !cloud_fs_options.keep_local_log_files) {
    return cloud_fs_options.cloud_log_controller->NewSequentialFile(
        fname, file_opts, result, dbg);
  }

  // This is neither a sst file or a log file. Read from default env.
  return base_fs_->NewSequentialFile(fname, file_opts, result, dbg);
}

// Ability to read a file directly from cloud storage
IOStatus CloudFileSystemImpl::NewSequentialFileCloud(
    const std::string& bucket, const std::string& fname,
    const FileOptions& file_opts, std::unique_ptr<FSSequentialFile>* result,
    IODebugContext* dbg) {
  std::unique_ptr<CloudStorageReadableFile> file;
  auto st = GetStorageProvider()->NewCloudReadableFile(bucket, fname, file_opts,
                                                       &file, dbg);
  if (!st.ok()) {
    return st;
  }

  result->reset(file.release());
  return st;
}

// open a file for random reading
IOStatus CloudFileSystemImpl::NewRandomAccessFile(
    const std::string& logical_fname, const FileOptions& file_opts,
    std::unique_ptr<FSRandomAccessFile>* result, IODebugContext* dbg) {
  result->reset();

  auto fname = RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       manifest = (file_type == RocksDBFileType::kManifestFile),
       identity = (file_type == RocksDBFileType::kIdentityFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  // Validate options
  auto st = status_to_io_status(CheckOption(file_opts));
  if (!st.ok()) {
    return st;
  }

  const IOOptions io_opts;
  if (sstfile || manifest || identity) {
    if (cloud_fs_options.keep_local_sst_files || !sstfile) {
      // Read from local storage and then from cloud storage.
      st = base_fs_->NewRandomAccessFile(fname, file_opts, result, dbg);

      if (!st.ok() && !base_fs_->FileExists(fname, io_opts, dbg).IsNotFound()) {
        // if status is not OK, but file does exist locally, something is wrong
        return st;
      }

      if (!st.ok()) {
        // copy the file to the local storage
        st = GetCloudObject(fname);
        if (st.ok()) {
          // we successfully copied the file, try opening it locally now
          st = base_fs_->NewRandomAccessFile(fname, file_opts, result, dbg);
        }
      }
      // If we are being paranoic, then we validate that our file size is
      // the same as in cloud storage.
      if (st.ok() && sstfile && cloud_fs_options.validate_filesize) {
        uint64_t remote_size = 0;
        uint64_t local_size = 0;
        auto stax = base_fs_->GetFileSize(fname, io_opts, &local_size, dbg);
        if (!stax.ok()) {
          return stax;
        }
        stax = IOStatus::NotFound();
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
          return IOStatus::IOError(msg);
        }
      }
    } else {
      // Only execute this code path if files are not cached locally
      std::unique_ptr<CloudStorageReadableFile> file;
      st = NewCloudReadableFile(fname, file_opts, &file, dbg);
      if (st.ok()) {
        result->reset(file.release());
      }
    }
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
        "[%s] NewRandomAccessFile file %s %s", Name(), fname.c_str(),
        st.ToString().c_str());
    return st;

  } else if (logfile && !cloud_fs_options.keep_local_log_files) {
    // read from LogController
    st = cloud_fs_options.cloud_log_controller->NewRandomAccessFile(
        fname, file_opts, result, dbg);
    return st;
  }

  // This is neither a sst file or a log file. Read from default env.
  return base_fs_->NewRandomAccessFile(fname, file_opts, result, dbg);
}

// create a new file for writing
IOStatus CloudFileSystemImpl::NewWritableFile(
    const std::string& logical_fname, const FileOptions& file_opts,
    std::unique_ptr<FSWritableFile>* result, IODebugContext* dbg) {
  result->reset();

  auto fname = RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       manifest = (file_type == RocksDBFileType::kManifestFile),
       identity = (file_type == RocksDBFileType::kIdentityFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  IOStatus s;
  if (HasDestBucket() && (sstfile || identity || manifest)) {
    std::unique_ptr<CloudStorageWritableFile> f;
    s = GetStorageProvider()->NewCloudWritableFile(
        fname, GetDestBucketName(), destname(fname), file_opts, &f, dbg);
    if (!s.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[%s] NewWritableFile fails while NewCloudWritableFile, src %s %s",
          Name(), fname.c_str(), s.ToString().c_str());
      return s;
    }
    s = f->status();
    if (!s.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[%s] NewWritableFile fails; unexpected WritableFile Status, src %s "
          "%s",
          Name(), fname.c_str(), s.ToString().c_str());
      return s;
    }
    result->reset(f.release());
  } else if (logfile && !cloud_fs_options.keep_local_log_files) {
    std::unique_ptr<CloudLogWritableFile> f(
        cloud_fs_options.cloud_log_controller->CreateWritableFile(
            fname, file_opts, dbg));
    if (!f || !f->status().ok()) {
      std::string msg = std::string("[") + Name() + "] NewWritableFile";
      s = IOStatus::IOError(msg, fname.c_str());
      Log(InfoLogLevel::ERROR_LEVEL, info_log_, "%s src %s %s", msg.c_str(),
          fname.c_str(), s.ToString().c_str());
      return s;
    }
    result->reset(f.release());
  } else {
    s = base_fs_->NewWritableFile(fname, file_opts, result, dbg);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] NewWritableFile src %s %s",
      Name(), fname.c_str(), s.ToString().c_str());
  return s;
}

IOStatus CloudFileSystemImpl::ReopenWritableFile(
    const std::string& fname, const FileOptions& file_opts,
    std::unique_ptr<FSWritableFile>* result, IODebugContext* dbg) {
  // This is not accurately correct because there is no wasy way to open
  // an provider file in append mode. We still need to support this because
  // rocksdb's ExternalSstFileIngestionJob invokes this api to reopen
  // a pre-created file to flush/sync it.
  return base_fs_->ReopenWritableFile(fname, file_opts, result, dbg);
}

//
// Check if the specified filename exists.
//
IOStatus CloudFileSystemImpl::FileExists(const std::string& logical_fname,
                                         const IOOptions& io_opts,
                                         IODebugContext* dbg) {
  IOStatus st;

  auto fname = RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       manifest = (file_type == RocksDBFileType::kManifestFile),
       identity = (file_type == RocksDBFileType::kIdentityFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  if (sstfile || manifest || identity) {
    // We read first from local storage and then from cloud storage.
    st = base_fs_->FileExists(fname, io_opts, dbg);
    if (st.IsNotFound()) {
      st = ExistsCloudObject(fname);
    }
  } else if (logfile && !cloud_fs_options.keep_local_log_files) {
    // read from controller
    st = cloud_fs_options.cloud_log_controller->FileExists(fname);
  } else {
    st = base_fs_->FileExists(fname, io_opts, dbg);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] FileExists path '%s' %s",
      Name(), fname.c_str(), st.ToString().c_str());
  return st;
}

IOStatus CloudFileSystemImpl::GetChildren(const std::string& path,
                                          const IOOptions& io_opts,
                                          std::vector<std::string>* result,
                                          IODebugContext* dbg) {
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] GetChildren path '%s' ",
      Name(), path.c_str());
  result->clear();

  IOStatus st;
  if (!cloud_fs_options.skip_cloud_files_in_getchildren) {
    // Fetch the list of children from the cloud
    st = ListCloudObjects(path, result);
    if (!st.ok()) {
      return st;
    }
  }

  // fetch all files that exist in the local posix directory
  std::vector<std::string> local_files;
  st = base_fs_->GetChildren(path, io_opts, &local_files, dbg);
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
  return IOStatus::OK();
}

IOStatus CloudFileSystemImpl::GetFileSize(const std::string& logical_fname,
                                          const IOOptions& io_opts,
                                          uint64_t* size, IODebugContext* dbg) {
  *size = 0L;

  auto fname = RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  IOStatus st;
  if (sstfile) {
    if (base_fs_->FileExists(fname, io_opts, dbg).ok()) {
      st = base_fs_->GetFileSize(fname, io_opts, size, dbg);
    } else {
      st = GetCloudObjectSize(fname, size);
    }
  } else if (logfile && !cloud_fs_options.keep_local_log_files) {
    st = cloud_fs_options.cloud_log_controller->GetFileSize(fname, size);
  } else {
    st = base_fs_->GetFileSize(fname, io_opts, size, dbg);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[%s] GetFileSize src '%s' %s %" PRIu64, Name(), fname.c_str(),
      st.ToString().c_str(), *size);
  return st;
}

IOStatus CloudFileSystemImpl::GetFileModificationTime(
    const std::string& logical_fname, const IOOptions& io_opts, uint64_t* time,
    IODebugContext* dbg) {
  *time = 0;

  auto fname = RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  IOStatus st;
  if (sstfile) {
    if (base_fs_->FileExists(fname, io_opts, dbg).ok()) {
      st = base_fs_->GetFileModificationTime(fname, io_opts, time, dbg);
    } else {
      st = GetCloudObjectModificationTime(fname, time);
    }
  } else if (logfile && !cloud_fs_options.keep_local_log_files) {
    st = cloud_fs_options.cloud_log_controller->GetFileModificationTime(fname,
                                                                        time);
  } else {
    st = base_fs_->GetFileModificationTime(fname, io_opts, time, dbg);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[%s] GetFileModificationTime src '%s' %s", Name(), fname.c_str(),
      st.ToString().c_str());
  return st;
}

// The rename may not be atomic. Some cloud vendords do not support renaming
// natively. Copy file to a new object and then delete original object.
IOStatus CloudFileSystemImpl::RenameFile(const std::string& logical_src,
                                         const std::string& logical_target,
                                         const IOOptions& io_opts,
                                         IODebugContext* dbg) {
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
    return IOStatus::NotSupported(Slice(src), Slice(target));
  } else if (logfile) {
    // Rename should never be called on log files as well
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[%s] RenameFile source logfile %s %s is not supported", Name(),
        src.c_str(), target.c_str());
    assert(0);
    return IOStatus::NotSupported(Slice(src), Slice(target));
  } else if (manifest) {
    // Rename should never be called on manifest files as well
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[%s] RenameFile source manifest %s %s is not supported", Name(),
        src.c_str(), target.c_str());
    assert(0);
    return IOStatus::NotSupported(Slice(src), Slice(target));

  } else if (!identity || !HasDestBucket()) {
    return base_fs_->RenameFile(src, target, io_opts, dbg);
  }
  // Only ID file should come here
  assert(identity);
  assert(HasDestBucket());
  assert(basename(target) == "IDENTITY");

  // Save Identity to Cloud
  auto st = SaveIdentityToCloud(src, destname(target));

  // Do the rename on local filesystem too
  if (st.ok()) {
    st = base_fs_->RenameFile(src, target, io_opts, dbg);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[%s] RenameFile src %s target %s: %s", Name(), src.c_str(),
      target.c_str(), st.ToString().c_str());
  return st;
}

IOStatus CloudFileSystemImpl::LinkFile(const std::string& src,
                                       const std::string& target,
                                       const IOOptions& io_opts,
                                       IODebugContext* dbg) {
  // We only know how to link file if both src and dest buckets are empty
  if (HasDestBucket() || HasSrcBucket()) {
    return IOStatus::NotSupported();
  }
  auto src_remapped = RemapFilename(src);
  auto target_remapped = RemapFilename(target);
  return base_fs_->LinkFile(src_remapped, target_remapped, io_opts, dbg);
}

namespace {

class CloudDirectory : public FSDirectory {
 public:
  CloudDirectory(CloudFileSystem* fs, const std::string& name,
                 const IOOptions& io_opts, IODebugContext* dbg)
      : cloud_fs_(fs), name_(name) {
    status_ = cloud_fs_->GetBaseFileSystem()->NewDirectory(name, io_opts,
                                                           &posix_dir_, dbg);
  }

  IOStatus Fsync(const IOOptions& io_opts, IODebugContext* dbg) override {
    if (!status_.ok()) {
      return status_;
    }
    return posix_dir_->Fsync(io_opts, dbg);
  }

  IOStatus status() const { return status_; }

 private:
  CloudFileSystem* cloud_fs_;
  std::string name_;
  IOStatus status_;
  std::unique_ptr<FSDirectory> posix_dir_;
};

}  // namespace

//  Returns success only if the directory-bucket exists in the
//  StorageProvider and the posixEnv local directory exists as well.
IOStatus CloudFileSystemImpl::NewDirectory(const std::string& name,
                                           const IOOptions& io_opts,
                                           std::unique_ptr<FSDirectory>* result,
                                           IODebugContext* dbg) {
  result->reset(nullptr);

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] NewDirectory name '%s'",
      Name(), name.c_str());

  // create new object.
  auto d = std::make_unique<CloudDirectory>(this, name, io_opts, dbg);

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
  return IOStatus::OK();
}

// Cloud storage providers have no concepts of directories,
// so we just have to forward the request to the base_env_
IOStatus CloudFileSystemImpl::CreateDir(const std::string& dirname,
                                        const IOOptions& io_opts,
                                        IODebugContext* dbg) {
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] CreateDir dir '%s'", Name(),
      dirname.c_str());

  // create local dir
  auto st = base_fs_->CreateDir(dirname, io_opts, dbg);

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] CreateDir dir %s %s", Name(),
      dirname.c_str(), st.ToString().c_str());
  return st;
};

// Cloud storage providers have no concepts of directories,
// so we just have to forward the request to the base_env_
IOStatus CloudFileSystemImpl::CreateDirIfMissing(const std::string& dirname,
                                                 const IOOptions& io_opts,
                                                 IODebugContext* dbg) {
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] CreateDirIfMissing dir '%s'",
      Name(), dirname.c_str());

  // create directory in base_env_
  auto st = base_fs_->CreateDirIfMissing(dirname, io_opts, dbg);

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[%s] CreateDirIfMissing created dir %s %s", Name(), dirname.c_str(),
      st.ToString().c_str());
  return st;
};

// Cloud storage providers have no concepts of directories,
// so we just have to forward the request to the base_env_
IOStatus CloudFileSystemImpl::DeleteDir(const std::string& dirname,
                                        const IOOptions& io_opts,
                                        IODebugContext* dbg) {
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] DeleteDir src '%s'", Name(),
      dirname.c_str());
  auto st = base_fs_->DeleteDir(dirname, io_opts, dbg);
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] DeleteDir dir %s %s", Name(),
      dirname.c_str(), st.ToString().c_str());
  return st;
};

IOStatus CloudFileSystemImpl::DeleteFile(const std::string& logical_fname,
                                         const IOOptions& io_opts,
                                         IODebugContext* dbg) {
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
    return IOStatus::OK();
  }

  IOStatus st;
  // Delete from destination bucket and local dir
  if (sstfile || manifest || identity) {
    if (HasDestBucket()) {
      // add the remote file deletion to the queue
      st = DeleteCloudFileFromDest(basename(fname));
    }
    // delete from local, too. Ignore the result, though. The file might not be
    // there locally.
    base_fs_->DeleteFile(fname, io_opts, dbg);
  } else if (logfile && !cloud_fs_options.keep_local_log_files) {
    // read from Log Controller
    st = status_to_io_status(
        Status(cloud_fs_options.cloud_log_controller->status()));
    if (st.ok()) {
      // Log a Delete record to controller stream
      std::unique_ptr<CloudLogWritableFile> f(
          cloud_fs_options.cloud_log_controller->CreateWritableFile(
              fname, FileOptions(), nullptr /*dbg*/));
      if (!f || !f->status().ok()) {
        std::string msg =
            "[" + std::string(cloud_fs_options.cloud_log_controller->Name()) +
            "] DeleteFile";
        st = IOStatus::IOError(msg, fname.c_str());
      } else {
        st = f->LogDelete();
      }
    }
  } else {
    st = base_fs_->DeleteFile(fname, io_opts, dbg);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] DeleteFile file %s %s",
      Name(), fname.c_str(), st.ToString().c_str());
  return st;
}

IOStatus CloudFileSystemImpl::CopyLocalFileToDest(
    const std::string& local_name, const std::string& dest_name) {
  if (cloud_file_deletion_scheduler_) {
    // Remove file from deletion queue
    cloud_file_deletion_scheduler_->UnscheduleFileDeletion(
        basename(local_name));
  }
  return GetStorageProvider()->PutCloudObject(local_name, GetDestBucketName(),
                                              dest_name);
}

IOStatus CloudFileSystemImpl::DeleteCloudFileFromDest(
    const std::string& fname) {
  assert(HasDestBucket());
  auto base = basename(fname);
  auto path = GetDestObjectPath() + pathsep + base;
  auto bucket = GetDestBucketName();
  if (!cloud_file_deletion_scheduler_) {
    return GetStorageProvider()->DeleteCloudObject(bucket, path);
  }
  std::weak_ptr<Logger> info_log_wp = info_log_;
  std::weak_ptr<CloudStorageProvider> storage_provider_wp =
      GetStorageProvider();
  auto file_deletion_runnable =
      [path = std::move(path), bucket = std::move(bucket),
       info_log_wp = std::move(info_log_wp),
       storage_provider_wp = std::move(storage_provider_wp)]() {
        auto storage_provider = storage_provider_wp.lock();
        auto info_log = info_log_wp.lock();
        if (!storage_provider || !info_log) {
          return;
        }
        auto st = storage_provider->DeleteCloudObject(bucket, path);
        if (!st.ok() && !st.IsNotFound()) {
          Log(InfoLogLevel::ERROR_LEVEL, info_log,
              "[CloudFileSystemImpl] DeleteFile file %s error %s", path.c_str(),
              st.ToString().c_str());
        }
      };
  return cloud_file_deletion_scheduler_->ScheduleFileDeletion(
      base, std::move(file_deletion_runnable));
}

// Copy my IDENTITY file to cloud storage. Update dbid registry.
IOStatus CloudFileSystemImpl::SaveIdentityToCloud(const std::string& localfile,
                                                  const std::string& idfile) {
  assert(basename(idfile) == "IDENTITY");

  // Read id into string
  std::string dbid;
  auto st = ReadFileToString(base_fs_.get(), localfile, &dbid);
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

void CloudFileSystemImpl::StopPurger() {
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

IOStatus CloudFileSystemImpl::LoadLocalCloudManifest(
    const std::string& dbname) {
  return LoadLocalCloudManifest(dbname, cloud_fs_options.cookie_on_open);
}

IOStatus CloudFileSystemImpl::LoadLocalCloudManifest(
    const std::string& dbname, const std::string& cookie) {
  if (cloud_manifest_) {
    cloud_manifest_.reset();
  }
  return CloudFileSystemEnv::LoadCloudManifest(dbname, GetBaseFileSystem(),
                                               cookie, &cloud_manifest_);
}

std::string RemapFilenameWithCloudManifest(const std::string& logical_path,
                                           CloudManifest* cloud_manifest) {
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
  std::string epoch;
  switch (type) {
    case kTableFile:
      // We should not be accessing sst files before CLOUDMANIFEST is loaded
      assert(cloud_manifest);
      epoch = cloud_manifest->GetEpoch(fileNumber);
      break;
    case kDescriptorFile:
      // We should not be accessing MANIFEST files before CLOUDMANIFEST is
      // loaded
      // Even though logical file might say MANIFEST-000001, we cut the number
      // suffix and store MANIFEST-[epoch] in the cloud and locally.
      file_name = "MANIFEST";
      assert(cloud_manifest);
      epoch = cloud_manifest->GetCurrentEpoch();
      break;
    default:
      return logical_path;
  };
  auto dir = dirname(logical_path);
  return dir + (dir.empty() ? "" : "/") + file_name +
         (epoch.empty() ? "" : ("-" + epoch));
}

std::string CloudFileSystemImpl::RemapFilename(
    const std::string& logical_path) const {
  if (UNLIKELY(test_disable_cloud_manifest_)) {
    return logical_path;
  }
  return RemapFilenameWithCloudManifest(logical_path, cloud_manifest_.get());
}

IOStatus CloudFileSystemImpl::DeleteCloudInvisibleFiles(
    const std::vector<std::string>& active_cookies) {
  assert(HasDestBucket());
  std::vector<std::string> pathnames;
  auto s = GetStorageProvider()->ListCloudObjects(
      GetDestBucketName(), GetDestObjectPath(), &pathnames);
  if (!s.ok()) {
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
        "Files in cloud are not scheduled to be deleted since listing cloud "
        "object fails: %s",
        s.ToString().c_str());
    return s;
  }

  for (auto& fname : pathnames) {
    if (IsFileInvisible(active_cookies, fname)) {
      // Ignore returned status on purpose.
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "DeleteCloudInvisibleFiles deleting %s from destination bucket",
          fname.c_str());
      DeleteCloudFileFromDest(fname);
    }
  }
  return s;
}

IOStatus CloudFileSystemImpl::DeleteLocalInvisibleFiles(
    const std::string& dbname, const std::vector<std::string>& active_cookies) {
  std::vector<std::string> children;
  const IOOptions io_opts;
  IODebugContext* dbg = nullptr;
  auto s = GetBaseFileSystem()->GetChildren(dbname, io_opts, &children, dbg);
  TEST_SYNC_POINT_CALLBACK(
      "CloudFileSystemImpl::DeleteLocalInvisibleFiles:AfterListLocalFiles", &s);
  if (!s.ok()) {
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
        "Local files are not deleted since listing local files fails: %s",
        s.ToString().c_str());
    return s;
  }
  for (auto& fname : children) {
    if (IsFileInvisible(active_cookies, fname)) {
      // Ignore returned status on purpose.
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "DeleteLocalInvisibleFiles deleting file %s from local dir",
          fname.c_str());
      GetBaseFileSystem()->DeleteFile(dbname + "/" + fname, io_opts, dbg);
    }
  }
  return s;
}

bool CloudFileSystemImpl::IsFileInvisible(
    const std::vector<std::string>& active_cookies,
    const std::string& fname) const {
  if (IsCloudManifestFile(fname)) {
    auto fname_cookie = GetCookie(fname);

    // empty cookie CM file is never deleted
    // TODO(wei): we can remove this assumption once L/F is fully rolled out
    if (fname_cookie.empty()) {
      return false;
    }

    bool is_active = false;
    for (auto& c : active_cookies) {
      if (c == fname_cookie) {
        is_active = true;
        break;
      }
    }

    return !is_active;
  } else {
    auto noepoch = RemoveEpoch(fname);
    if ((IsSstFile(noepoch) || IsManifestFile(noepoch)) &&
        (RemapFilename(noepoch) != fname)) {
      return true;
    }
  }
  return false;
}

IOStatus CloudFileSystemImpl::CreateNewIdentityFile(
    const std::string& dbid, const std::string& local_name) {
  const FileOptions file_opts;
  const IOOptions io_opts;
  IODebugContext* dbg = nullptr;
  auto tmp_identity_path = local_name + "/IDENTITY.tmp";
  const auto& fs = GetBaseFileSystem();
  IOStatus st;
  {
    std::unique_ptr<FSWritableFile> destfile;
    st = fs->NewWritableFile(tmp_identity_path, file_opts, &destfile, dbg);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[cloud_fs_impl] Unable to create local IDENTITY file to %s %s",
          tmp_identity_path.c_str(), st.ToString().c_str());
      return st;
    }
    st = destfile->Append(Slice(dbid), io_opts, dbg);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[cloud_fs_impl] Unable to write new dbid to local IDENTITY file "
          "%s %s",
          tmp_identity_path.c_str(), st.ToString().c_str());
      return st;
    }
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[cloud_fs_impl] Written new dbid %s to %s %s", dbid.c_str(),
      tmp_identity_path.c_str(), st.ToString().c_str());

  // Rename ID file on local filesystem and upload it to dest bucket too
  st = RenameFile(tmp_identity_path, local_name + "/IDENTITY", io_opts, dbg);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[cloud_fs_impl] Unable to rename newly created IDENTITY.tmp "
        " to IDENTITY. %s",
        st.ToString().c_str());
    return st;
  }
  return st;
}

IOStatus CloudFileSystemImpl::WriteCloudManifest(
    CloudManifest* manifest, const std::string& fname) const {
  const auto& local_fs = GetBaseFileSystem();
  // Write to tmp file and atomically rename later. This helps if we crash
  // mid-write :)
  auto tmp_fname = fname + ".tmp";
  std::unique_ptr<WritableFileWriter> writer;
  auto s = WritableFileWriter::Create(local_fs, tmp_fname, FileOptions(),
                                      &writer, nullptr);
  if (s.ok()) {
    s = manifest->WriteToLog(std::move(writer));
  }
  if (s.ok()) {
    s = local_fs->RenameFile(tmp_fname, fname, IOOptions(), nullptr /*dbg*/);
  }
  return s;
}

// we map a longer string given by env->GenerateUniqueId() into 16-byte string
std::string CloudFileSystemImpl::GenerateNewEpochId() {
  auto uniqueId = Env::Default()->GenerateUniqueId();
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
Status CloudFileSystemImpl::CheckOption(const FileOptions& file_opts) {
  // Cannot mmap files that reside on cloud storage, unless the file is also
  // local
  if (file_opts.use_mmap_reads && !cloud_fs_options.keep_local_sst_files) {
    std::string msg = "Mmap only if keep_local_sst_files is set";
    return Status::InvalidArgument(msg);
  }
  return Status::OK();
}

//
// prepends the configured src object path name
//
std::string CloudFileSystemImpl::srcname(const std::string& localname) {
  assert(cloud_fs_options.src_bucket.IsValid());
  return cloud_fs_options.src_bucket.GetObjectPath() + "/" +
         basename(localname);
}

//
// prepends the configured dest object path name
//
std::string CloudFileSystemImpl::destname(const std::string& localname) {
  assert(cloud_fs_options.dest_bucket.IsValid());
  return cloud_fs_options.dest_bucket.GetObjectPath() + "/" +
         basename(localname);
}

//
// Shall we re-initialize the local dir?
//
IOStatus CloudFileSystemImpl::NeedsReinitialization(
    const std::string& local_dir, bool* do_reinit) {
  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "[cloud_fs_impl] NeedsReinitialization: "
      "checking local dir %s src bucket %s src path %s "
      "dest bucket %s dest path %s",
      local_dir.c_str(), GetSrcBucketName().c_str(), GetSrcObjectPath().c_str(),
      GetDestBucketName().c_str(), GetDestObjectPath().c_str());

  // If no buckets are specified, then we cannot reinit anyways
  if (!HasSrcBucket() && !HasDestBucket()) {
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_fs_impl] NeedsReinitialization: "
        "Both src and dest buckets are empty");
    *do_reinit = false;
    return IOStatus::OK();
  }

  // assume that directory does needs reinitialization
  *do_reinit = true;

  // get local env
  const auto& base_fs = GetBaseFileSystem();
  const IOOptions io_opts;
  IODebugContext* dbg = nullptr;

  // Check if local directory exists
  auto st = base_fs->FileExists(local_dir, io_opts, dbg);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[cloud_fs_impl] NeedsReinitialization: "
        "failed to access local dir %s: %s",
        local_dir.c_str(), st.ToString().c_str());
    // If the directory is not found, we should create it. In case of an other
    // IO error, we need to fail
    return st.IsNotFound() ? IOStatus::OK() : st;
  }

  // Check if CURRENT file exists
  st = base_fs->FileExists(CurrentFileName(local_dir), io_opts, dbg);
  if (!st.ok()) {
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_fs_impl] NeedsReinitialization: "
        "failed to find CURRENT file %s: %s",
        CurrentFileName(local_dir).c_str(), st.ToString().c_str());
    return st.IsNotFound() ? IOStatus::OK() : st;
  }

  // Check if CLOUDMANIFEST file exists
  st = base_fs->FileExists(CloudManifestFile(local_dir), io_opts, dbg);
  if (!st.ok()) {
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_fs_impl] NeedsReinitialization: "
        "failed to find CLOUDMANIFEST file %s: %s",
        CloudManifestFile(local_dir).c_str(), st.ToString().c_str());
    return st.IsNotFound() ? IOStatus::OK() : st;
  }

  if (cloud_fs_options.skip_dbid_verification) {
    *do_reinit = false;
    return IOStatus::OK();
  }

  // Read DBID file from local dir
  std::string local_dbid;
  st =
      ReadFileToString(base_fs.get(), IdentityFileName(local_dir), &local_dbid);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[cloud_fs_impl] NeedsReinitialization: "
        "local dir %s unable to read local dbid: %s",
        local_dir.c_str(), st.ToString().c_str());
    return st.IsNotFound() ? IOStatus::OK() : st;
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
          "[cloud_fs_impl] NeedsReinitialization: "
          "Local dbid is %s but unable to find src dbid",
          local_dbid.c_str());
      return st;
    }
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_fs_impl] NeedsReinitialization: "
        "Local dbid is %s and src object path in registry is '%s'",
        local_dbid.c_str(), src_object_path.c_str());

    if (st.ok()) {
      src_object_path = rtrim_if(trim(src_object_path), '/');
    }
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_fs_impl] NeedsReinitialization: "
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
          "[cloud_fs_impl] NeedsReinitialization: "
          "Local dbid is %s but unable to find dest dbid",
          local_dbid.c_str());
      return st;
    }
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_fs_impl] NeedsReinitialization: "
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
            "[cloud_fs_impl] NeedsReinitialization: "
            "Local dbid %s dest path specified in env is %s "
            " but dest path in registry is %s",
            local_dbid.c_str(), GetDestObjectPath().c_str(),
            dest_object_path.c_str());
        return IOStatus::InvalidArgument(
            "[cloud_fs_impl] NeedsReinitialization: bad dest path");
      }
    }
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_fs_impl] NeedsReinitialization: "
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
        "[cloud_fs_impl] NeedsReinitialization: "
        "Unable to extract dbid from cloud paths %s",
        st.ToString().c_str());
    return st;
  }

  // If we found a src_dbid, then it should be a prefix of local_dbid
  if (!src_dbid.empty()) {
    size_t pos = local_dbid.find(src_dbid);
    if (pos == std::string::npos) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[cloud_fs_impl] NeedsReinitialization: "
          "dbid %s in src bucket %s is not a prefix of local dbid %s",
          src_dbid.c_str(), src_bucket.c_str(), local_dbid.c_str());
      return IOStatus::OK();
    }
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_fs_impl] NeedsReinitialization: "
        "dbid %s in src bucket %s is a prefix of local dbid %s",
        src_dbid.c_str(), src_bucket.c_str(), local_dbid.c_str());

    // If the local dbid is an exact match with the src dbid, then ensure
    // that we cannot run in a 'clone' mode.
    if (local_dbid == src_dbid) {
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_fs_impl] NeedsReinitialization: "
          "dbid %s in src bucket %s is same as local dbid",
          src_dbid.c_str(), src_bucket.c_str());

      if (HasDestBucket() && !SrcMatchesDest()) {
        Log(InfoLogLevel::ERROR_LEVEL, info_log_,
            "[cloud_fs_impl] NeedsReinitialization: "
            "local dbid %s in same as src dbid but clone mode specified",
            local_dbid.c_str());
        return IOStatus::OK();
      }
    }
  }

  // If we found a dest_dbid, then it should be a prefix of local_dbid
  if (!dest_dbid.empty()) {
    size_t pos = local_dbid.find(dest_dbid);
    if (pos == std::string::npos) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[cloud_fs_impl] NeedsReinitialization: "
          "dbid %s in dest bucket %s is not a prefix of local dbid %s",
          dest_dbid.c_str(), dest_bucket.c_str(), local_dbid.c_str());
      return IOStatus::OK();
    }
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_fs_impl] NeedsReinitialization: "
        "dbid %s in dest bucket %s is a prefix of local dbid %s",
        dest_dbid.c_str(), dest_bucket.c_str(), local_dbid.c_str());

    // If the local dbid is an exact match with the destination dbid, then
    // ensure that we are run not in a 'clone' mode.
    if (local_dbid == dest_dbid) {
      Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
          "[cloud_fs_impl] NeedsReinitialization: "
          "dbid %s in dest bucket %s is same as local dbid",
          dest_dbid.c_str(), dest_bucket.c_str());

      if (HasSrcBucket() && !SrcMatchesDest()) {
        Log(InfoLogLevel::ERROR_LEVEL, info_log_,
            "[cloud_fs_impl] NeedsReinitialization: "
            "local dbid %s in same as dest dbid but clone mode specified",
            local_dbid.c_str());
        return IOStatus::OK();
      }
    }
  }

  // We found a local dbid but we did not find this dbid in bucket registry.
  // This is an ephemeral clone.
  if (src_object_path.empty() && dest_object_path.empty()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[cloud_fs_impl] NeedsReinitialization: "
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
    auto load_status = CloudFileSystemEnv::LoadCloudManifest(
        local_dir, base_fs, cloud_fs_options.cookie_on_open, &cloud_manifest);
    if (load_status.ok()) {
      std::string current_epoch = cloud_manifest->GetCurrentEpoch();
      auto local_manifest_exists = base_fs->FileExists(
          ManifestFileWithEpoch(local_dir, current_epoch), io_opts, dbg);
      if (!local_manifest_exists.ok()) {
        Log(InfoLogLevel::WARN_LEVEL, info_log_,
            "[cloud_fs_impl] NeedsReinitialization: CLOUDMANIFEST exists "
            "locally, but no local MANIFEST is compatible");
        return IOStatus::OK();
      }
    }

    // Resync all files from cloud.
    // If the  resycn failed, then return success to indicate that
    // the local directory needs to be completely removed and recreated.
    st = ResyncDir(local_dir);
    if (!st.ok()) {
      return IOStatus::OK();
    }
  }
  // ID's in the local dir are valid.

  // The DBID of the local dir is compatible with the src and dest buckets.
  // We do not need any re-initialization of local dir.
  *do_reinit = false;
  return IOStatus::OK();
}

//
// Check and fix all files in local dir and cloud dir.
// This should be called only for ephemeral clones.
// Returns Status::OK if we want to keep all local data,
// otherwise all local data will be erased.
//
IOStatus CloudFileSystemImpl::ResyncDir(const std::string& local_dir) {
  if (HasDestBucket()) {
    auto& src_bucket = GetSrcBucketName();
    auto& dest_bucket = GetDestBucketName();
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[cloud_fs_impl] ResyncDir: "
        "not an ephemeral clone local dir %s "
        "src bucket %s dest bucket %s",
        local_dir.c_str(), src_bucket.c_str(), dest_bucket.c_str());
    return IOStatus::InvalidArgument();
  }
  return IOStatus::OK();
}

//
// Extract the src dbid and the dest dbid from the cloud paths
//
IOStatus CloudFileSystemImpl::GetCloudDbid(const std::string& local_dir,
                                           std::string* src_dbid,
                                           std::string* dest_dbid) {
  const auto& base_fs = GetBaseFileSystem();

  // use a tmp file in local dir
  std::string tmpfile = IdentityFileName(local_dir) + ".cloud.tmp";
  const IOOptions io_opts;
  IODebugContext* dbg = nullptr;

  // Delete any old data remaining there
  base_fs->DeleteFile(tmpfile, io_opts, dbg);

  // Read dbid from src bucket if it exists
  if (HasSrcBucket()) {
    auto st = GetStorageProvider()->GetCloudObject(
        GetSrcBucketName(), GetSrcObjectPath() + "/IDENTITY", tmpfile);
    if (!st.ok() && !st.IsNotFound()) {
      return st;
    }
    if (st.ok()) {
      std::string sid;
      st = ReadFileToString(base_fs.get(), tmpfile, &sid);
      if (st.ok()) {
        src_dbid->assign(rtrim_if(trim(sid), '\n'));
        base_fs->DeleteFile(tmpfile, io_opts, dbg);
      } else {
        Log(InfoLogLevel::ERROR_LEVEL, info_log_,
            "[cloud_fs_impl] GetCloudDbid: "
            "local dir %s unable to read src dbid: %s",
            local_dir.c_str(), st.ToString().c_str());
        return st;
      }
    }
  }

  // Read dbid from dest bucket if it exists
  if (HasDestBucket()) {
    auto st = GetStorageProvider()->GetCloudObject(
        GetDestBucketName(), GetDestObjectPath() + "/IDENTITY", tmpfile);
    if (!st.ok() && !st.IsNotFound()) {
      return st;
    }
    if (st.ok()) {
      std::string sid;
      st = ReadFileToString(base_fs.get(), tmpfile, &sid);
      if (st.ok()) {
        dest_dbid->assign(rtrim_if(trim(sid), '\n'));
        base_fs->DeleteFile(tmpfile, io_opts, dbg);
      } else {
        Log(InfoLogLevel::ERROR_LEVEL, info_log_,
            "[cloud_fs_impl] GetCloudDbid: "
            "local dir %s unable to read dest dbid: %s",
            local_dir.c_str(), st.ToString().c_str());
        return st;
      }
    }
  }
  return IOStatus::OK();
}

IOStatus CloudFileSystemImpl::MigrateFromPureRocksDB(
    const std::string& local_dbname) {
  std::unique_ptr<CloudManifest> manifest;
  CloudManifest::CreateForEmptyDatabase("", &manifest);
  auto st = WriteCloudManifest(manifest.get(), CloudManifestFile(local_dbname));
  if (!st.ok()) {
    return st;
  }
  st = LoadLocalCloudManifest(local_dbname);
  if (!st.ok()) {
    return st;
  }

  std::string manifest_filename;
  const auto& local_fs = GetBaseFileSystem();
  const IOOptions io_opts;
  IODebugContext* dbg = nullptr;
  st = local_fs->FileExists(CurrentFileName(local_dbname), io_opts, dbg);
  if (st.IsNotFound()) {
    // No need to migrate
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_fs_impl] MigrateFromPureRocksDB: No need to migrate %s",
        CurrentFileName(local_dbname).c_str());
    return IOStatus::OK();
  }
  if (!st.ok()) {
    return st;
  }
  st = ReadFileToString(local_fs.get(), CurrentFileName(local_dbname),
                        &manifest_filename);
  if (!st.ok()) {
    return st;
  }
  // Note: This rename is important for migration. If we are just starting on
  // an old database, our local MANIFEST filename will be something like
  // MANIFEST-00001 instead of MANIFEST. If we don't do the rename we'll
  // download MANIFEST file from the cloud, which might not be what we want do
  // to (especially for databases which don't have a destination bucket
  // specified).
  manifest_filename = local_dbname + "/" + rtrim_if(manifest_filename, '\n');
  if (local_fs->FileExists(manifest_filename, io_opts, dbg).IsNotFound()) {
    // manifest doesn't exist, shrug
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_fs_impl] MigrateFromPureRocksDB: Manifest %s does not exist",
        manifest_filename.c_str());
    return IOStatus::OK();
  }
  st = local_fs->RenameFile(manifest_filename, local_dbname + "/MANIFEST",
                            io_opts, dbg);
  if (st.ok() && cloud_fs_options.roll_cloud_manifest_on_open) {
    st = RollNewEpoch(local_dbname);
  }

  return st;
}

IOStatus CloudFileSystemImpl::PreloadCloudManifest(
    const std::string& local_dbname) {
  const auto& local_fs = GetBaseFileSystem();
  local_fs->CreateDirIfMissing(local_dbname, IOOptions(), nullptr /*dbg*/);
  // Init cloud manifest
  auto st = FetchCloudManifest(local_dbname);
  if (st.ok()) {
    // Inits CloudFileSystemImpl::cloud_manifest_, which will enable us to
    // read files from the cloud
    st = LoadLocalCloudManifest(local_dbname);
  }
  return st;
}

IOStatus CloudFileSystemImpl::LoadCloudManifest(const std::string& local_dbname,
                                                bool read_only) {
  // Init cloud manifest
  auto st = FetchCloudManifest(local_dbname);
  if (st.ok()) {
    // Inits CloudFileSystemImpl::cloud_manifest_, which will enable us to
    // read files from the cloud
    st = LoadLocalCloudManifest(local_dbname);
  }

  if (st.ok() && cloud_fs_options.resync_on_open) {
    auto epoch = cloud_manifest_->GetCurrentEpoch();
    st = FetchManifest(local_dbname, epoch);
    if (st.IsNotFound()) {
      // We always upload MANIFEST first before uploading CLOUDMANIFEST. So it's
      // not expected to have CLOUDMANIFEST in s3 which points to MANIFEST file
      // that doesn't exist.
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[CloudFileSystemImpl] CLOUDMANIFEST-%s points to MANIFEST-%s that "
          "doesn't "
          "exist in s3",
          cloud_fs_options.cookie_on_open.c_str(), epoch.c_str());
      st = IOStatus::Corruption(
          "CLOUDMANIFEST points to MANIFEST that doesn't exist in s3");
    }
  }

  // Do the cleanup, but don't fail if the cleanup fails.
  // We only cleanup files which don't belong to cookie_on_open. Also, we do it
  // before rolling the epoch, so that newly generated CM/M files won't be
  // cleaned up.
  if (st.ok() && !read_only) {
    std::vector<std::string> active_cookies{
        cloud_fs_options.cookie_on_open, cloud_fs_options.new_cookie_on_open};
    st = DeleteLocalInvisibleFiles(local_dbname, active_cookies);
    if (st.ok() && cloud_fs_options.delete_cloud_invisible_files_on_open &&
        HasDestBucket()) {
      st = DeleteCloudInvisibleFiles(active_cookies);
    }
    if (!st.ok()) {
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "Failed to delete invisible files: %s", st.ToString().c_str());
      // Ignore the fail
      st = IOStatus::OK();
    }
  }

  if (st.ok() && cloud_fs_options.roll_cloud_manifest_on_open) {
    // Rolls the new epoch in CLOUDMANIFEST (only for existing databases)
    st = RollNewEpoch(local_dbname);
    if (st.IsNotFound()) {
      st = IOStatus::Corruption(
          "CLOUDMANIFEST points to MANIFEST that doesn't exist");
    }
  }
  if (!st.ok()) {
    cloud_manifest_.reset();
    return st;
  }

  const IOOptions io_opts;
  IODebugContext* dbg = nullptr;
  if (FileExists(CurrentFileName(local_dbname), io_opts, dbg).IsNotFound()) {
    // Create dummy CURRENT file to point to the dummy manifest (cloud env
    // will remap the filename appropriately, this is just to fool the
    // underyling RocksDB)
    st = SetCurrentFile(WriteOptions(), GetBaseFileSystem().get(), local_dbname,
                        1 /* descriptor_number */,
                        nullptr /* dir_contains_current_file */);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "Unable to write local CURRENT file to %s %s",
          CurrentFileName(local_dbname).c_str(), st.ToString().c_str());
      return st;
    }
  }

  return st;
}

//
// Create appropriate files in the clone dir
//
IOStatus CloudFileSystemImpl::SanitizeLocalDirectory(
    const DBOptions& options, const std::string& local_name, bool read_only) {
  const auto& local_fs = GetBaseFileSystem();
  const IOOptions io_opts;
  IODebugContext* dbg = nullptr;
  if (!read_only) {
    local_fs->CreateDirIfMissing(local_name, io_opts, dbg);
  }

  // Shall we reinitialize the clone dir?
  bool do_reinit = true;
  auto st = NeedsReinitialization(local_name, &do_reinit);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[cloud_fs_impl] SanitizeDirectory error inspecting dir %s %s",
        local_name.c_str(), st.ToString().c_str());
    return st;
  }

  // If there is no destination bucket, then we prefer to suck in all sst files
  // from source bucket at db startup time. We do this by setting max_open_files
  // = -1
  if (!HasDestBucket()) {
    if (options.max_open_files != -1) {
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_fs_impl] SanitizeDirectory info.  "
          " No destination bucket specified and options.max_open_files != -1 "
          " so sst files from src bucket %s are not copied into local dir %s "
          "at startup",
          GetSrcObjectPath().c_str(), local_name.c_str());
    }
    if (!cloud_fs_options.keep_local_sst_files && !read_only) {
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_fs_impl] SanitizeDirectory info.  "
          " No destination bucket specified and options.keep_local_sst_files "
          "is false. Existing sst files from src bucket %s will not be "
          " downloaded into local dir but newly created sst files will "
          " remain in local dir %s",
          GetSrcObjectPath().c_str(), local_name.c_str());
    }
  }

  if (!do_reinit) {
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_fs_impl] SanitizeDirectory local directory %s is good",
        local_name.c_str());
    return IOStatus::OK();
  }
  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "[cloud_fs_impl] SanitizeDirectory local directory %s cleanup needed",
      local_name.c_str());

  // Delete all local files
  std::vector<Env::FileAttributes> result;
  st = local_fs->GetChildrenFileAttributes(local_name, io_opts, &result, dbg);
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
    bool is_dir;
    st = local_fs->IsDirectory(pathname, io_opts, &is_dir, dbg);
    if (!st.ok()) {
      Log(InfoLogLevel::WARN_LEVEL, info_log_,
          "[cloud_fs_impl] IsDirectory fails for %s %s", pathname.c_str(),
          st.ToString().c_str());
      // file checking failure ignored
      continue;
    }

    // Directories are ignored
    if (is_dir) {
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_fs_impl] Not deleting: %s since it's a directory",
          pathname.c_str());
      continue;
    }

    st = local_fs->DeleteFile(pathname, io_opts, dbg);
    TEST_SYNC_POINT_CALLBACK(
        "CloudFileSystemImpl::SanitizeDirectory:AfterDeleteFile", &st);
    if (!st.ok()) {
      Log(InfoLogLevel::WARN_LEVEL, info_log_,
          "[cloud_fs_impl] DeleteFile: %s fails %s", pathname.c_str(),
          st.ToString().c_str());
      // local file cleaning up failure will be ignored
      continue;
    }
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_fs_impl] SanitizeDirectory cleaned-up: '%s'", pathname.c_str());
  }

  if (!st.ok()) {
    // ignore all the errors generated during cleaning up
    st = IOStatus::OK();
  }

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[cloud_fs_impl] SanitizeDirectory dest_equal_src = %d",
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
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_fs_impl] No valid dbs in src bucket %s src path %s "
        "or dest bucket %s dest path %s",
        GetSrcBucketName().c_str(), GetSrcObjectPath().c_str(),
        GetDestBucketName().c_str(), GetDestObjectPath().c_str());
    return IOStatus::OK();
  }

  if (got_identity_from_src && !SrcMatchesDest()) {
    // If we got dbid from src but not from dest.
    // Then we are just opening this database as a clone (for the first time).
    // Either as a true clone or as an ephemeral clone.
    // Create a new dbid for this clone.
    std::string src_dbid;
    st = ReadFileToString(local_fs.get(), IdentityFileName(local_name),
                          &src_dbid);
    if (!st.ok()) {
      return st;
    }
    src_dbid = rtrim_if(trim(src_dbid), '\n');

    std::string new_dbid = src_dbid + std::string(DBID_SEPARATOR) +
                           Env::Default()->GenerateUniqueId();

    st = CreateNewIdentityFile(new_dbid, local_name);
    if (!st.ok()) {
      return st;
    }
  }

  return IOStatus::OK();
}

IOStatus CloudFileSystemImpl::FetchCloudManifest(
    const std::string& local_dbname) {
  return FetchCloudManifest(local_dbname, cloud_fs_options.cookie_on_open);
}

IOStatus CloudFileSystemImpl::FetchCloudManifest(
    const std::string& local_dbname, const std::string& cookie) {
  std::string cloudmanifest = MakeCloudManifestFile(local_dbname, cookie);
  // TODO(wei): following check is to make sure we maintain the same behavior
  // as before. Once we double check every service has right resync_on_open set,
  // we should remove.
  bool resync_on_open = cloud_fs_options.resync_on_open;
  if (SrcMatchesDest() && !resync_on_open) {
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
        "[cloud_fs_impl] FetchCloudManifest: Src bucket matches dest bucket "
        "but resync_on_open not enabled. Force enabling it for now");
    resync_on_open = true;
  }

  // If resync_on_open is false and we have a local cloud manifest, do nothing.
  if (!resync_on_open &&
      GetBaseFileSystem()
          ->FileExists(cloudmanifest, IOOptions(), nullptr /*dbg*/)
          .ok()) {
    // nothing to do here, we have our cloud manifest
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_fs_impl] FetchCloudManifest: Nothing to do, %s exists and "
        "resync_on_open is false",
        cloudmanifest.c_str());
    return IOStatus::OK();
  }
  // first try to get cloudmanifest from dest
  if (HasDestBucket()) {
    auto st = GetStorageProvider()->GetCloudObject(
        GetDestBucketName(), MakeCloudManifestFile(GetDestObjectPath(), cookie),
        cloudmanifest);
    if (!st.ok() && !st.IsNotFound()) {
      // something went wrong, bail out
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_fs_impl] FetchCloudManifest: Failed to fetch "
          " cloud manifest %s from dest %s",
          cloudmanifest.c_str(), GetDestBucketName().c_str());
      return st;
    }
    if (st.ok()) {
      // found it!
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_fs_impl] FetchCloudManifest: Fetched"
          " cloud manifest %s from dest %s",
          cloudmanifest.c_str(), GetDestBucketName().c_str());
      return st;
    }
  }
  // we couldn't get cloud manifest from dest, need to try from src?
  if (HasSrcBucket() && !SrcMatchesDest()) {
    auto st = GetStorageProvider()->GetCloudObject(
        GetSrcBucketName(), MakeCloudManifestFile(GetSrcObjectPath(), cookie),
        cloudmanifest);
    if (!st.ok() && !st.IsNotFound()) {
      // something went wrong, bail out
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_fs_impl] FetchCloudManifest: Failed to fetch "
          " cloud manifest %s from src %s",
          cloudmanifest.c_str(), GetSrcBucketName().c_str());
      return st;
    }
    if (st.ok()) {
      // found it!
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_fs_impl] FetchCloudManifest: Fetched"
          " cloud manifest %s from src %s",
          cloudmanifest.c_str(), GetSrcBucketName().c_str());
      return st;
    }
  }
  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "[cloud_fs_impl] FetchCloudManifest: No cloud manifest");
  return IOStatus::NotFound();
}

IOStatus CloudFileSystemImpl::FetchManifest(const std::string& local_dbname,
                                            const std::string& epoch) {
  auto local_manifest_file = ManifestFileWithEpoch(local_dbname, epoch);
  if (HasDestBucket()) {
    auto st = GetStorageProvider()->GetCloudObject(
        GetDestBucketName(), ManifestFileWithEpoch(GetDestObjectPath(), epoch),
        local_manifest_file);
    if (!st.ok() && !st.IsNotFound()) {
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_fs_impl] FetchManifest: Failed to fetch manifest %s from "
          "dest %s",
          local_manifest_file.c_str(), GetDestBucketName().c_str());
    }

    if (st.ok()) {
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_fs_impl] FetchManifest: Fetched manifest %s from dest: %s",
          local_manifest_file.c_str(), GetDestBucketName().c_str());
      return st;
    }
  }

  if (HasSrcBucket() && !SrcMatchesDest()) {
    auto st = GetStorageProvider()->GetCloudObject(
        GetSrcBucketName(), ManifestFileWithEpoch(GetSrcObjectPath(), epoch),
        local_manifest_file);
    if (!st.ok() && !st.IsNotFound()) {
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_fs_impl] FetchManifest: Failed to fetch manifest %s from "
          "src %s",
          local_manifest_file.c_str(), GetSrcBucketName().c_str());
    }
    if (st.ok()) {
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_fs_impl] FetchManifest: Fetched manifest %s from "
          "src %s",
          local_manifest_file.c_str(), GetSrcBucketName().c_str());

      return st;
    }
  }

  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "[cloud_fs_impl] FetchManifest: not manifest");
  return IOStatus::NotFound();
}

IOStatus CloudFileSystemImpl::CreateCloudManifest(
    const std::string& local_dbname, const std::string& cookie) {
  // No cloud manifest, create an empty one
  std::unique_ptr<CloudManifest> manifest;
  CloudManifest::CreateForEmptyDatabase(GenerateNewEpochId(), &manifest);
  auto st = WriteCloudManifest(manifest.get(),
                               MakeCloudManifestFile(local_dbname, cookie));
  if (st.ok()) {
    st = LoadLocalCloudManifest(local_dbname, cookie);
  }
  return st;
}

IOStatus CloudFileSystemImpl::GetMaxFileNumberFromCurrentManifest(
    const std::string& local_dbname, uint64_t* max_file_number) {
  return ManifestReader::GetMaxFileNumberFromManifest(
      this, local_dbname + "/MANIFEST-000001", max_file_number);
}

// REQ: This is an existing database.
IOStatus CloudFileSystemImpl::RollNewEpoch(const std::string& local_dbname) {
  assert(cloud_fs_options.roll_cloud_manifest_on_open);
  // Find next file number. We use dummy MANIFEST filename, which should get
  // remapped into the correct MANIFEST filename through CloudManifest.
  // After this call we should also have a local file named
  // MANIFEST-<current_epoch> (unless st.IsNotFound()).
  uint64_t maxFileNumber;
  auto st = ManifestReader::GetMaxFileNumberFromManifest(
      this, local_dbname + "/MANIFEST-000001", &maxFileNumber);
  if (!st.ok()) {
    // uh oh
    return st;
  }
  // roll new epoch
  auto newEpoch = GenerateNewEpochId();
  // To make sure `RollNewEpoch` is backwards compatible, we don't change
  // the cookie when applying CM delta
  auto newCookie = cloud_fs_options.new_cookie_on_open;
  auto cloudManifestDelta = CloudManifestDelta{maxFileNumber, newEpoch};

  st = RollNewCookie(local_dbname, newCookie, cloudManifestDelta);
  if (st.ok()) {
    // Apply the delta to our in-memory state, too.
    bool updateApplied = true;
    st = ApplyCloudManifestDelta(cloudManifestDelta, &updateApplied);
    // We know for sure that <maxFileNumber, newEpoch> hasn't been applied
    // in current CLOUDMANFIEST yet since maxFileNumber >= filenumber in
    // CLOUDMANIFEST and epoch is generated randomly
    assert(updateApplied);
  }

  return st;
}

IOStatus CloudFileSystemImpl::UploadManifest(const std::string& local_dbname,
                                             const std::string& epoch) const {
  if (!HasDestBucket()) {
    return IOStatus::InvalidArgument(
        "Dest bucket has to be specified when uploading manifest files");
  }

  auto st = GetStorageProvider()->PutCloudObject(
      ManifestFileWithEpoch(local_dbname, epoch), GetDestBucketName(),
      ManifestFileWithEpoch(GetDestObjectPath(), epoch));

  TEST_SYNC_POINT_CALLBACK(
      "CloudFileSystemImpl::UploadManifest:AfterUploadManifest", &st);
  return st;
}

IOStatus CloudFileSystemImpl::UploadCloudManifest(
    const std::string& local_dbname, const std::string& cookie) const {
  if (!HasDestBucket()) {
    return IOStatus::InvalidArgument(
        "Dest bucket has to be specified when uploading CloudManifest files");
  }
  // upload the cloud manifest file corresponds to cookie (i.e.,
  // CLOUDMANIFEST-cookie)
  auto st = GetStorageProvider()->PutCloudObject(
      MakeCloudManifestFile(local_dbname, cookie), GetDestBucketName(),
      MakeCloudManifestFile(GetDestObjectPath(), cookie));
  if (!st.ok()) {
    return st;
  }

  return st;
}

IOStatus CloudFileSystemImpl::ApplyCloudManifestDelta(
    const CloudManifestDelta& delta, bool* delta_applied) {
  *delta_applied = cloud_manifest_->AddEpoch(delta.file_num, delta.epoch);
  return IOStatus::OK();
}

IOStatus CloudFileSystemImpl::RollNewCookie(
    const std::string& local_dbname, const std::string& cookie,
    const CloudManifestDelta& delta) const {
  auto newCloudManifest = cloud_manifest_->clone();
  std::string old_epoch = newCloudManifest->GetCurrentEpoch();
  if (!newCloudManifest->AddEpoch(delta.file_num, delta.epoch)) {
    return IOStatus::InvalidArgument("Delta already applied in cloud manifest");
  }

  const auto& base_fs = GetBaseFileSystem();
  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "Rolling new CLOUDMANIFEST from file number %lu, renaming MANIFEST-%s to "
      "MANIFEST-%s, new cookie: %s",
      delta.file_num, old_epoch.c_str(), delta.epoch.c_str(), cookie.c_str());
  // ManifestFileWithEpoch(local_dbname, oldEpoch) should exist locally.
  // We have to move our old manifest to the new filename.
  // However, we don't move here, we copy. If we moved and crashed immediately
  // after (before writing CLOUDMANIFEST), we'd corrupt our database. The old
  // MANIFEST file will be cleaned up in DeleteInvisibleFiles().
  auto st = CopyFile(
      base_fs.get(), ManifestFileWithEpoch(local_dbname, old_epoch),
      Temperature::kUnknown, ManifestFileWithEpoch(local_dbname, delta.epoch),
      Temperature::kUnknown, 0 /* size */, true /* use_fsync */,
      nullptr /* io_tracer */);
  if (!st.ok()) {
    return st;
  }

  // TODO(igor): Compact cloud manifest by looking at live files in the database
  // and removing epochs that don't contain any live files.

  TEST_SYNC_POINT_CALLBACK(
      "CloudFileSystemImpl::RollNewCookie:AfterManifestCopy", &st);
  if (!st.ok()) {
    return st;
  }

  // Dump cloud_manifest into the CLOUDMANIFEST-cookie file
  st = WriteCloudManifest(newCloudManifest.get(),
                          MakeCloudManifestFile(local_dbname, cookie));
  if (!st.ok()) {
    return st;
  }

  if (HasDestBucket()) {
    // We have to upload the manifest file first. Otherwise, if the process
    // crashed in the middle, we'll endup with a CLOUDMANIFEST file pointing to
    // MANIFEST file which doesn't exist in s3
    st = UploadManifest(local_dbname, delta.epoch);
    if (!st.ok()) {
      return st;
    }
    st = UploadCloudManifest(local_dbname, cookie);
    if (!st.ok()) {
      return st;
    }
  }
  return IOStatus::OK();
}

// All db in a bucket are stored in path /.rockset/dbid/<dbid>
// The value of the object is the pathname where the db resides.
IOStatus CloudFileSystemImpl::SaveDbid(const std::string& bucket_name,
                                       const std::string& dbid,
                                       const std::string& dirname) {
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] SaveDbid dbid %s dir '%s'",
      Name(), dbid.c_str(), dirname.c_str());

  std::string dbidkey = GetDbIdKey(dbid);
  std::unordered_map<std::string, std::string> metadata;
  metadata["dirname"] = dirname;

  auto st = GetStorageProvider()->PutCloudObjectMetadata(bucket_name, dbidkey,
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
IOStatus CloudFileSystemImpl::GetPathForDbid(const std::string& bucket,
                                             const std::string& dbid,
                                             std::string* dirname) {
  std::string dbidkey = GetDbIdKey(dbid);

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[%s] Bucket %s GetPathForDbid dbid %s", Name(), bucket.c_str(),
      dbid.c_str());

  CloudObjectInformation info;
  auto st =
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
    st = IOStatus::NotFound("GetPathForDbid");
  }
  Log(InfoLogLevel::INFO_LEVEL, info_log_, "[%s] %s GetPathForDbid dbid %s %s",
      Name(), bucket.c_str(), dbid.c_str(), st.ToString().c_str());
  return st;
}

//
// Retrieves the list of all registered dbids and their paths
//
IOStatus CloudFileSystemImpl::GetDbidList(const std::string& bucket,
                                          DbidList* dblist) {
  // fetch the list all all dbids
  std::vector<std::string> dbid_list;
  auto st = GetStorageProvider()->ListCloudObjects(bucket, kDbIdRegistry(),
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
IOStatus CloudFileSystemImpl::DeleteDbid(const std::string& bucket,
                                         const std::string& dbid) {
  // fetch the list all all dbids
  std::string dbidkey = GetDbIdKey(dbid);
  auto st = GetStorageProvider()->DeleteCloudObject(bucket, dbidkey);
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[%s] %s DeleteDbid DeleteDbid(%s) %s", Name(), bucket.c_str(),
      dbid.c_str(), st.ToString().c_str());
  return st;
}

IOStatus CloudFileSystemImpl::LockFile(const std::string& /*fname*/,
                                       const IOOptions& /*opts*/,
                                       FileLock** lock,
                                       IODebugContext* /*dbg*/) {
  // there isn's a very good way to atomically check and create cloud file
  *lock = nullptr;
  return IOStatus::OK();
}

IOStatus CloudFileSystemImpl::UnlockFile(FileLock* /*lock*/,
                                         const IOOptions& /*opts*/,
                                         IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

std::string CloudFileSystemImpl::GetWALCacheDir() {
  return cloud_fs_options.cloud_log_controller->GetCacheDir();
}

Status CloudFileSystemImpl::PrepareOptions(const ConfigOptions& options) {
  // If underlying env is not defined, then use PosixEnv
  if (!base_fs_) {
    base_fs_ = FileSystem::Default();
  }
  Status status;
  if (!cloud_fs_options.cloud_log_controller &&
      !cloud_fs_options.keep_local_log_files) {
    if (cloud_fs_options.log_type == LogType::kLogKinesis) {
      status = CloudLogController::CreateFromString(
          options, CloudLogControllerImpl::kKinesis(),
          &cloud_fs_options.cloud_log_controller);
    } else if (cloud_fs_options.log_type == LogType::kLogKafka) {
      status = CloudLogController::CreateFromString(
          options, CloudLogControllerImpl::kKafka(),
          &cloud_fs_options.cloud_log_controller);
    } else {
      status = Status::NotSupported("Unsupported log controller type");
    }
    if (!status.ok()) {
      return status;
    }
  }

  status = CheckValidity();
  if (!status.ok()) {
    return status;
  }
  // start the purge thread only if there is a destination bucket
  if (cloud_fs_options.dest_bucket.IsValid() && cloud_fs_options.run_purger) {
    CloudFileSystemImpl* cloud = this;
    purge_thread_ = std::thread([cloud] { cloud->Purger(); });
  }
  return CloudFileSystem::PrepareOptions(options);
}

Status CloudFileSystemImpl::ValidateOptions(
    const DBOptions& db_opts, const ColumnFamilyOptions& cf_opts) const {
  if (info_log_ == nullptr) {
    info_log_ = db_opts.info_log;
  }
  Status s = CheckValidity();
  if (s.ok()) {
    return CloudFileSystem::ValidateOptions(db_opts, cf_opts);
  } else {
    return s;
  }
}

Status CloudFileSystemImpl::CheckValidity() const {
  if (cloud_fs_options.src_bucket.GetBucketName().empty() !=
      cloud_fs_options.src_bucket.GetObjectPath().empty()) {
    return Status::InvalidArgument(
        "Must specify both src bucket name and path");
  } else if (cloud_fs_options.dest_bucket.GetBucketName().empty() !=
             cloud_fs_options.dest_bucket.GetObjectPath().empty()) {
    return Status::InvalidArgument(
        "Must specify both dest bucket name and path");
  } else if (!cloud_fs_options.storage_provider) {
    return Status::InvalidArgument(
        "Cloud environment requires a storage provider");
  } else if (!cloud_fs_options.keep_local_log_files &&
             !cloud_fs_options.cloud_log_controller) {
    return Status::InvalidArgument(
        "Log controller required for remote log files");
  } else {
    return Status::OK();
  }
}

void CloudFileSystemImpl::RemapFileNumbers(
    const std::set<uint64_t>& file_numbers,
    std::vector<std::string>* sst_file_names) {
  sst_file_names->resize(file_numbers.size());

  size_t idx = 0;
  for (auto num : file_numbers) {
    std::string logical_path = MakeTableFileName("" /* path */, num);
    (*sst_file_names)[idx] = RemapFilename(logical_path);
    idx++;
  }
}

IOStatus CloudFileSystemImpl::FindAllLiveFiles(
    const std::string& local_dbname, std::vector<std::string>* live_sst_files,
    std::string* manifest_file) {
  std::unique_ptr<LocalManifestReader> extractor(
      new LocalManifestReader(info_log_, this));
  std::set<uint64_t> file_nums;
  auto st = extractor->GetLiveFilesLocally(local_dbname, &file_nums);
  if (!st.ok()) {
    return st;
  }

  // filename will be remapped correctly based on current_epoch of
  // cloud_manifest
  *manifest_file =
      RemapFilename(ManifestFileWithEpoch("" /* dbname */, "" /* epoch */));

  RemapFileNumbers(file_nums, live_sst_files);

  return IOStatus::OK();
}

IOStatus CloudFileSystemImpl::FindLiveFilesFromLocalManifest(
    const std::string& manifest_file,
    std::vector<std::string>* live_sst_files) {
  std::unique_ptr<LocalManifestReader> extractor(
      new LocalManifestReader(info_log_, this));
  std::set<uint64_t> file_nums;
  auto st = extractor->GetManifestLiveFiles(manifest_file, &file_nums);
  if (!st.ok()) {
    return st;
  }

  RemapFileNumbers(file_nums, live_sst_files);

  return IOStatus::OK();
}

std::string CloudFileSystemImpl::CloudManifestFile(const std::string& dbname) {
  if (dbname.empty()) {
    return MakeCloudManifestFile(cloud_fs_options.cookie_on_open);
  }
  return MakeCloudManifestFile(dbname, cloud_fs_options.cookie_on_open);
}

#ifndef NDEBUG
void CloudFileSystemImpl::TEST_InitEmptyCloudManifest() {
  CloudManifest::CreateForEmptyDatabase("", &cloud_manifest_);
}

size_t CloudFileSystemImpl::TEST_NumScheduledJobs() const {
  return cloud_file_deletion_scheduler_
             ? cloud_file_deletion_scheduler_->TEST_NumScheduledJobs()
             : 0;
}

#endif

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
