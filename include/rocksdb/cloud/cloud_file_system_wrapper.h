//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.

#pragma once
#include <atomic>
#include <thread>

#include "rocksdb/cloud/cloud_file_system_impl.h"
#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class MockStorageProvider : public CloudStorageProvider {
 public:
  MockStorageProvider() { notsup_ = IOStatus::NotSupported(); }
  virtual const char* Name() const override { return "Mock"; }
  IOStatus CreateBucket(const std::string& /*bucket_name*/) override {
    return notsup_;
  }

  IOStatus ExistsBucket(const std::string& /*bucket_name*/) override {
    return notsup_;
  }

  IOStatus EmptyBucket(const std::string& /*bucket_name*/,
                       const std::string& /*path_prefix*/) override {
    return notsup_;
  }
  IOStatus ListCloudObjects(const std::string& /*bucket_name*/,
                            const std::string& /*object_path*/,
                            std::vector<std::string>* /*result*/) override {
    return notsup_;
  }
  IOStatus DeleteCloudObject(const std::string& /*bucket_name*/,
                             const std::string& /*object_path*/) override {
    return notsup_;
  }
  IOStatus ExistsCloudObject(const std::string& /*bucket_name*/,
                             const std::string& /*object_path*/) override {
    return notsup_;
  }
  IOStatus GetCloudObjectSize(const std::string& /*bucket_name*/,
                              const std::string& /*object_path*/,
                              uint64_t* /*size*/) override {
    return notsup_;
  }
  IOStatus GetCloudObjectModificationTime(const std::string& /*bucket_name*/,
                                          const std::string& /*object_path*/,
                                          uint64_t* /*time*/) override {
    return notsup_;
  }
  IOStatus GetCloudObjectMetadata(const std::string& /*bucket_name*/,
                                  const std::string& /*object_path*/,
                                  CloudObjectInformation* /* info */) override {
    return notsup_;
  }
  IOStatus CopyCloudObject(const std::string& /*bucket_name_src*/,
                           const std::string& /*object_path_src*/,
                           const std::string& /*bucket_name_dest*/,
                           const std::string& /*object_path_dest*/) override {
    return notsup_;
  }
  IOStatus PutCloudObjectMetadata(
      const std::string& /*bucket_name*/, const std::string& /*object_path*/,
      const std::unordered_map<std::string, std::string>& /*metadata*/)
      override {
    return notsup_;
  }
  IOStatus NewCloudWritableFile(
      const std::string& /*local_path*/, const std::string& /*bucket_name*/,
      const std::string& /*object_path*/, const FileOptions& /*options*/,
      std::unique_ptr<CloudStorageWritableFile>* /*result*/,
      IODebugContext* /*dbg*/) override {
    return notsup_;
  }

  IOStatus NewCloudReadableFile(
      const std::string& /*bucket*/, const std::string& /*fname*/,
      const FileOptions& /*options*/,
      std::unique_ptr<CloudStorageReadableFile>* /*result*/,
      IODebugContext* /*dbg*/) override {
    return notsup_;
  }

  IOStatus GetCloudObject(const std::string& /*bucket_name*/,
                          const std::string& /*object_path*/,
                          const std::string& /*local_path*/) override {
    return notsup_;
  }

  IOStatus PutCloudObject(const std::string& /*local_path*/,
                          const std::string& /*bucket_name*/,
                          const std::string& /*object_path*/) override {
    return notsup_;
  }

 protected:
  IOStatus notsup_;
};
// An implementation of FileSystem that forwards all calls to another
// FileSystem. May be useful to clients who wish to override just part of the
// functionality of another FileSystem.

class MockCloudFileSystem : public CloudFileSystem {
 public:
  // Initialize an EnvWrapper that delegates all calls to *t
  explicit MockCloudFileSystem(
      const CloudFileSystemOptions& opts = CloudFileSystemOptions())
      : CloudFileSystem(opts, FileSystem::Default(), nullptr) {
    notsup_ = IOStatus::NotSupported();
  }

  virtual ~MockCloudFileSystem() {}

  const char* Name() const override { return "MockCloudFileSystem"; }

  IOStatus PreloadCloudManifest(const std::string& /*local_dbname*/) override {
    return notsup_;
  }

  IOStatus NewSequentialFileCloud(const std::string& /*bucket_name*/,
                                  const std::string& /*fname*/,
                                  const FileOptions& /*options*/,
                                  std::unique_ptr<FSSequentialFile>* /*result*/,
                                  IODebugContext* /*dbg*/) override {
    return notsup_;
  }
  IOStatus SaveDbid(const std::string& /*bucket_name*/,
                    const std::string& /*dbid */,
                    const std::string& /*dirname*/) override {
    return notsup_;
  }
  IOStatus GetPathForDbid(const std::string& /*bucket_name*/,
                          const std::string& /*dbid*/,
                          std::string* /*dirname*/) override {
    return notsup_;
  }
  IOStatus GetDbidList(const std::string& /*bucket_name*/,
                       DbidList* /*dblist*/) override {
    return notsup_;
  }
  IOStatus DeleteDbid(const std::string& /*bucket_name*/,
                      const std::string& /*dbid*/) override {
    return notsup_;
  }

  // The following text is boilerplate that forwards all methods to base_env
  IOStatus NewSequentialFile(const std::string& f, const FileOptions& o,
                             std::unique_ptr<FSSequentialFile>* r,
                             IODebugContext* dbg) override {
    return base_fs_->NewSequentialFile(f, o, r, dbg);
  }
  IOStatus NewRandomAccessFile(const std::string& f, const FileOptions& o,
                               std::unique_ptr<FSRandomAccessFile>* r,
                               IODebugContext* dbg) override {
    return base_fs_->NewRandomAccessFile(f, o, r, dbg);
  }
  IOStatus NewWritableFile(const std::string& f, const FileOptions& o,
                           std::unique_ptr<FSWritableFile>* r,
                           IODebugContext* dbg) override {
    return base_fs_->NewWritableFile(f, o, r, dbg);
  }
  IOStatus ReuseWritableFile(const std::string& fname,
                             const std::string& old_fname, const FileOptions& o,
                             std::unique_ptr<FSWritableFile>* r,
                             IODebugContext* dbg) override {
    return base_fs_->ReuseWritableFile(fname, old_fname, o, r, dbg);
  }
  IOStatus NewRandomRWFile(const std::string& fname, const FileOptions& o,
                           std::unique_ptr<FSRandomRWFile>* result,
                           IODebugContext* dbg) override {
    return base_fs_->NewRandomRWFile(fname, o, result, dbg);
  }
  IOStatus NewDirectory(const std::string& name, const IOOptions& o,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override {
    return base_fs_->NewDirectory(name, o, result, dbg);
  }
  IOStatus FileExists(const std::string& f, const IOOptions& o,
                      IODebugContext* dbg) override {
    return base_fs_->FileExists(f, o, dbg);
  }
  IOStatus GetChildren(const std::string& dir, const IOOptions& o,
                       std::vector<std::string>* r,
                       IODebugContext* dbg) override {
    return base_fs_->GetChildren(dir, o, r, dbg);
  }
  IOStatus GetChildrenFileAttributes(const std::string& dir, const IOOptions& o,
                                     std::vector<FileAttributes>* result,
                                     IODebugContext* dbg) override {
    return base_fs_->GetChildrenFileAttributes(dir, o, result, dbg);
  }
  IOStatus DeleteFile(const std::string& f, const IOOptions& o,
                      IODebugContext* dbg) override {
    return base_fs_->DeleteFile(f, o, dbg);
  }
  IOStatus CreateDir(const std::string& d, const IOOptions& o,
                     IODebugContext* dbg) override {
    return base_fs_->CreateDir(d, o, dbg);
  }
  IOStatus CreateDirIfMissing(const std::string& d, const IOOptions& o,
                              IODebugContext* dbg) override {
    return base_fs_->CreateDirIfMissing(d, o, dbg);
  }
  IOStatus DeleteDir(const std::string& d, const IOOptions& o,
                     IODebugContext* dbg) override {
    return base_fs_->DeleteDir(d, o, dbg);
  }
  IOStatus GetFileSize(const std::string& f, const IOOptions& o, uint64_t* s,
                       IODebugContext* dbg) override {
    return base_fs_->GetFileSize(f, o, s, dbg);
  }

  IOStatus GetFileModificationTime(const std::string& fname, const IOOptions& o,
                                   uint64_t* file_mtime,
                                   IODebugContext* dbg) override {
    return base_fs_->GetFileModificationTime(fname, o, file_mtime, dbg);
  }

  IOStatus RenameFile(const std::string& s, const std::string& t,
                      const IOOptions& o, IODebugContext* dbg) override {
    return base_fs_->RenameFile(s, t, o, dbg);
  }

  IOStatus LinkFile(const std::string& s, const std::string& t,
                    const IOOptions& o, IODebugContext* dbg) override {
    return base_fs_->LinkFile(s, t, o, dbg);
  }

  IOStatus LockFile(const std::string& f, const IOOptions& o, FileLock** l,
                    IODebugContext* dbg) override {
    return base_fs_->LockFile(f, o, l, dbg);
  }

  IOStatus UnlockFile(FileLock* l, const IOOptions& o,
                      IODebugContext* dbg) override {
    return base_fs_->UnlockFile(l, o, dbg);
  }

  IOStatus DeleteCloudFileFromDest(const std::string& /*path*/) override {
    return notsup_;
  }

  IOStatus FindAllLiveFiles(const std::string& /* local_dbname */,
                            std::vector<std::string>* /* live_sst_files */,
                            std::string* /* manifest_file */) override {
    return notsup_;
  }

  IOStatus FindLiveFilesFromLocalManifest(
      const std::string& /* manifest_file */,
      std::vector<std::string>* /* live_sst_files */) override {
    return notsup_;
  }

 private:
  IOStatus notsup_;
  std::string empty_;
};
}  // namespace ROCKSDB_NAMESPACE
