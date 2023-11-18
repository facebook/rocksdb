//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.

#pragma once
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <set>

#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class CloudManifest;
class CloudScheduler;
class CloudStorageReadableFile;
class ObjectLibrary;
class CloudFileDeletionScheduler;

//
// The Cloud file system
//
class CloudFileSystemImpl : public CloudFileSystem {
  friend class CloudFileSystem;

 public:
  static int RegisterAwsObjects(ObjectLibrary& library, const std::string& arg);
  // Constructor
  CloudFileSystemImpl(const CloudFileSystemOptions& options,
                      const std::shared_ptr<FileSystem>& base_fs,
                      const std::shared_ptr<Logger>& logger);

  virtual ~CloudFileSystemImpl();
  static const char* kClassName() { return kCloud(); }
  virtual const char* Name() const override { return kClassName(); }

  IOStatus NewSequentialFile(const std::string& fname,
                             const FileOptions& file_opts,
                             std::unique_ptr<FSSequentialFile>* result,
                             IODebugContext* dbg) override;

  IOStatus NewSequentialFileCloud(const std::string& bucket_prefix,
                                  const std::string& fname,
                                  const FileOptions& file_opts,
                                  std::unique_ptr<FSSequentialFile>* result,
                                  IODebugContext* dbg) override;

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& file_opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override;

  IOStatus NewWritableFile(const std::string& fname,
                           const FileOptions& file_opts,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override;

  IOStatus ReopenWritableFile(const std::string& fname,
                              const FileOptions& options,
                              std::unique_ptr<FSWritableFile>* result,
                              IODebugContext* dbg) override;

  IOStatus RenameFile(const std::string& src, const std::string& target,
                      const IOOptions& options, IODebugContext* dbg) override;

  IOStatus LinkFile(const std::string& src, const std::string& target,
                    const IOOptions& options, IODebugContext* dbg) override;

  IOStatus FileExists(const std::string& fname, const IOOptions& options,
                      IODebugContext* dbg) override;

  IOStatus GetChildren(const std::string& dir, const IOOptions& options,
                       std::vector<std::string>* result,
                       IODebugContext* dbg) override;

  IOStatus GetFileSize(const std::string& fname, const IOOptions& options,
                       uint64_t* file_size, IODebugContext* dbg) override;

  IOStatus GetFileModificationTime(const std::string& fname,
                                   const IOOptions& options,
                                   uint64_t* file_mtime,
                                   IODebugContext* dbg) override;

  IOStatus NewDirectory(const std::string& name, const IOOptions& io_opts,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override;

  IOStatus CreateDir(const std::string& dirname, const IOOptions& options,
                     IODebugContext* dbg) override;

  IOStatus CreateDirIfMissing(const std::string& dirname,
                              const IOOptions& options,
                              IODebugContext* dbg) override;

  IOStatus DeleteDir(const std::string& dirname, const IOOptions& options,
                     IODebugContext* dbg) override;

  IOStatus DeleteFile(const std::string& fname, const IOOptions& options,
                      IODebugContext* dbg) override;

  IOStatus NewLogger(const std::string& fname, const IOOptions& io_opts,
                     std::shared_ptr<Logger>* result,
                     IODebugContext* dbg) override {
    return base_fs_->NewLogger(fname, io_opts, result, dbg);
  }

  IOStatus GetTestDirectory(const IOOptions& options, std::string* path,
                            IODebugContext* dbg) override {
    return base_fs_->GetTestDirectory(options, path, dbg);
  }

  IOStatus GetAbsolutePath(const std::string& db_path, const IOOptions& options,
                           std::string* output_path,
                           IODebugContext* dbg) override {
    return base_fs_->GetAbsolutePath(db_path, options, output_path, dbg);
  }

  IOStatus LockFile(const std::string& fname, const IOOptions& options,
                    FileLock** lock, IODebugContext* dbg) override;

  IOStatus UnlockFile(FileLock* lock, const IOOptions& options,
                      IODebugContext* dbg) override;

  std::string GetWALCacheDir();

  // Saves and retrieves the dbid->dirname mapping in S3
  IOStatus SaveDbid(const std::string& bucket_name, const std::string& dbid,
                    const std::string& dirname) override;
  IOStatus GetPathForDbid(const std::string& bucket, const std::string& dbid,
                          std::string* dirname) override;
  IOStatus GetDbidList(const std::string& bucket, DbidList* dblist) override;
  IOStatus DeleteDbid(const std::string& bucket,
                      const std::string& dbid) override;

  IOStatus SanitizeDirectory(const DBOptions& options,
                             const std::string& clone_name, bool read_only);
  IOStatus LoadCloudManifest(const std::string& local_dbname, bool read_only);
  // The separator used to separate dbids while creating the dbid of a clone
  static constexpr const char* DBID_SEPARATOR = "rockset";

  // A map from a dbid to the list of all its parent dbids.
  typedef std::map<std::string, std::vector<std::string>> DbidParents;

  IOStatus FindObsoleteFiles(const std::string& bucket_name_prefix,
                             std::vector<std::string>* pathnames);
  IOStatus FindObsoleteDbid(const std::string& bucket_name_prefix,
                            std::vector<std::string>* dbids);

  // Find all live files based on cloud_manifest_ and local MANIFEST FILE
  // If local MANIFEST file doesn't exist, it will pull from cloud
  //
  // REQUIRES: cloud_manifest_ is loaded
  // REQUIRES: cloud_manifest_ is not updated when calling this function
  IOStatus FindAllLiveFiles(const std::string& local_dbname,
                            std::vector<std::string>* live_sst_files,
                            std::string* manifest_file) override;

  IOStatus FindLiveFilesFromLocalManifest(
      const std::string& manifest_file,
      std::vector<std::string>* live_sst_files) override;

  IOStatus extractParents(const std::string& bucket_name_prefix,
                          const DbidList& dbid_list, DbidParents* parents);
  IOStatus PreloadCloudManifest(const std::string& local_dbname) override;
  IOStatus MigrateFromPureRocksDB(const std::string& local_dbname) override;

  // Load CLOUDMANIFEST if exists in local disk to current env.
  IOStatus LoadLocalCloudManifest(const std::string& dbname);
  // TODO(wei): this function is used to temporarily support open db and switch
  // cookie. Remove it once that's not needed
  IOStatus LoadLocalCloudManifest(const std::string& dbname,
                                  const std::string& cookie);

  // Local CLOUDMANIFEST from `base_env` into `cloud_manifest`.
  static IOStatus LoadLocalCloudManifest(
      const std::string& dbname, const std::shared_ptr<FileSystem>& base_fs,
      const std::string& cookie,
      std::unique_ptr<CloudManifest>* cloud_manifest);

  IOStatus CreateCloudManifest(const std::string& local_dbname);
  // TODO(wei): this function is used to temporarily support open db and switch
  // cookie. Remove it once that's not needed
  IOStatus CreateCloudManifest(const std::string& local_dbname,
                               const std::string& cookie);

  // Transfers the filename from RocksDB's domain to the physical domain, based
  // on information stored in CLOUDMANIFEST.
  // For example, it will map 00010.sst to 00010.sst-[epoch] where [epoch] is
  // an epoch during which that file was created.
  // Files both in S3 and in the local directory have this [epoch] suffix.
  std::string RemapFilename(const std::string& logical_path) const override;

  FileOptions OptimizeForLogRead(
      const FileOptions& file_options) const override {
    return base_fs_->OptimizeForLogRead(file_options);
  }
  FileOptions OptimizeForManifestRead(
      const FileOptions& file_options) const override {
    return base_fs_->OptimizeForManifestRead(file_options);
  }
  FileOptions OptimizeForLogWrite(const FileOptions& file_options,
                                  const DBOptions& db_options) const override {
    auto fo = base_fs_->OptimizeForLogWrite(file_options, db_options);
    if (!fo.use_direct_writes) {
      // RocksDB-Cloud doesn't use WALs, so don't waste memory on allocating
      // their buffers.
      fo.writable_file_max_buffer_size = 0;
    }
    return fo;
  }
  FileOptions OptimizeForManifestWrite(
      const FileOptions& file_options) const override {
    return base_fs_->OptimizeForManifestWrite(file_options);
  }
  FileOptions OptimizeForCompactionTableWrite(
      const FileOptions& file_options,
      const ImmutableDBOptions& immutable_ops) const override {
    return base_fs_->OptimizeForCompactionTableWrite(file_options,
                                                     immutable_ops);
  }
  FileOptions OptimizeForCompactionTableRead(
      const FileOptions& file_options,
      const ImmutableDBOptions& db_options) const override {
    return base_fs_->OptimizeForCompactionTableRead(file_options, db_options);
  }
  IOStatus GetFreeSpace(const std::string& path, const IOOptions& options,
                        uint64_t* diskfree, IODebugContext* dbg) override {
    return base_fs_->GetFreeSpace(path, options, diskfree, dbg);
  }
  IOStatus IsDirectory(const std::string& /*path*/,
                       const IOOptions& /*options*/, bool* /*is_dir*/,
                       IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported(
        "CloudFileSystemImpl::IsDirectory() not supported.");
  }

  CloudManifest* GetCloudManifest() { return cloud_manifest_.get(); }

  IOStatus DeleteCloudFileFromDest(const std::string& fname) override;
  IOStatus CopyLocalFileToDest(const std::string& local_name,
                               const std::string& cloud_name) override;

  Status PrepareOptions(const ConfigOptions& config_options) override;
  Status ValidateOptions(const DBOptions& /*db_opts*/,
                         const ColumnFamilyOptions& /*cf_opts*/) const override;

  std::string CloudManifestFile(const std::string& dbname);

  // Apply cloud manifest delta to in-memory cloud manifest. Does not change the
  // on-disk state.
  IOStatus ApplyCloudManifestDelta(const CloudManifestDelta& delta,
                                   bool* delta_applied) override;

  // See comments in the parent class
  IOStatus RollNewCookie(const std::string& local_dbname,
                         const std::string& cookie,
                         const CloudManifestDelta& delta) const override;

  IOStatus GetMaxFileNumberFromCurrentManifest(
      const std::string& local_dbname, uint64_t* max_file_number) override;

  // Upload MANIFEST-epoch  to the cloud
  IOStatus UploadManifest(const std::string& local_dbname,
                          const std::string& epoch) const;

  // Upload local CLOUDMANIFEST-cookie file only.
  // REQURIES: the file exists locally
  IOStatus UploadCloudManifest(const std::string& local_dbname,
                               const std::string& cookie) const;

  // Delete invisible files in cloud.
  //
  // REQUIRES: Dest bucket set
  IOStatus DeleteCloudInvisibleFiles(
      const std::vector<std::string>& active_cookies) override;

#ifndef NDEBUG
  void TEST_InitEmptyCloudManifest();
  void TEST_DisableCloudManifest() { test_disable_cloud_manifest_ = true; }
  // Get current number of scheduled jobs in cloud scheduler
  // Used for test only
  size_t TEST_NumScheduledJobs() const;
#endif

 protected:
  Status CheckValidity() const;
  // Status TEST_Initialize(const std::string& name) override;
  // The pathname that contains a list of all db's inside a bucket.
  virtual const char* kDbIdRegistry() const { return "/.rockset/dbid/"; }

  std::string GetDbIdKey(const std::string& dbid) {
    return kDbIdRegistry() + dbid;
  }

  // Checks to see if the input fname exists in the dest or src bucket
  IOStatus ExistsCloudObject(const std::string& fname);

  // Gets the cloud object fname from the dest or src bucket
  IOStatus GetCloudObject(const std::string& fname);

  // Gets the size of the named cloud object from the dest or src bucket
  IOStatus GetCloudObjectSize(const std::string& fname, uint64_t* remote_size);

  // Gets the modification time of the named cloud object from the dest or src
  // bucket
  IOStatus GetCloudObjectModificationTime(const std::string& fname,
                                          uint64_t* time);

  // Returns the list of cloud objects from the src and dest buckets.
  IOStatus ListCloudObjects(const std::string& path,
                            std::vector<std::string>* result);

  // Returns a CloudStorageReadableFile from the dest or src bucket
  IOStatus NewCloudReadableFile(
      const std::string& fname, const FileOptions& options,
      std::unique_ptr<CloudStorageReadableFile>* result, IODebugContext* dbg);

  // Copy IDENTITY file to cloud storage. Update dbid registry.
  IOStatus SaveIdentityToCloud(const std::string& localfile,
                               const std::string& idfile);

  // Check if options are compatible with the storage system
  virtual Status CheckOption(const FileOptions& file_opts);

  // Converts a local pathname to an object name in the src bucket
  std::string srcname(const std::string& localname);

  // Converts a local pathname to an object name in the dest bucket
  std::string destname(const std::string& localname);

  // Does the dir need to be re-initialized?
  IOStatus NeedsReinitialization(const std::string& clone_dir, bool* do_reinit);

  IOStatus GetCloudDbid(const std::string& local_dir, std::string* src_dbid,
                        std::string* dest_dbid);

  IOStatus ResyncDir(const std::string& local_dir);

  IOStatus CreateNewIdentityFile(const std::string& dbid,
                                 const std::string& local_name);

  IOStatus FetchCloudManifest(const std::string& local_dbname);

  IOStatus RollNewEpoch(const std::string& local_dbname);

  // The dbid of the source database that is cloned
  std::string src_dbid_;

  // The pathname of the source database that is cloned
  std::string src_dbdir_;

  // Protects purger_cv_
  std::mutex purger_lock_;
  std::condition_variable purger_cv_;
  // The purger keep on running till this is set to false. (and is notified on
  // purger_cv_);
  bool purger_is_running_;
  std::thread purge_thread_;

  // A background thread that deletes orphaned objects in cloud storage
  void Purger();
  void StopPurger();

  // Delete all local files that are invisible
  IOStatus DeleteLocalInvisibleFiles(
      const std::string& dbname,
      const std::vector<std::string>& active_cookies) override;
 private:
  // Files are invisibile if:
  // - It's CLOUDMANFIEST file and cookie is not active. NOTE: empty cookie is
  // always active
  // - It's MANIFEST/SST file but their epoch is not current epoch of current
  // CLOUDMANFIEST(the one loaded in-memory). For example, if we find
  // 00010.sst-[epochX], but the real mapping for 00010.sst is [epochY], the
  // file will be treated as invisible
  bool IsFileInvisible(const std::vector<std::string>& active_cookies,
                       const std::string& fname) const;

  void log(InfoLogLevel level, const std::string& fname,
           const std::string& msg);

  // Remap SST file numbers to file names
  void RemapFileNumbers(const std::set<uint64_t>& file_numbers,
                        std::vector<std::string>* sst_file_names);

  // Fetch the cloud manifest based on the cookie
  IOStatus FetchCloudManifest(const std::string& local_dbname,
                              const std::string& cookie);
  IOStatus WriteCloudManifest(CloudManifest* manifest,
                              const std::string& fname) const;
  IOStatus FetchManifest(const std::string& local_dbname,
                         const std::string& epoch);
  std::string GenerateNewEpochId();

  std::unique_ptr<CloudManifest> cloud_manifest_;
  // This runs only in tests when we want to disable cloud manifest
  // functionality
  bool test_disable_cloud_manifest_{false};

  // scratch space in local dir
  static constexpr const char* SCRATCH_LOCAL_DIR = "/tmp";
  std::shared_ptr<CloudFileDeletionScheduler> cloud_file_deletion_scheduler_;
};

}  // namespace ROCKSDB_NAMESPACE
