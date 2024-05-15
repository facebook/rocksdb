// Copyright (c) 2017 Rockset

#ifndef ROCKSDB_LITE

#ifdef USE_AWS

#include "rocksdb/cloud/db_cloud.h"

#include <aws/core/Aws.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cinttypes>
#include <filesystem>

#include "cloud/cloud_manifest.h"
#include "cloud/cloud_scheduler.h"
#include "cloud/db_cloud_impl.h"
#include "cloud/filename.h"
#include "cloud/manifest_reader.h"
#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "file/filename.h"
#include "logging/logging.h"
#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/cloud/cloud_file_deletion_scheduler.h"
#include "rocksdb/cloud/cloud_file_system_impl.h"
#include "rocksdb/cloud/cloud_storage_provider_impl.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/random.h"
#include "util/string_util.h"
#ifndef OS_WIN
#include <unistd.h>
#endif

namespace ROCKSDB_NAMESPACE {

namespace {
const FileOptions kFileOptions;
const IOOptions kIOOptions;
IODebugContext* const kDbg = nullptr;
}  // namespace

class CloudTest : public testing::Test {
 public:
  CloudTest() {
    Random64 rng(time(nullptr));
    test_id_ = std::to_string(rng.Next());
    fprintf(stderr, "Test ID: %s\n", test_id_.c_str());

    base_env_ = Env::Default();
    dbname_ = test::TmpDir() + "/db_cloud-" + test_id_;
    clone_dir_ = test::TmpDir() + "/ctest-" + test_id_;
    cloud_fs_options_.TEST_Initialize("dbcloudtest.", dbname_);
    cloud_fs_options_.use_aws_transfer_manager = true;
    // To catch any possible file deletion bugs, cloud files are deleted
    // right away
    cloud_fs_options_.cloud_file_deletion_delay = std::chrono::seconds(0);

    options_.create_if_missing = true;
    options_.stats_dump_period_sec = 0;
    options_.stats_persist_period_sec = 0;
    persistent_cache_path_ = "";
    persistent_cache_size_gb_ = 0;
    db_ = nullptr;

    DestroyDir(dbname_);
    base_env_->CreateDirIfMissing(dbname_);
    base_env_->NewLogger(test::TmpDir(base_env_) + "/rocksdb-cloud.log",
                         &options_.info_log);
    options_.info_log->SetInfoLogLevel(InfoLogLevel::DEBUG_LEVEL);

    Cleanup();
  }

  void Cleanup() {
    ASSERT_TRUE(!aenv_);

    // check cloud credentials
    ASSERT_TRUE(cloud_fs_options_.credentials.HasValid().ok());

    CloudFileSystem* afs;
    // create a dummy aws env
    ASSERT_OK(CloudFileSystemEnv::NewAwsFileSystem(base_env_->GetFileSystem(),
                                                   cloud_fs_options_,
                                                   options_.info_log, &afs));
    ASSERT_NE(afs, nullptr);
    // delete all pre-existing contents from the bucket
    auto st = afs->GetStorageProvider()->EmptyBucket(afs->GetSrcBucketName(),
                                                     dbname_);
    delete afs;
    ASSERT_TRUE(st.ok() || st.IsNotFound());

    DestroyDir(clone_dir_);
    ASSERT_OK(base_env_->CreateDir(clone_dir_));
  }

  std::set<std::string> GetSSTFiles(std::string name) {
    std::vector<std::string> files;
    GetCloudFileSystem()->GetBaseFileSystem()->GetChildren(name, kIOOptions,
                                                           &files, kDbg);
    std::set<std::string> sst_files;
    for (auto& f : files) {
      if (IsSstFile(RemoveEpoch(f))) {
        sst_files.insert(f);
      }
    }
    return sst_files;
  }

  // Return total size of all sst files available locally
  void GetSSTFilesTotalSize(std::string name, uint64_t* total_size) {
    std::vector<std::string> files;
    GetCloudFileSystem()->GetBaseFileSystem()->GetChildren(name, kIOOptions,
                                                           &files, kDbg);
    std::set<std::string> sst_files;
    uint64_t local_size = 0;
    for (auto& f : files) {
      if (IsSstFile(RemoveEpoch(f))) {
        sst_files.insert(f);
        std::string lpath = dbname_ + "/" + f;
        ASSERT_OK(GetCloudFileSystem()->GetBaseFileSystem()->GetFileSize(
            lpath, kIOOptions, &local_size, kDbg));
        (*total_size) += local_size;
      }
    }
  }

  std::set<std::string> GetSSTFilesClone(std::string name) {
    std::string cname = clone_dir_ + "/" + name;
    return GetSSTFiles(cname);
  }

  void DestroyDir(const std::string& dir) {
    std::string cmd = "rm -rf " + dir;
    int rc = system(cmd.c_str());
    ASSERT_EQ(rc, 0);
  }

  virtual ~CloudTest() {
    // Cleanup the cloud bucket
    if (!cloud_fs_options_.src_bucket.GetBucketName().empty()) {
      CloudFileSystem* afs;
      Status st = CloudFileSystemEnv::NewAwsFileSystem(
          base_env_->GetFileSystem(), cloud_fs_options_, options_.info_log,
          &afs);
      if (st.ok()) {
        afs->GetStorageProvider()->EmptyBucket(afs->GetSrcBucketName(),
                                               dbname_);
        delete afs;
      }
    }

    CloseDB();
  }

  void CreateCloudEnv() {
    CloudFileSystem* cfs;
    ASSERT_OK(CloudFileSystemEnv::NewAwsFileSystem(base_env_->GetFileSystem(),
                                                   cloud_fs_options_,
                                                   options_.info_log, &cfs));
    std::shared_ptr<FileSystem> fs(cfs);
    aenv_ = CloudFileSystemEnv::NewCompositeEnv(base_env_, std::move(fs));
  }

  // Open database via the cloud interface
  void OpenDB() {
    std::vector<ColumnFamilyHandle*> handles;
    OpenDB(&handles);
    // Delete the handle for the default column family because the DBImpl
    // always holds a reference to it.
    ASSERT_TRUE(handles.size() > 0);
    delete handles[0];
  }

  // Open database via the cloud interface
  void OpenDB(std::vector<ColumnFamilyHandle*>* handles) {
    // default column family
    OpenWithColumnFamilies({kDefaultColumnFamilyName}, handles);
  }

  void OpenWithColumnFamilies(const std::vector<std::string>& cfs,
                              std::vector<ColumnFamilyHandle*>* handles) {
    ASSERT_TRUE(cloud_fs_options_.credentials.HasValid().ok());

    // Create new AWS env
    CreateCloudEnv();
    options_.env = aenv_.get();
    // Sleep for a second because S3 is eventual consistency.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    ASSERT_TRUE(db_ == nullptr);
    std::vector<ColumnFamilyDescriptor> column_families;
    for (size_t i = 0; i < cfs.size(); ++i) {
      column_families.emplace_back(cfs[i], options_);
    }
    ASSERT_OK(DBCloud::Open(options_, dbname_, column_families,
                            persistent_cache_path_, persistent_cache_size_gb_,
                            handles, &db_));
    ASSERT_OK(db_->GetDbIdentity(dbid_));
  }

  // Try to open and return status
  Status checkOpen() {
    // Create new AWS env
    CreateCloudEnv();
    options_.env = aenv_.get();
    // Sleep for a second because S3 is eventual consistency.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    return DBCloud::Open(options_, dbname_, persistent_cache_path_,
                         persistent_cache_size_gb_, &db_);
  }

  void CreateColumnFamilies(const std::vector<std::string>& cfs,
                            std::vector<ColumnFamilyHandle*>* handles) {
    ASSERT_NE(db_, nullptr);
    size_t cfi = handles->size();
    handles->resize(cfi + cfs.size());
    for (auto cf : cfs) {
      ASSERT_OK(db_->CreateColumnFamily(options_, cf, &handles->at(cfi++)));
    }
  }

  // Creates and Opens a clone
  Status CloneDB(const std::string& clone_name,
                 const std::string& dest_bucket_name,
                 const std::string& dest_object_path,
                 std::unique_ptr<DBCloud>* cloud_db, std::unique_ptr<Env>* env,
                 bool force_keep_local_on_invalid_dest_bucket = true) {
    // The local directory where the clone resides
    std::string cname = clone_dir_ + "/" + clone_name;

    CloudFileSystem* cfs;
    DBCloud* clone_db;

    // If there is no destination bucket, then the clone needs to copy
    // all sst fies from source bucket to local dir
    auto copt = cloud_fs_options_;
    if (dest_bucket_name == copt.src_bucket.GetBucketName()) {
      copt.dest_bucket = copt.src_bucket;
    } else {
      copt.dest_bucket.SetBucketName(dest_bucket_name);
    }
    copt.dest_bucket.SetObjectPath(dest_object_path);
    if (!copt.dest_bucket.IsValid() &&
        force_keep_local_on_invalid_dest_bucket) {
      copt.keep_local_sst_files = true;
    }
    // Create new AWS env
    Status st = CloudFileSystemEnv::NewAwsFileSystem(
        base_env_->GetFileSystem(), copt, options_.info_log, &cfs);
    if (!st.ok()) {
      return st;
    }

    // sets the env to be used by the env wrapper, and returns that env
    env->reset(
        new CompositeEnvWrapper(base_env_, std::shared_ptr<FileSystem>(cfs)));
    options_.env = env->get();

    // default column family
    ColumnFamilyOptions cfopt = options_;

    std::vector<ColumnFamilyDescriptor> column_families;
    column_families.emplace_back(
        ColumnFamilyDescriptor(kDefaultColumnFamilyName, cfopt));
    std::vector<ColumnFamilyHandle*> handles;

    st = DBCloud::Open(options_, cname, column_families, persistent_cache_path_,
                       persistent_cache_size_gb_, &handles, &clone_db);
    if (!st.ok()) {
      return st;
    }

    cloud_db->reset(clone_db);

    // Delete the handle for the default column family because the DBImpl
    // always holds a reference to it.
    assert(handles.size() > 0);
    delete handles[0];

    return st;
  }

  void CloseDB(std::vector<ColumnFamilyHandle*>* handles) {
    for (auto h : *handles) {
      delete h;
    }
    handles->clear();
    CloseDB();
  }

  void CloseDB() {
    if (db_) {
      db_->Flush(FlushOptions());  // convert pending writes to sst files
      delete db_;
      db_ = nullptr;
    }
  }

  void SetPersistentCache(const std::string& path, uint64_t size_gb) {
    persistent_cache_path_ = path;
    persistent_cache_size_gb_ = size_gb;
  }

  Status GetCloudLiveFilesSrc(std::set<uint64_t>* list) {
    auto* cfs = GetCloudFileSystem();
    std::unique_ptr<ManifestReader> manifest(
        new ManifestReader(options_.info_log, cfs, cfs->GetSrcBucketName()));
    return manifest->GetLiveFiles(cfs->GetSrcObjectPath(), list);
  }

  // Verify that local files are the same as cloud files in src bucket path
  void ValidateCloudLiveFilesSrcSize() {
    // Loop though all the files in the cloud manifest
    std::set<uint64_t> cloud_files;
    ASSERT_OK(GetCloudLiveFilesSrc(&cloud_files));
    for (uint64_t num : cloud_files) {
      std::string pathname = MakeTableFileName(dbname_, num);
      Log(options_.info_log, "cloud file list  %s\n", pathname.c_str());
    }

    std::set<std::string> localFiles = GetSSTFiles(dbname_);
    uint64_t cloudSize = 0;
    uint64_t localSize = 0;

    // loop through all the local files and validate
    for (std::string path : localFiles) {
      std::string cpath = GetCloudFileSystem()->GetSrcObjectPath() + "/" + path;
      ASSERT_OK(GetCloudFileSystem()->GetStorageProvider()->GetCloudObjectSize(
          GetCloudFileSystem()->GetSrcBucketName(), cpath, &cloudSize));

      // find the size of the file on local storage
      std::string lpath = dbname_ + "/" + path;
      ASSERT_OK(GetCloudFileSystem()->GetBaseFileSystem()->GetFileSize(
          lpath, kIOOptions, &localSize, kDbg));
      ASSERT_TRUE(localSize == cloudSize);
      Log(options_.info_log, "local file %s size %" PRIu64 "\n", lpath.c_str(),
          localSize);
      Log(options_.info_log, "cloud file %s size %" PRIu64 "\n", cpath.c_str(),
          cloudSize);
      printf("local file %s size %" PRIu64 "\n", lpath.c_str(), localSize);
      printf("cloud file %s size %" PRIu64 "\n", cpath.c_str(), cloudSize);
    }
  }

  CloudFileSystem* GetCloudFileSystem() const {
    EXPECT_TRUE(aenv_);
    return static_cast<CloudFileSystem*>(aenv_->GetFileSystem().get());
  }
  CloudFileSystemImpl* GetCloudFileSystemImpl() const {
    EXPECT_TRUE(aenv_);
    return static_cast<CloudFileSystemImpl*>(aenv_->GetFileSystem().get());
  }

  DBImpl* GetDBImpl() const {
    return static_cast<DBImpl*>(db_->GetBaseDB());
  }

  Status SwitchToNewCookie(std::string new_cookie) {
    CloudManifestDelta delta{
      db_->GetNextFileNumber(),
      new_cookie
    };
    return ApplyCMDeltaToCloudDB(delta);
  }

  Status ApplyCMDeltaToCloudDB(const CloudManifestDelta& delta) {
    auto st = GetCloudFileSystem()->RollNewCookie(dbname_, delta.epoch, delta);
    if (!st.ok()) {
      return st;
    }
    bool applied = false;
    st = GetCloudFileSystem()->ApplyCloudManifestDelta(delta, &applied);
    assert(applied);
    if (!st.ok()) {
      return st;
    }
    db_->NewManifestOnNextUpdate();
    return st;
  }

 protected:
  void WaitUntilNoScheduledJobs() {
    while (true) {
      auto num = GetCloudFileSystemImpl()->TEST_NumScheduledJobs();
      if (num > 0) {
        usleep(100);
      } else {
        return;
      }
    }
  }

  std::vector<Env::FileAttributes> GetAllLocalFiles() {
    std::vector<Env::FileAttributes> local_files;
    assert(base_env_->GetChildrenFileAttributes(dbname_, &local_files).ok());
    return local_files;
  }

  // Generate a few obsolete sst files on an empty db
  static void GenerateObsoleteFilesOnEmptyDB(
      DBImpl* db, CloudFileSystem* cfs,
      std::vector<std::string>* obsolete_files) {
    ASSERT_OK(db->Put({}, "k1", "v1"));
    ASSERT_OK(db->Flush({}));

    ASSERT_OK(db->Put({}, "k1", "v2"));
    ASSERT_OK(db->Flush({}));

    std::vector<LiveFileMetaData> sst_files;
    db->GetLiveFilesMetaData(&sst_files);
    ASSERT_EQ(sst_files.size(), 2);
    for (auto& f: sst_files) {
      obsolete_files->push_back(cfs->RemapFilename(f.relative_filename));
    }

    // trigger compaction, so previous 2 sst files will be obsolete
    ASSERT_OK(
        db->TEST_CompactRange(0, nullptr, nullptr, nullptr, true));
    sst_files.clear();
    db->GetLiveFilesMetaData(&sst_files);
    ASSERT_EQ(sst_files.size(), 1);
  }

  // check that fname exists in in src bucket/object path
  rocksdb::Status ExistsCloudObject(const std::string& filename) const {
    return GetCloudFileSystem()->GetStorageProvider()->ExistsCloudObject(
        GetCloudFileSystem()->GetSrcBucketName(),
        GetCloudFileSystem()->GetSrcObjectPath() + pathsep + filename);
  }

  std::string test_id_;
  Env* base_env_;
  Options options_;
  std::string dbname_;
  std::string clone_dir_;
  CloudFileSystemOptions cloud_fs_options_;
  std::string dbid_;
  std::string persistent_cache_path_;
  uint64_t persistent_cache_size_gb_;
  DBCloud* db_;
  std::unique_ptr<Env> aenv_;
};

//
// Most basic test. Create DB, write one key, close it and then check to see
// that the key exists.
//
TEST_F(CloudTest, BasicTest) {
  // Put one key-value
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_TRUE(value.compare("World") == 0);
  CloseDB();
  value.clear();

  // Reopen and validate
  OpenDB();
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_EQ(value, "World");

  std::set<uint64_t> live_files;
  ASSERT_OK(GetCloudLiveFilesSrc(&live_files));
  ASSERT_GT(live_files.size(), 0);
  CloseDB();
}

TEST_F(CloudTest, FindAllLiveFilesTest) {
  OpenDB();
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // wait until files are persisted into s3
  GetDBImpl()->TEST_WaitForBackgroundWork();

  CloseDB();

  std::vector<std::string> tablefiles;
  std::string manifest;
  // fetch latest manifest to local
  ASSERT_OK(GetCloudFileSystem()->FindAllLiveFiles(dbname_, &tablefiles, &manifest));
  EXPECT_EQ(tablefiles.size(), 1);

  for (auto name: tablefiles) {
    EXPECT_EQ(GetFileType(name), RocksDBFileType::kSstFile);
    // verify that the sst file indeed exists in cloud
    EXPECT_OK(GetCloudFileSystem()->GetStorageProvider()->ExistsCloudObject(
        GetCloudFileSystem()->GetSrcBucketName(),
        GetCloudFileSystem()->GetSrcObjectPath() + pathsep + name));
  }

  EXPECT_EQ(GetFileType(manifest), RocksDBFileType::kManifestFile);
  // verify that manifest file indeed exists in cloud
  auto storage_provider = GetCloudFileSystem()->GetStorageProvider();
  auto bucket_name = GetCloudFileSystem()->GetSrcBucketName();
  auto object_path = GetCloudFileSystem()->GetSrcObjectPath() + pathsep + manifest;
  EXPECT_OK(storage_provider->ExistsCloudObject(bucket_name, object_path));
}

// Files of dropped CF should not be included in live files
TEST_F(CloudTest, LiveFilesOfDroppedCFTest) {
  std::vector<ColumnFamilyHandle*> handles;
  OpenDB(&handles);

  std::vector<std::string> tablefiles;
  std::string manifest;
  ASSERT_OK(
      GetCloudFileSystem()->FindAllLiveFiles(dbname_, &tablefiles, &manifest));

  EXPECT_TRUE(tablefiles.empty());
  CreateColumnFamilies({"cf1"}, &handles);

  // write to CF
  ASSERT_OK(db_->Put(WriteOptions(), handles[1], "hello", "world"));
  // flush cf1
  ASSERT_OK(db_->Flush({}, handles[1]));

  tablefiles.clear();
  ASSERT_OK(
      GetCloudFileSystem()->FindAllLiveFiles(dbname_, &tablefiles, &manifest));
  EXPECT_TRUE(tablefiles.size() == 1);

  // Drop the CF
  ASSERT_OK(db_->DropColumnFamily(handles[1]));
  tablefiles.clear();
  // make sure that files are not listed as live for dropped CF
  ASSERT_OK(
      GetCloudFileSystem()->FindAllLiveFiles(dbname_, &tablefiles, &manifest));
  EXPECT_TRUE(tablefiles.empty());
  CloseDB(&handles);
}

// Verifies that when we move files across levels, the files are still listed as
// live files
TEST_F(CloudTest, LiveFilesAfterChangingLevelTest) {
  options_.num_levels = 3;
  OpenDB();
  ASSERT_OK(db_->Put(WriteOptions(), "a", "1"));
  ASSERT_OK(db_->Put(WriteOptions(), "b", "2"));
  ASSERT_OK(db_->Flush({}));
  auto db_impl = GetDBImpl();

  std::vector<std::string> tablefiles_before_move;
  std::string manifest;
  ASSERT_OK(GetCloudFileSystem()->FindAllLiveFiles(
      dbname_, &tablefiles_before_move, &manifest));
  EXPECT_EQ(tablefiles_before_move.size(), 1);

  CompactRangeOptions cro;
  cro.change_level = true;
  cro.target_level = 2;
  // Move the sst files to another level by compacting entire range
  ASSERT_OK(db_->CompactRange(cro, nullptr /* begin */, nullptr /* end */));

  ASSERT_OK(db_impl->TEST_WaitForBackgroundWork());

  std::vector<std::string> tablefiles_after_move;
  ASSERT_OK(GetCloudFileSystem()->FindAllLiveFiles(
      dbname_, &tablefiles_after_move, &manifest));
  EXPECT_EQ(tablefiles_before_move, tablefiles_after_move);
}

TEST_F(CloudTest, GetChildrenTest) {
  // Create some objects in S3
  OpenDB();
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  CloseDB();
  DestroyDir(dbname_);
  OpenDB();

  std::vector<std::string> children;
  ASSERT_OK(aenv_->GetFileSystem()->GetChildren(dbname_, kIOOptions, &children, kDbg));
  int sst_files = 0;
  for (auto c : children) {
    if (IsSstFile(c)) {
      sst_files++;
    }
  }
  // This verifies that GetChildren() works on S3. We deleted the S3 file
  // locally, so the only way to actually get it through GetChildren() if
  // listing S3 buckets works.
  EXPECT_EQ(sst_files, 1);
}

TEST_F(CloudTest, FindLiveFilesFromLocalManifestTest) {
  OpenDB();
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "Universe"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // wait until files are persisted into s3
  GetDBImpl()->TEST_WaitForBackgroundWork();

  CloseDB();

  // determine the manifest name and store a copy in a different location
  auto cfs = GetCloudFileSystem();
  auto manifest_file = cfs->RemapFilename("MANIFEST");
  auto manifest_path = std::filesystem::path(dbname_) / manifest_file;

  auto alt_manifest_path =
      std::filesystem::temp_directory_path() / ("ALT-" + manifest_file);
  std::filesystem::copy_file(manifest_path, alt_manifest_path);

  DestroyDir(dbname_);

  std::vector<std::string> tablefiles;
  // verify the copied manifest can be processed correctly
  ASSERT_OK(GetCloudFileSystem()->FindLiveFilesFromLocalManifest(
      alt_manifest_path, &tablefiles));

  // verify the result
  EXPECT_EQ(tablefiles.size(), 1);

  for (auto name : tablefiles) {
    EXPECT_EQ(GetFileType(name), RocksDBFileType::kSstFile);
    // verify that the sst file indeed exists in cloud
    EXPECT_OK(GetCloudFileSystem()->GetStorageProvider()->ExistsCloudObject(
        GetCloudFileSystem()->GetSrcBucketName(),
        GetCloudFileSystem()->GetSrcObjectPath() + pathsep + name));
  }

  // clean up
  std::filesystem::remove(alt_manifest_path);
}

//
// Create and read from a clone.
//
TEST_F(CloudTest, Newdb) {
  std::string master_dbid;
  std::string newdb1_dbid;
  std::string newdb2_dbid;

  // Put one key-value
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_TRUE(value.compare("World") == 0);
  ASSERT_OK(db_->GetDbIdentity(master_dbid));
  CloseDB();
  value.clear();

  {
    // Create and Open a new ephemeral instance
    std::unique_ptr<Env> env;
    std::unique_ptr<DBCloud> cloud_db;
    CloneDB("newdb1", "", "", &cloud_db, &env);

    // Retrieve the id of the first reopen
    ASSERT_OK(cloud_db->GetDbIdentity(newdb1_dbid));

    // This is an ephemeral clone. Its dbid is a prefix of the master's.
    ASSERT_NE(newdb1_dbid, master_dbid);
    auto res = std::mismatch(master_dbid.begin(), master_dbid.end(),
                             newdb1_dbid.begin());
    ASSERT_TRUE(res.first == master_dbid.end());

    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("World") == 0);

    // Open master and write one more kv to it. This is written to
    // src bucket as well.
    OpenDB();
    ASSERT_OK(db_->Put(WriteOptions(), "Dhruba", "Borthakur"));

    // check that the newly written kv exists
    value.clear();
    ASSERT_OK(db_->Get(ReadOptions(), "Dhruba", &value));
    ASSERT_TRUE(value.compare("Borthakur") == 0);

    // check that the earlier kv exists too
    value.clear();
    ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("World") == 0);
    CloseDB();

    // Assert  that newdb1 cannot see the second kv because the second kv
    // was written to local dir only of the ephemeral clone.
    ASSERT_TRUE(cloud_db->Get(ReadOptions(), "Dhruba", &value).IsNotFound());
  }
  {
    // Create another ephemeral instance using a different local dir but the
    // same two buckets as newdb1. This should be identical in contents with
    // newdb1.
    std::unique_ptr<Env> env;
    std::unique_ptr<DBCloud> cloud_db;
    CloneDB("newdb2", "", "", &cloud_db, &env);

    // Retrieve the id of the second clone db
    ASSERT_OK(cloud_db->GetDbIdentity(newdb2_dbid));

    // Since we use two different local directories for the two ephemeral
    // clones, their dbids should be different from one another
    ASSERT_NE(newdb1_dbid, newdb2_dbid);

    // check that both the kvs appear in the new ephemeral clone
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("World") == 0);
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Dhruba", &value));
    ASSERT_TRUE(value.compare("Borthakur") == 0);
  }

  CloseDB();
}

TEST_F(CloudTest, ColumnFamilies) {
  std::vector<ColumnFamilyHandle*> handles;
  // Put one key-value
  OpenDB(&handles);

  CreateColumnFamilies({"cf1", "cf2"}, &handles);

  ASSERT_OK(db_->Put(WriteOptions(), handles[0], "hello", "a"));
  ASSERT_OK(db_->Put(WriteOptions(), handles[1], "hello", "b"));
  ASSERT_OK(db_->Put(WriteOptions(), handles[2], "hello", "c"));

  auto validate = [&]() {
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), handles[0], "hello", &value));
    ASSERT_EQ(value, "a");
    ASSERT_OK(db_->Get(ReadOptions(), handles[1], "hello", &value));
    ASSERT_EQ(value, "b");
    ASSERT_OK(db_->Get(ReadOptions(), handles[2], "hello", &value));
    ASSERT_EQ(value, "c");
  };

  validate();

  CloseDB(&handles);
  OpenWithColumnFamilies({kDefaultColumnFamilyName, "cf1", "cf2"}, &handles);

  validate();
  CloseDB(&handles);

  // destory local state
  DestroyDir(dbname_);

  // new cloud env
  CreateCloudEnv();
  options_.env = aenv_.get();

  std::vector<std::string> families;
  ASSERT_OK(DBCloud::ListColumnFamilies(options_, dbname_, &families));
  std::sort(families.begin(), families.end());
  ASSERT_TRUE(families == std::vector<std::string>(
                              {"cf1", "cf2", kDefaultColumnFamilyName}));

  OpenWithColumnFamilies({kDefaultColumnFamilyName, "cf1", "cf2"}, &handles);
  validate();
  CloseDB(&handles);
}

//
// Create and read from a clone.
//
TEST_F(CloudTest, DISABLED_TrueClone) {
  std::string master_dbid;
  std::string newdb1_dbid;
  std::string newdb2_dbid;
  std::string newdb3_dbid;

  // Put one key-value
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_TRUE(value.compare("World") == 0);
  ASSERT_OK(db_->GetDbIdentity(master_dbid));
  ASSERT_OK(db_->Flush(FlushOptions()));
  CloseDB();
  value.clear();
  auto clone_path1 = "clone1_path-" + test_id_;
  {
    // Create a new instance with different src and destination paths.
    // This is true clone and should have all the contents of the masterdb
    std::unique_ptr<Env> env;
    std::unique_ptr<DBCloud> cloud_db;
    CloneDB("localpath1", cloud_fs_options_.src_bucket.GetBucketName(),
            clone_path1, &cloud_db, &env);

    // Retrieve the id of the clone db
    ASSERT_OK(cloud_db->GetDbIdentity(newdb1_dbid));

    // Since we used the different src and destination paths for both
    // the master and clone1, the clone should have its own identity.
    ASSERT_NE(master_dbid, newdb1_dbid);

    // check that the original kv appears in the clone
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("World") == 0);

    // write a new value to the clone
    ASSERT_OK(cloud_db->Put(WriteOptions(), "Hello", "Clone1"));
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("Clone1") == 0);
    ASSERT_OK(cloud_db->Flush(FlushOptions()));
  }
  {
    // Reopen clone1 with a different local path
    std::unique_ptr<Env> env;
    std::unique_ptr<DBCloud> cloud_db;
    CloneDB("localpath2", cloud_fs_options_.src_bucket.GetBucketName(),
            clone_path1, &cloud_db, &env);

    // Retrieve the id of the clone db
    ASSERT_OK(cloud_db->GetDbIdentity(newdb2_dbid));
    ASSERT_EQ(newdb1_dbid, newdb2_dbid);
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("Clone1") == 0);
    ASSERT_OK(cloud_db->Flush(FlushOptions()));
  }
  {
    // Reopen clone1 with the same local path as above.
    std::unique_ptr<Env> env;
    std::unique_ptr<DBCloud> cloud_db;
    CloneDB("localpath2", cloud_fs_options_.src_bucket.GetBucketName(),
            clone_path1, &cloud_db, &env);

    // Retrieve the id of the clone db
    ASSERT_OK(cloud_db->GetDbIdentity(newdb2_dbid));
    ASSERT_EQ(newdb1_dbid, newdb2_dbid);
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("Clone1") == 0);
    ASSERT_OK(cloud_db->Flush(FlushOptions()));
  }
  auto clone_path2 = "clone2_path-" + test_id_;
  {
    // Create clone2
    std::unique_ptr<Env> env;
    std::unique_ptr<DBCloud> cloud_db;
    CloneDB("localpath3",  // xxx try with localpath2
            cloud_fs_options_.src_bucket.GetBucketName(), clone_path2,
            &cloud_db, &env);

    // Retrieve the id of the clone db
    ASSERT_OK(cloud_db->GetDbIdentity(newdb3_dbid));
    ASSERT_NE(newdb2_dbid, newdb3_dbid);

    // verify that data is still as it was in the original db.
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("World") == 0);

    // Assert that there are no redundant sst files
    auto* cimpl = static_cast<CloudFileSystemImpl*>(env->GetFileSystem().get());
    std::vector<std::string> to_be_deleted;
    ASSERT_OK(
        cimpl->FindObsoleteFiles(cimpl->GetSrcBucketName(), &to_be_deleted));
    // TODO(igor): Re-enable once purger code is fixed
    // ASSERT_EQ(to_be_deleted.size(), 0);

    // Assert that there are no redundant dbid
    ASSERT_OK(
        cimpl->FindObsoleteDbid(cimpl->GetSrcBucketName(), &to_be_deleted));
    // TODO(igor): Re-enable once purger code is fixed
    // ASSERT_EQ(to_be_deleted.size(), 0);
  }

  GetCloudFileSystem()->GetStorageProvider()->EmptyBucket(
      GetCloudFileSystem()->GetSrcBucketName(), clone_path1);
  GetCloudFileSystem()->GetStorageProvider()->EmptyBucket(
      GetCloudFileSystem()->GetSrcBucketName(), clone_path2);
}

//
// verify that dbid registry is appropriately handled
//
TEST_F(CloudTest, DbidRegistry) {
  // Put one key-value
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_TRUE(value.compare("World") == 0);

  // Assert that there is one db in the registry
  DbidList dbs;
  ASSERT_OK(GetCloudFileSystem()->GetDbidList(
      GetCloudFileSystem()->GetSrcBucketName(), &dbs));
  ASSERT_GE(dbs.size(), 1);

  CloseDB();
}

TEST_F(CloudTest, KeepLocalFiles) {
  cloud_fs_options_.keep_local_sst_files = true;
  for (int iter = 0; iter < 4; ++iter) {
    cloud_fs_options_.use_direct_io_for_cloud_download =
        iter == 0 || iter == 1;
    cloud_fs_options_.use_aws_transfer_manager = iter == 0 || iter == 3;
    // Create two files
    OpenDB();
    std::string value;
    ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
    ASSERT_OK(db_->Flush(FlushOptions()));
    ASSERT_OK(db_->Put(WriteOptions(), "Hello2", "World2"));
    ASSERT_OK(db_->Flush(FlushOptions()));

    CloseDB();
    DestroyDir(dbname_);
    OpenDB();

    std::vector<std::string> files;
    ASSERT_OK(Env::Default()->GetChildren(dbname_, &files));
    long sst_files =
        std::count_if(files.begin(), files.end(), [](const std::string& file) {
          return file.find("sst") != std::string::npos;
        });
    ASSERT_EQ(sst_files, 2);

    ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
    ASSERT_EQ(value, "World");
    ASSERT_OK(db_->Get(ReadOptions(), "Hello2", &value));
    ASSERT_EQ(value, "World2");

    CloseDB();
    ValidateCloudLiveFilesSrcSize();
    GetCloudFileSystem()->GetStorageProvider()->EmptyBucket(
        GetCloudFileSystem()->GetSrcBucketName(), dbname_);
    DestroyDir(dbname_);
  }
}

TEST_F(CloudTest, CopyToFromS3) {
  std::string fname = dbname_ + "/100000.sst";

  // iter 0 -- not using transfer manager
  // iter 1 -- using transfer manager
  for (int iter = 0; iter < 2; ++iter) {
    // Create aws env
    cloud_fs_options_.keep_local_sst_files = true;
    cloud_fs_options_.use_aws_transfer_manager = iter == 1;
    CreateCloudEnv();
    auto* cimpl = GetCloudFileSystemImpl();
    cimpl->TEST_InitEmptyCloudManifest();
    char buffer[1 * 1024 * 1024];

    // create a 10 MB file and upload it to cloud
    {
      std::unique_ptr<FSWritableFile> writer;
      ASSERT_OK(aenv_->GetFileSystem()->NewWritableFile(fname, kFileOptions,
                                                        &writer, kDbg));

      for (int i = 0; i < 10; i++) {
        ASSERT_OK(
            writer->Append(Slice(buffer, sizeof(buffer)), kIOOptions, kDbg));
      }
      // sync and close file
    }

    // delete the file manually.
    ASSERT_OK(base_env_->DeleteFile(fname));

    // reopen file for reading. It should be refetched from cloud storage.
    {
      std::unique_ptr<FSRandomAccessFile> reader;
      ASSERT_OK(aenv_->GetFileSystem()->NewRandomAccessFile(fname, kFileOptions,
                                                            &reader, kDbg));

      uint64_t offset = 0;
      for (int i = 0; i < 10; i++) {
        Slice result;
        char* scratch = &buffer[0];
        ASSERT_OK(reader->Read(offset, sizeof(buffer), kIOOptions, &result,
                               scratch, kDbg));
        ASSERT_EQ(result.size(), sizeof(buffer));
        offset += sizeof(buffer);
      }
    }
  }
}

TEST_F(CloudTest, DelayFileDeletion) {
  std::string fname = dbname_ + "/000010.sst";

  // Create aws env
  cloud_fs_options_.keep_local_sst_files = true;
  cloud_fs_options_.cloud_file_deletion_delay = std::chrono::seconds(2);
  CreateCloudEnv();
  auto* cimpl = GetCloudFileSystemImpl();
  cimpl->TEST_InitEmptyCloudManifest();

  auto createFile = [&]() {
    std::unique_ptr<FSWritableFile> writer;
    ASSERT_OK(aenv_->GetFileSystem()->NewWritableFile(fname, kFileOptions,
                                                      &writer, kDbg));

    for (int i = 0; i < 10; i++) {
      ASSERT_OK(writer->Append("igor", kIOOptions, kDbg));
    }
    // sync and close file
  };

  for (int iter = 0; iter <= 1; ++iter) {
    createFile();
    // delete the file
    ASSERT_OK(aenv_->GetFileSystem()->DeleteFile(fname, kIOOptions, kDbg));
    // file should still be there
    ASSERT_OK(aenv_->GetFileSystem()->FileExists(fname, kIOOptions, kDbg));

    if (iter == 1) {
      // should prevent the deletion
      createFile();
    }

    std::this_thread::sleep_for(std::chrono::seconds(3));
    auto st = aenv_->GetFileSystem()->FileExists(fname, kIOOptions, kDbg);
    if (iter == 0) {
      // in iter==0 file should be deleted after 2 seconds
      ASSERT_TRUE(st.IsNotFound());
    } else {
      // in iter==1 file should not be deleted because we wrote the new file
      ASSERT_OK(st);
    }
  }
}

// Verify that a savepoint copies all src files to destination
TEST_F(CloudTest, Savepoint) {
  // Put one key-value
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_TRUE(value.compare("World") == 0);
  CloseDB();
  value.clear();
  std::string dest_path = "/clone2_path-" + test_id_;
  {
    // Create a new instance with different src and destination paths.
    // This is true clone and should have all the contents of the masterdb
    std::unique_ptr<Env> env;
    std::unique_ptr<DBCloud> cloud_db;
    CloneDB("localpath1", cloud_fs_options_.src_bucket.GetBucketName(),
            dest_path, &cloud_db, &env);

    // check that the original kv appears in the clone
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("World") == 0);

    // there should be only one sst file
    std::vector<LiveFileMetaData> flist;
    cloud_db->GetLiveFilesMetaData(&flist);
    ASSERT_TRUE(flist.size() == 1);

    auto* cimpl = static_cast<CloudFileSystemImpl*>(env->GetFileSystem().get());
    auto remapped_fname = cimpl->RemapFilename(flist[0].name);
    // source path
    std::string spath = cimpl->GetSrcObjectPath() + "/" + remapped_fname;
    ASSERT_OK(cimpl->GetStorageProvider()->ExistsCloudObject(
        cimpl->GetSrcBucketName(), spath));

    // Verify that the destination path does not have any sst files
    std::string dpath = dest_path + "/" + remapped_fname;
    ASSERT_TRUE(cimpl->GetStorageProvider()
                    ->ExistsCloudObject(cimpl->GetSrcBucketName(), dpath)
                    .IsNotFound());

    // write a new value to the clone
    ASSERT_OK(cloud_db->Put(WriteOptions(), "Hell", "Done"));
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hell", &value));
    ASSERT_TRUE(value.compare("Done") == 0);

    // Invoke savepoint to populate destination path from source path
    ASSERT_OK(cloud_db->Savepoint());

    // check that the sst file is copied to dest path
    ASSERT_OK(cimpl->GetStorageProvider()->ExistsCloudObject(
        cimpl->GetSrcBucketName(), dpath));
    ASSERT_OK(cloud_db->Flush(FlushOptions()));
  }
  {
    // Reopen the clone
    std::unique_ptr<Env> env;
    std::unique_ptr<DBCloud> cloud_db;
    CloneDB("localpath2", cloud_fs_options_.src_bucket.GetBucketName(),
            dest_path, &cloud_db, &env);

    // check that the both kvs appears in the clone
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("World") == 0);
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hell", &value));
    ASSERT_TRUE(value.compare("Done") == 0);
  }
  GetCloudFileSystem()->GetStorageProvider()->EmptyBucket(
      GetCloudFileSystem()->GetSrcBucketName(), dest_path);
}

TEST_F(CloudTest, Encryption) {
  // Create aws env
  cloud_fs_options_.server_side_encryption = true;
  char* key_id = getenv("AWS_KMS_KEY_ID");
  if (key_id != nullptr) {
    cloud_fs_options_.encryption_key_id = std::string(key_id);
    Log(options_.info_log, "Found encryption key id in env variable %s",
        key_id);
  }

  OpenDB();

  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  // create a file
  ASSERT_OK(db_->Flush(FlushOptions()));
  CloseDB();

  OpenDB();
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_EQ(value, "World");
  CloseDB();
}

TEST_F(CloudTest, DISABLED_DirectReads) {
  options_.use_direct_reads = true;
  options_.use_direct_io_for_flush_and_compaction = true;
  BlockBasedTableOptions bbto;
  bbto.no_block_cache = true;
  bbto.block_size = 1024;
  options_.table_factory.reset(NewBlockBasedTableFactory(bbto));

  OpenDB();

  for (int i = 0; i < 50; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), "Hello" + std::to_string(i), "World"));
  }
  // create a file
  ASSERT_OK(db_->Flush(FlushOptions()));

  std::string value;
  for (int i = 0; i < 50; ++i) {
    ASSERT_OK(db_->Get(ReadOptions(), "Hello" + std::to_string(i), &value));
    ASSERT_EQ(value, "World");
  }
  CloseDB();
}

#ifdef USE_KAFKA
TEST_F(CloudTest, KeepLocalLogKafka) {
  cloud_fs_options_.keep_local_log_files = false;
  cloud_fs_options_.log_type = LogType::kLogKafka;
  cloud_fs_options_.kafka_log_options
      .client_config_params["metadata.broker.list"] = "localhost:9092";

  OpenDB();

  ASSERT_OK(db_->Put(WriteOptions(), "Franz", "Kafka"));

  // Destroy DB in memory and on local file system.
  delete db_;
  db_ = nullptr;
  aenv_.reset();
  DestroyDir(dbname_);
  DestroyDir("/tmp/ROCKSET");

  // Create new env.
  CreateCloudEnv();

  // Give env enough time to consume WALs
  std::this_thread::sleep_for(std::chrono::seconds(3));

  // Open DB.
  cloud_fs_options_.keep_local_log_files = true;
  auto* cimpl = GetCloudFileSystemImpl();
  options_.wal_dir = cimpl->GetWALCacheDir();
  OpenDB();

  // Test read.
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "Franz", &value));
  ASSERT_EQ(value, "Kafka");

  CloseDB();
}
#endif /* USE_KAFKA */

// TODO(igor): determine why this fails,
// https://github.com/rockset/rocksdb-cloud/issues/35
TEST_F(CloudTest, DISABLED_KeepLocalLogKinesis) {
  cloud_fs_options_.keep_local_log_files = false;
  cloud_fs_options_.log_type = LogType::kLogKinesis;

  OpenDB();

  // Test write.
  ASSERT_OK(db_->Put(WriteOptions(), "Tele", "Kinesis"));

  // Destroy DB in memory and on local file system.
  delete db_;
  db_ = nullptr;
  aenv_.reset();
  DestroyDir(dbname_);
  DestroyDir("/tmp/ROCKSET");

  // Create new env.
  CreateCloudEnv();

  // Give env enough time to consume WALs
  std::this_thread::sleep_for(std::chrono::seconds(3));

  // Open DB.
  cloud_fs_options_.keep_local_log_files = true;
  auto* cimpl = GetCloudFileSystemImpl();
  options_.wal_dir = cimpl->GetWALCacheDir();
  OpenDB();

  // Test read.
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "Tele", &value));
  ASSERT_EQ(value, "Kinesis");

  CloseDB();
}

// Test whether we are able to recover nicely from two different writers to the
// same S3 bucket. (The feature that was enabled by CLOUDMANIFEST)
TEST_F(CloudTest, TwoDBsOneBucket) {
  auto firstDB = dbname_;
  auto secondDB = dbname_ + "-1";
  cloud_fs_options_.keep_local_sst_files = true;
  std::string value;

  cloud_fs_options_.resync_on_open = true;
  OpenDB();
  auto* cimpl = GetCloudFileSystemImpl();
  auto firstManifestFile =
      cimpl->GetDestObjectPath() + "/" + cimpl->RemapFilename("MANIFEST-1");
  EXPECT_OK(cimpl->GetStorageProvider()->ExistsCloudObject(
      cimpl->GetDestBucketName(), firstManifestFile));
  // Create two files
  ASSERT_OK(db_->Put(WriteOptions(), "First", "File"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Put(WriteOptions(), "Second", "File"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  auto files = GetSSTFiles(dbname_);
  EXPECT_EQ(files.size(), 2);
  CloseDB();

  cloud_fs_options_.resync_on_open = false;
  // Open again, with no destination bucket
  cloud_fs_options_.dest_bucket.SetBucketName("");
  cloud_fs_options_.dest_bucket.SetObjectPath("");
  OpenDB();
  ASSERT_OK(db_->Put(WriteOptions(), "Third", "File"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  auto newFiles = GetSSTFiles(dbname_);
  EXPECT_EQ(newFiles.size(), 3);
  // Remember the third file we created
  std::vector<std::string> diff;
  std::set_difference(newFiles.begin(), newFiles.end(), files.begin(),
                      files.end(), std::inserter(diff, diff.begin()));
  ASSERT_EQ(diff.size(), 1);
  auto thirdFile = diff[0];
  CloseDB();

  // Open in a different directory with destination bucket set
  dbname_ = secondDB;
  cloud_fs_options_.dest_bucket = cloud_fs_options_.src_bucket;
  cloud_fs_options_.resync_on_open = true;
  OpenDB();
  ASSERT_OK(db_->Put(WriteOptions(), "Third", "DifferentFile"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  CloseDB();

  // Open back in the first directory with no destination
  dbname_ = firstDB;
  cloud_fs_options_.dest_bucket.SetBucketName("");
  cloud_fs_options_.dest_bucket.SetObjectPath("");
  cloud_fs_options_.resync_on_open = false;
  OpenDB();
  // Changes to the cloud database should make no difference for us. This is an
  // important check because we should not reinitialize from the cloud if we
  // have a valid local directory!
  ASSERT_OK(db_->Get(ReadOptions(), "Third", &value));
  EXPECT_EQ(value, "File");
  CloseDB();

  // Reopen in the first directory, this time with destination path
  dbname_ = firstDB;
  cloud_fs_options_.dest_bucket = cloud_fs_options_.src_bucket;
  cloud_fs_options_.resync_on_open = true;
  OpenDB();
  // Changes to the cloud database should be pulled down now.
  ASSERT_OK(db_->Get(ReadOptions(), "Third", &value));
  EXPECT_EQ(value, "DifferentFile");
  files = GetSSTFiles(dbname_);
  // Should no longer be in my directory because it's not part of the new
  // MANIFEST.
  EXPECT_TRUE(files.find(thirdFile) == files.end());

  // We need to sleep a bit because file deletion happens in a different thread,
  // so it might not be immediately deleted.
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  EXPECT_TRUE(GetCloudFileSystem()
                  ->GetStorageProvider()
                  ->ExistsCloudObject(GetCloudFileSystem()->GetDestBucketName(),
                                      firstManifestFile)
                  .IsNotFound());
  CloseDB();
}

// This test is similar to TwoDBsOneBucket, but is much more chaotic and illegal
// -- it runs two databases on exact same S3 bucket. The work on CLOUDMANIFEST
// enables us to run in that configuration for extended amount of time (1 hour
// by default) without any issues -- the last CLOUDMANIFEST writer wins.
// This test only applies when cookie is empty. So whenever db is reopened, it
// always fetches the latest CM/M files from s3
TEST_F(CloudTest, TwoConcurrentWritersCookieEmpty) {
  cloud_fs_options_.resync_on_open = true;
  auto firstDB = dbname_;
  auto secondDB = dbname_ + "-1";

  DBCloud *db1, *db2;
  Env *aenv1, *aenv2;

  auto openDB1 = [&] {
    dbname_ = firstDB;
    OpenDB();
    db1 = db_;
    db_ = nullptr;
    aenv1 = aenv_.release();
  };
  auto openDB2 = [&] {
    dbname_ = secondDB;
    OpenDB();
    db2 = db_;
    db_ = nullptr;
    aenv2 = aenv_.release();
  };
  auto closeDB1 = [&] {
    db_ = db1;
    aenv_.reset(aenv1);
    CloseDB();
  };
  auto closeDB2 = [&] {
    db_ = db2;
    aenv_.reset(aenv2);
    CloseDB();
  };

  openDB1();
  openDB2();

  // Create bunch of files, reopening the databases during
  for (int i = 0; i < 5; ++i) {
    closeDB1();
    if (i == 2) {
      DestroyDir(firstDB);
    }
    // opening the database makes me a master (i.e. CLOUDMANIFEST points to my
    // manifest), my writes are applied to the shared space!
    openDB1();
    for (int j = 0; j < 5; ++j) {
      auto key = std::to_string(i) + std::to_string(j) + "1";
      ASSERT_OK(db1->Put(WriteOptions(), key, "FirstDB"));
      ASSERT_OK(db1->Flush(FlushOptions()));
    }
    closeDB2();
    if (i == 2) {
      DestroyDir(secondDB);
    }
    // opening the database makes me a master (i.e. CLOUDMANIFEST points to my
    // manifest), my writes are applied to the shared space!
    openDB2();
    for (int j = 0; j < 5; ++j) {
      auto key = std::to_string(i) + std::to_string(j) + "2";
      ASSERT_OK(db2->Put(WriteOptions(), key, "SecondDB"));
      ASSERT_OK(db2->Flush(FlushOptions()));
    }
  }

  dbname_ = firstDB;
  // This write should not be applied, because DB2 is currently the owner of the
  // S3 bucket
  ASSERT_OK(db1->Put(WriteOptions(), "ShouldNotBeApplied", ""));
  ASSERT_OK(db1->Flush(FlushOptions()));
  closeDB1();
  closeDB2();

  openDB1();
  for (int i = 0; i < 5; ++i) {
    for (int j = 0; j < 5; ++j) {
      std::string val;
      auto key = std::to_string(i) + std::to_string(j);
      ASSERT_OK(db1->Get(ReadOptions(), key + "1", &val));
      EXPECT_EQ(val, "FirstDB");
      ASSERT_OK(db1->Get(ReadOptions(), key + "2", &val));
      EXPECT_EQ(val, "SecondDB");
    }
  }

  std::string v;
  ASSERT_TRUE(db1->Get(ReadOptions(), "ShouldNotBeApplied", &v).IsNotFound());
  closeDB1();
}

// Creates a pure RocksDB database and makes sure we can migrate to RocksDB
// Cloud
TEST_F(CloudTest, MigrateFromPureRocksDB) {
  {  // Create local RocksDB
    Options options;
    options.create_if_missing = true;
    DB* dbptr;
    std::unique_ptr<DB> db;
    ASSERT_OK(DB::Open(options, dbname_, &dbptr));
    db.reset(dbptr);
    // create 5 files
    for (int i = 0; i < 5; ++i) {
      auto key = "key" + std::to_string(i);
      ASSERT_OK(db->Put(WriteOptions(), key, key));
      ASSERT_OK(db->Flush(FlushOptions()));
    }
  }

  CreateCloudEnv();
  ASSERT_OK(GetCloudFileSystem()->MigrateFromPureRocksDB(dbname_));

  // Now open RocksDB cloud
  // TODO(dhruba) Figure out how to make this work without skipping dbid
  // verification
  cloud_fs_options_.skip_dbid_verification = true;
  cloud_fs_options_.keep_local_sst_files = true;
  cloud_fs_options_.validate_filesize = false;
  OpenDB();
  for (int i = 5; i < 10; ++i) {
    auto key = "key" + std::to_string(i);
    ASSERT_OK(db_->Put(WriteOptions(), key, key));
    ASSERT_OK(db_->Flush(FlushOptions()));
  }

  for (int i = 0; i < 10; ++i) {
    auto key = "key" + std::to_string(i);
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), key, &value));
    ASSERT_EQ(value, key);
  }
  CloseDB();
}

// Tests that we can open cloud DB without destination and source bucket set.
// This is useful for tests.
TEST_F(CloudTest, NoDestOrSrc) {
  DestroyDir(dbname_);
  cloud_fs_options_.keep_local_sst_files = true;
  cloud_fs_options_.src_bucket.SetBucketName("");
  cloud_fs_options_.src_bucket.SetObjectPath("");
  cloud_fs_options_.dest_bucket.SetBucketName("");
  cloud_fs_options_.dest_bucket.SetObjectPath("");
  OpenDB();
  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "key", &value));
  ASSERT_EQ(value, "value");
  CloseDB();
  OpenDB();
  ASSERT_OK(db_->Get(ReadOptions(), "key", &value));
  ASSERT_EQ(value, "value");
  CloseDB();
}

TEST_F(CloudTest, PreloadCloudManifest) {
  DestroyDir(dbname_);
  // Put one key-value
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_TRUE(value.compare("World") == 0);
  CloseDB();
  value.clear();

  // Reopen and validate, preload cloud manifest
  GetCloudFileSystem()->PreloadCloudManifest(dbname_);

  OpenDB();
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_EQ(value, "World");
}

//
// Test Ephemeral mode. In this mode, the database is cloned
// from a cloud bucket but new writes are not propagated
// back to any cloud bucket. Once cloned, all updates are local.
//
TEST_F(CloudTest, Ephemeral) {
  cloud_fs_options_.keep_local_sst_files = true;
  options_.level0_file_num_compaction_trigger = 100;  // never compact

  // Create a primary DB with two files
  OpenDB();
  std::string value;
  std::string newdb1_dbid;
  std::set<uint64_t> cloud_files;
  ASSERT_OK(db_->Put(WriteOptions(), "Name", "dhruba"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Put(WriteOptions(), "Hello2", "borthakur"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  CloseDB();
  ASSERT_EQ(2, GetSSTFiles(dbname_).size());

  // Reopen the same database in ephemeral mode by cloning the original.
  // Do not destroy the local dir. Writes to this db does not make it back
  // to any cloud storage.
  {
    std::unique_ptr<Env> env;
    std::unique_ptr<DBCloud> cloud_db;
    CloneDB("db_ephemeral", "", "", &cloud_db, &env);

    // Retrieve the id of the first reopen
    ASSERT_OK(cloud_db->GetDbIdentity(newdb1_dbid));

    // verify that we still have two sst files
    ASSERT_EQ(2, GetSSTFilesClone("db_ephemeral").size());

    ASSERT_OK(cloud_db->Get(ReadOptions(), "Name", &value));
    ASSERT_EQ(value, "dhruba");
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello2", &value));
    ASSERT_EQ(value, "borthakur");

    // Write one more record.
    // There should be 3 local sst files in the ephemeral db.
    ASSERT_OK(cloud_db->Put(WriteOptions(), "zip", "94087"));
    ASSERT_OK(cloud_db->Flush(FlushOptions()));
    ASSERT_EQ(3, GetSSTFilesClone("db_ephemeral").size());

    // check that cloud files did not get updated
    ASSERT_OK(GetCloudLiveFilesSrc(&cloud_files));
    ASSERT_EQ(2, cloud_files.size());
    cloud_files.clear();
  }

  // reopen main db and write two more records to it
  OpenDB();
  ASSERT_EQ(2, GetSSTFiles(dbname_).size());

  // write two more records to it.
  ASSERT_OK(db_->Put(WriteOptions(), "Key1", "onlyInMainDB"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Put(WriteOptions(), "Key2", "onlyInMainDB"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(4, GetSSTFiles(dbname_).size());
  CloseDB();
  ASSERT_OK(GetCloudLiveFilesSrc(&cloud_files));
  ASSERT_EQ(4, cloud_files.size());
  cloud_files.clear();

  // At this point, the main db has 4 files while the ephemeral
  // database has diverged earlier with 3 local files. If we try
  // to reopen the ephemeral clone, it should not download new
  // files from the cloud
  {
    std::unique_ptr<Env> env;
    std::unique_ptr<DBCloud> cloud_db;
    std::string dbid;
    options_.info_log = nullptr;
    CreateLoggerFromOptions(clone_dir_ + "/db_ephemeral", options_,
                            &options_.info_log);

    CloneDB("db_ephemeral", "", "", &cloud_db, &env);

    // Retrieve the id of this clone. It should be same as before
    ASSERT_OK(cloud_db->GetDbIdentity(dbid));
    ASSERT_EQ(newdb1_dbid, dbid);

    ASSERT_EQ(3, GetSSTFilesClone("db_ephemeral").size());

    // verify that a key written to the ephemeral db still exists
    ASSERT_OK(cloud_db->Get(ReadOptions(), "zip", &value));
    ASSERT_EQ(value, "94087");

    // verify that keys written to the main db after the ephemeral
    // was clones do not appear in the ephemeral db.
    ASSERT_NOK(cloud_db->Get(ReadOptions(), "Key1", &value));
    ASSERT_NOK(cloud_db->Get(ReadOptions(), "Key2", &value));
  }
}

// This test is performed in a rare race condition where ephemral clone is
// started after durable clone upload its CLOUDMANIFEST but before it uploads
// one of the MANIFEST. In this case, we want to verify that ephemeral clone is
// able to reinitialize instead of crash looping.
TEST_F(CloudTest, EphemeralOnCorruptedDB) {
  cloud_fs_options_.keep_local_sst_files = true;
  cloud_fs_options_.resync_on_open = true;
  options_.level0_file_num_compaction_trigger = 100;  // never compact

  OpenDB();

  std::vector<std::string> files;
  base_env_->GetChildren(dbname_, &files);

  // Get the MANIFEST file
  std::string manifest_file_name;
  for (const auto& file_name : files) {
    if (file_name.rfind("MANIFEST", 0) == 0) {
      manifest_file_name = file_name;
      break;
    }
  }

  ASSERT_FALSE(manifest_file_name.empty());

  // Delete MANIFEST file from S3 bucket.
  // This is to simulate the scenario where CLOUDMANIFEST is uploaded, but
  // MANIFEST is not yet uploaded from the durable shard.
  ASSERT_NE(aenv_.get(), nullptr);
  GetCloudFileSystem()->GetStorageProvider()->DeleteCloudObject(
      GetCloudFileSystem()->GetSrcBucketName(),
      GetCloudFileSystem()->GetSrcObjectPath() + "/" + manifest_file_name);

  // Ephemeral clone should fail.
  std::unique_ptr<DBCloud> clone_db;
  std::unique_ptr<Env> env;
  Status st = CloneDB("clone1", "", "", &clone_db, &env);
  ASSERT_TRUE(st.IsCorruption());

  // Put the MANIFEST file back
  GetCloudFileSystem()->GetStorageProvider()->PutCloudObject(
      dbname_ + "/" + manifest_file_name,
      GetCloudFileSystem()->GetSrcBucketName(),
      GetCloudFileSystem()->GetSrcObjectPath() + "/" + manifest_file_name);

  // Try one more time. This time it should succeed.
  clone_db.reset();
  env.reset();
  st = CloneDB("clone1", "", "", &clone_db, &env);
  ASSERT_OK(st);

  clone_db->Close();
  CloseDB();
}

//
// Test Ephemeral clones with resyncOnOpen mode.
// In this mode, every open of the ephemeral clone db causes its
// data to be resynced with the master db.
//
TEST_F(CloudTest, EphemeralResync) {
  cloud_fs_options_.keep_local_sst_files = true;
  cloud_fs_options_.resync_on_open = true;
  options_.level0_file_num_compaction_trigger = 100;  // never compact

  // Create a primary DB with two files
  OpenDB();
  std::string value;
  std::string newdb1_dbid;
  std::set<uint64_t> cloud_files;
  ASSERT_OK(db_->Put(WriteOptions(), "Name", "dhruba"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Put(WriteOptions(), "Hello2", "borthakur"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  CloseDB();
  ASSERT_EQ(2, GetSSTFiles(dbname_).size());

  // Reopen the same database in ephemeral mode by cloning the original.
  // Do not destroy the local dir. Writes to this db does not make it back
  // to any cloud storage.
  {
    std::unique_ptr<Env> env;
    std::unique_ptr<DBCloud> cloud_db;
    CloneDB("db_ephemeral", "", "", &cloud_db, &env);

    // Retrieve the id of the first reopen
    ASSERT_OK(cloud_db->GetDbIdentity(newdb1_dbid));

    // verify that we still have two sst files
    ASSERT_EQ(2, GetSSTFilesClone("db_ephemeral").size());

    ASSERT_OK(cloud_db->Get(ReadOptions(), "Name", &value));
    ASSERT_EQ(value, "dhruba");
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello2", &value));
    ASSERT_EQ(value, "borthakur");

    // Write one more record.
    // There should be 3 local sst files in the ephemeral db.
    ASSERT_OK(cloud_db->Put(WriteOptions(), "zip", "94087"));
    ASSERT_OK(cloud_db->Flush(FlushOptions()));
    ASSERT_EQ(3, GetSSTFilesClone("db_ephemeral").size());

    // check that cloud files did not get updated
    ASSERT_OK(GetCloudLiveFilesSrc(&cloud_files));
    ASSERT_EQ(2, cloud_files.size());
    cloud_files.clear();
  }

  // reopen main db and write two more records to it
  OpenDB();
  ASSERT_EQ(2, GetSSTFiles(dbname_).size());

  // write two more records to it.
  ASSERT_OK(db_->Put(WriteOptions(), "Key1", "onlyInMainDB"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Put(WriteOptions(), "Key2", "onlyInMainDB"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(4, GetSSTFiles(dbname_).size());
  CloseDB();
  ASSERT_OK(GetCloudLiveFilesSrc(&cloud_files));
  ASSERT_EQ(4, cloud_files.size());
  cloud_files.clear();

  // At this point, the main db has 4 files while the ephemeral
  // database has diverged earlier with 3 local files.
  // Reopen the ephemeral db with resync_on_open flag.
  // This means that earlier updates to the ephemeral db are lost.
  // It also means that the most latest updates in the master db
  // are reflected in the newly opened ephemeral database.
  {
    std::unique_ptr<Env> env;
    std::unique_ptr<DBCloud> cloud_db;
    std::string dbid;
    options_.info_log = nullptr;
    CreateLoggerFromOptions(clone_dir_ + "/db_ephemeral", options_,
                            &options_.info_log);

    CloneDB("db_ephemeral", "", "", &cloud_db, &env);

    // Retrieve the id of this clone. It should be same as before
    ASSERT_OK(cloud_db->GetDbIdentity(dbid));
    ASSERT_EQ(newdb1_dbid, dbid);

    // verify that a key written to the ephemeral db does not exist
    ASSERT_NOK(cloud_db->Get(ReadOptions(), "zip", &value));

    // verify that keys written to the main db after the ephemeral
    // was clones appear in the ephemeral db.
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Key1", &value));
    ASSERT_EQ(value, "onlyInMainDB");
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Key2", &value));
    ASSERT_EQ(value, "onlyInMainDB");
  }
}

TEST_F(CloudTest, CheckpointToCloud) {
  cloud_fs_options_.keep_local_sst_files = true;
  options_.level0_file_num_compaction_trigger = 100;  // never compact

  // Pre-create the bucket.
  CreateCloudEnv();
  aenv_.reset();

  // S3 is eventual consistency.
  std::this_thread::sleep_for(std::chrono::seconds(1));

  auto checkpoint_bucket = cloud_fs_options_.dest_bucket;

  cloud_fs_options_.src_bucket = BucketOptions();
  cloud_fs_options_.dest_bucket = BucketOptions();

  // Create a DB with two files
  OpenDB();
  ASSERT_OK(db_->Put(WriteOptions(), "a", "b"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Put(WriteOptions(), "c", "d"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ASSERT_OK(
      db_->CheckpointToCloud(checkpoint_bucket, CheckpointToCloudOptions()));

  ASSERT_EQ(2, GetSSTFiles(dbname_).size());
  CloseDB();

  DestroyDir(dbname_);

  cloud_fs_options_.src_bucket = checkpoint_bucket;

  OpenDB();
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "a", &value));
  ASSERT_EQ(value, "b");
  ASSERT_OK(db_->Get(ReadOptions(), "c", &value));
  ASSERT_EQ(value, "d");
  CloseDB();

  GetCloudFileSystem()->GetStorageProvider()->EmptyBucket(
      checkpoint_bucket.GetBucketName(), checkpoint_bucket.GetObjectPath());
}

// Basic test to copy object within S3.
TEST_F(CloudTest, CopyObjectTest) {
  CreateCloudEnv();

  // We need to open an empty DB in order for epoch to work.
  OpenDB();

  std::string content = "This is a test file";
  std::string fname = dbname_ + "/100000.sst";
  std::string dst_fname = dbname_ + "/200000.sst";

  {
    std::unique_ptr<FSWritableFile> writableFile;
    aenv_->GetFileSystem()->NewWritableFile(fname, kFileOptions, &writableFile,
                                            kDbg);
    writableFile->Append(content, kIOOptions, kDbg);
    writableFile->Fsync(kIOOptions, kDbg);
  }

  auto st = GetCloudFileSystem()->GetStorageProvider()->CopyCloudObject(
      GetCloudFileSystem()->GetSrcBucketName(),
      GetCloudFileSystem()->RemapFilename(fname),
      GetCloudFileSystem()->GetSrcBucketName(), dst_fname);
  ASSERT_OK(st);

  {
    std::unique_ptr<CloudStorageReadableFile> readableFile;
    st = GetCloudFileSystem()->GetStorageProvider()->NewCloudReadableFile(
        GetCloudFileSystem()->GetSrcBucketName(), dst_fname, kFileOptions,
        &readableFile, kDbg);
    ASSERT_OK(st);

    char scratch[100];
    Slice result;
    std::unique_ptr<FSSequentialFile> sequentialFile(readableFile.release());
    st = sequentialFile->Read(100, kIOOptions, &result, scratch, kDbg);
    ASSERT_OK(st);
    ASSERT_EQ(19, result.size());
    ASSERT_EQ(result, Slice(content));
  }

  CloseDB();
}

//
// Verify that we can cache data from S3 in persistent cache.
//
TEST_F(CloudTest, PersistentCache) {
  std::string pcache = test::TmpDir() + "/persistent_cache";
  SetPersistentCache(pcache, 1);

  // Put one key-value
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_TRUE(value.compare("World") == 0);
  CloseDB();
  value.clear();

  // Reopen and validate
  OpenDB();
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_EQ(value, "World");
  CloseDB();
}

// This test create 2 DBs that shares a block cache. Ensure that reads from one
// DB do not get the values from the other DB.
TEST_F(CloudTest, SharedBlockCache) {
  cloud_fs_options_.keep_local_sst_files = false;

  // Share the block cache.
  BlockBasedTableOptions bbto;
  bbto.block_cache = NewLRUCache(10 * 1024 * 1024);
  bbto.format_version = 4;
  options_.table_factory.reset(NewBlockBasedTableFactory(bbto));

  OpenDB();

  std::unique_ptr<Env> clone_env;
  std::unique_ptr<DBCloud> clone_db;
  CloneDB("newdb1", cloud_fs_options_.src_bucket.GetBucketName(),
          cloud_fs_options_.src_bucket.GetObjectPath() + "-clone", &clone_db,
          &clone_env, false /* force_keep_local_on_invalid_dest_bucket */);

  // Flush the first DB.
  db_->Put(WriteOptions(), "db", "original");
  db_->Flush(FlushOptions());

  // Flush the second DB.
  clone_db->Put(WriteOptions(), "db", "clone");
  clone_db->Flush(FlushOptions());

  std::vector<LiveFileMetaData> file_metadatas;
  db_->GetLiveFilesMetaData(&file_metadatas);
  ASSERT_EQ(1, file_metadatas.size());

  file_metadatas.clear();
  clone_db->GetLiveFilesMetaData(&file_metadatas);
  ASSERT_EQ(1, file_metadatas.size());

  std::string value;
  clone_db->Get(ReadOptions(), "db", &value);
  ASSERT_EQ("clone", value);

  db_->Get(ReadOptions(), "db", &value);
  ASSERT_EQ("original", value);

  // Cleanup
  clone_db->Close();
  CloseDB();
  auto* clone_cloud_fs =
      dynamic_cast<CloudFileSystem*>(clone_env->GetFileSystem().get());
  clone_cloud_fs->GetStorageProvider()->EmptyBucket(
      cloud_fs_options_.src_bucket.GetBucketName(),
      cloud_fs_options_.src_bucket.GetObjectPath() + "-clone");
}

TEST_F(CloudTest, FindLiveFilesFetchManifestTest) {
  OpenDB();
  ASSERT_OK(db_->Put({}, "a", "1"));
  ASSERT_OK(db_->Flush({}));
  CloseDB();

  DestroyDir(dbname_);

  // recreate cloud env, which points to the same bucket and objectpath
  CreateCloudEnv();

  std::vector<std::string> live_sst_files;
  std::string manifest_file;

  // fetch and load CloudManifest
  ASSERT_OK(GetCloudFileSystem()->PreloadCloudManifest(dbname_));

  // manifest file will be fetched to local db
  ASSERT_OK(GetCloudFileSystem()->FindAllLiveFiles(dbname_, &live_sst_files,
                                                   &manifest_file));
  EXPECT_EQ(live_sst_files.size(), 1);
}

TEST_F(CloudTest, FileModificationTimeTest) {
  OpenDB();
  ASSERT_OK(db_->Put({}, "a", "1"));
  ASSERT_OK(db_->Flush({}));
  std::vector<std::string> live_sst_files;
  std::string manifest_file;
  ASSERT_OK(GetCloudFileSystem()->FindAllLiveFiles(dbname_, &live_sst_files,
                                                   &manifest_file));
  uint64_t modtime1;
  ASSERT_OK(aenv_->GetFileSystem()->GetFileModificationTime(
      dbname_ + pathsep + manifest_file, kIOOptions, &modtime1, kDbg));
  CloseDB();
  DestroyDir(dbname_);
  // don't roll cloud manifest so that manifest file epoch is not updated
  cloud_fs_options_.roll_cloud_manifest_on_open = false;
  OpenDB();
  uint64_t modtime2;
  ASSERT_OK(aenv_->GetFileSystem()->GetFileModificationTime(
      dbname_ + pathsep + manifest_file, kIOOptions, &modtime2, kDbg));
  // we read local file modification time, so the second time we open db, the
  // modification time is changed
  EXPECT_GT(modtime2, modtime1);
}

TEST_F(CloudTest, EmptyCookieTest) {
  // By default cookie is empty
  OpenDB();
  auto* cfs_impl = GetCloudFileSystemImpl();
  auto cloud_manifest_file = cfs_impl->CloudManifestFile(dbname_);
  EXPECT_EQ(basename(cloud_manifest_file), "CLOUDMANIFEST");
  CloseDB();
}

TEST_F(CloudTest, NonEmptyCookieTest) {
  cloud_fs_options_.new_cookie_on_open = "000001";
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_EQ(value, "World");

  auto cloud_manifest_file =
      MakeCloudManifestFile(dbname_, cloud_fs_options_.new_cookie_on_open);
  ASSERT_OK(GetCloudFileSystem()->GetStorageProvider()->ExistsCloudObject(
      GetCloudFileSystem()->GetSrcBucketName(), cloud_manifest_file));
  EXPECT_EQ(basename(cloud_manifest_file), "CLOUDMANIFEST-000001");
  CloseDB();
  DestroyDir(dbname_);
  cloud_fs_options_.cookie_on_open = "000001";
  cloud_fs_options_.new_cookie_on_open = "000001";
  OpenDB();

  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_EQ(value, "World");
  ASSERT_OK(GetCloudFileSystem()->GetStorageProvider()->ExistsCloudObject(
      GetCloudFileSystem()->GetSrcBucketName(), cloud_manifest_file));
  EXPECT_EQ(basename(cloud_manifest_file), "CLOUDMANIFEST-000001");
  CloseDB();
}

// Verify that live sst files are the same after applying cloud manifest delta
TEST_F(CloudTest, LiveFilesConsistentAfterApplyCloudManifestDeltaTest) {
  cloud_fs_options_.cookie_on_open = "1";
  cloud_fs_options_.new_cookie_on_open = "1";
  OpenDB();

  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  std::vector<std::string> live_sst_files1;
  std::string manifest_file1;
  ASSERT_OK(GetCloudFileSystem()->FindAllLiveFiles(dbname_, &live_sst_files1,
                                                   &manifest_file1));

  std::string new_cookie = "2";
  std::string new_epoch = "dca7f3e19212c4b3";
  auto delta = CloudManifestDelta{GetDBImpl()->GetNextFileNumber(), new_epoch};
  ASSERT_OK(
      GetCloudFileSystemImpl()->RollNewCookie(dbname_, new_cookie, delta));
  bool applied = false;
  ASSERT_OK(GetCloudFileSystemImpl()->ApplyCloudManifestDelta(delta, &applied));
  ASSERT_TRUE(applied);

  std::vector<std::string> live_sst_files2;
  std::string manifest_file2;
  ASSERT_OK(GetCloudFileSystem()->FindAllLiveFiles(dbname_, &live_sst_files2,
                                                   &manifest_file2));

  EXPECT_EQ(live_sst_files1, live_sst_files2);
  EXPECT_NE(manifest_file1, manifest_file2);

  CloseDB();
}


// After calling `ApplyCloudManifestDelta`, writes should be persisted in
// sst files only visible in new Manifest
TEST_F(CloudTest, WriteAfterUpdateCloudManifestArePersistedInNewEpoch) {
  cloud_fs_options_.cookie_on_open = "1";
  cloud_fs_options_.new_cookie_on_open = "1";
  OpenDB();
  ASSERT_OK(db_->Put(WriteOptions(), "Hello1", "world1"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  std::string new_cookie = "2";
  std::string new_epoch = "dca7f3e19212c4b3";

  auto delta = CloudManifestDelta{GetDBImpl()->GetNextFileNumber(), new_epoch};
  ASSERT_OK(
      GetCloudFileSystemImpl()->RollNewCookie(dbname_, new_cookie, delta));
  bool applied = false;
  ASSERT_OK(GetCloudFileSystemImpl()->ApplyCloudManifestDelta(delta, &applied));
  ASSERT_TRUE(applied);
  GetDBImpl()->NewManifestOnNextUpdate();

  // following writes are not visible for old cookie
  ASSERT_OK(db_->Put(WriteOptions(), "Hello2", "world2"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // reopen with cookie = 1, new updates after rolling are not visible
  CloseDB();
  cloud_fs_options_.cookie_on_open = "1";
  cloud_fs_options_.new_cookie_on_open = "1";
  cloud_fs_options_.dest_bucket.SetBucketName("");
  cloud_fs_options_.dest_bucket.SetObjectPath("");
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "Hello1", &value));
  EXPECT_EQ(value, "world1");
  EXPECT_NOK(db_->Get(ReadOptions(), "Hello2", &value));
  CloseDB();

  // reopen with cookie = 2, new updates should still be visible
  CloseDB();
  cloud_fs_options_.cookie_on_open = "2";
  cloud_fs_options_.new_cookie_on_open = "2";
  OpenDB();
  ASSERT_OK(db_->Get(ReadOptions(), "Hello1", &value));
  EXPECT_EQ(value, "world1");
  ASSERT_OK(db_->Get(ReadOptions(), "Hello2", &value));
  EXPECT_EQ(value, "world2");
  CloseDB();

  // Make sure that the changes in cloud are correct
  DestroyDir(dbname_);
  cloud_fs_options_.cookie_on_open = "2";
  cloud_fs_options_.new_cookie_on_open = "2";
  OpenDB();
  ASSERT_OK(db_->Get(ReadOptions(), "Hello1", &value));
  EXPECT_EQ(value, "world1");
  ASSERT_OK(db_->Get(ReadOptions(), "Hello2", &value));
  EXPECT_EQ(value, "world2");
  CloseDB();
}

// Test various cases of crashing in the middle during CloudManifestSwitch
TEST_F(CloudTest, CMSwitchCrashInMiddleTest) {
  cloud_fs_options_.roll_cloud_manifest_on_open = false;
  cloud_fs_options_.cookie_on_open = "1";

  SyncPoint::GetInstance()->SetCallBack(
      "CloudFileSystemImpl::RollNewCookie:AfterManifestCopy", [](void* arg) {
        // Simulate the case of crash in the middle of
        // RollNewCookie
        *reinterpret_cast<Status*>(arg) = Status::Aborted("Aborted");
      });

  SyncPoint::GetInstance()->EnableProcessing();

  // case 1: Crash in the middle of updating local manifest files
  // our guarantee: no CLOUDMANIFEST_new_cookie locally and remotely
  OpenDB();

  std::string new_cookie = "2";
  std::string new_epoch = "dca7f3e19212c4b3";

  ASSERT_NOK(GetCloudFileSystemImpl()->RollNewCookie(
      dbname_, new_cookie,
      CloudManifestDelta{GetDBImpl()->GetNextFileNumber(), new_epoch}));

  CloseDB();

  EXPECT_NOK(base_env_->FileExists(MakeCloudManifestFile(dbname_, new_cookie)));

  // case 2: Crash in the middle of uploading local manifest files
  // our guarantee: no CLOUDMANFIEST_cookie remotely
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "CloudFileSystemImpl::UploadManifest:AfterUploadManifest", [](void* arg) {
        // Simulate the case of crashing in the middle of
        // UploadManifest
        *reinterpret_cast<Status*>(arg) = Status::Aborted("Aborted");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  OpenDB();

  auto delta = CloudManifestDelta{GetDBImpl()->GetNextFileNumber(), new_epoch};
  ASSERT_NOK(
      GetCloudFileSystemImpl()->RollNewCookie(dbname_, new_cookie, delta));

  ASSERT_NOK(GetCloudFileSystemImpl()->GetStorageProvider()->ExistsCloudObject(
      GetCloudFileSystemImpl()->GetDestBucketName(),
      MakeCloudManifestFile(GetCloudFileSystemImpl()->GetDestObjectPath(),
                            new_cookie)));

  CloseDB();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(CloudTest, RollNewEpochTest) {
  OpenDB();
  auto epoch1 = GetCloudFileSystemImpl()->GetCloudManifest()->GetCurrentEpoch();
  EXPECT_OK(GetCloudFileSystemImpl()->GetStorageProvider()->ExistsCloudObject(
      GetCloudFileSystemImpl()->GetDestBucketName(),
      ManifestFileWithEpoch(GetCloudFileSystemImpl()->GetDestObjectPath(),
                            epoch1)));
  CloseDB();
  OpenDB();
  auto epoch2 = GetCloudFileSystemImpl()->GetCloudManifest()->GetCurrentEpoch();
  EXPECT_OK(GetCloudFileSystemImpl()->GetStorageProvider()->ExistsCloudObject(
      GetCloudFileSystemImpl()->GetDestBucketName(),
      ManifestFileWithEpoch(GetCloudFileSystemImpl()->GetDestObjectPath(),
                            epoch2)));
  CloseDB();
  EXPECT_NE(epoch1, epoch2);
}

// Test that we can rollback to empty cookie
TEST_F(CloudTest, CookieBackwardsCompatibilityTest) {
  cloud_fs_options_.resync_on_open = true;
  cloud_fs_options_.roll_cloud_manifest_on_open = true;

  cloud_fs_options_.cookie_on_open = "";
  cloud_fs_options_.new_cookie_on_open = "1";
  OpenDB();
  ASSERT_OK(db_->Put({}, "k1", "v1"));
  ASSERT_OK(db_->Flush({}));
  CloseDB();

  // switch cookie
  cloud_fs_options_.cookie_on_open = "1";
  cloud_fs_options_.new_cookie_on_open = "2";
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Get({}, "k1", &value));
  EXPECT_EQ(value, "v1");

  ASSERT_OK(db_->Put({}, "k2", "v2"));
  ASSERT_OK(db_->Flush({}));
  CloseDB();

  // switch back to empty cookie
  cloud_fs_options_.cookie_on_open = "2";
  cloud_fs_options_.new_cookie_on_open = "";
  OpenDB();
  ASSERT_OK(db_->Get({}, "k1", &value));
  EXPECT_EQ(value, "v1");

  ASSERT_OK(db_->Get({}, "k2", &value));
  EXPECT_EQ(value, "v2");
  CloseDB();

  // open with both cookies being empty
  cloud_fs_options_.cookie_on_open = "";
  cloud_fs_options_.new_cookie_on_open = "";
  OpenDB();
  ASSERT_OK(db_->Get({}, "k1", &value));
  EXPECT_EQ(value, "v1");

  ASSERT_OK(db_->Get({}, "k2", &value));
  EXPECT_EQ(value, "v2");
  CloseDB();
}

// Test that once we switch to non empty cookie, we can rollback to
// empty cookie immediately and files are not deleted mistakenly
TEST_F(CloudTest, CookieRollbackTest) {
  cloud_fs_options_.resync_on_open = true;

  // Create CLOUDMANFIEST with empty cookie
  cloud_fs_options_.cookie_on_open = "";
  cloud_fs_options_.new_cookie_on_open = "";

  OpenDB();
  ASSERT_OK(db_->Put({}, "k1", "v1"));
  ASSERT_OK(db_->Flush({}));
  CloseDB();

  // Switch to cookie 1
  cloud_fs_options_.cookie_on_open = "";
  cloud_fs_options_.new_cookie_on_open = "1";
  OpenDB();
  CloseDB();

  // rollback to empty cookie
  cloud_fs_options_.cookie_on_open = "1";
  cloud_fs_options_.new_cookie_on_open = "";

  // Setup syncpoint so that file deletion jobs are executed after we open db,
  // but before we close db. This is to make sure that file deletion job
  // won't delete files that are created when we open db (e.g., CLOUDMANIFEST
  // files and MANIFEST files) and we can catch it in test if something is
  // messed up
  SyncPoint::GetInstance()->LoadDependency({
      {// only trigger file deletion job after db open
       "CloudTest::CookieRollbackTest:AfterOpenDB",
       "CloudSchedulerImpl::DoWork:BeforeGetJob"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  OpenDB();
  TEST_SYNC_POINT("CloudTest::CookieRollbackTest:AfterOpenDB");
  // File deletion jobs are only triggered after this. Once it's triggered,
  // the job deletion queue is not empty

  std::string v;
  ASSERT_OK(db_->Get({}, "k1", &v));
  EXPECT_EQ(v, "v1");

  // wait until no scheduled jobs for current local cloud env
  // After waiting, we know for sure that all the deletion jobs scheduled
  // when opening db are executed
  WaitUntilNoScheduledJobs();
  CloseDB();

  SyncPoint::GetInstance()->DisableProcessing();

  // reopen with empty cookie
  cloud_fs_options_.cookie_on_open = "";
  cloud_fs_options_.new_cookie_on_open = "";
  OpenDB();
  ASSERT_OK(db_->Get({}, "k1", &v));
  EXPECT_EQ(v, "v1");
  CloseDB();
}

TEST_F(CloudTest, NewCookieOnOpenTest) {
  cloud_fs_options_.cookie_on_open = "1";

  // when opening new db, only new_cookie_on_open is used as CLOUDMANIFEST suffix
  cloud_fs_options_.new_cookie_on_open = "2";
  OpenDB();
  ASSERT_OK(db_->Put({}, "k1", "v1"));
  ASSERT_OK(db_->Flush({}));

  ASSERT_NOK(GetCloudFileSystem()->GetStorageProvider()->ExistsCloudObject(
      GetCloudFileSystem()->GetSrcBucketName(),
      MakeCloudManifestFile(dbname_, "1")));
  // CLOUDMANIFEST-2 should exist since this is a new db
  ASSERT_OK(GetCloudFileSystem()->GetStorageProvider()->ExistsCloudObject(
      GetCloudFileSystem()->GetSrcBucketName(),
      MakeCloudManifestFile(dbname_, "2")));
  CloseDB();

  // reopen and switch cookie
  cloud_fs_options_.cookie_on_open = "2";
  cloud_fs_options_.new_cookie_on_open = "3";
  OpenDB();
  // CLOUDMANIFEST-3 is the new cloud manifest
  ASSERT_OK(GetCloudFileSystem()->GetStorageProvider()->ExistsCloudObject(
      GetCloudFileSystem()->GetSrcBucketName(),
      MakeCloudManifestFile(dbname_, "3")));

  std::string value;
  ASSERT_OK(db_->Get({}, "k1", &value));
  EXPECT_EQ(value, "v1");

  ASSERT_OK(db_->Put({}, "k2", "v2"));
  ASSERT_OK(db_->Flush({}));
  CloseDB();

  // reopen DB, but don't switch CLOUDMANIFEST
  cloud_fs_options_.cookie_on_open = "3";
  cloud_fs_options_.new_cookie_on_open = "3";
  OpenDB();
  ASSERT_OK(db_->Get({}, "k2", &value));
  EXPECT_EQ(value, "v2");
  CloseDB();
}

// Test invisible file deletion when db is opened.
TEST_F(CloudTest, InvisibleFileDeletionOnDBOpenTest) {
  std::string cookie1 = "", cookie2 = "-1-1";
  cloud_fs_options_.keep_local_sst_files = true;

  // opening with cookie1
  OpenDB();
  ASSERT_OK(db_->Put({}, "k1", "v1"));
  ASSERT_OK(db_->Flush({}));
  std::vector<std::string> cookie1_sst_files;
  std::string cookie1_manifest_file;
  ASSERT_OK(GetCloudFileSystem()->FindAllLiveFiles(dbname_, &cookie1_sst_files,
                                                   &cookie1_manifest_file));
  ASSERT_EQ(cookie1_sst_files.size(), 1);
  CloseDB();

  // MANIFEST file path of cookie1
  auto cookie1_manifest_filepath = dbname_ + pathsep + cookie1_manifest_file;
  // CLOUDMANIFEST file path of cookie1
  auto cookie1_cm_filepath =
      MakeCloudManifestFile(dbname_, cloud_fs_options_.cookie_on_open);
  // sst file path of cookie1
  auto cookie1_sst_filepath = dbname_ + pathsep + cookie1_sst_files[0];

  // opening with cookie1 and switch to cookie2
  cloud_fs_options_.cookie_on_open = cookie1;
  cloud_fs_options_.new_cookie_on_open = cookie2;
  OpenDB();
  ASSERT_OK(db_->Put({}, "k2", "v2"));
  ASSERT_OK(db_->Flush({}));
  // CM/M/sst files of cookie1 won't be deleted
  for (auto path :
       {cookie1_cm_filepath, cookie1_manifest_filepath, cookie1_sst_filepath}) {
    EXPECT_OK(GetCloudFileSystem()->GetBaseFileSystem()->FileExists(
        path, kIOOptions, kDbg));
    EXPECT_OK(GetCloudFileSystem()->GetStorageProvider()->ExistsCloudObject(
        GetCloudFileSystem()->GetSrcBucketName(), path));
  }

  std::vector<std::string> cookie2_sst_files;
  std::string cookie2_manifest_file;
  ASSERT_OK(GetCloudFileSystem()->FindAllLiveFiles(dbname_, &cookie2_sst_files,
                                                   &cookie2_manifest_file));
  ASSERT_EQ(cookie2_sst_files.size(), 2);
  CloseDB();

  // MANIFEST file path of cookie2
  auto cookie2_manifest_filepath = dbname_ + pathsep + cookie2_manifest_file;
  // CLOUDMANIFEST file path of cookie2
  auto cookie2_cm_filepath =
      MakeCloudManifestFile(dbname_, cloud_fs_options_.new_cookie_on_open);
  // find sst file path of cookie2
  auto cookie2_sst_filepath = dbname_ + pathsep + cookie2_sst_files[0];
  if (cookie2_sst_filepath == cookie1_sst_filepath) {
    cookie2_sst_filepath = dbname_ + pathsep + cookie2_sst_files[1];
  }

  // Now we reopen db with cookie1 to force deleting all files generated in
  // cookie2

  // number of file deletion jobs is executed
  std::atomic_int num_job_executed(0);

  // Syncpoint callback so that we can check when the files are actually
  // deleted(which is async)
  SyncPoint::GetInstance()->SetCallBack(
      "LocalCloudScheduler::ScheduleJob:AfterEraseJob", [&](void* /*arg*/) {
        num_job_executed++;
        if (num_job_executed == 3) {
          // CM/M/SST files of cookie2 are deleted in s3
          for (auto path : {cookie2_manifest_filepath, cookie2_cm_filepath,
                            cookie2_sst_filepath}) {
            EXPECT_NOK(
                GetCloudFileSystem()->GetStorageProvider()->ExistsCloudObject(
                    GetCloudFileSystem()->GetSrcBucketName(), path));
          }
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // reopen db with cookie1 will force all files generated in cookie2 to be
  // deleted
  cloud_fs_options_.cookie_on_open = cookie1;
  cloud_fs_options_.new_cookie_on_open = cookie1;
  OpenDB();
  // local obsolete CM/M/SST files will be deleted immediately
  // files in cloud will be deleted later (checked in the callback)
  for (auto path :
       {cookie2_cm_filepath, cookie2_manifest_filepath, cookie2_sst_filepath}) {
    EXPECT_NOK(GetCloudFileSystem()->GetBaseFileSystem()->FileExists(
        path, kIOOptions, kDbg))
        << path;
  }
  CloseDB();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  WaitUntilNoScheduledJobs();
  // Make sure that these files are indeed deleted
  EXPECT_EQ(num_job_executed, 3);
}

// Verify that when opening with `delete_cloud_invisible_files_on_open`, local
// files will be deleted while cloud files will be kept
TEST_F(CloudTest, DisableInvisibleFileDeletionOnOpenTest) {
  std::string cookie1 = "", cookie2 = "1";
  cloud_fs_options_.keep_local_sst_files = true;
  cloud_fs_options_.cookie_on_open = cookie1;
  cloud_fs_options_.new_cookie_on_open = cookie1;

  // opening with cookie1
  OpenDB();
  // generate sst file with cookie1
  ASSERT_OK(db_->Put({}, "k1", "v1"));
  ASSERT_OK(db_->Flush({}));

  std::vector<std::string> cookie1_sst_files;
  std::string cookie1_manifest_file;
  ASSERT_OK(GetCloudFileSystem()->FindAllLiveFiles(dbname_, &cookie1_sst_files,
                                                   &cookie1_manifest_file));
  ASSERT_EQ(cookie1_sst_files.size(), 1);

  auto cookie1_manifest_filepath = dbname_ + pathsep + cookie1_manifest_file;
  auto cookie1_cm_filepath =
      MakeCloudManifestFile(dbname_, cloud_fs_options_.cookie_on_open);
  auto cookie1_sst_filepath = dbname_ + pathsep + cookie1_sst_files[0];

  ASSERT_OK(SwitchToNewCookie(cookie2));

  // generate sst file with cookie2
  ASSERT_OK(db_->Put({}, "k2", "v2"));
  ASSERT_OK(db_->Flush({}));

  std::vector<std::string> cookie2_sst_files;
  std::string cookie2_manifest_file;

  ASSERT_OK(GetCloudFileSystem()->FindAllLiveFiles(dbname_, &cookie2_sst_files,
                                                   &cookie2_manifest_file));
  ASSERT_EQ(cookie2_sst_files.size(), 2);

  // exclude cookie1_sst_files from cookie2_sst_files
  std::sort(cookie2_sst_files.begin(), cookie2_sst_files.end());
  std::set_difference(cookie2_sst_files.begin(), cookie2_sst_files.end(),
                      cookie1_sst_files.begin(), cookie1_sst_files.end(),
                      cookie2_sst_files.begin());
  cookie2_sst_files.resize(1);

  auto cookie2_manifest_filepath = dbname_ + pathsep + cookie2_manifest_file;
  auto cookie2_cm_filepath =
      MakeCloudManifestFile(dbname_, cookie2);
  auto cookie2_sst_filepath = dbname_ + pathsep + cookie2_sst_files[0];

  CloseDB();

  // reopen with cookie1 = "". cookie2 sst files are not visible
  cloud_fs_options_.delete_cloud_invisible_files_on_open = false;
  OpenDB();
  // files from cookie2 are deleted locally but exists in s3
  for (auto path: {cookie2_cm_filepath, cookie2_manifest_filepath, cookie2_sst_filepath}) {
    EXPECT_NOK(GetCloudFileSystem()->GetBaseFileSystem()->FileExists(
        path, kIOOptions, kDbg));
    EXPECT_OK(GetCloudFileSystem()->GetStorageProvider()->ExistsCloudObject(
        GetCloudFileSystem()->GetSrcBucketName(), path));
  }
  std::string value;
  EXPECT_OK(db_->Get({}, "k1", &value));
  EXPECT_NOK(db_->Get({}, "k2", &value));
  CloseDB();

  cloud_fs_options_.cookie_on_open = cookie2;
  cloud_fs_options_.new_cookie_on_open = cookie2;
  // reopen with cookie2 also works since it will fetch files from s3 directly
  OpenDB();
  EXPECT_OK(db_->Get({}, "k1", &value));
  EXPECT_OK(db_->Get({}, "k2", &value));
  CloseDB();
}

TEST_F(CloudTest, DisableObsoleteFileDeletionOnOpenTest) {
  // Generate a few obsolete files first
  options_.num_levels = 3;
  options_.level0_file_num_compaction_trigger = 3;
  options_.write_buffer_size = 110 << 10;  // 110KB
  options_.arena_block_size = 4 << 10;
  options_.keep_log_file_num = 1;
  options_.use_options_file = false;
  // put wal files into one directory so that we don't need to count number of local
  // wal files
  options_.wal_dir = dbname_ + "/wal";
  cloud_fs_options_.keep_local_sst_files = true;
  // disable cm roll so that no new manifest files generated
  cloud_fs_options_.roll_cloud_manifest_on_open = false;

  WriteOptions wo;
  wo.disableWAL = true;
  OpenDB();
  ASSERT_OK(SwitchToNewCookie(""));
  db_->DisableFileDeletions();

  std::vector<LiveFileMetaData> files;

  ASSERT_OK(db_->Put(wo, "k1", "v1"));
  ASSERT_OK(db_->Flush({}));
  ASSERT_OK(db_->Put(wo, "k1", "v2"));
  ASSERT_OK(db_->Flush({}));
  db_->GetLiveFilesMetaData(&files);
  ASSERT_EQ(files.size(), 2);

  auto local_files = GetAllLocalFiles();
  // CM, MANIFEST1, MANIFEST2, CURRENT, IDENTITY, 2 sst files, wal directory
  EXPECT_EQ(local_files.size(), 9);

  ASSERT_OK(GetDBImpl()->TEST_CompactRange(0, nullptr, nullptr, nullptr, true));

  files.clear();
  db_->GetLiveFilesMetaData(&files);
  ASSERT_EQ(files.size(), 1);

  local_files = GetAllLocalFiles();
  // obsolete files are not deleted, also one extra sst files generated after compaction
  EXPECT_EQ(local_files.size(), 10);

  CloseDB();

  options_.disable_delete_obsolete_files_on_open = true;
  OpenDB();
  // obsolete files are not deleted
  EXPECT_EQ(GetAllLocalFiles().size(), 10);
  // obsolete files are deleted!
  db_->EnableFileDeletions();
  EXPECT_EQ(GetAllLocalFiles().size(), 8);
  CloseDB();
}

// Verify invisible CLOUDMANIFEST file deleteion
TEST_F(CloudTest, CloudManifestFileDeletionTest) {
  // create CLOUDMANIFEST file in s3
  cloud_fs_options_.cookie_on_open = "";
  cloud_fs_options_.new_cookie_on_open = "";
  OpenDB();
  CloseDB();

  // create CLOUDMANIFEST-1 file in s3
  cloud_fs_options_.cookie_on_open = "";
  cloud_fs_options_.new_cookie_on_open = "1";
  OpenDB();
  CloseDB();

  auto checkCloudManifestFileExistence = [&](std::vector<std::string> cookies) {
    for (auto cookie : cookies) {
      EXPECT_OK(
          GetCloudFileSystemImpl()->GetStorageProvider()->ExistsCloudObject(
              GetCloudFileSystemImpl()->GetDestBucketName(),
              MakeCloudManifestFile(
                  GetCloudFileSystemImpl()->GetDestObjectPath(), cookie)));
    }
  };

  // double check that the CM files are indeed created
  checkCloudManifestFileExistence({"", "1"});

  // set large file deletion delay so that files are not deleted immediately
  cloud_fs_options_.cloud_file_deletion_delay = std::chrono::hours(1);
  EXPECT_EQ(GetCloudFileSystemImpl()->TEST_NumScheduledJobs(), 0);

  // now we reopen the db with empty cookie_on_open and new_cookie_on_open =
  // "1". Double check that CLOUDMANIFEST-1 is not deleted!
  OpenDB();
  checkCloudManifestFileExistence({"", "1"});
  CloseDB();

  // switch to new cookie
  cloud_fs_options_.cookie_on_open = "1";
  cloud_fs_options_.new_cookie_on_open = "2";
  OpenDB();
  // double check that CLOUDMANIFEST is never deleted
  checkCloudManifestFileExistence({"", "1", "2"});
  CloseDB();
}

// verify that two writers with different cookies can write concurrently
TEST_F(CloudTest, TwoConcurrentWritersCookieNotEmpty) {
  auto firstDB = dbname_;
  auto secondDB = dbname_ + "-1";

  DBCloud *db1, *db2;
  Env *aenv1, *aenv2;

  auto openDB1 = [&] {
    dbname_ = firstDB;
    cloud_fs_options_.cookie_on_open = "1";
    cloud_fs_options_.new_cookie_on_open = "2";
    OpenDB();
    db1 = db_;
    db_ = nullptr;
    aenv1 = aenv_.release();
  };
  auto openDB1NoCookieSwitch = [&](const std::string& cookie) {
    dbname_ = firstDB;
    // when reopening DB1, we should set cookie_on_open = 2 to make sure
    // we are opening with the right CM/M files
    cloud_fs_options_.cookie_on_open = cookie;
    cloud_fs_options_.new_cookie_on_open = cookie;
    OpenDB();
    db1 = db_;
    db_ = nullptr;
    aenv1 = aenv_.release();
  };
  auto openDB2 = [&] {
    dbname_ = secondDB;
    cloud_fs_options_.cookie_on_open = "2";
    cloud_fs_options_.new_cookie_on_open = "3";
    OpenDB();
    db2 = db_;
    db_ = nullptr;
    aenv2 = aenv_.release();
  };
  auto openDB2NoCookieSwitch = [&](const std::string& cookie) {
    dbname_ = secondDB;
    // when reopening DB1, we should set cookie_on_open = 3 to make sure
    // we are opening with the right CM/M files
    cloud_fs_options_.cookie_on_open = cookie;
    cloud_fs_options_.new_cookie_on_open = cookie;
    OpenDB();
    db2 = db_;
    db_ = nullptr;
    aenv2 = aenv_.release();
  };
  auto closeDB1 = [&] {
    db_ = db1;
    aenv_.reset(aenv1);
    CloseDB();
  };
  auto closeDB2 = [&] {
    db_ = db2;
    aenv_.reset(aenv2);
    CloseDB();
  };

  openDB1();
  db1->Put({}, "k1", "v1");
  db1->Flush({});
  closeDB1();

  // cleanup memtable of db1 to make sure k1/v1 indeed exists in sst files
  DestroyDir(firstDB);
  openDB1NoCookieSwitch("2" /* cookie */);

  // opening DB2 and running concurrently
  openDB2();

  db1->Put({}, "k2", "v2");
  db1->Flush({});

  db2->Put({}, "k3", "v3");
  db2->Flush({});

  std::string v;
  ASSERT_OK(db1->Get({}, "k1", &v));
  EXPECT_EQ(v, "v1");
  ASSERT_OK(db2->Get({}, "k1", &v));
  EXPECT_EQ(v, "v1");

  ASSERT_OK(db1->Get({}, "k2", &v));
  EXPECT_EQ(v, "v2");
  // k2 is written in db1 after db2 is opened, so it's not visible by db2
  EXPECT_NOK(db2->Get({}, "k2", &v));

  // k3 is written in db2 after db1 is opened, so it's not visible by db1
  EXPECT_NOK(db1->Get({}, "k3", &v));
  ASSERT_OK(db2->Get({}, "k3", &v));
  EXPECT_EQ(v, "v3");

  closeDB1();
  closeDB2();

  // cleanup local state to make sure writes indeed exist in sst files
  DestroyDir(firstDB);
  DestroyDir(secondDB);

  // We can't reopen db with cookie=2 anymore, since that will remove all the
  // files for cookie=3. This is guaranteed since whenever we reopen db, we
  // always get the latest cookie from metadata store.
  openDB2NoCookieSwitch("3" /* cookie */);

  ASSERT_OK(db2->Get({}, "k1", &v));
  EXPECT_EQ(v, "v1");
  EXPECT_NOK(db2->Get({}, "k2", &v));
  ASSERT_OK(db2->Get({}, "k3", &v));
  EXPECT_EQ(v, "v3");
  closeDB2();
}

// if file deletion fails, db should still be reopend
TEST_F(CloudTest, FileDeletionFailureIgnoredTest) {
  std::string manifest_file_path;
  OpenDB();
  auto epoch = GetCloudFileSystemImpl()->GetCloudManifest()->GetCurrentEpoch();
  manifest_file_path = ManifestFileWithEpoch(dbname_, epoch);
  ASSERT_OK(db_->Put({}, "k1", "v1"));
  ASSERT_OK(db_->Flush({}));
  CloseDB();

  // bump the manifest epoch so that next time opening it, manifest file will be deleted
  OpenDB();
  CloseDB();

  // return error during file deletion
  SyncPoint::GetInstance()->SetCallBack(
      "CloudFileSystemImpl::DeleteLocalInvisibleFiles:AfterListLocalFiles",
      [](void* arg) {
        auto st = reinterpret_cast<Status*>(arg);
        *st =
            Status::Aborted("Manual abortion to simulate file listing failure");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  OpenDB();
  std::string v;
  ASSERT_OK(db_->Get({}, "k1", &v));
  EXPECT_EQ(v, "v1");
  // Due to the Aborted error we generated, the manifest file which should have
  // been deleted still exists.
  EXPECT_OK(GetCloudFileSystem()->GetBaseFileSystem()->FileExists(
      manifest_file_path, kIOOptions, kDbg));
  CloseDB();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // reopen the db should delete the obsolete manifest file after we cleanup syncpoint
  OpenDB();
  EXPECT_NOK(GetCloudFileSystem()->GetBaseFileSystem()->FileExists(
      manifest_file_path, kIOOptions, kDbg));
  CloseDB();
}

// verify that as long as CloudFileSystem is destructed, the file delection jobs
// waiting in the queue will be canceled
TEST_F(CloudTest, FileDeletionJobsCanceledWhenCloudEnvDestructed) {
  std::string manifest_file_path;
  OpenDB();
  auto epoch = GetCloudFileSystemImpl()->GetCloudManifest()->GetCurrentEpoch();
  manifest_file_path = ManifestFileWithEpoch(dbname_, epoch);
  CloseDB();

  // bump epoch of manifest file so next open will delete previous manifest file
  OpenDB();
  CloseDB();

  // Setup syncpoint dependency to prevent cloud scheduler from executing file
  // deletion job in the queue until CloudFileSystem is destructed
  SyncPoint::GetInstance()->LoadDependency(
      {{"CloudTest::FileDeletionJobsCanceledWhenCloudEnvDestructed:"
        "AfterCloudEnvDestruction",
        "CloudSchedulerImpl::DoWork:BeforeGetJob"}});
  SyncPoint::GetInstance()->EnableProcessing();
  OpenDB();
  CloseDB();

  // delete CloudFileSystem will cancel all file deletion jobs in the queue
  aenv_.reset();

  // jobs won't be executed until after this point. But the file deletion job
  // in the queue should have already been canceled
  TEST_SYNC_POINT(
      "CloudTest::FileDeletionJobsCanceledWhenCloudEnvDestructed:"
      "AfterCloudEnvDestruction");

  SyncPoint::GetInstance()->DisableProcessing();

  // recreate cloud env to check s3 file existence
  CreateCloudEnv();

  // wait for a while so that the rest uncanceled jobs are indeed executed by
  // cloud scheduler.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // the old manifest file is still there!
  EXPECT_OK(GetCloudFileSystem()->GetStorageProvider()->ExistsCloudObject(
      GetCloudFileSystem()->GetSrcBucketName(), manifest_file_path));

  // reopen db to delete the old manifest file
  OpenDB();
  EXPECT_NOK(GetCloudFileSystem()->GetStorageProvider()->ExistsCloudObject(
      GetCloudFileSystem()->GetSrcBucketName(), manifest_file_path));
  CloseDB();
}

// The failure case of opening a corrupted db which doesn't have MANIFEST file
TEST_F(CloudTest, OpenWithManifestMissing) {
  cloud_fs_options_.resync_on_open = true;
  OpenDB();
  auto epoch = GetCloudFileSystemImpl()->GetCloudManifest()->GetCurrentEpoch();
  CloseDB();

  // Remove the MANIFEST file from s3
  ASSERT_OK(GetCloudFileSystem()->GetStorageProvider()->DeleteCloudObject(
      GetCloudFileSystem()->GetSrcBucketName(),
      ManifestFileWithEpoch(GetCloudFileSystem()->GetSrcObjectPath(), epoch)));
  DestroyDir(dbname_);

  EXPECT_TRUE(checkOpen().IsCorruption());
}

// verify that ephemeral clone won't reference old sst file if it's reopened
// after sst file deletion on durable
// Ordering of events:
// - open durable (epoch = 1)
// - open ephemeral (epoch = 1, new_epoch=?)
// - durable delete sst files
// - reopen ephemeral (epoch = 1)
TEST_F(CloudTest, ReopenEphemeralAfterFileDeletion) {
  cloud_fs_options_.resync_on_open = true;
  cloud_fs_options_.keep_local_sst_files = false;

  auto durableDBName = dbname_;

  DBCloud *durable, *ephemeral;
  Env *durableEnv, *ephemeralEnv;
  std::vector<ColumnFamilyHandle*> durableHandles;

  auto openDurable = [&] {
    dbname_ = durableDBName;

    OpenDB(&durableHandles);
    durable = db_;
    db_ = nullptr;
    durableEnv = aenv_.release();
  };

  auto openEphemeral = [&] {
    std::unique_ptr<Env> env;
    std::unique_ptr<DBCloud> cloud_db;
    // open ephemeral clone with force_keep_local_on_invalid_dest_bucket=false
    // so that sst files are not kept locally
    ASSERT_OK(CloneDB("ephemeral" /* clone_name */, "" /* dest_bucket_name */,
                      "" /* dest_object_path */, &cloud_db, &env,
                      false /* force_keep_local_on_invalid_dest_bucket */));
    ephemeral = cloud_db.release();
    ephemeralEnv = env.release();
  };

  auto closeDurable = [&] {
    db_ = durable;
    aenv_.reset(durableEnv);
    CloseDB(&durableHandles);
  };

  auto closeEphemeral = [&] {
    db_ = ephemeral;
    aenv_.reset(ephemeralEnv);
    CloseDB();
  };

  options_.disable_auto_compactions = true;
  openDurable();

  ASSERT_OK(durable->Put({}, "key1", "val1"));
  ASSERT_OK(durable->Flush({}));

  ASSERT_OK(durable->Put({}, "key1", "val2"));
  ASSERT_OK(durable->Flush({}));

  closeDurable();

  openDurable();
  openEphemeral();

  std::vector<LiveFileMetaData> files;
  durable->GetLiveFilesMetaData(&files);
  ASSERT_EQ(files.size(), 2);
  // trigger compaction on durable with trivial file moves disabled, which will delete previously generated sst files
  ASSERT_OK(
      static_cast<DBImpl*>(durable->GetBaseDB())
          ->TEST_CompactRange(0, nullptr, nullptr, durableHandles[0], true));
  files.clear();
  durable->GetLiveFilesMetaData(&files);
  ASSERT_EQ(files.size(), 1);

  // reopen ephemeral
  closeEphemeral();
  openEphemeral();

  std::string val;
  ASSERT_OK(ephemeral->Get({}, "key1", &val));
  EXPECT_EQ(val, "val2");
  closeEphemeral();
  closeDurable();
}

TEST_F(CloudTest, SanitizeDirectoryTest) {
  cloud_fs_options_.keep_local_sst_files = true;
  OpenDB();
  ASSERT_OK(db_->Put({}, "k1", "v1"));
  ASSERT_OK(db_->Flush({}));
  CloseDB();

  auto local_files = GetAllLocalFiles();
  // Files exist locally: cm/m, sst, options-xxx, xxx.log, identity, current
  EXPECT_EQ(local_files.size(), 7);

  EXPECT_OK(GetCloudFileSystemImpl()->SanitizeLocalDirectory(options_, dbname_,
                                                             false));

  // cleaning up during sanitization not triggered
  EXPECT_EQ(local_files.size(), GetAllLocalFiles().size());

  // Delete the local CLOUDMANIFEST file to force cleaning up
  ASSERT_OK(
      base_env_->DeleteFile(MakeCloudManifestFile(dbname_, "" /* cooke */)));

  EXPECT_OK(GetCloudFileSystemImpl()->SanitizeLocalDirectory(options_, dbname_,
                                                             false));

  local_files = GetAllLocalFiles();
  // IDENTITY file is downloaded after cleaning up, which is the only file that
  // exists locally
  EXPECT_EQ(GetAllLocalFiles().size(), 1);

  // reinitialize local directory
  OpenDB();
  CloseDB();
  local_files = GetAllLocalFiles();
  // we have two local MANIFEST files after opening second time.
  EXPECT_EQ(local_files.size(), 8);

  // create some random directory, which is expected to be not deleted
  ASSERT_OK(base_env_->CreateDir(dbname_ + "/tmp_writes"));

  // Delete the local CLOUDMANIFEST file to force cleaning up
  ASSERT_OK(
      base_env_->DeleteFile(MakeCloudManifestFile(dbname_, "" /* cooke */)));

  ASSERT_OK(GetCloudFileSystemImpl()->SanitizeLocalDirectory(options_, dbname_,
                                                             false));

  // IDENTITY file + the random directory we created
  EXPECT_EQ(GetAllLocalFiles().size(), 2);

  // reinitialize local directory
  OpenDB();
  CloseDB();

  // inject io errors during cleaning up. The io errors should be ignored
  SyncPoint::GetInstance()->SetCallBack(
      "CloudFileSystemImpl::SanitizeDirectory:AfterDeleteFile", [](void* arg) {
        auto st = reinterpret_cast<Status*>(arg);
        *st = Status::IOError("Inject io error during cleaning up");
      });

  SyncPoint::GetInstance()->EnableProcessing();
  // Delete the local CLOUDMANIFEST file to force cleaning up
  ASSERT_OK(
      base_env_->DeleteFile(MakeCloudManifestFile(dbname_, "" /* cooke */)));

  ASSERT_OK(GetCloudFileSystemImpl()->SanitizeLocalDirectory(options_, dbname_,
                                                             false));
  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(CloudTest, CloudFileDeletionNotTriggeredIfDestBucketNotSet) {
  std::vector<std::string> files_to_delete;

  // generate invisible MANIFEST file to delete
  OpenDB();
  std::string manifest_file = ManifestFileWithEpoch(
      dbname_, GetCloudFileSystemImpl()->GetCloudManifest()->GetCurrentEpoch());
  files_to_delete.push_back(basename(manifest_file));
  CloseDB();

  // generate obsolete sst files to delete
  options_.disable_delete_obsolete_files_on_open = true;
  cloud_fs_options_.delete_cloud_invisible_files_on_open = false;
  OpenDB();
  GenerateObsoleteFilesOnEmptyDB(GetDBImpl(), GetCloudFileSystem(),
                                 &files_to_delete);
  CloseDB();

  options_.disable_delete_obsolete_files_on_open = false;
  cloud_fs_options_.dest_bucket.SetBucketName("");
  cloud_fs_options_.dest_bucket.SetObjectPath("");
  cloud_fs_options_.delete_cloud_invisible_files_on_open = true;
  OpenDB();
  WaitUntilNoScheduledJobs();
  for (auto& fname: files_to_delete) {
    EXPECT_OK(ExistsCloudObject(fname));
  }
  CloseDB();

  cloud_fs_options_.dest_bucket = cloud_fs_options_.src_bucket;
  OpenDB();
  WaitUntilNoScheduledJobs();
  for (auto& fname: files_to_delete) {
    EXPECT_NOK(ExistsCloudObject(fname));
  }
  CloseDB();
}

TEST_F(CloudTest, ScheduleFileDeletionTest) {
  auto scheduler = CloudScheduler::Get();
  auto deletion_scheduler =
      CloudFileDeletionScheduler::Create(scheduler, std::chrono::seconds(0));

  std::atomic_int counter{0};
  int num_file_deletions = 10;
  for (int i = 0; i < num_file_deletions; i++) {
    ASSERT_OK(deletion_scheduler->ScheduleFileDeletion(
        std::to_string(i) + ".sst", [&counter]() { counter++; }));
  }

  // wait until no scheduled jobs
  while (scheduler->TEST_NumScheduledJobs() > 0) {
    usleep(100);
  }
  EXPECT_EQ(counter, num_file_deletions);
  EXPECT_EQ(deletion_scheduler->TEST_FilesToDelete().size(), 0);
}

TEST_F(CloudTest, SameFileDeletedMultipleTimesTest) {
  auto scheduler = CloudScheduler::Get();
  auto deletion_scheduler =
      CloudFileDeletionScheduler::Create(scheduler, std::chrono::hours(1));
  ASSERT_OK(deletion_scheduler->ScheduleFileDeletion("filename", []() {}));
  ASSERT_OK(deletion_scheduler->ScheduleFileDeletion("filename", []() {}));
  EXPECT_EQ(deletion_scheduler->TEST_FilesToDelete().size(), 1);
}

TEST_F(CloudTest, UnscheduleFileDeletionTest) {
  auto scheduler = CloudScheduler::Get();
  auto deletion_scheduler =
      CloudFileDeletionScheduler::Create(scheduler, std::chrono::hours(1));

  std::atomic_int counter{0};
  int num_file_deletions = 10;
  std::vector<std::string> files_to_delete;
  for (int i = 0; i < num_file_deletions; i++) {
    std::string filename = std::to_string(i) + ".sst";
    files_to_delete.push_back(filename);
    ASSERT_OK(
        deletion_scheduler->ScheduleFileDeletion(filename, [&counter]() { counter++; }));
  }
  auto actual_files_to_delete = deletion_scheduler->TEST_FilesToDelete();
  std::sort(actual_files_to_delete.begin(), actual_files_to_delete.end());
  EXPECT_EQ(actual_files_to_delete, files_to_delete);

  int num_scheduled_jobs = num_file_deletions;
  for (auto& fname: files_to_delete) {
    deletion_scheduler->UnscheduleFileDeletion(fname);
    num_scheduled_jobs -= 1;
    EXPECT_EQ(scheduler->TEST_NumScheduledJobs(), num_scheduled_jobs);
  }
}

TEST_F(CloudTest, UnscheduleUnknownFileTest) {
  auto scheduler = CloudScheduler::Get();
  auto deletion_scheduler =
      CloudFileDeletionScheduler::Create(scheduler, std::chrono::hours(1));
  deletion_scheduler->UnscheduleFileDeletion("unknown file");
}

// Verifies that as long as `CloudFileDeletionScheduler` is destructed, no file
// deletion job will actually be scheduled
// This is also a repro of SYS-3456, which is a race between CloudFileSystemImpl
// destruction and cloud file deletion
// TODO(SYS-3996) Re-enable
TEST_F(
    CloudTest,
    DISABLED_FileDeletionNotScheduledOnceCloudFileDeletionSchedulerDestructed) {
  // Generate some invisible files to delete
  // Disable file deletion to make sure these files are not deleted
  // automatically
  options_.disable_delete_obsolete_files_on_open = true;
  cloud_fs_options_.delete_cloud_invisible_files_on_open = false;
  OpenDB();
  std::vector<std::string> obsolete_files;
  GenerateObsoleteFilesOnEmptyDB(GetDBImpl(), GetCloudFileSystem(),
                                 &obsolete_files);
  CloseDB();

  // Order of execution:
  // - scheduled file deletion job starts running (but file not deleted yet)
  // - destruct CloudFileDeletionScheduler
  // - file deletion job deletes the file
  SyncPoint::GetInstance()->LoadDependency({
    {
      // `BeforeCancelJobs` happens-after `BeforeFileDeletion`
      "CloudFileDeletionScheduler::ScheduleFileDeletion:BeforeFileDeletion",
      "CloudFileDeletionScheduler::~CloudFileDeletionScheduler:BeforeCancelJobs",
    },
    {
      "CloudFileDeletionScheduler::~CloudFileDeletionScheduler:BeforeCancelJobs",
      "CloudFileDeletionScheduler::ScheduleFileDeletion:AfterFileDeletion"
    }
  });

  std::atomic<size_t> num_jobs_finished{0};
  SyncPoint::GetInstance()->SetCallBack(
      "CloudFileDeletionScheduler::ScheduleFileDeletion:AfterFileDeletion",
      [&](void* arg) {
        ASSERT_NE(nullptr, arg);
        auto file_deleted = *reinterpret_cast<bool *>(arg);
        EXPECT_FALSE(file_deleted);
        num_jobs_finished++;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  // file not deleted immediately but just scheduled
  ASSERT_OK(aenv_->GetFileSystem()->DeleteFile(obsolete_files[0], kIOOptions, kDbg));
  EXPECT_EQ(GetCloudFileSystemImpl()->TEST_NumScheduledJobs(), 1);
  // destruct `CloudFileSystem`, which will cause `CloudFileDeletionScheduler`
  // to be destructed
  aenv_.reset();
  // wait until file deletion job is done
  while (num_jobs_finished.load() != 1) {
    usleep(100);
  }
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(CloudTest, UniqueCurrentEpochAcrossDBRestart) {
  constexpr int kNumRestarts = 3;
  std::unordered_set<std::string> epochs;
  for (int i = 0; i < kNumRestarts; i++) {
    OpenDB();
    auto [it, inserted] = epochs.emplace(
        GetCloudFileSystemImpl()->GetCloudManifest()->GetCurrentEpoch());
    EXPECT_TRUE(inserted);
    CloseDB();
  }
}

TEST_F(CloudTest, ReplayCloudManifestDeltaTest) {
  OpenDB();
  constexpr int kNumKeys = 3;
  std::vector<CloudManifestDelta> deltas;
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(db_->Put({}, "k" + std::to_string(i), "v" + std::to_string(i)));
    ASSERT_OK(db_->Flush({}));

    auto cookie1 =  std::to_string(i) + "0";
    auto filenum1 = db_->GetNextFileNumber();
    deltas.push_back({filenum1, cookie1});
    ASSERT_OK(SwitchToNewCookie(cookie1));

    // apply again with same file number but different cookie
    auto cookie2 = std::to_string(i) + "1";
    auto filenum2 = db_->GetNextFileNumber();
    EXPECT_EQ(filenum1, filenum2);
    deltas.push_back({filenum2, cookie2});
    ASSERT_OK(SwitchToNewCookie(cookie2));
  }

  auto currentEpoch =
      GetCloudFileSystemImpl()->GetCloudManifest()->GetCurrentEpoch();

  // replay the deltas one more time
  for (const auto& delta : deltas) {
    EXPECT_TRUE(GetCloudFileSystem()
                    ->RollNewCookie(dbname_, delta.epoch, delta)
                    .IsInvalidArgument());
    bool applied = false;
    ASSERT_OK(GetCloudFileSystem()->ApplyCloudManifestDelta(delta, &applied));
    EXPECT_FALSE(applied);
    // current epoch not changed
    EXPECT_EQ(GetCloudFileSystemImpl()->GetCloudManifest()->GetCurrentEpoch(),
              currentEpoch);
  }

  for (int i = 0; i < kNumKeys; i++) {
    std::string v;
    ASSERT_OK(db_->Get({}, "k" + std::to_string(i), &v));
    EXPECT_EQ(v, "v" + std::to_string(i));
  }
  CloseDB();
}

TEST_F(CloudTest, CreateIfMissing) {
  options_.create_if_missing = false;
  ASSERT_TRUE(checkOpen().IsNotFound());
  options_.create_if_missing = true;
  OpenDB();
  CloseDB();

  // delete `CURRENT` file
  DestroyDir(dbname_);
  OpenDB();
  CloseDB();

  // Delete `CLOUDMANFIEST` file in cloud
  auto cloudManifestFile =
      MakeCloudManifestFile(dbname_, cloud_fs_options_.new_cookie_on_open);
  ASSERT_OK(GetCloudFileSystem()->GetStorageProvider()->DeleteCloudObject(
      GetCloudFileSystem()->GetSrcBucketName(), cloudManifestFile));

  options_.create_if_missing = false;
  ASSERT_TRUE(checkOpen().IsNotFound());
}

}  //  namespace ROCKSDB_NAMESPACE

// A black-box test for the cloud wrapper around rocksdb
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  Aws::InitAPI(Aws::SDKOptions());
  auto r = RUN_ALL_TESTS();
  Aws::ShutdownAPI(Aws::SDKOptions());
  return r;
}

#else  // USE_AWS

#include <stdio.h>

int main(int, char**) {
  fprintf(stderr,
          "SKIPPED as DBCloud is supported only when USE_AWS is defined.\n");
  return 0;
}
#endif

#else  // ROCKSDB_LITE

#include <stdio.h>

int main(int, char**) {
  fprintf(stderr, "SKIPPED as DBCloud is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE
